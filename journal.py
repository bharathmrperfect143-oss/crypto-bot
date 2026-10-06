"""
Trade Journal for the renko bot.

Captures every signal attempt by bot.py with full context (brick value,
indicator direction, vol regime, amount sent, wtalerts response, 3Commas
verify outcome). Then fetches closed deals from 3Commas and backfills the
realized P/L. Finally prints per-strategy summary stats.

THREE CLI MODES:
  python journal.py collect     # fetch closed 3Commas deals, backfill P/L
  python journal.py analyze     # print summary stats
  python journal.py            # default: same as analyze (no args)

INTENDED USAGE FROM bot.py:
  from journal import append_signal, update_signal
  sid = append_signal(strategy, symbol, action, brick_value, fast_dir,
                      slow_dir, vol_pct, amount_usdt)
  if verify_ok:
      update_signal(sid, status="confirmed", deal_id=deal_id, notes=...)
  else:
      update_signal(sid, status="rejected", notes=verify_reason)

The journal CSV grows append-only. There is no in-place edit except
update_signal(), which targets one specific row by signal_id.

CSV SCHEMA (trades_journal.csv):
  signal_id            unique id: f"{unix_ts}_{strategy}_{symbol}_{action}"
  ts_signal_utc        ISO 8601 UTC timestamp of signal
  strategy             "A", "B", "C", or "D"
  symbol               "SOLUSDT" or "XRPUSDT"
  action               "ENTER_LONG", "ENTER_SHORT", "EXIT_ALL"
  brick_value          current Renko brick price at signal time (float)
  fast_dir             TEMA/ALMA direction on FAST timeframe (B/C only; "" otherwise)
  slow_dir             TEMA/ALMA direction on SLOW timeframe (B/C only; "" otherwise)
  d_dir                single-TF direction (D only; "" otherwise)
  vol_pct              annualized realized vol percent at signal time (C only; "" otherwise)
  amount_usdt          USDT amount sent in the webhook
  wtalerts_status      "ok", "fail", or "" (not attempted)
  wtalerts_http        HTTP code returned by wtalerts (200 on success)
  verify_attempt       1..4 - which poll attempt succeeded (or 0 if failed)
  deal_id              3Commas deal id after verify success
  entry_price          actual deal entry price (filled by collect_deals)
  exit_price           actual deal exit price (filled by collect_deals)
  pnl_pct              realized P/L percent (filled by collect_deals)
  pnl_usdt             realized P/L USDT (filled by collect_deals)
  time_in_trade_min    minutes between entry and exit (filled by collect_deals)
  status               "sent" -> "confirmed"/"rejected" -> "closed"
  notes                freeform text from bot.py or collector

DO NOT change the column order - downstream tooling reads by position.
"""

import csv
import json
import os
import statistics
import time
import urllib.request
import urllib.error
import base64
import hmac
import hashlib
from datetime import datetime, timezone, timedelta

JOURNAL_FILE = "trades_journal.csv"

JOURNAL_HEADER = [
    "signal_id", "ts_signal_utc", "strategy", "symbol", "action",
    "brick_value", "fast_dir", "slow_dir", "d_dir", "vol_pct", "amount_usdt",
    "btc_price", "btc_change_1h_pct", "btc_change_4h_pct", "btc_change_24h_pct",
    "funding_rate_pct", "hour_of_day_utc", "day_of_week_utc",
    "wtalerts_status", "wtalerts_http", "verify_attempt", "deal_id",
    "entry_price", "exit_price", "pnl_pct", "pnl_usdt", "time_in_trade_min",
    "status", "notes",
]

THREECOMMAS_API_BASE = "https://trade.3commas.io"
THREECOMMAS_RECV_WINDOW_MS = 60000

# Public Binance endpoints - no auth needed
BINANCE_SPOT_BASE = "https://api.binance.com"
BINANCE_FAPI_BASE = "https://fapi.binance.com"

# In-memory cache so we don't hammer Binance on every signal in a bot run
_CONTEXT_CACHE = {}


# ---------------------------------------------------------------- market context

def _http_get_json(url, timeout=10):
    req = urllib.request.Request(url, headers={"User-Agent": "renko-journal"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def fetch_market_context(symbol):
    """Fetch BTC price changes and the symbol's current funding rate.

    Called once per bot run, NOT once per signal. Cached in _CONTEXT_CACHE
    so subsequent append_signal() calls reuse the same context.

    Returns a dict with keys:
      btc_price, btc_change_1h_pct, btc_change_4h_pct, btc_change_24h_pct,
      funding_rate_pct (for the symbol, e.g. SOLUSDT),
      hour_of_day_utc, day_of_week_utc

    All values default to "" on error so a context fetch failure does not
    break signal logging.

    IMPORTANT - geo restrictions:
      Binance blocks requests from some regions with HTTP 451. This
      function will fail silently in those environments, leaving the
      BTC/funding columns empty but still filling in hour_of_day and
      day_of_week. GitHub Actions runners are US-based and work fine.
      If your self-hosted runner is geo-blocked, swap
      BINANCE_SPOT_BASE / BINANCE_FAPI_BASE for a mirror (e.g.
      https://data-api.binance.vision for spot, which the bot already
      uses for klines).

    Funding rate interpretation:
      Binance perpetual futures charge `fundingRate` every 8h. A positive
      rate means longs pay shorts (market is bullish-leaning). Typical
      rates are 0.01% per 8h = ~0.03% per day. Over a multi-week hold
      (Strategy D's swing trade) this is significant drag.
    """
    cache_key = f"{symbol}_{int(time.time()) // 300}"  # cache for 5 minutes
    if cache_key in _CONTEXT_CACHE:
        return _CONTEXT_CACHE[cache_key]

    ctx = {
        "btc_price": "",
        "btc_change_1h_pct": "",
        "btc_change_4h_pct": "",
        "btc_change_24h_pct": "",
        "funding_rate_pct": "",
        "hour_of_day_utc": "",
        "day_of_week_utc": "",
    }

    # Time context (always available, no API call)
    now_utc = datetime.now(tz=timezone.utc)
    ctx["hour_of_day_utc"] = now_utc.hour
    ctx["day_of_week_utc"] = now_utc.strftime("%a")  # "Mon", "Tue", etc.

    # BTC hourly klines for change percentages
    try:
        raw = _http_get_json(
            f"{BINANCE_SPOT_BASE}/api/v3/klines?symbol=BTCUSDT&interval=1h&limit=25"
        )
        closes = [float(k[4]) for k in raw]
        if closes:
            ctx["btc_price"] = closes[-1]
            if len(closes) >= 2:
                ctx["btc_change_1h_pct"] = (closes[-1] / closes[-2] - 1) * 100
            if len(closes) >= 5:
                ctx["btc_change_4h_pct"] = (closes[-1] / closes[-5] - 1) * 100
            if len(closes) >= 25:
                ctx["btc_change_24h_pct"] = (closes[-1] / closes[-25] - 1) * 100
    except Exception as e:
        # Don't break journal if Binance is down
        pass

    # Funding rate for the symbol's perp
    try:
        data = _http_get_json(
            f"{BINANCE_FAPI_BASE}/fapi/v1/fundingRate?symbol={symbol}&limit=1"
        )
        if isinstance(data, list) and data:
            rate = data[-1].get("fundingRate")
            if rate is not None:
                ctx["funding_rate_pct"] = float(rate) * 100
    except Exception:
        pass

    _CONTEXT_CACHE[cache_key] = ctx
    return ctx


# ---------------------------------------------------------------- journal I/O

def _signal_id(ts, strategy, symbol, action):
    return f"{int(ts)}_{strategy}_{symbol}_{action}"


def _read_all():
    """Load the journal CSV. Returns list of dicts (empty if file missing)."""
    if not os.path.exists(JOURNAL_FILE):
        return []
    with open(JOURNAL_FILE, "r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _write_all(rows):
    """Rewrite the journal CSV with the given rows (preserves column order)."""
    with open(JOURNAL_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=JOURNAL_HEADER)
        w.writeheader()
        for r in rows:
            # Ensure all keys exist, default to "" if missing
            for k in JOURNAL_HEADER:
                r.setdefault(k, "")
            w.writerow(r)


def append_signal(strategy, symbol, action, brick_value, fast_dir="",
                  slow_dir="", d_dir="", vol_pct="", amount_usdt="",
                  wtalerts_status="", wtalerts_http="",
                  context=None):
    """Append a new row to the journal. Returns the signal_id.

    Call this IMMEDIATELY after send_signal returns, BEFORE verify_3commas.
    The caller should then call update_signal() once verify completes.

    `context` is an optional dict from fetch_market_context(). If omitted,
    journal.py will fetch it internally (cached for 5 minutes).
    """
    if context is None:
        context = fetch_market_context(symbol)
    ts = time.time()
    sid = _signal_id(ts, strategy, symbol, action)
    row = dict.fromkeys(JOURNAL_HEADER, "")
    row.update({
        "signal_id": sid,
        "ts_signal_utc": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
        "strategy": strategy,
        "symbol": symbol,
        "action": action,
        "brick_value": brick_value,
        "fast_dir": fast_dir,
        "slow_dir": slow_dir,
        "d_dir": d_dir,
        "vol_pct": vol_pct,
        "amount_usdt": amount_usdt,
        "btc_price": context.get("btc_price", ""),
        "btc_change_1h_pct": context.get("btc_change_1h_pct", ""),
        "btc_change_4h_pct": context.get("btc_change_4h_pct", ""),
        "btc_change_24h_pct": context.get("btc_change_24h_pct", ""),
        "funding_rate_pct": context.get("funding_rate_pct", ""),
        "hour_of_day_utc": context.get("hour_of_day_utc", ""),
        "day_of_week_utc": context.get("day_of_week_utc", ""),
        "wtalerts_status": wtalerts_status,
        "wtalerts_http": wtalerts_http,
        "verify_attempt": "",
        "deal_id": "",
        "entry_price": "",
        "exit_price": "",
        "pnl_pct": "",
        "pnl_usdt": "",
        "time_in_trade_min": "",
        "status": "sent",
        "notes": "",
    })
    file_exists = os.path.exists(JOURNAL_FILE)
    with open(JOURNAL_FILE, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=JOURNAL_HEADER)
        if not file_exists:
            w.writeheader()
        w.writerow(row)
    return sid


def update_signal(signal_id, **kwargs):
    """Update an existing row by signal_id. Returns True if found.

    Valid kwargs: any of JOURNAL_HEADER. Common uses:
      update_signal(sid, status="confirmed", verify_attempt=2, deal_id=12345)
      update_signal(sid, status="rejected", notes="3commas no deal after 4 attempts")
    """
    rows = _read_all()
    updated = False
    for r in rows:
        if r["signal_id"] == signal_id:
            for k, v in kwargs.items():
                if k in JOURNAL_HEADER:
                    r[k] = v
            updated = True
            break
    if updated:
        _write_all(rows)
    return updated


# ---------------------------------------------------------------- 3Commas API

def _3commas_sign(method, path, body, secret):
    ts = str(int(time.time() * 1000))
    recv = str(THREECOMMAS_RECV_WINDOW_MS)
    payload = f"{method}\n{path}\n{ts}\n{recv}\n{body}"
    sig = base64.b64encode(
        hmac.new(secret.encode(), payload.encode(), hashlib.sha256).digest()
    ).decode()
    return sig, ts


def _3commas_api_get(path):
    api_key = os.environ.get("THREECOMMAS_API_KEY", "")
    secret = os.environ.get("THREECOMMAS_API_SECRET", "")
    if not api_key or not secret:
        raise RuntimeError("3Commas API credentials missing in env")
    sig, ts = _3commas_sign("GET", path, "", secret)
    url = f"{THREECOMMAS_API_BASE}{path}"
    headers = {
        "X-API-Key": api_key,
        "X-Signature": sig,
        "X-Timestamp": ts,
        "X-Recv-Window": str(THREECOMMAS_RECV_WINDOW_MS),
        "User-Agent": "renko-journal/1.0",
    }
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode())


def _flatten_live_strategies_to_deals(live_resp):
    """Take a /open_api/strategies/live response and emit one deal-shaped
    dict per bot's profileStrategies entry whose status is "exited".

    Why this exists: 3Commas v2 has NO standalone /open_api/deals endpoint
    (it returns 404 on /open_api/deals and /open_api/deals/finished).
    Closed deals are returned INSIDE the strategies/live payload, embedded
    inside each bot's profileStrategies array. When a position closes,
    the bot stays in "live" but that entry flips from status="entered" to
    status="exited" with exitPrice populated. So the data we need is
    always reachable - just through the wrong-named endpoint.

    The output shape is what collect_deals() and _parse_bot_name() already
    expect: id, pair, entry_price, exit_price, created_at, closed_at,
    profit_percentage, profit_usdt, signalBot (with .name). Field
    extraction tries multiple possible v2 aliases because the v2 docs
    are not all enumerated in one place.

    KNOWN UNVERIFIED: this user has 0 closed deals on 3Commas as of
    6 Oct 2026. The status="exited" branch in profileStrategies has
    never been observed in production against this codebase. The shape
    is inferred from the active-bot response + v2 schema guesses. The
    loud-error behaviour is unchanged, so the first time this fires
    wrong, the collector raises immediately.
    """
    out = []
    if not isinstance(live_resp, list):
        live_resp = (live_resp or {}).get("data", []) if isinstance(live_resp, dict) else []
    for bot in live_resp:
        if not isinstance(bot, dict):
            continue
        signal_bot = bot.get("signalBot") or {}
        profiles = bot.get("profileStrategies") or []
        for ps in profiles:
            if not isinstance(ps, dict):
                continue
            status = (ps.get("status") or "").lower()
            if status != "exited":
                continue
            entry_ts = (ps.get("enteredAt") or ps.get("createdAt")
                        or ps.get("created_at") or "")
            close_ts = (ps.get("closedAt") or ps.get("updatedAt")
                        or ps.get("finished_at") or entry_ts)
            # profitLoss can be a number or a percentage depending on the
            # account setup; on Binance Futures the trade path exposes
            # it as the percentage directly. The journal expects percent.
            pnl_pct = ps.get("profitLoss") or ps.get("profit_loss")                       or ps.get("profitPercentage") or ps.get("profit_percentage") or ""
            pnl_usdt = (ps.get("profitLossUsdt") or ps.get("profit_usdt")
                        or ps.get("profitUsd") or ps.get("profit") or "")
            deal = {
                    "id":                ps.get("id") or ps.get("profileStrategyId") or "",
                    "type":              "Deal",
                    "pair":              bot.get("pair") or "",
                    "amount":            ps.get("amount") or "",
                    "base_order_volume": "",
                    "safety_order_volume": "",
                    "created_at":        entry_ts,
                    "activated_at":      entry_ts,
                    "closed_at":         close_ts,
                    "finished_at":       close_ts,
                    "entry_price":       ps.get("entryPrice") or ps.get("entry_price") or "",
                    "exit_price":        ps.get("exitPrice") or ps.get("exit_price") or "",
                    "close_price":       ps.get("exitPrice") or ps.get("exit_price") or "",
                    "base_order_price":  ps.get("entryPrice") or "",
                    "profit_percentage":  pnl_pct,
                    "profit_usdt":        pnl_usdt,
                    "profit":             pnl_usdt,
                    "signalBot":         signal_bot,
                }
            if deal["id"]:
                out.append(deal)
    return out


def fetch_closed_deals(scope="completed", limit=100):
    """Fetch closed/completed deals from 3Commas v2 API.

    scope: "completed" (default), "failed", or "all" for both
    Returns list of deal dicts. Each deal has at minimum:
      id, type, pair, amount, base_order_volume, safety_order_volume,
      finished_at, closed_at, profit_percentage, profit_usdt,
      entry_price, exit_price, bot_name (in signalBot.name),
      signalBot.id, created_at

    v2 has NO /open_api/deals endpoint. Closed deals live inside the
    strategies/live response under each bot's profileStrategies[] array
    (entries with status="exited"). This function probes three
    candidates in order: the documented-but-dead /open_api/deals,
    /open_api/deals/finished (also dead), and finally the actually-
    working /open_api/strategies/live. The third candidate's response
    is transformed via _flatten_live_strategies_to_deals() into the
    same shape the first two would have produced.
    """
    out = []
    offset = 0
    page_size = min(limit, 100)
    candidates = [
        f"/open_api/deals?limit={page_size}&offset={offset}&scope={scope}",
        f"/open_api/deals/finished?limit={page_size}&offset={offset}",
        # Third candidate: the working endpoint. No offset/offset
        # pagination here - the live endpoint returns ALL live bots in
        # one shot, and closed deals are a subset of those bots'
        # profileStrategies entries. We only call this once.
        "/open_api/strategies/live",
    ]
    used_endpoint = None
    last_err = None
    data = None
    for path in candidates:
        try:
            data = _3commas_api_get(path)
            used_endpoint = path
            break
        except urllib.error.HTTPError as e:
            last_err = e
            continue
        except Exception as e:
            last_err = e
            continue

    if data is None:
        # LOUD-ERROR FIX (3 Oct 2026, M3): do not return an empty list
        # silently. A permanently-wrong endpoint used to look exactly
        # like "no trades yet" forever.
        raise RuntimeError(
            f"3Commas deals endpoints not usable. Tried: {candidates}. "
            f"Last error: {last_err!r}. The third candidate "
            f"/open_api/strategies/live - which the reconciler confirms "
            f"works - is expected to return a transformable result."
        ) from last_err

    if used_endpoint == "/open_api/strategies/live":
        # Transform the bot-list response into deal-shaped records.
        # Only status="exited" entries are emitted.
        out = _flatten_live_strategies_to_deals(data)
    else:
        # Legacy path: deals endpoint returned a flat list.
        if not isinstance(data, list):
            data = data.get("data", []) if isinstance(data, dict) else []
        out.extend(data)

    if not out:
        # ZERO results is a real signal that needs to be visible. If
        # the live endpoint correctly returns 4 bots but no
        # status="exited" entries, that means there are no closed deals
        # YET - the journal collector should treat that as "nothing to
        # do this run" rather than a failure. So we do NOT raise here
        # when the live endpoint was the source and it succeeded.
        if used_endpoint != "/open_api/strategies/live":
            raise RuntimeError(
                "3Commas deals endpoint answered successfully but "
                "returned ZERO deals. That is a real result but must "
                "not be silently indistinguishable from a broken "
                "endpoint."
            )
        # else: live endpoint returned no exited profiles. Genuine.
        # Quiet success, return the empty list.

    return out


# ---------------------------------------------------------------- collector

# Bot name -> strategy letter map. Adjust if 3Commas bot names change.
BOT_NAME_TO_STRATEGY = {
    "A-SOL - Breakout": ("A", "SOLUSDT"),
    "A-XRP - Breakout": ("A", "XRPUSDT"),
    "B-SOL - TwoTF Base": ("B", "SOLUSDT"),
    "B-XRP - TwoTF Base": ("B", "XRPUSDT"),
    "C-SOL - TwoTF Vol": ("C", "SOLUSDT"),
    "C-XRP - TwoTF Vol": ("C", "XRPUSDT"),
    "D-SOL - SingleTF": ("D", "SOLUSDT"),
    "D-XRP - SingleTF": ("D", "XRPUSDT"),
}


def _parse_bot_name(deal):
    sig_bot = deal.get("signalBot") or {}
    name = sig_bot.get("name", "") if isinstance(sig_bot, dict) else ""
    return BOT_NAME_TO_STRATEGY.get(name)


def _holding_minutes(deal):
    """Minutes between a deal's created_at and its closed_at, or ""."""
    for cf in ("created_at", "activated_at", "createdAt"):
        for xf in ("closed_at", "finished_at", "closedAt"):
            c, x = deal.get(cf), deal.get(xf)
            if not c or not x:
                continue
            try:
                cs = str(c)
                xs = str(x)
                if cs.endswith("Z"):
                    cs = cs[:-1] + "+00:00"
                if xs.endswith("Z"):
                    xs = xs[:-1] + "+00:00"
                td = (datetime.fromisoformat(xs)
                      - datetime.fromisoformat(cs)).total_seconds() / 60.0
                return f"{td:.1f}"
            except Exception:
                continue
    return ""


def collect_deals():
    """Fetch closed deals from 3Commas, backfill entry/exit/P/L into journal.

    Matching strategy (revised after review):
      - ENTER_LONG / ENTER_SHORT rows are matched against the deal's
        CREATED_AT (the entry). They receive deal_id, entry_price,
        exit_price, pnl_pct, pnl_usdt, time_in_trade_min, and their
        status becomes "closed". These are the ONLY rows analyze()
        counts, so each trade is counted exactly once.

      - EXIT_ALL rows are matched against the deal's CLOSED_AT (the
        exit) - NOT created_at. This was a real bug in v1: an EXIT_ALL
        signal's timestamp sits near the deal's close, so matching it
        against created_at would never find anything and the row would
        sit unresolved forever. EXIT_ALL rows get deal_id, exit_price,
        time_in_trade_min, and status "exited" - but NOT pnl_pct or
        pnl_usdt, because the P/L already lives on the matching entry
        row. analyze() ignores status="exited" rows entirely.

      - A single deal can legitimately be referenced by BOTH its entry
        row and its exit row (same deal, two signals). So entry-claimed
        and exit-claimed deal ids are tracked in SEPARATE sets - a
        shared set would incorrectly prevent an exit row from ever
        matching the deal its own entry row already claimed.

    Returns: (entry_rows_updated, exit_rows_updated).
    """
    rows = _read_all()
    if not rows:
        print("journal: no rows to update")
        return (0, 0)

    deals = fetch_closed_deals(scope="completed", limit=200)
    if not deals:
        print("journal: no closed deals returned by 3Commas")
        return (0, 0)

    # Index deals by (strategy, symbol)
    deals_by_key = {}
    for d in deals:
        info = _parse_bot_name(d)
        if not info:
            continue
        strategy, symbol = info
        deals_by_key.setdefault((strategy, symbol), []).append(d)

    # SEPARATE dedup sets - an entry row and an exit row may reference
    # the same deal without that being a collision.
    # Seed from rows resolved in EARLIER runs, otherwise a fresh row could
    # re-claim a deal an older row already owns and count the trade twice.
    entry_claimed = {str(r["deal_id"]) for r in rows
                     if r.get("status") == "closed" and r.get("deal_id")}
    exit_claimed = {str(r["deal_id"]) for r in rows
                    if r.get("status") == "exited" and r.get("deal_id")}
    entries_updated = 0
    exits_updated = 0

    def _ts_of(deal, *fields):
        """Parse the first present ISO-8601 timestamp field on a deal."""
        for f in fields:
            v = deal.get(f)
            if not v:
                continue
            if isinstance(v, (int, float)):
                return float(v)
            s = str(v)
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            try:
                return datetime.fromisoformat(s).timestamp()
            except Exception:
                continue
        return None

    for r in rows:
        if r.get("status") in ("closed", "exited"):
            continue
        is_exit = r.get("action", "").upper().startswith("EXIT")
        claimed = exit_claimed if is_exit else entry_claimed
        if r.get("deal_id") and r["deal_id"] in claimed:
            continue

        key = (r["strategy"], r["symbol"])
        candidates = deals_by_key.get(key, [])
        if not candidates:
            continue

        try:
            sig_ts = datetime.fromisoformat(r["ts_signal_utc"]).timestamp()
        except Exception:
            continue

        best = None
        best_dt = None
        for d in candidates:
            did = str(d.get("id") or d.get("strategyId") or "")
            if not did or did in claimed:
                continue
            if is_exit:
                dts = _ts_of(d, "closed_at", "finished_at", "closedAt")
            else:
                dts = _ts_of(d, "created_at", "activated_at", "createdAt")
            if dts is None:
                continue
            dt = abs(dts - sig_ts)
            if dt > 1800:  # only match within 30 minutes
                continue
            if best_dt is None or dt < best_dt:
                best = d
                best_dt = dt

        if best is None:
            continue

        did = str(best.get("id") or best.get("strategyId") or "")
        claimed.add(did)

        r["deal_id"] = did

        if is_exit:
            # Exit row: reference only, no P/L (it lives on the entry row)
            r["exit_price"] = (best.get("exit_price")
                               or best.get("close_price") or "")
            r["time_in_trade_min"] = _holding_minutes(best)
            r["status"] = "exited"
            exits_updated += 1
            continue

        # Entry row: full trade record
        r["entry_price"] = (best.get("entry_price")
                            or best.get("base_order_price") or "")
        r["exit_price"] = (best.get("exit_price")
                           or best.get("close_price") or "")
        pnl_pct = best.get("profit_percentage")
        if pnl_pct in ("", None):
            pnl_pct = best.get("profitPercent")
        pnl_usdt = best.get("profit_usdt")
        if pnl_usdt in ("", None):
            pnl_usdt = best.get("profit")
        r["pnl_pct"] = pnl_pct if pnl_pct not in ("", None) else ""
        r["pnl_usdt"] = pnl_usdt if pnl_usdt not in ("", None) else ""
        r["time_in_trade_min"] = _holding_minutes(best)
        r["status"] = "closed"
        entries_updated += 1

    if entries_updated or exits_updated:
        _write_all(rows)
    print(f"journal: updated {entries_updated} entry rows (with P/L) "
          f"and {exits_updated} exit rows (reference only)")
    return (entries_updated, exits_updated)


# ---------------------------------------------------------------- analyzer

def analyze():
    """Print per-strategy summary stats from the journal.

    For each strategy that has at least 1 closed trade, prints:
      - total closed trades
      - win count / loss count / breakeven count
      - win rate (%)
      - average pnl_pct per trade
      - total pnl_usdt
      - average time-in-trade (minutes)
      - average amount_usdt sent
    """
    rows = _read_all()
    if not rows:
        print("journal: no rows to analyze")
        return

    closed = [r for r in rows if r.get("status") == "closed"
              and r.get("pnl_pct") not in ("", None)]
    if not closed:
        print(f"journal: {len(rows)} total rows, 0 closed deals with P/L yet")
        print("(run 'python journal.py collect' after deals close in 3Commas)")
        return

    by_strategy = {}
    for r in closed:
        by_strategy.setdefault(r["strategy"], []).append(r)

    print(f"journal: {len(closed)} closed trades across "
          f"{len(by_strategy)} strategies")
    print()
    print(f"{'strat':<6} {'n':>4} {'wins':>5} {'loss':>5} {'win%':>7} "
          f"{'avg%':>8} {'total$':>10} {'avg_min':>9} {'avg_amt':>10}")
    print("-" * 70)

    for strategy in sorted(by_strategy):
        trades = by_strategy[strategy]
        n = len(trades)
        pnls_pct = [float(t["pnl_pct"]) for t in trades
                    if t.get("pnl_pct") not in ("", None)]
        pnls_usd = [float(t["pnl_usdt"]) for t in trades
                    if t.get("pnl_usdt") not in ("", None)]
        wins = sum(1 for p in pnls_pct if p > 0)
        losses = sum(1 for p in pnls_pct if p < 0)
        be = n - wins - losses
        win_rate = (wins / n * 100.0) if n else 0.0
        avg_pct = statistics.mean(pnls_pct) if pnls_pct else 0.0
        total_usd = sum(pnls_usd) if pnls_usd else 0.0
        times = [float(t["time_in_trade_min"]) for t in trades
                 if t.get("time_in_trade_min") not in ("", None)]
        avg_min = statistics.mean(times) if times else 0.0
        amounts = [float(t["amount_usdt"]) for t in trades
                   if t.get("amount_usdt") not in ("", None)]
        avg_amt = statistics.mean(amounts) if amounts else 0.0

        print(f"{strategy:<6} {n:>4} {wins:>5} {losses:>5} {win_rate:>6.1f}% "
              f"{avg_pct:>+7.2f}% {total_usd:>+10.2f} {avg_min:>8.1f}m "
              f"{avg_amt:>10.2f}")

    print()
    # Overall expectancy: win_rate * avg_win - loss_rate * avg_loss
    pnls = [float(t["pnl_pct"]) for t in closed
            if t.get("pnl_pct") not in ("", None)]
    if pnls:
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]
        if wins and losses:
            win_rate = len(wins) / len(pnls)
            loss_rate = len(losses) / len(pnls)
            avg_win = statistics.mean(wins)
            avg_loss = abs(statistics.mean(losses))
            expectancy = win_rate * avg_win - loss_rate * avg_loss
            print(f"overall expectancy: {expectancy:+.3f}% per trade "
                  f"(win_rate={win_rate:.2%}, avg_win={avg_win:.2f}%, "
                  f"avg_loss={avg_loss:.2f}%)")


# ---------------------------------------------------------------- CLI

def main():
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "analyze"
    if cmd == "collect":
        collect_deals()
    elif cmd == "analyze":
        analyze()
    else:
        print(f"usage: python journal.py [collect|analyze]")
        sys.exit(1)


if __name__ == "__main__":
    main()
