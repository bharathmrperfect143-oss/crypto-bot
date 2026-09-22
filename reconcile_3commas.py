"""
reconcile_3commas.py
====================

Compare the bot's believed positions (state.json) against the actual
open positions on 3Commas (queried via REST API). Surfaces silent
failures where the bot thinks it has a position but 3Commas doesn't,
or vice versa.

Runs hourly via GitHub Action (or manually). READ-ONLY: never sends
signals, never modifies 3Commas or state.

Authentication
--------------
3Commas SIGNED endpoints:
  total_params = "/public/api<path>?<query_string>"
  signature    = HMAC-SHA256(secret_key, total_params).hexdigest()
  headers      = {"Apikey": <api_key>, "Signature": <signature>}

Required env vars:
  THREECOMMAS_API_KEY     - from 3Commas account API settings
  THREECOMMAS_API_SECRET  - keep secret

Exit codes
----------
  0 - all strategy/symbol pairs in sync with 3Commas
  1 - silent failure(s) detected (mismatch or stuck-streak)
  2 - state.json missing
  3 - 3Commas API error (auth, network, rate limit)
  99 - unhandled exception
"""

import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))
LOG_FILE = "trades.log"
STATE_FILE = "state.json"

# Strategy key -> bot identification. Match either by 3Commas bot_name
# substring (case-insensitive) OR by exact bot_id (preferred - more robust
# if you rename a bot in 3Commas later).
#
# bot names per master record:
#   A: "DB160"   (DB breakout, 160 bricks)
#   B: "DB-TEMA" (TEMA/ALMA two-timeframe)
#   C: "DB-STAGE1" (vol-targeted variant)
#   D: "SingleTF"  (single-TF experimental)
BOT_CONFIG = {
    "A:SOLUSDT": {"name_substr": "DB160",     "bot_id": None, "pair": "USDT_SOL"},
    "A:XRPUSDT": {"name_substr": "DB160",     "bot_id": None, "pair": "USDT_XRP"},
    "B:SOLUSDT": {"name_substr": "DB-TEMA",   "bot_id": None, "pair": "USDT_SOL"},
    "B:XRPUSDT": {"name_substr": "DB-TEMA",   "bot_id": None, "pair": "USDT_XRP"},
    "C:SOLUSDT": {"name_substr": "DB-STAGE1", "bot_id": None, "pair": "USDT_SOL"},
    "C:XRPUSDT": {"name_substr": "DB-STAGE1", "bot_id": None, "pair": "USDT_XRP"},
    "D:SOLUSDT": {"name_substr": "SingleTF",  "bot_id": None, "pair": "USDT_SOL"},
    "D:XRPUSDT": {"name_substr": "SingleTF",  "bot_id": None, "pair": "USDT_XRP"},
}

# Strategy D confirmation threshold. Must match bot.py D_CONFIRM_N.
D_CONFIRM_N = 8

API_BASE = "https://api.3commas.io/public/api"
API_TIMEOUT = 30
MAX_RETRIES = 3


def log(level, msg):
    """Mirror bot.py's log format so output blends in trades.log."""
    line = (f"{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST  "
            f"reconcile: [{level}] {msg}")
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def sign_3commas(uri_with_query):
    """Compute HMAC-SHA256 of "/public/api<path>?<query>" with secret_key."""
    secret = os.environ.get("THREECOMMAS_API_SECRET", "").encode()
    if not secret:
        raise RuntimeError("THREECOMMAS_API_SECRET not set in env")
    return hmac.new(secret, uri_with_query.encode(),
                    hashlib.sha256).hexdigest()


def api_get(path, query_params=None):
    """Signed GET against 3Commas API. Returns parsed JSON or raises."""
    api_key = os.environ.get("THREECOMMAS_API_KEY", "")
    if not api_key:
        raise RuntimeError("THREECOMMAS_API_KEY not set in env")

    if query_params:
        clean = {k: v for k, v in query_params.items() if v is not None}
        qs = urllib.parse.urlencode(clean)
        full_uri = f"/public/api{path}?{qs}"
        url = f"{API_BASE}{path}?{qs}"
    else:
        full_uri = f"/public/api{path}"
        url = f"{API_BASE}{path}"

    sig = sign_3commas(full_uri)
    headers = {
        "Apikey": api_key,
        "Signature": sig,
        "User-Agent": "renko-reconcile/1.0",
    }

    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=API_TIMEOUT) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                body = ""
                try:
                    body = e.read().decode()[:200]
                except Exception:
                    pass
                raise RuntimeError(
                    f"3Commas auth failed: HTTP {e.code} - check API key/secret. "
                    f"Response: {body}")
            last_err = f"HTTP {e.code}"
            log("WARN", f"  retry {attempt + 1}/{MAX_RETRIES} after HTTP {e.code}")
        except urllib.error.URLError as e:
            last_err = f"URLError: {e.reason}"
            log("WARN", f"  retry {attempt + 1}/{MAX_RETRIES} after {last_err}")
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            log("WARN", f"  retry {attempt + 1}/{MAX_RETRIES} after {last_err}")
        time.sleep(2 * (attempt + 1))

    raise RuntimeError(f"3Commas API failed after {MAX_RETRIES} retries: {last_err}")


def fetch_active_deals():
    """GET /ver1/deals?scope=active - returns all currently-open deals
    across all bots the API key has access to."""
    return api_get("/ver1/deals", {
        "scope": "active",
        "limit": 1000,
    })


def deal_matches_strategy(deal, cfg):
    """True if this deal belongs to the strategy described by cfg.

    Matching logic:
      1. If cfg has bot_id -> exact match on deal.bot_id wins (most robust)
      2. Otherwise -> require BOTH name_substr AND pair to match
         (substring on bot_name, exact on pair). The pair check is
         essential because each strategy runs one bot per symbol.
    """
    if cfg.get("bot_id"):
        return deal.get("bot_id") == cfg["bot_id"]
    substr = cfg.get("name_substr", "").lower()
    pair = cfg.get("pair", "")
    if not substr or not pair:
        return False
    bot_name = str(deal.get("bot_name", "")).lower()
    return substr in bot_name and deal.get("pair") == pair


def deal_side(deal):
    """Map 3Commas deal status to +/-1/0."""
    s = str(deal.get("status", "")).lower()
    if s in ("bought", "buy", "long"):
        return 1
    if s in ("sold", "sell", "short"):
        return -1
    return 0


def main():
    log("INFO", "starting reconciliation against 3Commas")

    if not os.path.exists(STATE_FILE):
        log("ERROR", f"{STATE_FILE} not found in CWD ({os.getcwd()}) - cannot reconcile")
        return 2
    with open(STATE_FILE, encoding="utf-8") as f:
        state = json.load(f)
    log("INFO", f"loaded {len(state)} strategy/symbol pairs from {STATE_FILE}")

    try:
        actual_deals = fetch_active_deals()
    except Exception as e:
        log("ERROR", f"could not query 3Commas deals endpoint: {e}")
        return 3
    log("INFO", f"3Commas reports {len(actual_deals)} active deals")

    if actual_deals:
        names = sorted({(d.get('bot_name', '?'), d.get('pair', '?'))
                        for d in actual_deals})
        log("INFO", f"distinct (bot_name, pair) seen: {names}")

    actual_by_key = {key: [] for key in BOT_CONFIG}
    unmatched = []
    for d in actual_deals:
        matched_any = False
        for key, cfg in BOT_CONFIG.items():
            if deal_matches_strategy(d, cfg):
                actual_by_key[key].append(d)
                matched_any = True
        if not matched_any:
            unmatched.append(d)

    if unmatched:
        log("WARN", f"{len(unmatched)} active deal(s) did not match any "
            f"strategy - update BOT_CONFIG if these are yours:")
        for d in unmatched[:5]:
            log("WARN", f"  bot_name={d.get('bot_name')!r}  "
                f"pair={d.get('pair')!r}  status={d.get('status')!r}")
        if len(unmatched) > 5:
            log("WARN", f"  ...and {len(unmatched) - 5} more")

    mismatches = []
    silent_stucks = []
    healthy = 0
    skipped = 0

    for key in sorted(state.keys()):
        if key not in BOT_CONFIG:
            skipped += 1
            continue
        cfg = BOT_CONFIG[key]
        st = state[key]
        strategy, symbol = key.split(":")
        believed_pos = st.get("position", 0)
        trades_count = st.get("trades", 0)
        actual_for_key = actual_by_key[key]

        actual_dirs = [deal_side(d) for d in actual_for_key]
        actual_count = len(actual_for_key)
        if actual_dirs:
            actual_pos = max(set(actual_dirs), key=actual_dirs.count)
            if actual_dirs.count(0) == len(actual_dirs):
                actual_pos = 0
        else:
            actual_pos = 0

        if believed_pos == 0 and actual_count > 0:
            side_str = {1: "LONG", -1: "SHORT"}.get(actual_pos, "?")
            mismatches.append((key, f"bot flat but 3Commas has {side_str} "
                                    f"position ({actual_count} deal(s))",
                              actual_for_key))

        elif believed_pos != 0 and actual_count == 0:
            side_str = "LONG" if believed_pos == 1 else "SHORT"
            mismatches.append((key, f"bot believes {side_str} but 3Commas "
                                    f"has no open position",
                              []))

        elif believed_pos != 0 and actual_count > 0:
            if actual_pos != believed_pos:
                bot_side = "LONG" if believed_pos == 1 else "SHORT"
                thc_side = "LONG" if actual_pos == 1 else "SHORT"
                mismatches.append((key, f"direction mismatch: bot={bot_side}, "
                                        f"3Commas={thc_side}",
                                  actual_for_key))
            else:
                healthy += 1

        elif believed_pos == 0 and actual_count == 0:
            healthy += 1

        if (strategy == "D"
                and believed_pos == 0
                and trades_count == 0
                and st.get("streak_len") is not None
                and st["streak_len"] >= D_CONFIRM_N
                and not st.get("streak_fired", False)):
            silent_stucks.append((key, st["streak_len"]))

    log("INFO", f"summary: healthy={healthy}, mismatches={len(mismatches)}, "
        f"silent-stuck={len(silent_stucks)}, skipped={skipped}")

    if silent_stucks:
        log("WARN", f"{len(silent_stucks)} strategy D instance(s) stuck "
            f"with pending fire (streak >= {D_CONFIRM_N}, fired=False):")
        for key, sl in silent_stucks:
            log("WARN", f"  {key}: streak={sl}/{D_CONFIRM_N}, pos=0, "
                f"trades=0 -> webhook likely failing. Retry should kick in "
                f"on next brick with the fixed bot.py.")

    if mismatches:
        log("FAIL", f"{len(mismatches)} strategy/3Commas mismatch(es):")
        for key, why, trades in mismatches:
            log("FAIL", f"  {key}: {why}")
            for t in trades[:3]:
                log("FAIL", f"      deal id={t.get('id')}  pair={t.get('pair')}  "
                    f"status={t.get('status')}  bot_name={t.get('bot_name')!r}")
        return 1

    if silent_stucks:
        return 1

    log("OK", f"all {healthy} strategy/symbol pair(s) in sync with 3Commas")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        log("ERROR", f"unhandled exception: {type(e).__name__}: {e}")
        sys.exit(99)
