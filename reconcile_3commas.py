"""
reconcile_3commas.py
====================

Compare the bot's believed positions (state.json) against the actual
open positions on 3Commas v2 (queried via REST API). Surfaces silent
failures where the bot thinks it has a position but 3Commas doesn't,
or vice versa.

v2 auth (per https://trade.3commas.io/docs/rest-api/auth):
  payload = METHOD + "\n" + PATH + "\n" + TIMESTAMP + "\n" + RECV_WINDOW + "\n" + BODY
  signature = Base64(HMAC_SHA256(secret, payload))
  headers: X-API-Key, X-Signature, X-Timestamp, X-Recv-Window

Live strategies endpoint: GET /open_api/strategies/live

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

import base64
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))
LOG_FILE = "trades.log"
STATE_FILE = "state.json"

API_BASE = "https://trade.3commas.io"
API_TIMEOUT = 30
MAX_RETRIES = 3
RECV_WINDOW_MS = 60000

# Strategy key -> bot identification
# FIXED 23 Sep 2026: the previous substrings (DB160/DB-TEMA/DB-STAGE1)
# were leftover from an OLD 3Commas bot-naming convention, before a
# rename that this project's own record confirms happened. The REAL
# current bot names (confirmed against a live 3Commas dashboard
# screenshot, not assumed) are:
#   A-SOL - Breakout   / A-XRP - Breakout
#   B-SOL - TwoTF Base / B-XRP - TwoTF Base
#   C-SOL - TwoTF Vol  / C-XRP - TwoTF Vol
#   D-SOL - SingleTF   / D-XRP - SingleTF
# Only "SingleTF" (Strategy D) happened to still match the old
# substring by coincidence - A, B, and C were NEVER actually being
# matched against real 3Commas data. Any "healthy" reading for A/B/C
# from a prior run was either a false positive (both sides showing
# nothing, at a moment when the strategy was genuinely still flat) or
# should be re-examined - it was not a real confirmation.
# Each substring below includes the strategy+symbol prefix (e.g.
# "A-SOL") rather than a bare strategy word, so the match is precise on
# its own and does not rely solely on the separate pair-field check
# below as a second line of defense.
BOT_CONFIG = {
    "A:SOLUSDT": {"name_substr": "A-SOL",     "bot_id": None, "pair": "USDT_SOL"},
    "A:XRPUSDT": {"name_substr": "A-XRP",     "bot_id": None, "pair": "USDT_XRP"},
    "B:SOLUSDT": {"name_substr": "B-SOL",     "bot_id": None, "pair": "USDT_SOL"},
    "B:XRPUSDT": {"name_substr": "B-XRP",     "bot_id": None, "pair": "USDT_XRP"},
    "C:SOLUSDT": {"name_substr": "C-SOL",     "bot_id": None, "pair": "USDT_SOL"},
    "C:XRPUSDT": {"name_substr": "C-XRP",     "bot_id": None, "pair": "USDT_XRP"},
    "D:SOLUSDT": {"name_substr": "D-SOL",     "bot_id": None, "pair": "USDT_SOL"},
    "D:XRPUSDT": {"name_substr": "D-XRP",     "bot_id": None, "pair": "USDT_XRP"},
}

D_CONFIRM_N = 8


def log(level, msg):
    line = (f"{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST  "
            f"reconcile: [{level}] {msg}")
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def sign_v2(method, path, body, secret):
    """v2 HMAC: Base64(HMAC_SHA256(secret, payload))"""
    ts = str(int(time.time() * 1000))
    recv = str(RECV_WINDOW_MS)
    payload = f"{method}\n{path}\n{ts}\n{recv}\n{body}"
    sig = base64.b64encode(
        hmac.new(secret.encode(), payload.encode(), hashlib.sha256).digest()
    ).decode()
    return sig, ts


def api_get(path):
    api_key = os.environ.get("THREECOMMAS_API_KEY", "")
    secret = os.environ.get("THREECOMMAS_API_SECRET", "")
    if not api_key:
        raise RuntimeError("THREECOMMAS_API_KEY not set in env")
    if not secret:
        raise RuntimeError("THREECOMMAS_API_SECRET not set in env")

    body = ""
    sig, ts = sign_v2("GET", path, body, secret)
    url = f"{API_BASE}{path}"
    headers = {
        "X-API-Key": api_key,
        "X-Signature": sig,
        "X-Timestamp": ts,
        "X-Recv-Window": str(RECV_WINDOW_MS),
        "User-Agent": "renko-reconcile/2.0",
    }

    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            req = urllib.request.Request(url, headers=headers, method="GET")
            with urllib.request.urlopen(req, timeout=API_TIMEOUT) as r:
                raw = r.read()
                body_text = raw.decode("utf-8", errors="replace")
                if not body_text.strip():
                    raise RuntimeError(
                        f"empty response body from {url} (HTTP {r.status})")
                try:
                    return json.loads(body_text)
                except json.JSONDecodeError as je:
                    preview = body_text[:300].replace("\n", " ")
                    raise RuntimeError(
                        f"non-JSON response from {url} (HTTP {r.status}): "
                        f"{preview!r} (parse error: {je.msg})")
        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                body = ""
                try:
                    body = e.read().decode("utf-8", errors="replace")[:200]
                except Exception:
                    pass
                raise RuntimeError(
                    f"3Commas auth failed: HTTP {e.code}. Response: {body}")
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


def fetch_active_strategies():
    """GET /open_api/strategies/live - currently-active strategies."""
    return api_get("/open_api/strategies/live")


def normalize_pair(p):
    """Normalize pair to BASEQUOTE form. Accepts SOLUSDT, USDT_SOL, SOL/USDT, etc."""
    p = str(p or "").upper().replace("_", "").replace("/", "").replace("-", "")
    if not p:
        return ""
    for quote in ("USDT", "USDC", "BUSD", "USD"):
        if p.startswith(quote) and len(p) > len(quote):
            base = p[len(quote):]
            if base and base != quote:
                return f"{base}{quote}"
    return p


def get_bot_name(deal):
    """Extract bot name from various possible v2 fields."""
    for f in ("name", "bot_name", "strategy_name", "title"):
        if deal.get(f):
            return str(deal[f])
    sig = deal.get("signalBot") or {}
    if isinstance(sig, dict):
        for f in ("name", "title", "botName"):
            if sig.get(f):
                return str(sig[f])
    prof = deal.get("profileStrategies") or {}
    if isinstance(prof, dict):
        for f in ("name", "title"):
            if prof.get(f):
                return str(prof[f])
    return ""


def deal_matches_strategy(deal, cfg):
    if cfg.get("bot_id"):
        return deal.get("strategyId") == cfg["bot_id"] or deal.get("id") == cfg["bot_id"]
    substr = cfg.get("name_substr", "").lower()
    cfg_pair_norm = normalize_pair(cfg.get("pair", ""))
    if not substr or not cfg_pair_norm:
        return False
    bot_name = get_bot_name(deal).lower()
    deal_pair_norm = normalize_pair(deal.get("pair", "") or deal.get("market", ""))
    return substr in bot_name and deal_pair_norm == cfg_pair_norm


def deal_side(deal):
    """Direction inference, explicit fallback chain (order matters):
    1. explicit side/direction/position_side text field, if present
    2. numeric currentPosition sign (this is a POSITION SIZE, e.g.
       12.61 or 7.98 - not a side word)
    3. status string ("entered"=long, "sold"/"short"=short)
    4. unknown (0) - logged as WARN elsewhere, never treated as FAIL

    FIXED 23 Sep 2026: the previous version built one combined string
    via `a or b or c or d or e` and lowercased it. Since Python's `or`
    short-circuits on the FIRST truthy value, and currentPosition (a
    non-zero number for any real open position) is always truthy, that
    version NEVER reached the actual side/status fields for any real
    deal - str(12.61).lower() == '12.61', which matches none of the
    hardcoded words, so every real position silently returned 0
    ("unknown"). Confirmed with a concrete example before this fix:
    {"currentPosition": 12.61, "status": "entered"} (a real LONG
    position) returned 0, not 1. This version checks each field
    separately, in the priority order described above, so a real
    position is never silently misread as unknown."""
    for f in ("side", "direction", "position_side"):
        v = deal.get(f)
        if v not in (None, "", 0):
            s = str(v).lower()
            if s in ("bought", "buy", "long", "active_long", "1"):
                return 1
            if s in ("sold", "sell", "short", "active_short", "-1"):
                return -1

    cp = deal.get("currentPosition")
    if isinstance(cp, (int, float)):
        if cp > 0:
            return 1
        if cp < 0:
            return -1

    status = str(deal.get("status", "")).lower()
    if status == "entered":
        return 1
    if status in ("sold", "short"):
        return -1

    return 0


def extract_items(response):
    if isinstance(response, list):
        return response
    if isinstance(response, dict):
        for key in ("items", "data", "result", "strategies", "results"):
            if key in response and isinstance(response[key], list):
                return response[key]
        return []
    return []


def main():
    log("INFO", "starting reconciliation against 3Commas v2")
    log("INFO", f"using base URL: {API_BASE}")

    if not os.path.exists(STATE_FILE):
        log("ERROR", f"{STATE_FILE} not found in CWD ({os.getcwd()})")
        return 2
    with open(STATE_FILE, encoding="utf-8") as f:
        state = json.load(f)
    log("INFO", f"loaded {len(state)} strategy/symbol pairs")

    try:
        actual = fetch_active_strategies()
    except Exception as e:
        log("ERROR", f"could not query 3Commas: {e}")
        return 3

    actual = extract_items(actual)
    if not isinstance(actual, list):
        log("ERROR", f"unexpected response shape: {type(actual).__name__}: "
            f"{str(actual)[:200]}")
        return 3
    log("INFO", f"3Commas reports {len(actual)} live strategies")

    if actual:
        names = sorted({(d.get('name', d.get('bot_name', '?')),
                        d.get('pair', d.get('market', '?')))
                       for d in actual[:50]})
        log("INFO", f"distinct (name, pair) seen: {names}")
        log("INFO", f"first item keys: {sorted(actual[0].keys())}")
        log("INFO", f"first item sample: {json.dumps(actual[0], default=str)[:300]}")
        # Show nested structures - top-level name is missing in v2, bot
        # info is inside signalBot or profileStrategies dicts.
        first = actual[0]
        for nested_field in ("signalBot", "profileStrategies"):
            nested = first.get(nested_field)
            if isinstance(nested, dict):
                log("INFO", f"first.{nested_field} keys: {sorted(nested.keys())}")
                log("INFO", f"first.{nested_field} content: {json.dumps(nested, default=str)[:400]}")
            elif nested is not None:
                log("INFO", f"first.{nested_field} value: {nested!r}")

    actual_by_key = {key: [] for key in BOT_CONFIG}
    unmatched = []
    for d in actual:
        matched = False
        for key, cfg in BOT_CONFIG.items():
            if deal_matches_strategy(d, cfg):
                actual_by_key[key].append(d)
                matched = True
        if not matched:
            unmatched.append(d)

    if unmatched:
        log("WARN", f"{len(unmatched)} strategy(ies) did not match any in BOT_CONFIG:")
        for d in unmatched[:5]:
            log("WARN", f"  keys={sorted(d.keys())}  "
                f"name={d.get('name', d.get('bot_name', '?'))!r}  "
                f"pair={d.get('pair', d.get('market', '?'))!r}")

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

        # DIAGNOSTIC (added 23 Sep 2026, after deal_side() was found to
        # misread LONG as SHORT on real D-strategy deals - the real v2
        # API response for these deals has NO side/direction/
        # position_side field at all, so the fallback chain reaches
        # currentPosition, whose sign convention turned out not to match
        # what was assumed). Rather than guess again, dump the FULL raw
        # deal JSON whenever the direction read is going to matter for
        # this key, so the next mismatch is diagnosable from ONE run
        # instead of a back-and-forth.
        if believed_pos != 0 and actual_for_key:
            for d in actual_for_key:
                log("INFO", f"  {key} raw deal JSON (for direction "
                    f"debugging): {json.dumps(d, default=str)}")

        if believed_pos == 0 and actual_count > 0:
            side_str = {1: "LONG", -1: "SHORT"}.get(actual_pos, "?")
            mismatches.append((key, f"bot flat but 3Commas has {side_str} "
                                    f"({actual_count} deal(s))", actual_for_key))
        elif believed_pos != 0 and actual_count == 0:
            side_str = "LONG" if believed_pos == 1 else "SHORT"
            mismatches.append((key, f"bot believes {side_str} but 3Commas "
                                    f"has no position", []))
        elif believed_pos != 0 and actual_count > 0:
            if actual_pos == 0:
                # FIXED 23 Sep 2026: a genuinely UNKNOWN direction (none of
                # the deal's fields could be read, distinct from a real
                # opposite-direction conflict) was previously treated as a
                # FAIL-level mismatch, contradicting this project's own
                # stated design ("don't call 0-direction a mismatch, would
                # create noise every reconcile - treat as WARN not FAIL")
                # which was documented but never actually implemented here.
                bot_side = "LONG" if believed_pos == 1 else "SHORT"
                log("WARN", f"  {key}: bot believes {bot_side}, 3Commas has "
                    f"{actual_count} deal(s) but direction could not be "
                    f"determined from any known field - not counted as a "
                    f"mismatch, but worth a human glance")
                healthy += 1
            elif actual_pos != believed_pos:
                bot_side = "LONG" if believed_pos == 1 else "SHORT"
                thc_side = "LONG" if actual_pos == 1 else "SHORT"
                mismatches.append((key, f"direction mismatch: bot={bot_side}, "
                                        f"3Commas={thc_side}", actual_for_key))
            else:
                healthy += 1
        else:
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
        log("WARN", f"{len(silent_stucks)} strategy D instance(s) stuck:")
        for key, sl in silent_stucks:
            log("WARN", f"  {key}: streak={sl}/{D_CONFIRM_N}, pos=0")

    if mismatches:
        log("FAIL", f"{len(mismatches)} mismatch(es):")
        for key, why, trades in mismatches:
            log("FAIL", f"  {key}: {why}")
            for t in trades[:3]:
                log("FAIL", f"      name={t.get('name', t.get('bot_name', '?'))!r}  "
                    f"pair={t.get('pair', t.get('market', '?'))!r}  "
                    f"status={t.get('status', '?')!r}")
        return 1

    if silent_stucks:
        return 1

    log("OK", f"all {healthy} pair(s) in sync with 3Commas")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        log("ERROR", f"unhandled exception: {type(e).__name__}: {e}")
        sys.exit(99)
