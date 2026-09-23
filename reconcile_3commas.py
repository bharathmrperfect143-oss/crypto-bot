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
    """Signed GET against 3Commas v2 API."""
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


def deal_matches_strategy(deal, cfg):
    if cfg.get("bot_id"):
        return deal.get("id") == cfg["bot_id"] or deal.get("bot_id") == cfg["bot_id"]
    substr = cfg.get("name_substr", "").lower()
    pair = cfg.get("pair", "")
    if not substr or not pair:
        return False
    bot_name = str(
        deal.get("name", "") or
        deal.get("bot_name", "") or
        deal.get("strategy_name", "") or
        ""
    ).lower()
    deal_pair = str(deal.get("pair", "") or deal.get("market", ""))
    return substr in bot_name and deal_pair == pair


def deal_side(deal):
    s = str(
        deal.get("side", "") or
        deal.get("direction", "") or
        deal.get("position_side", "") or
        deal.get("status", "")
    ).lower()
    if s in ("bought", "buy", "long", "active_long", "1"):
        return 1
    if s in ("sold", "sell", "short", "active_short", "-1"):
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

        if believed_pos == 0 and actual_count > 0:
            side_str = {1: "LONG", -1: "SHORT"}.get(actual_pos, "?")
            mismatches.append((key, f"bot flat but 3Commas has {side_str} "
                                    f"({actual_count} deal(s))", actual_for_key))
        elif believed_pos != 0 and actual_count == 0:
            side_str = "LONG" if believed_pos == 1 else "SHORT"
            mismatches.append((key, f"bot believes {side_str} but 3Commas "
                                    f"has no position", []))
        elif believed_pos != 0 and actual_count > 0:
            if actual_pos != believed_pos:
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
