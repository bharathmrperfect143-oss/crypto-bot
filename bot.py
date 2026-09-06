"""
Live signal bot: Renko breakout with trailing exit.

Strategy (validated in phases 8-11):
  Renko percentage box  2.7%   (2-box reversal)
  Entry   break above 160-brick high -> LONG
          break below 160-brick low  -> SHORT
  Exit    3% retrace from best price reached -> FLAT
  Re-entry only on a fresh breakout

Runs on GitHub Actions every 5 minutes. State is kept in state.json
and committed back to the repo, so the bot remembers its position
between runs.

DO NOT change the parameters during the forward test.
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

# ---------------------------------------------------------------- config
BOX_PCT      = 2.7     # renko brick as % of price
BREAKOUT_N   = 160     # bricks in the high/low channel
RETRACE_PCT  = 3.0     # trailing exit
REVERSAL     = 2       # bricks needed to reverse direction

WARMUP_DAYS  = 400     # history pulled on first run to build bricks

WEBHOOK = "https://3c.wtalerts.com/bot/other"

# symbol -> the three secret names holding its 3Commas codes
MARKETS = {
    "SOLUSDT": ("SOL_ENTER_LONG", "SOL_ENTER_SHORT", "SOL_EXIT_ALL"),
    # add more later, e.g.
    # "XRPUSDT": ("XRP_ENTER_LONG", "XRP_ENTER_SHORT", "XRP_EXIT_ALL"),
}

STATE_FILE = "state.json"
LOG_FILE   = "trades.log"
DRY_RUN    = os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes")


# ---------------------------------------------------------------- utils
def log(msg):
    line = f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}Z  {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def http_get(url, tries=3):
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "renko-bot"})
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            if i == tries - 1:
                raise
            log(f"  retry {i+1} after error: {e}")
            time.sleep(3)


def send_signal(code, label):
    if not code:
        log(f"  !! no code configured for {label} - skipped")
        return False
    if DRY_RUN:
        log(f"  DRY RUN - would send {label}")
        return True
    body = json.dumps({"code": code}).encode()
    req = urllib.request.Request(
        WEBHOOK, data=body,
        headers={"Content-Type": "application/json", "User-Agent": "renko-bot"},
        method="POST")
    for i in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                log(f"  SENT {label}  (http {r.status})")
                return True
        except urllib.error.HTTPError as e:
            log(f"  webhook http {e.code} on {label}")
            if e.code < 500:
                return False
        except Exception as e:
            log(f"  webhook error on {label}: {e}")
        time.sleep(3)
    return False


# ------------------------------------------------------------ price data
def klines(symbol, start_ms=None, limit=1000):
    """Binance USD-M futures 1-minute closes. Public, no API key."""
    url = ("https://fapi.binance.com/fapi/v1/klines"
           f"?symbol={symbol}&interval=1m&limit={limit}")
    if start_ms:
        url += f"&startTime={start_ms}"
    raw = http_get(url)
    return [(int(k[0]), float(k[4])) for k in raw]


def fetch_history(symbol, days):
    """Pull `days` of 1-minute closes, paging backwards from now."""
    now_ms = int(time.time() * 1000)
    start = now_ms - days * 24 * 60 * 60 * 1000
    out = []
    cursor = start
    while cursor < now_ms:
        batch = klines(symbol, start_ms=cursor, limit=1000)
        if not batch:
            break
        out.extend(batch)
        nxt = batch[-1][0] + 60_000
        if nxt <= cursor:
            break
        cursor = nxt
        if len(out) % 50000 < 1000:
            log(f"  ...{len(out):,} candles")
        time.sleep(0.15)
    out.sort()
    dedup = {}
    for t, c in out:
        dedup[t] = c
    return [dedup[t] for t in sorted(dedup)], max(dedup) if dedup else 0


# ----------------------------------------------------------------- renko
def build_bricks(closes, pct, reversal, anchor=None, direction=0):
    """Percentage renko in log space. Returns (brick_closes, anchor, direction)."""
    import math
    box = math.log1p(pct / 100.0)
    bricks = []
    if anchor is None:
        anchor = math.floor(math.log(closes[0]) / box) * box
    d = direction
    for c in closes:
        lp = math.log(c)
        while True:
            up = box if d >= 0 else box * reversal
            dn = box if d <= 0 else box * reversal
            if lp >= anchor + up:
                steps = max(int((lp - anchor) // box), 1)
                anchor += box * steps
                d = 1
            elif lp <= anchor - dn:
                steps = max(int((anchor - lp) // box), 1)
                anchor -= box * steps
                d = -1
            else:
                break
            bricks.append(math.exp(anchor))
    return bricks, anchor, d


# ----------------------------------------------------------------- state
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state(s):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(s, f, indent=2)


# ------------------------------------------------------------------ main
def process(symbol, codes, state):
    enter_long = os.environ.get(codes[0], "")
    enter_short = os.environ.get(codes[1], "")
    exit_all = os.environ.get(codes[2], "")

    st = state.get(symbol)

    if st is None:
        log(f"{symbol}: first run - pulling {WARMUP_DAYS} days of history")
        closes, last_ms = fetch_history(symbol, WARMUP_DAYS)
        if len(closes) < 10000:
            log(f"{symbol}: only {len(closes)} candles, aborting")
            return
        bricks, anchor, d = build_bricks(closes, BOX_PCT, REVERSAL)
        log(f"{symbol}: {len(closes):,} candles -> {len(bricks):,} bricks")
        if len(bricks) < BREAKOUT_N + 5:
            log(f"{symbol}: not enough bricks yet, need {BREAKOUT_N}")
            return
        st = {
            "bricks": bricks[-(BREAKOUT_N * 3):],
            "anchor": anchor,
            "direction": d,
            "last_ms": last_ms,
            "position": 0,      # 0 flat, 1 long, -1 short
            "best": 0.0,
            "opened_at": None,
            "trades": 0,
        }
        state[symbol] = st
        log(f"{symbol}: warm-up complete, starting FLAT")
        return

    # incremental update
    new = klines(symbol, start_ms=st["last_ms"] + 60_000, limit=1000)
    if not new:
        log(f"{symbol}: no new candles")
        return
    closes = [c for _, c in new]
    st["last_ms"] = new[-1][0]
    price = closes[-1]

    fresh, anchor, d = build_bricks(closes, BOX_PCT, REVERSAL,
                                    st["anchor"], st["direction"])
    st["anchor"] = anchor
    st["direction"] = d
    if fresh:
        st["bricks"] = (st["bricks"] + fresh)[-(BREAKOUT_N * 3):]
        log(f"{symbol}: {len(new)} candles, {len(fresh)} new brick(s), "
            f"price {price:.4f}")
    else:
        log(f"{symbol}: {len(new)} candles, no new bricks, price {price:.4f}")

    bricks = st["bricks"]
    if len(bricks) < BREAKOUT_N + 1:
        log(f"{symbol}: only {len(bricks)} bricks, need {BREAKOUT_N + 1}")
        return

    # channel EXCLUDES the newest brick, matching the backtest
    window = bricks[-(BREAKOUT_N + 1):-1]
    hi = max(window)
    lo = min(window)
    latest = bricks[-1]
    pos = st["position"]
    r = RETRACE_PCT / 100.0

    # ---- exit check first
    if pos == 1:
        st["best"] = max(st["best"], latest)
        if latest <= st["best"] * (1 - r):
            log(f"{symbol}: LONG exit - brick {latest:.4f} <= "
                f"{st['best'] * (1 - r):.4f} (best {st['best']:.4f})")
            if send_signal(exit_all, f"{symbol} EXIT-ALL"):
                st["position"] = 0
                st["best"] = 0.0
                st["opened_at"] = None
                pos = 0
    elif pos == -1:
        st["best"] = min(st["best"], latest)
        if latest >= st["best"] * (1 + r):
            log(f"{symbol}: SHORT exit - brick {latest:.4f} >= "
                f"{st['best'] * (1 + r):.4f} (best {st['best']:.4f})")
            if send_signal(exit_all, f"{symbol} EXIT-ALL"):
                st["position"] = 0
                st["best"] = 0.0
                st["opened_at"] = None
                pos = 0

    # ---- entry check, only when flat
    if pos == 0:
        if latest >= hi:
            log(f"{symbol}: LONG entry - brick {latest:.4f} >= "
                f"{BREAKOUT_N}-brick high {hi:.4f}")
            if send_signal(enter_long, f"{symbol} ENTER-LONG"):
                st["position"] = 1
                st["best"] = latest
                st["opened_at"] = datetime.now(timezone.utc).isoformat()
                st["trades"] += 1
        elif latest <= lo:
            log(f"{symbol}: SHORT entry - brick {latest:.4f} <= "
                f"{BREAKOUT_N}-brick low {lo:.4f}")
            if send_signal(enter_short, f"{symbol} ENTER-SHORT"):
                st["position"] = -1
                st["best"] = latest
                st["opened_at"] = datetime.now(timezone.utc).isoformat()
                st["trades"] += 1
        else:
            log(f"{symbol}: flat. brick {latest:.4f}  channel "
                f"{lo:.4f} .. {hi:.4f}")
    else:
        side = "LONG" if st["position"] == 1 else "SHORT"
        log(f"{symbol}: holding {side}, brick {latest:.4f}, "
            f"best {st['best']:.4f}, total trades {st['trades']}")


def main():
    log("=" * 60)
    if DRY_RUN:
        log("DRY RUN MODE - no signals will actually be sent")
    state = load_state()
    for symbol, codes in MARKETS.items():
        try:
            process(symbol, codes, state)
        except Exception as e:
            log(f"{symbol}: ERROR {type(e).__name__}: {e}")
    save_state(state)
    log("done")


if __name__ == "__main__":
    main()
    sys.exit(0)
