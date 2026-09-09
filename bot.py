"""
Live signal bot - TWO strategies running side by side.

STRATEGY A - BREAKOUT + TRAILING          (already running, unchanged)
    Renko percentage box   2.7%
    Entry   break above the 160-brick high -> LONG
            break below the 160-brick low  -> SHORT
    Exit    3% retrace from the best price reached -> FLAT
    Backtest: 2021 +57.5%  2022-23 +32.8%  2024-25 +21.0%  DD -11.2%

STRATEGY B - TEMA/ALMA TWO-TIMEFRAME      (new, phase 28 winner)
    FAST Renko box 2.0%   TEMA 15 / ALMA 17   -> the signal
    SLOW Renko box 7.8%   TEMA 15 / ALMA 17   -> the direction filter
    Entry   both timeframes agree -> take it. Disagree -> FLAT.
    Exit    fast timeframe flips, or 8.5% retrace from best -> FLAT
    Backtest: 2021 +23.8%  2022-23 +23.2%  2024-25 +23.7%  DD -19.7%

WHY BOTH: their daily returns correlate only 0.47, so they cover each
other's bad patches. Phase 29 measured an 80/20 blend at -14.5% drawdown,
better than either strategy alone, with the worst year rising from +2.8%
to +7.4%.

Strategy A keeps its existing bots and capital. Strategy B gets its own
3Commas bots at roughly a quarter of A's trade size.

BAR RESOLUTION - THIS MATTERS
    Both backtests resample price to HOURLY closes before building Renko
    bricks. This bot therefore does the same. Building bricks from raw
    1-minute closes produces 28-48% MORE bricks - extra bricks created by
    intra-hour wiggles that an hourly close smooths away - and that is a
    different strategy with different trades and a different return.
    Measured on a test series: minute bricks gave 35 trades and +20.5%,
    hourly bricks gave 20 trades and +128.4%.

    So: minute klines are downloaded, then reduced to the last close of
    each COMPLETED hour. A partial hour is carried in state until it
    completes, so nothing is lost between runs.

State is kept per strategy in state.json, so the two never interfere.

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
# strategy A - breakout
A_BOX        = 2.7      # renko brick as % of price
A_BREAKOUT_N = 160      # bricks in the high/low channel
A_TRAIL      = 3.0      # trailing exit %

# strategy B - TEMA/ALMA two timeframe
B_FAST_BOX   = 2.0
B_SLOW_BOX   = 7.8
B_TEMA       = 15
B_ALMA       = 17
B_ALMA_OFF   = 0.85
B_ALMA_SIG   = 1.0
B_TRAIL      = 8.5

REVERSAL     = 2        # bricks needed to reverse direction
WARMUP_DAYS  = 900      # history pulled on first run.
                        # Strategy B's slow box is 7.8%, which produces
                        # far fewer bricks than A's 2.7% box. B needs at
                        # least max(TEMA*3, ALMA)+2 = 47 slow bricks, and
                        # 400 days was borderline. 900 days is safe.
                        # The download is shared between A and B so this
                        # costs one fetch per coin, not two.

WEBHOOK = "https://3c.wtalerts.com/bot/other"

# strategy -> symbol -> the three secret names holding its 3Commas codes
MARKETS = {
    "A": {
        "SOLUSDT": ("SOL_ENTER_LONG", "SOL_ENTER_SHORT", "SOL_EXIT_ALL"),
        "XRPUSDT": ("XRP_ENTER_LONG", "XRP_ENTER_SHORT", "XRP_EXIT_ALL"),
    },
    "B": {
        "SOLUSDT": ("B_SOL_ENTER_LONG", "B_SOL_ENTER_SHORT", "B_SOL_EXIT_ALL"),
        "XRPUSDT": ("B_XRP_ENTER_LONG", "B_XRP_ENTER_SHORT", "B_XRP_EXIT_ALL"),
    },
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
    """Binance 1-minute closes from the public archive host.
    fapi.binance.com returns HTTP 451 from GitHub's US servers;
    data-api.binance.vision does not."""
    url = ("https://data-api.binance.vision/api/v3/klines"
           f"?symbol={symbol}&interval=1m&limit={limit}")
    if start_ms:
        url += f"&startTime={start_ms}"
    raw = http_get(url)
    return [(int(k[0]), float(k[4])) for k in raw]


def fetch_history(symbol, days):
    """Returns [(open_time_ms, close), ...] sorted, plus the last timestamp."""
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
            log(f"    ...{len(out):,} candles")
        time.sleep(0.15)
    dedup = {}
    for ts, c in out:
        dedup[ts] = c
    keys = sorted(dedup)
    return [(k, dedup[k]) for k in keys], (keys[-1] if keys else 0)


HOUR_MS = 3_600_000


def to_hourly(pairs, st):
    """Reduce 1-minute (time, close) pairs to the last close of each
    COMPLETED hour - the same thing pandas resample('1h').last() does in
    the backtests.

    The in-progress hour is carried in state (hour_bucket / hour_close)
    so a partial hour at the end of one run is completed by the next.
    """
    out = []
    bucket = st.get("hour_bucket")
    bclose = st.get("hour_close")
    for ts, c in pairs:
        h = ts // HOUR_MS
        if bucket is None:
            bucket, bclose = h, c
        elif h == bucket:
            bclose = c
        else:
            out.append(bclose)          # previous hour is now complete
            bucket, bclose = h, c
    st["hour_bucket"] = bucket
    st["hour_close"] = bclose
    return out


# ----------------------------------------------------------------- renko
def build_bricks(closes, pct, reversal, anchor=None, direction=0):
    """Percentage renko in log space. Returns (bricks, anchor, direction)."""
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
                anchor += box * max(int((lp - anchor) // box), 1)
                d = 1
            elif lp <= anchor - dn:
                anchor -= box * max(int((anchor - lp) // box), 1)
                d = -1
            else:
                break
            bricks.append(math.exp(anchor))
    return bricks, anchor, d


# ------------------------------------------------------------ indicators
def ema_list(x, n):
    a = 2.0 / (n + 1.0)
    out = []
    prev = None
    for v in x:
        prev = v if prev is None else a * v + (1 - a) * prev
        out.append(prev)
    return out


def tema_list(x, n):
    e1 = ema_list(x, n)
    e2 = ema_list(e1, n)
    e3 = ema_list(e2, n)
    return [3 * a - 3 * b + c for a, b, c in zip(e1, e2, e3)]


def alma_last(x, w, offset, sigma):
    """ALMA of the most recent w values. Returns None if too few."""
    import math
    if len(x) < w:
        return None
    m = offset * (w - 1)
    s = w / sigma
    wt = [math.exp(-((i - m) ** 2) / (2 * s * s)) for i in range(w)]
    tot = sum(wt)
    seg = x[-w:]
    return sum(v * k for v, k in zip(seg, wt)) / tot


def tema_alma_dir(bricks, t_len, a_len):
    """+1 if TEMA above ALMA on the latest brick, -1 if below, 0 if
    not enough bricks yet."""
    warm = max(t_len * 3, a_len) + 2
    if len(bricks) < warm:
        return 0
    t = tema_list(bricks, t_len)[-1]
    a = alma_last(bricks, a_len, B_ALMA_OFF, B_ALMA_SIG)
    if a is None:
        return 0
    return 1 if t > a else -1


# ----------------------------------------------------------------- state
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state(s):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(s, f, indent=2)


def codes_for(names):
    return tuple(os.environ.get(n, "") for n in names)


# --------------------------------------------------------- strategy A
def run_strategy_a(symbol, names, st, new_closes, price):
    """Strategy A: 160-brick channel breakout with a trailing exit.

    IMPORTANT: this evaluates EVERY newly formed brick in order, not just
    the most recent one. If two bricks form between runs - which happens
    during fast moves, and whenever GitHub delays a scheduled run - only
    checking the last brick silently skips entries and exits that the
    backtest would have taken.
    """
    enter_long, enter_short, exit_all = codes_for(names)
    tag = f"A/{symbol}"

    fresh, anchor, d = build_bricks(new_closes, A_BOX, REVERSAL,
                                    st["anchor"], st["direction"])
    st["anchor"], st["direction"] = anchor, d

    if not fresh:
        bricks = st["bricks"]
        if len(bricks) >= A_BREAKOUT_N + 1:
            window = bricks[-(A_BREAKOUT_N + 1):-1]
            side = {0: "flat", 1: "holding LONG", -1: "holding SHORT"}[st["position"]]
            log(f"{tag}: {side}. brick {bricks[-1]:.4f}  "
                f"channel {min(window):.4f} .. {max(window):.4f}")
        return

    r = A_TRAIL / 100.0
    if len(fresh) > 1:
        log(f"{tag}: {len(fresh)} bricks formed this run - evaluating each")

    for brick in fresh:
        st["bricks"] = (st["bricks"] + [brick])[-(A_BREAKOUT_N * 3):]
        bricks = st["bricks"]
        if len(bricks) < A_BREAKOUT_N + 1:
            continue

        window = bricks[-(A_BREAKOUT_N + 1):-1]   # excludes the new brick
        hi, lo = max(window), min(window)
        pos = st["position"]

        if pos == 1:
            st["best"] = max(st["best"], brick)
            if brick <= st["best"] * (1 - r):
                log(f"{tag}: LONG exit - {brick:.4f} <= {st['best']*(1-r):.4f}")
                if send_signal(exit_all, f"{tag} EXIT-ALL"):
                    st.update(position=0, best=0.0)
                    pos = 0
        elif pos == -1:
            st["best"] = min(st["best"], brick)
            if brick >= st["best"] * (1 + r):
                log(f"{tag}: SHORT exit - {brick:.4f} >= {st['best']*(1+r):.4f}")
                if send_signal(exit_all, f"{tag} EXIT-ALL"):
                    st.update(position=0, best=0.0)
                    pos = 0

        if pos == 0:
            if brick >= hi:
                log(f"{tag}: LONG entry - {brick:.4f} >= "
                    f"{A_BREAKOUT_N}-brick high {hi:.4f}")
                if send_signal(enter_long, f"{tag} ENTER-LONG"):
                    st.update(position=1, best=brick, trades=st["trades"] + 1)
            elif brick <= lo:
                log(f"{tag}: SHORT entry - {brick:.4f} <= "
                    f"{A_BREAKOUT_N}-brick low {lo:.4f}")
                if send_signal(enter_short, f"{tag} ENTER-SHORT"):
                    st.update(position=-1, best=brick, trades=st["trades"] + 1)

    bricks = st["bricks"]
    if len(bricks) >= A_BREAKOUT_N + 1:
        window = bricks[-(A_BREAKOUT_N + 1):-1]
        side = {0: "flat", 1: "holding LONG", -1: "holding SHORT"}[st["position"]]
        log(f"{tag}: {side}. brick {bricks[-1]:.4f}  "
            f"channel {min(window):.4f} .. {max(window):.4f}  "
            f"trades {st['trades']}")
    else:
        log(f"{tag}: only {len(bricks)} bricks, need {A_BREAKOUT_N + 1}")


# --------------------------------------------------------- strategy B
def run_strategy_b(symbol, names, st, new_closes, price):
    """Strategy B: fast/slow Renko TEMA-ALMA agreement with a trailing exit.

    IMPORTANT: this steps through the new closes ONE MINUTE AT A TIME.
    The phase 28 backtest iterates bar by bar, so the live bot has to as
    well. Evaluating only the final state of a multi-minute chunk skips
    agreement changes that the backtest acts on.
    """
    enter_long, enter_short, exit_all = codes_for(names)
    tag = f"B/{symbol}"
    keep = max(B_TEMA * 3, B_ALMA) + 40
    r = B_TRAIL / 100.0

    fast_dir = slow_dir = 0
    latest = 0.0

    for c in new_closes:
        f_new, st["f_anchor"], st["f_direction"] = build_bricks(
            [c], B_FAST_BOX, REVERSAL, st["f_anchor"], st["f_direction"])
        s_new, st["s_anchor"], st["s_direction"] = build_bricks(
            [c], B_SLOW_BOX, REVERSAL, st["s_anchor"], st["s_direction"])
        if f_new:
            st["f_bricks"] = (st["f_bricks"] + f_new)[-keep:]
        if s_new:
            st["s_bricks"] = (st["s_bricks"] + s_new)[-keep:]
        if not f_new and not s_new:
            continue

        fast_dir = tema_alma_dir(st["f_bricks"], B_TEMA, B_ALMA)
        slow_dir = tema_alma_dir(st["s_bricks"], B_TEMA, B_ALMA)
        if fast_dir == 0 or slow_dir == 0:
            continue

        latest = st["f_bricks"][-1]
        agree = fast_dir if fast_dir == slow_dir else 0
        pos = st["position"]

        if pos != 0:
            out = (fast_dir != pos)
            why = "fast flipped"
            if not out:
                if pos == 1:
                    st["best"] = max(st["best"], latest)
                    if latest <= st["best"] * (1 - r):
                        out, why = True, f"{B_TRAIL}% retrace"
                else:
                    st["best"] = min(st["best"], latest)
                    if latest >= st["best"] * (1 + r):
                        out, why = True, f"{B_TRAIL}% retrace"
            if out:
                side = "LONG" if pos == 1 else "SHORT"
                log(f"{tag}: {side} exit - {why}, brick {latest:.4f}, "
                    f"best {st['best']:.4f}")
                if send_signal(exit_all, f"{tag} EXIT-ALL"):
                    st.update(position=0, best=0.0)
                    pos = 0

        if pos == 0 and agree != 0 and agree != st["prev_agree"]:
            if agree == 1:
                log(f"{tag}: LONG entry - both timeframes bullish, "
                    f"brick {latest:.4f}")
                if send_signal(enter_long, f"{tag} ENTER-LONG"):
                    st.update(position=1, best=latest, trades=st["trades"] + 1)
            else:
                log(f"{tag}: SHORT entry - both timeframes bearish, "
                    f"brick {latest:.4f}")
                if send_signal(enter_short, f"{tag} ENTER-SHORT"):
                    st.update(position=-1, best=latest, trades=st["trades"] + 1)

        st["prev_agree"] = agree

    if fast_dir == 0 or slow_dir == 0:
        log(f"{tag}: warming up - fast {len(st['f_bricks'])} bricks, "
            f"slow {len(st['s_bricks'])} bricks")
        return
    side = {0: "flat", 1: "holding LONG", -1: "holding SHORT"}[st["position"]]
    log(f"{tag}: {side}. fast {fast_dir:+d}  slow {slow_dir:+d}  "
        f"brick {latest:.4f}  trades {st['trades']}")


# ------------------------------------------------------------------ main
_HISTORY_CACHE = {}


def warmup(strategy, symbol, state):
    if symbol in _HISTORY_CACHE:
        pairs, last_ms = _HISTORY_CACHE[symbol]
        log(f"{strategy}/{symbol}: reusing the history already downloaded")
    else:
        log(f"{strategy}/{symbol}: first run - pulling {WARMUP_DAYS} days")
        pairs, last_ms = fetch_history(symbol, WARMUP_DAYS)
        _HISTORY_CACHE[symbol] = (pairs, last_ms)
    if len(pairs) < 10000:
        log(f"{strategy}/{symbol}: only {len(pairs)} candles, aborting")
        return None

    st_seed = {}
    hourly = to_hourly(pairs, st_seed)
    log(f"{strategy}/{symbol}: {len(pairs):,} minutes -> {len(hourly):,} hourly bars")

    if strategy == "A":
        bricks, anchor, d = build_bricks(hourly, A_BOX, REVERSAL)
        log(f"A/{symbol}: {len(hourly):,} hourly bars -> {len(bricks):,} bricks")
        if len(bricks) < A_BREAKOUT_N + 5:
            log(f"A/{symbol}: not enough bricks, need {A_BREAKOUT_N}")
            return None
        return dict(bricks=bricks[-(A_BREAKOUT_N * 3):], anchor=anchor,
                    direction=d, last_ms=last_ms, position=0, best=0.0,
                    trades=0, hour_bucket=st_seed["hour_bucket"],
                    hour_close=st_seed["hour_close"])

    fb, fa, fd = build_bricks(hourly, B_FAST_BOX, REVERSAL)
    sb, sa, sd = build_bricks(hourly, B_SLOW_BOX, REVERSAL)
    log(f"B/{symbol}: {len(hourly):,} hourly bars -> {len(fb):,} fast bricks, "
        f"{len(sb):,} slow bricks")
    need = max(B_TEMA * 3, B_ALMA) + 2
    if len(fb) < need or len(sb) < need:
        log(f"B/{symbol}: not enough bricks, need {need} on both")
        return None
    keep = max(B_TEMA * 3, B_ALMA) + 40
    return dict(f_bricks=fb[-keep:], s_bricks=sb[-keep:],
                f_anchor=fa, f_direction=fd, s_anchor=sa, s_direction=sd,
                last_ms=last_ms, position=0, best=0.0, trades=0,
                prev_agree=0, hour_bucket=st_seed["hour_bucket"],
                hour_close=st_seed["hour_close"])


def main():
    log("=" * 60)
    if DRY_RUN:
        log("DRY RUN MODE - no signals will actually be sent")
    state = load_state()

    for strategy, markets in MARKETS.items():
        for symbol, names in markets.items():
            key = f"{strategy}:{symbol}"
            try:
                st = state.get(key)
                if st is None:
                    st = warmup(strategy, symbol, state)
                    if st is None:
                        continue
                    state[key] = st
                    log(f"{strategy}/{symbol}: warm-up complete, starting FLAT")
                    continue

                new = klines(symbol, start_ms=st["last_ms"] + 60_000, limit=1000)
                if not new:
                    log(f"{strategy}/{symbol}: no new candles")
                    continue
                st["last_ms"] = new[-1][0]
                price = new[-1][1]

                hourly = to_hourly(new, st)
                if not hourly:
                    log(f"{strategy}/{symbol}: {len(new)} min, hour not "
                        f"complete yet, price {price:.4f}")
                    continue

                if strategy == "A":
                    run_strategy_a(symbol, names, st, hourly, price)
                else:
                    run_strategy_b(symbol, names, st, hourly, price)

            except Exception as e:
                log(f"{strategy}/{symbol}: ERROR {type(e).__name__}: {e}")

    save_state(state)
    log("done")


if __name__ == "__main__":
    main()
    sys.exit(0)
