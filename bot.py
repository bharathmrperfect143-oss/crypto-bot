"""
Live signal bot - THREE strategies running side by side (A, B baseline,
C = B + Stage 1 vol-sizing), for direct A/B comparison on real capital.

STRATEGY A - BREAKOUT + TRAILING          (unchanged)
    Renko percentage box   2.7%
    Entry   break above the 160-brick high -> LONG
            break below the 160-brick low  -> SHORT
    Exit    3% retrace from the best price reached -> FLAT
    Backtest: 2021 +57.5%  2022-23 +32.8%  2024-25 +21.0%  DD -11.2%

STRATEGY B - TEMA/ALMA TWO-TIMEFRAME, BASELINE (unchanged, control group)
    FAST Renko box 2.0%   TEMA 15 / ALMA 17   -> the signal
    SLOW Renko box 7.8%   TEMA 15 / ALMA 17   -> the direction filter
    Entry   both timeframes agree AND this is a genuine change from the
            last computed agreement value (edge-trigger) -> take it.
    Exit    fast timeframe flips, or a flat 8.5% retrace from best price.
    Fixed $1000 per trade, always - no sizing changes.
    Baseline backtest: 2021 +23.8%  2022-23 +23.2%  2024-25 +23.7%  DD -19.7%
    (these are the known 8-12 coin PORTFOLIO numbers from phase28.py -
    SOL/XRP alone will differ, that's expected, see validate_strategy_b.py)

STRATEGY C - SAME AS B, PLUS STAGE 1.1 (volatility-targeted sizing)
    Identical entry/exit rules to B. The only difference: position size at
    entry scales as min(1.0, 40%/realized_30d_vol) * $1000, instead of
    always sending the full $1000. Runs on its own 3Commas bots
    (SOL/XRP DB-STAGE1) so it can be compared head-to-head against B with
    real forward-test capital, not just backtest numbers.

STAGE 1 RESEARCH RESULT (2026-09-12): of everything tested in isolation
(vol-sizing, profit-tiered trailing exit, confirmation delay, asymmetric
long/short trail), only volatility-targeted sizing showed a consistent
improvement across all 6 tested coin-regime combinations (SOL and XRP,
all 3 regimes) with no case where it hurt - see test_stage1_sol.py for
the full isolation-test results this decision was based on. That's why
only that one change is in Strategy C; the others are documented but not
deployed.

BAR RESOLUTION - THIS MATTERS
    Both backtests resample price to HOURLY closes before building Renko
    bricks. This bot therefore does the same. Building bricks from raw
    1-minute closes produces 28-48% MORE bricks - extra bricks created by
    intra-hour wiggles that an hourly close smooths away - and that is a
    different strategy with different trades and a different return.

    So: minute klines are downloaded, then reduced to the last close of
    each COMPLETED hour. A partial hour is carried in state until it
    completes, so nothing is lost between runs.

State is kept per strategy in state.json (keyed "A:SYMBOL", "B:SYMBOL",
"C:SYMBOL"), so the three never interfere with each other.

DO NOT change the parameters during the forward test, except as part of
a deliberately isolated experiment.
"""

import json
import math
import os
import statistics
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

# ---------------------------------------------------------------- config
# strategy A - breakout (unchanged)
A_BOX        = 2.7      # renko brick as % of price
A_BREAKOUT_N = 160      # bricks in the high/low channel
A_TRAIL      = 3.0      # trailing exit %

# strategy B - TEMA/ALMA two timeframe (baseline params, unchanged)
B_FAST_BOX   = 2.0
B_SLOW_BOX   = 7.8
B_TEMA       = 15
B_ALMA       = 17
B_ALMA_OFF   = 0.85
B_ALMA_SIG   = 1.0
B_TRAIL      = 8.5      # fallback trail % if entry price is unknown

REVERSAL     = 2        # bricks needed to reverse direction
WARMUP_DAYS  = 900      # history pulled on first run.

# ---- STAGE 1 addition (validated 2026-09-12: only 1.1 survived isolation
# testing on both SOL and XRP across all 3 regimes; 1.2 tiered-trail and
# 1.3 confirm-delay both hurt returns in at least one bull regime and were
# dropped - see test_stage1_sol.py results) ---------------------------------
VOL_TARGET       = 0.40   # annualized target volatility (40%)
VOL_LOOKBACK_HRS = 720    # 30 days of hourly closes for realized-vol calc
VOL_LMAX         = 1.0    # cap: never size above 1.0x the base amount
# ---------------------------------------------------------------------------


WEBHOOK = "https://3c.wtalerts.com/bot/other"

MARKETS = {
    "A": {
        "SOLUSDT": ("SOL_ENTER_LONG", "SOL_ENTER_SHORT", "SOL_EXIT_ALL"),
        "XRPUSDT": ("XRP_ENTER_LONG", "XRP_ENTER_SHORT", "XRP_EXIT_ALL"),
    },
    "B": {
        "SOLUSDT": ("B_SOL_ENTER_LONG", "B_SOL_ENTER_SHORT", "B_SOL_EXIT_ALL"),
        "XRPUSDT": ("B_XRP_ENTER_LONG", "B_XRP_ENTER_SHORT", "B_XRP_EXIT_ALL"),
    },
    "C": {
        "SOLUSDT": ("C_SOL_ENTER_LONG", "C_SOL_ENTER_SHORT", "C_SOL_EXIT_ALL"),
        "XRPUSDT": ("C_XRP_ENTER_LONG", "C_XRP_ENTER_SHORT", "C_XRP_EXIT_ALL"),
    },
}

# base trade amount per (strategy, symbol), in USDT.
# Strategy C's amount here is the BASE amount at vol-weight = 1.0 (i.e.
# when realized vol == VOL_TARGET exactly); actual sent amount is scaled
# by vol_target_size() at entry time. A and B always send this amount
# exactly, no sizing changes.
TRADE_AMOUNTS = {
    "A": {"SOLUSDT": 1000, "XRPUSDT": 1000},
    "B": {"SOLUSDT": 1000, "XRPUSDT": 1000},
    "C": {"SOLUSDT": 1000, "XRPUSDT": 1000},
}

STATE_FILE = "state.json"
LOG_FILE   = "trades.log"
DRY_RUN    = os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes")


IST = timezone(timedelta(hours=5, minutes=30))

def log(msg):
    # printed in IST so it matches 3Commas/your-local-time directly, no
    # mental timezone conversion needed. All internal logic (hourly bar
    # boundaries, state timestamps) still uses UTC/epoch ms underneath -
    # only the human-readable log line is shown in IST.
    line = f"{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST  {msg}"
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


def send_signal(strategy, symbol, code, label, amount=None):
    if not code:
        log(f"  !! no code configured for {label} - skipped")
        return False
    if amount is None:
        amount = TRADE_AMOUNTS[strategy][symbol]
    if DRY_RUN:
        log(f"  DRY RUN - would send {label}  amount={amount} USDT  "
            f"type=quote  order=market")
        return True
    # FLAT structure - per 3Commas' own official JSON guide
    # (https://help.3commas.io/en/articles/16281112), the Pine Script
    # reference example builds code/orderType/amountPerTradeType/
    # amountPerTrade as SIBLING keys in one flat object - there is no
    # "data" wrapper. An earlier version of this bot nested these three
    # fields under "data", which is why 3Commas kept reporting
    # amountPerTrade as missing/nan even though the value was present -
    # it was just present in the wrong place in the JSON.
    body = json.dumps({
        "code": code,
        "orderType": "market",
        "amountPerTradeType": "quote",
        "amountPerTrade": amount,
    }).encode()
    req = urllib.request.Request(
        WEBHOOK, data=body,
        headers={"Content-Type": "application/json", "User-Agent": "renko-bot"},
        method="POST")
    for i in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                log(f"  SENT {label}  amount={amount} USDT  (http {r.status})")
                # Small courtesy delay between webhook sends. (The
                # repeated "amountPerTrade: nan" declines were actually
                # caused by the wrong JSON structure above, not a race
                # condition - keeping this delay anyway is harmless.)
                time.sleep(2)
                return True
        except urllib.error.HTTPError as e:
            log(f"  webhook http {e.code} on {label}")
            if e.code < 500:
                return False
        except Exception as e:
            log(f"  webhook error on {label}: {e}")
        time.sleep(3)
    return False


def klines(symbol, start_ms=None, limit=1000):
    url = ("https://data-api.binance.vision/api/v3/klines"
           f"?symbol={symbol}&interval=1m&limit={limit}")
    if start_ms:
        url += f"&startTime={start_ms}"
    raw = http_get(url)
    return [(int(k[0]), float(k[4])) for k in raw]


def fetch_history(symbol, days):
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
            out.append(bclose)
            bucket, bclose = h, c
    st["hour_bucket"] = bucket
    st["hour_close"] = bclose
    return out


def build_bricks(closes, pct, reversal, anchor=None, direction=0):
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
    if len(x) < w:
        return None
    m = offset * (w - 1)
    s = w / sigma
    wt = [math.exp(-((i - m) ** 2) / (2 * s * s)) for i in range(w)]
    tot = sum(wt)
    seg = x[-w:]
    return sum(v * k for v, k in zip(seg, wt)) / tot


def tema_alma_dir(bricks, t_len, a_len):
    warm = max(t_len * 3, a_len) + 2
    if len(bricks) < warm:
        return 0
    t = tema_list(bricks, t_len)[-1]
    a = alma_last(bricks, a_len, B_ALMA_OFF, B_ALMA_SIG)
    if a is None:
        return 0
    return 1 if t > a else -1


# ---- STAGE 1.1 - volatility-targeted position sizing ----------------------

def realized_vol_annualized(closes, lookback=VOL_LOOKBACK_HRS):
    """Annualized realized volatility from a list of hourly closes."""
    seg = closes[-lookback:]
    if len(seg) < 30:
        return None
    rets = [math.log(seg[i] / seg[i - 1]) for i in range(1, len(seg))
             if seg[i - 1] > 0 and seg[i] > 0]
    if len(rets) < 2:
        return None
    sd = statistics.pstdev(rets)
    return sd * math.sqrt(24 * 365)  # hourly bars -> annualized


def vol_target_size(base_amount, realized_vol):
    """Scale base_amount by VOL_TARGET / realized_vol, capped at VOL_LMAX."""
    if not realized_vol or realized_vol <= 0:
        return base_amount
    weight = min(VOL_LMAX, VOL_TARGET / realized_vol)
    return round(base_amount * weight, 2)


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


def run_strategy_a(symbol, names, st, new_closes, price):
    """Strategy A - unchanged from the previous version."""
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

        window = bricks[-(A_BREAKOUT_N + 1):-1]
        hi, lo = max(window), min(window)
        pos = st["position"]

        if pos == 1:
            st["best"] = max(st["best"], brick)
            if brick <= st["best"] * (1 - r):
                log(f"{tag}: LONG exit - {brick:.4f} <= {st['best']*(1-r):.4f}")
                if send_signal("A", symbol, exit_all, f"{tag} EXIT-ALL"):
                    st.update(position=0, best=0.0)
                    pos = 0
        elif pos == -1:
            st["best"] = min(st["best"], brick)
            if brick >= st["best"] * (1 + r):
                log(f"{tag}: SHORT exit - {brick:.4f} >= {st['best']*(1+r):.4f}")
                if send_signal("A", symbol, exit_all, f"{tag} EXIT-ALL"):
                    st.update(position=0, best=0.0)
                    pos = 0

        if pos == 0:
            if brick >= hi:
                log(f"{tag}: LONG entry - {brick:.4f} >= "
                    f"{A_BREAKOUT_N}-brick high {hi:.4f}")
                if send_signal("A", symbol, enter_long, f"{tag} ENTER-LONG"):
                    st.update(position=1, best=brick, trades=st["trades"] + 1)
            elif brick <= lo:
                log(f"{tag}: SHORT entry - {brick:.4f} <= "
                    f"{A_BREAKOUT_N}-brick low {lo:.4f}")
                if send_signal("A", symbol, enter_short, f"{tag} ENTER-SHORT"):
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


def run_strategy_c(symbol, names, st, new_closes, price):
    """Strategy C - identical to Strategy B except position size at entry
    is volatility-targeted (Stage 1.1). See module docstring."""
    enter_long, enter_short, exit_all = codes_for(names)
    tag = f"C/{symbol}"
    keep = max(B_TEMA * 3, B_ALMA) + 40
    need = max(B_TEMA * 3, B_ALMA) + 2
    base_amount = TRADE_AMOUNTS["C"][symbol]
    r = B_TRAIL / 100.0

    # STAGE 1.1 - keep a rolling window of raw hourly closes for the
    # realized-volatility calc (separate from the Renko brick arrays,
    # since bricks discretize price and would distort a vol estimate).
    st["price_history"] = (st.get("price_history", []) + list(new_closes))[-VOL_LOOKBACK_HRS:]

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
                if send_signal("C", symbol, exit_all, f"{tag} EXIT-ALL"):
                    st.update(position=0, best=0.0)
                    pos = 0

        # baseline edge-trigger: only enter on a genuine change of `agree`
        # (this dedup is what makes the bot stay flat-and-out after an exit
        # until direction actually flips, instead of re-entering every run -
        # isolation testing confirmed this matters a lot in strong trends)
        if pos == 0 and agree != 0 and agree != st.get("prev_agree", 0):
            rvol = realized_vol_annualized(st.get("price_history", []))
            amount = vol_target_size(base_amount, rvol)
            rvol_pct = rvol * 100 if rvol else 0.0
            if agree == 1:
                log(f"{tag}: LONG entry - both timeframes bullish, "
                    f"brick {latest:.4f}, realized vol {rvol_pct:.1f}% "
                    f"-> size {amount} USDT")
                if send_signal("C", symbol, enter_long, f"{tag} ENTER-LONG", amount):
                    st.update(position=1, best=latest, trades=st["trades"] + 1)
            else:
                log(f"{tag}: SHORT entry - both timeframes bearish, "
                    f"brick {latest:.4f}, realized vol {rvol_pct:.1f}% "
                    f"-> size {amount} USDT")
                if send_signal("C", symbol, enter_short, f"{tag} ENTER-SHORT", amount):
                    st.update(position=-1, best=latest, trades=st["trades"] + 1)

        st["prev_agree"] = agree

    if len(st["f_bricks"]) < need or len(st["s_bricks"]) < need:
        log(f"{tag}: warming up - fast {len(st['f_bricks'])} bricks, "
            f"slow {len(st['s_bricks'])} bricks")
        return
    if fast_dir == 0 or slow_dir == 0:
        fast_dir = tema_alma_dir(st["f_bricks"], B_TEMA, B_ALMA)
        slow_dir = tema_alma_dir(st["s_bricks"], B_TEMA, B_ALMA)
        latest = st["f_bricks"][-1] if st["f_bricks"] else 0.0
        if fast_dir == 0 or slow_dir == 0:
            log(f"{tag}: warming up - fast {len(st['f_bricks'])} bricks, "
                f"slow {len(st['s_bricks'])} bricks")
            return
    side = {0: "flat", 1: "holding LONG", -1: "holding SHORT"}[st["position"]]
    log(f"{tag}: {side}. fast {fast_dir:+d}  slow {slow_dir:+d}  "
        f"brick {latest:.4f}  trades {st['trades']}")


def run_strategy_b(symbol, names, st, new_closes, price):
    """Strategy B - the ORIGINAL, UNCHANGED baseline. Fixed $1000 per
    trade, flat 8.5% trail, plain edge-trigger entry. No Stage 1 changes
    at all - this is the control group Strategy C is compared against."""
    enter_long, enter_short, exit_all = codes_for(names)
    tag = f"B/{symbol}"
    keep = max(B_TEMA * 3, B_ALMA) + 40
    need = max(B_TEMA * 3, B_ALMA) + 2
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
                if send_signal("B", symbol, exit_all, f"{tag} EXIT-ALL"):
                    st.update(position=0, best=0.0)
                    pos = 0

        if pos == 0 and agree != 0 and agree != st.get("prev_agree", 0):
            if agree == 1:
                log(f"{tag}: LONG entry - both timeframes bullish, "
                    f"brick {latest:.4f}")
                if send_signal("B", symbol, enter_long, f"{tag} ENTER-LONG"):
                    st.update(position=1, best=latest, trades=st["trades"] + 1)
            else:
                log(f"{tag}: SHORT entry - both timeframes bearish, "
                    f"brick {latest:.4f}")
                if send_signal("B", symbol, enter_short, f"{tag} ENTER-SHORT"):
                    st.update(position=-1, best=latest, trades=st["trades"] + 1)

        st["prev_agree"] = agree

    if len(st["f_bricks"]) < need or len(st["s_bricks"]) < need:
        log(f"{tag}: warming up - fast {len(st['f_bricks'])} bricks, "
            f"slow {len(st['s_bricks'])} bricks")
        return
    if fast_dir == 0 or slow_dir == 0:
        fast_dir = tema_alma_dir(st["f_bricks"], B_TEMA, B_ALMA)
        slow_dir = tema_alma_dir(st["s_bricks"], B_TEMA, B_ALMA)
        latest = st["f_bricks"][-1] if st["f_bricks"] else 0.0
        if fast_dir == 0 or slow_dir == 0:
            log(f"{tag}: warming up - fast {len(st['f_bricks'])} bricks, "
                f"slow {len(st['s_bricks'])} bricks")
            return
    side = {0: "flat", 1: "holding LONG", -1: "holding SHORT"}[st["position"]]
    log(f"{tag}: {side}. fast {fast_dir:+d}  slow {slow_dir:+d}  "
        f"brick {latest:.4f}  trades {st['trades']}")


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

    # strategy B and C share identical Renko/indicator warmup - C just
    # also carries a price_history seed for the volatility calc.
    fb, fa, fd = build_bricks(hourly, B_FAST_BOX, REVERSAL)
    sb, sa, sd = build_bricks(hourly, B_SLOW_BOX, REVERSAL)
    log(f"{strategy}/{symbol}: {len(hourly):,} hourly bars -> {len(fb):,} fast bricks, "
        f"{len(sb):,} slow bricks")
    need = max(B_TEMA * 3, B_ALMA) + 2
    if len(fb) < need or len(sb) < need:
        log(f"{strategy}/{symbol}: not enough bricks, need {need} on both")
        return None
    keep = max(B_TEMA * 3, B_ALMA) + 40
    seed = dict(f_bricks=fb[-keep:], s_bricks=sb[-keep:],
                f_anchor=fa, f_direction=fd, s_anchor=sa, s_direction=sd,
                last_ms=last_ms, position=0, best=0.0, trades=0,
                prev_agree=0, hour_bucket=st_seed["hour_bucket"],
                hour_close=st_seed["hour_close"])
    if strategy == "C":
        seed["price_history"] = hourly[-VOL_LOOKBACK_HRS:]
    return seed


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
                elif strategy == "B":
                    run_strategy_b(symbol, names, st, hourly, price)
                else:
                    run_strategy_c(symbol, names, st, hourly, price)

            except Exception as e:
                log(f"{strategy}/{symbol}: ERROR {type(e).__name__}: {e}")

    save_state(state)
    log("done")


if __name__ == "__main__":
    main()
    sys.exit(0)
