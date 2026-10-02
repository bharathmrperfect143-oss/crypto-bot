"""
Live signal bot - FOUR strategies running side by side (A, B baseline,
C = B + Stage 1 vol-sizing, D = unvalidated single-timeframe experiment).

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

STRATEGY D - SINGLE-TIMEFRAME TEMA/ALMA, ALWAYS IN POSITION  (EXPERIMENTAL,
                                                    NOT OUT-OF-SAMPLE VALIDATED)
    ONE Renko box (0.3%), TEMA 30 / ALMA 13 crossover, no second timeframe,
    no flat state - always long or short, switches direction directly.
    Entry   crossover direction change must hold for D_CONFIRM_N=8
            consecutive bricks before the position actually flips.
    Exit    none separately - flipping to the opposite direction IS the
            exit (3Commas swing-trade mode: only 2 webhook codes used,
            Enter Long / Enter Short, no Exit-All).
    Parameters are the BEST IN-SAMPLE result from a 7,000-config grid
    search (phase30_single_tf.py) - 95.65% neighbourhood robustness
    in-sample. CRITICAL CAVEAT: this did NOT survive out-of-sample
    testing in ANY of 5 independently tried single-timeframe variants
    (raw crossover, this confirm-delay version, KAMA, deadband,
    volatility-adaptive box sizing - 30,800 configs total, 0 survivors
    across all 5). This is deployed as a live experiment at the user's
    explicit request, not as a validated strategy - treat its results
    with real skepticism, not as confirmation it works.

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

import base64
import hashlib
import hmac
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
VOL_LMIN         = 0.30   # floor: never size below 0.30x the base amount.
                          # Added 3 Oct 2026. Without it a vol spike drives
                          # the order size toward zero and C stops being a
                          # meaningful comparison against B.
# ---------------------------------------------------------------------------


WEBHOOK = "https://3c.wtalerts.com/bot/other"

# ---- ENTRY VERIFICATION (added after a real silent-loss incident: a
# webhook got HTTP 200 from wtalerts.com but 3Commas rejected the order
# downstream - the bot believed it held a position that never actually
# existed, and stayed stuck since streak_fired/prev_agree had already
# been marked consumed). This re-uses the SAME v2 auth scheme and the
# SAME bot-name-matching approach already built and verified in
# reconcile_3commas.py, on purpose, rather than inventing a second
# implementation of the same thing.
#
# HONEST SCOPE: this only checks WHETHER 3Commas shows an open deal for
# the strategy/symbol - it does NOT check LONG vs SHORT direction.
# reconcile_3commas.py's own project record shows direction cannot
# currently be reliably read from this API response shape (a numeric
# field that looked like a direction indicator turned out to track
# unrealized P&L instead). Checking existence only is still the exact
# protection this incident needed - the failure was "believed a deal
# existed when it did not," not "believed the wrong direction."
THREECOMMAS_API_KEY    = os.environ.get("THREECOMMAS_API_KEY", "")
THREECOMMAS_API_SECRET = os.environ.get("THREECOMMAS_API_SECRET", "")
THREECOMMAS_API_BASE   = "https://trade.3commas.io"
THREECOMMAS_RECV_MS    = 60000

# strategy+symbol -> the bot-name prefix confirmed against the live
# 3Commas dashboard (see the project record) - kept identical to
# reconcile_3commas.py's BOT_CONFIG substrings on purpose.
_BOT_NAME_PREFIX = {
    ("A", "SOLUSDT"): "A-SOL", ("A", "XRPUSDT"): "A-XRP",
    ("B", "SOLUSDT"): "B-SOL", ("B", "XRPUSDT"): "B-XRP",
    ("C", "SOLUSDT"): "C-SOL", ("C", "XRPUSDT"): "C-XRP",
    ("D", "SOLUSDT"): "D-SOL", ("D", "XRPUSDT"): "D-XRP",
}

_verify_warned_no_creds = False


def _v2_sign(method, path, body, secret):
    ts = str(int(time.time() * 1000))
    payload = f"{method}\n{path}\n{ts}\n{THREECOMMAS_RECV_MS}\n{body}"
    sig = base64.b64encode(
        hmac.new(secret.encode(), payload.encode(), hashlib.sha256).digest()
    ).decode()
    return sig, ts


def _fetch_live_deals():
    """GET /open_api/strategies/live - returns the raw list, or None on
    any error (network, auth, parse). Never raises - a verification
    check that itself fails should not crash the whole bot run."""
    path = "/open_api/strategies/live"
    sig, ts = _v2_sign("GET", path, "", THREECOMMAS_API_SECRET)
    req = urllib.request.Request(
        THREECOMMAS_API_BASE + path,
        headers={
            "X-API-Key": THREECOMMAS_API_KEY,
            "X-Signature": sig,
            "X-Timestamp": ts,
            "X-Recv-Window": str(THREECOMMAS_RECV_MS),
            "User-Agent": "renko-bot-verify",
        },
        method="GET")
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read().decode())
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for k in ("items", "data", "result", "strategies", "results"):
                if isinstance(data.get(k), list):
                    return data[k]
        return None
    except Exception as e:
        log(f"  verify: could not query 3Commas ({type(e).__name__}: {e})")
        return None


def verify_entry_accepted(strategy, symbol, tries=3, wait_s=6):
    """Poll 3Commas a few times (order processing is not instant) to
    confirm a deal now actually exists for this strategy/symbol, after
    send_signal() already returned True. Returns True if confirmed, or
    if verification cannot be performed at all (missing API
    credentials) - see the module-level comment above for why this
    fails open rather than blocking every entry when the operator
    simply hasn't set the two API secrets yet. Returns False only when
    a real check ran and genuinely found no matching deal."""
    global _verify_warned_no_creds
    if not THREECOMMAS_API_KEY or not THREECOMMAS_API_SECRET:
        if not _verify_warned_no_creds:
            log("  verify: THREECOMMAS_API_KEY/SECRET not set - entry "
                "verification is DISABLED, trusting webhook http status "
                "only (this was the exact gap that caused the original "
                "incident)")
            _verify_warned_no_creds = True
        return True

    prefix = _BOT_NAME_PREFIX.get((strategy, symbol))
    if not prefix:
        return True  # unknown combo, nothing to check against

    # IMPORTANT: a query that could not even be COMPLETED (network error,
    # 3Commas API temporarily unreachable, etc. - _fetch_live_deals()
    # returns None for these) is NOT the same as a query that completed
    # and genuinely found no matching deal. Only the second case should
    # ever reject the entry - rejecting on the first case would risk a
    # DUPLICATE order on the next retry if the original webhook had
    # actually succeeded and only the verification check itself failed
    # to reach 3Commas. If every attempt errors out, this fails open
    # (trusts the webhook), matching the missing-credentials behaviour
    # above.
    any_successful_query = False
    for attempt in range(tries):
        deals = _fetch_live_deals()
        if deals is not None:
            any_successful_query = True
            for d in deals:
                name = ""
                sig_bot = d.get("signalBot")
                if isinstance(sig_bot, dict):
                    name = str(sig_bot.get("name", ""))
                pair = str(d.get("pair", "") or "").upper()
                if prefix.lower() in name.lower() and pair == symbol:
                    return True
        if attempt < tries - 1:
            time.sleep(wait_s)

    if not any_successful_query:
        log(f"  verify: {strategy}/{symbol} - could not reach 3Commas in "
            f"{tries} attempts (network/API error, not a real rejection) "
            f"- trusting the webhook result instead of blocking the entry")
        return True

    log(f"  verify: {strategy}/{symbol} - webhook returned OK but no "
        f"matching deal found on 3Commas after {tries} checks - "
        f"treating the entry as NOT confirmed")
    return False


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
    "D": {
        # only 2 codes actually used (D never sends exit-all - it's always
        # in a position, swing-trade style; the 3rd slot is kept only so
        # codes_for() stays a uniform 3-tuple across all strategies)
        "SOLUSDT": ("D_SOL_ENTER_LONG", "D_SOL_ENTER_SHORT", "D_SOL_EXIT_ALL"),
        "XRPUSDT": ("D_XRP_ENTER_LONG", "D_XRP_ENTER_SHORT", "D_XRP_EXIT_ALL"),
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
    "D": {"SOLUSDT": 1000, "XRPUSDT": 1000},
}

# ---- Strategy D config: single-timeframe TEMA/ALMA, always in position ----
# Best IN-SAMPLE parameters from phase30_single_tf.py's 7,000-config grid
# search (box/TEMA/ALMA/confirm_n), 95.65% neighbourhood robustness.
# UNVALIDATED OUT-OF-SAMPLE: none of 5 independent single-timeframe
# variants tested (raw, this confirm-delay version, KAMA, deadband,
# adaptive-box-sizing) survived out-of-sample testing - see the project
# record. This is a live experiment, not a proven strategy - deployed at
# the user's explicit request to get real forward-test data.
D_BOX      = 0.3
D_TEMA     = 30
D_ALMA     = 13
D_CONFIRM_N = 8

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
    """Scale base_amount by VOL_TARGET / realized_vol.

    Capped ABOVE at VOL_LMAX (never size up past base) and now also
    capped BELOW at VOL_LMIN. The lower bound did not exist before 3 Oct
    2026, and that was a real latent bug: a volatility spike scales the
    position down without limit, so an 800% realized vol would have sent
    a 50 USDT order - so small it rounds to noise on 3Commas and does not
    represent a meaningful test of the strategy. The floor keeps every C
    trade big enough to actually measure something.
    """
    if not realized_vol or realized_vol <= 0:
        return base_amount
    weight = min(VOL_LMAX, VOL_TARGET / realized_vol)
    weight = max(VOL_LMIN, weight)
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
                if (send_signal("A", symbol, enter_long, f"{tag} ENTER-LONG")
                        and verify_entry_accepted("A", symbol)):
                    st.update(position=1, best=brick, trades=st["trades"] + 1)
            elif brick <= lo:
                log(f"{tag}: SHORT entry - {brick:.4f} <= "
                    f"{A_BREAKOUT_N}-brick low {lo:.4f}")
                if (send_signal("A", symbol, enter_short, f"{tag} ENTER-SHORT")
                        and verify_entry_accepted("A", symbol)):
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
        # isolation testing confirmed this matters a lot in strong trends).
        # CRITICAL: prev_agree is only marked CONSUMED on a SUCCESSFUL send,
        # so a failed first attempt is retried on the next brick (the original
        # code set it unconditionally, silently dropping the signal).
        if pos == 0 and agree != 0 and agree != st.get("prev_agree", 0):
            rvol = realized_vol_annualized(st.get("price_history", []))
            amount = vol_target_size(base_amount, rvol)
            rvol_pct = rvol * 100 if rvol else 0.0
            log(f"  {tag}: VOL-SIZING - realized vol {rvol_pct:.1f}% "
                f"(target {VOL_TARGET * 100:.0f}%, band "
                f"{VOL_LMIN * 100:.0f}%-{VOL_LMAX * 100:.0f}%), "
                f"base {base_amount} USDT -> size {amount} USDT "
                f"from {len(st.get('price_history', []))} hourly closes")
            if agree == 1:
                log(f"{tag}: LONG entry - both timeframes bullish, "
                    f"brick {latest:.4f}, realized vol {rvol_pct:.1f}% "
                    f"-> size {amount} USDT")
                if (send_signal("C", symbol, enter_long, f"{tag} ENTER-LONG", amount)
                        and verify_entry_accepted("C", symbol)):
                    st.update(position=1, best=latest, trades=st["trades"] + 1)
                    st["prev_agree"] = agree
            else:
                log(f"{tag}: SHORT entry - both timeframes bearish, "
                    f"brick {latest:.4f}, realized vol {rvol_pct:.1f}% "
                    f"-> size {amount} USDT")
                if (send_signal("C", symbol, enter_short, f"{tag} ENTER-SHORT", amount)
                        and verify_entry_accepted("C", symbol)):
                    st.update(position=-1, best=latest, trades=st["trades"] + 1)
                    st["prev_agree"] = agree
            # On failure: leave prev_agree untouched so the next brick retries.

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

        # Edge-trigger entry: only on a genuine agree change from prev_agree.
        # CRITICAL: prev_advance is only marked CONSUMED on a SUCCESSFUL send
        # (the old code set it unconditionally, which silently dropped the
        # signal after a single failed webhook attempt - if the first ENTER-
        # LONG failed (HTTP 5xx, secret empty, etc.), the bot stayed flat
        # even though agree stayed +1 for many subsequent bricks).
        if pos == 0 and agree != 0 and agree != st.get("prev_agree", 0):
            if agree == 1:
                log(f"{tag}: LONG entry - both timeframes bullish, "
                    f"brick {latest:.4f}")
                if (send_signal("B", symbol, enter_long, f"{tag} ENTER-LONG")
                        and verify_entry_accepted("B", symbol)):
                    st.update(position=1, best=latest, trades=st["trades"] + 1)
                    st["prev_agree"] = agree
            else:
                log(f"{tag}: SHORT entry - both timeframes bearish, "
                    f"brick {latest:.4f}")
                if (send_signal("B", symbol, enter_short, f"{tag} ENTER-SHORT")
                        and verify_entry_accepted("B", symbol)):
                    st.update(position=-1, best=latest, trades=st["trades"] + 1)
                    st["prev_agree"] = agree
            # On failure: leave prev_agree untouched so the next brick can retry.
        elif pos == 0 and agree != 0 and agree == st.get("prev_agree", 0):
            # agree stable but unprocessed (previous fire must have failed) -
            # keep the edge-trigger alive by NOT touching prev_agree here.
            pass

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


def run_strategy_d(symbol, names, st, new_closes, price):
    """Strategy D - single-timeframe TEMA/ALMA crossover, ALWAYS in a
    position (no flat state - long or short, switches directly between
    them). Confirmation-delayed: a new crossover direction must hold for
    D_CONFIRM_N consecutive bricks before the position actually flips.
    The fire is RETRYABLE - once streak_len >= D_CONFIRM_N and d != pos,
    every subsequent matching brick attempts the send until success
    (st["streak_fired"] gates retry). The original `streak_len ==
    D_CONFIRM_N` exact-match check was one-shot: if the first send
    failed (HTTP 5xx, secret empty, etc.), streak_len kept climbing past
    8 and the fire condition could never be true again until direction
    flipped. That's exactly the silent-failure mode Strategy D got stuck
    in on 2026-09-17 (codes empty -> streak=45, pos=0, trades=0 forever).
    No trailing exit, no separate EXIT-ALL - this relies on 3Commas'
    swing-trade mode, which switches direction using only Enter Long/
    Enter Short signals."""
    enter_long, enter_short, _ = codes_for(names)
    tag = f"D/{symbol}"
    keep = max(D_TEMA * 3, D_ALMA) + 40
    need = max(D_TEMA * 3, D_ALMA) + 2

    d = 0
    latest = 0.0

    for c in new_closes:
        f_new, st["anchor"], st["direction"] = build_bricks(
            [c], D_BOX, REVERSAL, st["anchor"], st["direction"])
        if f_new:
            st["bricks"] = (st["bricks"] + f_new)[-keep:]
        if not f_new:
            continue
        if len(st["bricks"]) < need:
            continue

        d = tema_alma_dir(st["bricks"], D_TEMA, D_ALMA)
        if d == 0:
            continue
        latest = st["bricks"][-1]

        if d == st.get("streak_dir", 0):
            st["streak_len"] = st.get("streak_len", 0) + 1
        else:
            st["streak_dir"] = d
            st["streak_len"] = 1
            # reset retry gate when direction changes - new streak gets a
            # fresh shot at firing.
            st["streak_fired"] = False

        pos = st["position"]
        # Retryable fire: every brick with streak_len >= D_CONFIRM_N and
        # d != pos attempts the send, until either success (mark fired)
        # or direction change (reset above).
        if (st["streak_len"] >= D_CONFIRM_N and d != pos
                and not st.get("streak_fired", False)):
            side = "LONG" if d == 1 else "SHORT"
            code = enter_long if d == 1 else enter_short
            label = f"{tag} ENTER-{side}"
            log(f"{tag}: {side} entry (confirmed {D_CONFIRM_N} bricks), "
                f"brick {latest:.4f}, flipping from position {pos:+d}")
            if (send_signal("D", symbol, code, label)
                    and verify_entry_accepted("D", symbol)):
                st.update(position=d, trades=st["trades"] + 1)
                st["streak_fired"] = True
            # On failure: leave streak_fired False so the next brick retries.

    if len(st["bricks"]) < need:
        log(f"{tag}: warming up - {len(st['bricks'])} bricks, need {need}")
        return
    if d == 0:
        d = tema_alma_dir(st["bricks"], D_TEMA, D_ALMA)
        latest = st["bricks"][-1] if st["bricks"] else 0.0
        if d == 0:
            log(f"{tag}: warming up - {len(st['bricks'])} bricks, need {need}")
            return
    side = {0: "flat(pre-first-entry)", 1: "holding LONG", -1: "holding SHORT"}[st["position"]]
    log(f"{tag}: {side}. dir {d:+d}  brick {latest:.4f}  trades {st['trades']}  "
        f"streak {st.get('streak_len', 0)}/{D_CONFIRM_N}")


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

    if strategy == "D":
        need = max(D_TEMA * 3, D_ALMA) + 2
        bricks, anchor, d = build_bricks(hourly, D_BOX, REVERSAL)
        log(f"D/{symbol}: {len(hourly):,} hourly bars -> {len(bricks):,} bricks "
            f"(box={D_BOX}%)")
        if len(bricks) < need:
            log(f"D/{symbol}: not enough bricks, need {need}")
            return None
        keep = max(D_TEMA * 3, D_ALMA) + 40
        return dict(bricks=bricks[-keep:], anchor=anchor, direction=d,
                    last_ms=last_ms, position=0, trades=0,
                    streak_dir=0, streak_len=0, streak_fired=False,
                    hour_bucket=st_seed["hour_bucket"],
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
                elif strategy == "C":
                    run_strategy_c(symbol, names, st, hourly, price)
                else:
                    run_strategy_d(symbol, names, st, hourly, price)

                # ---- PER-STRATEGY EVALUATION HEARTBEAT (M3, 3 Oct 2026)
                # Records that this strategy was actually RUN against a
                # closed hourly bucket just now. Without this, "flat
                # because the strategy chose to sit" and "flat because
                # the strategy stopped being evaluated" are the exact
                # same observable state - which is precisely how Strategy
                # C stayed dark for six days with nothing detecting it
                # (see CRYPTO_MASTER_RECORD.txt PART 32.5). The workflow
                # healthcheck only proves the PROCESS ran; this proves
                # each STRATEGY within it did real work.
                st["last_eval_ms"] = int(time.time() * 1000)
                st["last_eval_hour"] = hourly[-1]

            except Exception as e:
                log(f"{strategy}/{symbol}: ERROR {type(e).__name__}: {e}")

    save_state(state)
    log("done")

    # ---- STALENESS CHECK (M3, 2026-10-02) -------------------------------
    # A green workflow exit only proves the CODE ran. It does not prove the
    # bot made PROGRESS. This project had a 4-day silent freeze where the
    # workflow was green 2233 times, healthchecks.io pinged "healthy" every
    # time, and Telegram stayed silent - because nothing checked whether
    # state.json actually advanced toward real time.
    #
    # This check is the fix: if the OLDEST last_ms across all strategies is
    # further behind than STALE_HOURS, the bot is stuck. Exit non-zero so the
    # workflow fails, which makes the existing "Heartbeat (failure)" step
    # curl healthchecks.io /fail, which fires the Telegram alert. No new
    # infrastructure required.
    #
    # STALE_HOURS = 1 is deliberate and correct. This measures DATA LAG, not
    # how long a position is held - a bot can hold a position for months and
    # still be perfectly healthy, because it keeps fetching fresh candles
    # every run. The only thing that should ever push last_ms behind real
    # time is a genuine freeze, and the workflow runs every 5 minutes, so
    # anything beyond ~1h behind means the bot stopped making progress.
    # While catching up, last_ms moves FORWARD past real time (1000 x 1-min
    # candles = ~16.7h of data per run), so catch-up never trips this check.
    STALE_HOURS = 1
    now_ms = int(time.time() * 1000)
    lags = []
    for _k, _st in state.items():
        if isinstance(_st, dict) and isinstance(_st.get("last_ms"), (int, float)):
            lags.append((now_ms - int(_st["last_ms"])) / 3_600_000.0)

    if not lags:
        log("  STALENESS: no last_ms found in state - FAILING to be safe")
        sys.exit(2)

    # ---- PER-STRATEGY EVALUATION CHECK (M3, 3 Oct 2026) -----------------
    # last_ms being current only proves the DATA is current. It does not
    # prove each strategy is still being EVALUATED against it - a
    # strategy whose runner started throwing on every hour, or whose
    # hourly bucket stopped advancing, would keep last_ms fresh forever
    # while silently never being evaluated. Strategy C lived in exactly
    # that blind spot for six days.
    #
    # STALE_EVAL_HOURS is deliberately larger than the workflow interval
    # (5 min) because strategies only act on CLOSED hourly buckets, so a
    # healthy strategy is only evaluated roughly once an hour. 3 hours
    # allows three consecutive missed hours before we call it stuck -
    # tight enough to catch a day-old failure the same day, loose enough
    # that a single slow or retried run never false-alarms.
    STALE_EVAL_HOURS = 3
    now_eval = int(time.time() * 1000)
    starved = []
    for _k, _st in state.items():
        if not isinstance(_st, dict):
            continue
        _le = _st.get("last_eval_ms")
        if not isinstance(_le, (int, float)):
            # No heartbeat at all for this strategy. Expected ONLY on the
            # first run after this change is deployed - every strategy
            # gets one on its next closed hour. Do not fail on it.
            continue
        _lag_h = (now_eval - int(_le)) / 3_600_000.0
        if _lag_h > STALE_EVAL_HOURS:
            starved.append((_k, _lag_h))

    if starved:
        for _k, _lag_h in starved:
            log(f"  STALENESS: FAIL - {_k} has not been evaluated in "
                f"{_lag_h:.1f}h (threshold {STALE_EVAL_HOURS}h). Data is "
                f"current but this STRATEGY is not being evaluated.")
        log("  This will trigger the Telegram alert via healthchecks.io.")
        sys.exit(4)

    worst = max(lags)
    if worst > STALE_HOURS:
        log(f"  STALENESS: FAIL - worst last_ms is {worst:.1f}h behind real time "
            f"(threshold {STALE_HOURS}h). Bot is STUCK. "
            f"This will trigger the Telegram alert via healthchecks.io.")
        sys.exit(3)

    log(f"  STALENESS: OK - worst last_ms is {worst:.1f}h behind "
        f"(threshold {STALE_HOURS}h, {len(lags)} strategies checked)")
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
