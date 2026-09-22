"""
check_secrets.py
================

Webhook-code presence check for the live trading bot.

Looks up every webhook code the bot expects (defined in bot.MARKETS)
in os.environ, reports any that are missing or empty.

Why this exists: the bot logs "no code configured for X - skipped" at
fire time, but that warning can be drowned out in a busy log and the
resulting failure mode (silent no-op) is the worst possible for a
trading bot. This check runs BEFORE main() and exits non-zero if any
code is missing, so the failure surfaces immediately.

Usage:
    # CI pre-step (loud)
    python check_secrets.py || exit 1

    # Standalone diagnostic (always prints + writes to trades.log)
    python check_secrets.py

Exit codes:
    0 - all webhook codes present
    1 - at least one code missing (CI should fail)
    2 - could not import bot.MARKETS (bot.py not on PYTHONPATH)
"""

import os
import sys
from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))
LOG_FILE = "trades.log"


def log(msg):
    """Mirror the bot's log format so messages blend in trades.log."""
    line = f"{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST  check_secrets: {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def get_required_codes():
    """Pull (strategy, symbol, code_name) tuples from bot.MARKETS."""
    try:
        import bot
    except ImportError:
        return None
    out = []
    for strategy, markets in bot.MARKETS.items():
        for symbol, names in markets.items():
            for code_name in names:
                out.append((strategy, symbol, code_name))
    return out


def main():
    codes = get_required_codes()
    if codes is None:
        log("ERROR - could not import bot.MARKETS. Is bot.py in the same directory?")
        return 2

    missing = []
    present = []
    for strategy, symbol, code_name in codes:
        val = os.environ.get(code_name, "")
        if val:
            present.append((strategy, symbol, code_name))
        else:
            missing.append((strategy, symbol, code_name))

    log(f"checked {len(codes)} webhook codes: {len(present)} present, "
        f"{len(missing)} missing")

    if not missing:
        log(f"OK - all {len(codes)} webhook codes present in env")
        return 0

    log(f"FAIL - {len(missing)} webhook codes are missing or empty:")
    by_strategy = {}
    for strategy, symbol, code_name in missing:
        by_strategy.setdefault(strategy, []).append((symbol, code_name))

    for strategy in sorted(by_strategy):
        items = by_strategy[strategy]
        log(f"  strategy {strategy}:")
        for symbol, code_name in items:
            log(f"    - {code_name}  (used by {strategy}/{symbol})")

    log("Bot will silently skip signals for these codes (no-op). "
        "Add them to GitHub Secrets (or Windows env vars) before deploying.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
