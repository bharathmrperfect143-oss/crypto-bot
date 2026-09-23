"""Diagnostic: test 3Commas v2 REST API endpoints.

v2 base URL: https://trade.3commas.io (paths under /open_api/...)
v2 auth: HMAC-SHA256 of METHOD\\nPATH\\nTS\\nRECV\\nBODY, Base64 encoded
        headers: X-API-Key, X-Signature, X-Timestamp, X-Recv-Window

CONFIRMED WORKING: /open_api/api_profiles  (returned real JSON)
STILL SEARCHING:  the Strategy endpoint path (404 on /open_api/strategies)
"""

import os
import sys
import hmac
import hashlib
import base64
import time
import urllib.request
import urllib.error


API_BASE = "https://trade.3commas.io"
RECV_WINDOW = 60000


def call(method, path):
    api_key = os.environ.get("THREECOMMAS_API_KEY", "")
    secret = os.environ.get("THREECOMMAS_API_SECRET", "")
    if not api_key or not secret:
        return None, "MISSING_KEYS"

    body = ""
    ts = str(int(time.time() * 1000))
    payload = f"{method}\n{path}\n{ts}\n{RECV_WINDOW}\n{body}"
    sig = base64.b64encode(
        hmac.new(secret.encode(), payload.encode(), hashlib.sha256).digest()
    ).decode()
    url = f"{API_BASE}{path}"
    headers = {
        "X-API-Key": api_key,
        "X-Signature": sig,
        "X-Timestamp": ts,
        "X-Recv-Window": str(RECV_WINDOW),
        "User-Agent": "diag/2.0",
    }

    req = urllib.request.Request(url, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            body_text = r.read().decode("utf-8", errors="replace")
            preview = body_text[:200].replace("\n", " ")
            ct = r.headers.get("Content-Type", "?")
            return True, f"HTTP {r.status}  ct={ct}  body[:200]={preview!r}"
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")[:200]
        except Exception:
            pass
        if e.code == 404:
            return None, f"404 (not found)"
        return False, f"HTTP {e.code}  body={body!r}"
    except urllib.error.URLError as e:
        return False, f"URLError: {e.reason}"
    except Exception as e:
        return False, f"ERROR: {type(e).__name__}: {e}"


def main():
    print("=" * 70)
    print("3Commas v2 REST API endpoint discovery")
    print(f"base: {API_BASE}")
    print("=" * 70)

    endpoints = [
        ("/open_api/api_profiles?exchanges=BINANCE",
         "API Profiles (CONFIRMED WORKING - sanity check)"),
        ("/open_api/strategies", "tried, returned 404"),
        ("/open_api/strategy", "singular variant"),
        ("/open_api/strategies/list", "list subpath"),
        ("/open_api/strategies?limit=20", "limit param only"),
        ("/open_api/bots", "DCA bots endpoint"),
        ("/open_api/bots?limit=20", "DCA bots with limit"),
        ("/open_api/signal_bots", "Signal bots"),
        ("/open_api/smart_trades", "Smart trades"),
        ("/open_api/positions", "positions"),
        ("/open_api/positions?state=active", "active positions"),
        ("/open_api/accounts/positions", "account positions"),
        ("/open_api/deals", "deals"),
        ("/open_api/deals?scope=active", "active deals"),
    ]

    for path, desc in endpoints:
        ok, result = call("GET", path)
        if ok is True:
            status = "OK"
        elif ok is None:
            status = "404"
        else:
            status = "FAIL"
        print(f"\n[{status}] {path}")
        print(f"        {desc}")
        print(f"        {result}")

    print("\n" + "=" * 70)
    print("Goal: find an [OK] that returns a list/array of strategies or deals.")
    print("If all 404, click 'Strategy' in docs sidebar and tell me the endpoint path.")


if __name__ == "__main__":
    main()
