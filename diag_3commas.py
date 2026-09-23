"""Diagnostic: test 3Commas v2 REST API endpoints.

v2 base URL: https://trade.3commas.io (paths under /open_api/...)
v2 auth: HMAC-SHA256 of METHOD\\nPATH\\nTS\\nRECV\\nBODY, Base64 encoded
        headers: X-API-Key, X-Signature, X-Timestamp, X-Recv-Window
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
            body = e.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            pass
        return False, f"HTTP {e.code}  body={body!r}"
    except urllib.error.URLError as e:
        return False, f"URLError: {e.reason}"
    except Exception as e:
        return False, f"ERROR: {type(e).__name__}: {e}"


def main():
    print("=" * 70)
    print("3Commas v2 REST API diagnostic")
    print(f"base: {API_BASE}")
    print("=" * 70)

    endpoints = [
        ("/open_api/api_profiles?exchanges=BINANCE",
         "API Profiles - lists exchanges/accounts (per docs GET example)"),
        ("/open_api/strategies",
         "Strategy - lists all strategies"),
        ("/open_api/strategies?state=active",
         "Strategy - active only"),
        ("/open_api/strategies?limit=10",
         "Strategy - first 10"),
    ]

    for path, desc in endpoints:
        print(f"\n[TEST] {desc}")
        print(f"  URL: {API_BASE}{path}")
        ok, result = call("GET", path)
        status = "OK" if ok else "FAIL"
        print(f"  [{status}] {result}")

    print("\n" + "=" * 70)
    print("If any returned JSON (not HTML/error), that's the endpoint to use.")
    print("If all FAIL with HTML, the docs URL or endpoint paths are wrong.")


if __name__ == "__main__":
    main()
