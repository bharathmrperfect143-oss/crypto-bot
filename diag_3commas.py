"""Extended diagnostic: try BOTH v1 and v2 API endpoints to find where v2 lives.

The user's v2 bots have webhook codes that work (signals execute),
but the v1 API at api.3commas.io/public/api returns HTML.
The v2 API may be at a different URL entirely - we test multiple
possibilities with both v1 (HMAC-SHA256 hex) and v2 (HMAC-SHA256
Base64 with timestamp) auth schemes.
"""

import os
import sys
import hmac
import hashlib
import base64
import time
import urllib.request
import urllib.error


# Possible base URLs for v2 API
BASES = [
    "https://api.3commas.io/public/api",
    "https://api.wundertrading.com",
    "https://api.wundertrading.com/open_api",
]

# Endpoints to test per base
PATHS_V1 = [
    "/ver1/accounts",
    "/ver1/deals?scope=active&limit=5",
    "/ver1/bots?limit=5",
]
PATHS_V2 = [
    "/api_profiles?exchanges=BINANCE",
    "/strategies",
    "/strategies?limit=5",
]


def call_v1_style(base, path):
    """Original v1 HMAC: hex of /public/api<path>?<query>"""
    full_uri = f"/public/api{path}"
    secret = os.environ.get("THREECOMMAS_API_SECRET", "").encode()
    api_key = os.environ.get("THREECOMMAS_API_KEY", "")
    if not secret or not api_key:
        return None, "MISSING_KEYS"
    sig = hmac.new(secret, full_uri.encode(), hashlib.sha256).hexdigest()
    url = f"{base}{path}"
    headers = {"Apikey": api_key, "Signature": sig}
    return _do_request(url, headers)


def call_v2_style(base, path):
    """WunderTrading v2 HMAC: Base64 of METHOD\nPATH\nTS\nRECV\nBODY."""
    secret = os.environ.get("THREECOMMAS_API_SECRET", "").encode()
    api_key = os.environ.get("THREECOMMAS_API_KEY", "")
    if not secret or not api_key:
        return None, "MISSING_KEYS"
    ts = str(int(time.time() * 1000))
    recv = "60000"
    payload = f"GET\n{path}\n{ts}\n{recv}\n"
    sig = base64.b64encode(
        hmac.new(secret, payload.encode(), hashlib.sha256).digest()
    ).decode()
    url = f"{base}{path}"
    headers = {
        "X-API-Key": api_key,
        "X-Signature": sig,
        "X-Timestamp": ts,
        "X-Recv-Window": recv,
    }
    return _do_request(url, headers)


def _do_request(url, headers):
    headers.setdefault("User-Agent", "diag-v2/1.0")
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", errors="replace")
            preview = body[:100].replace("\n", " ")
            ct = r.headers.get("Content-Type", "?")
            return (True, f"HTTP {r.status}  ct={ct}  body[:100]={preview!r}")
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")[:200]
        except Exception:
            pass
        return (False, f"HTTP {e.code}  body={body!r}")
    except urllib.error.URLError as e:
        return (False, f"URLError: {e.reason}")
    except Exception as e:
        return (False, f"ERROR: {type(e).__name__}: {e}")


def main():
    print("=" * 70)
    print("3Commas v2 API discovery")
    print("=" * 70)

    print("\n--- v1 auth (Apikey + Signature, hex digest) ---")
    for base in BASES:
        for path in PATHS_V1:
            ok, result = call_v1_style(base, path)
            status = "OK" if ok else "FAIL"
            print(f"  [{status}] {base}{path}")
            print(f"         {result[:200]}")
        print()

    print("--- v2 auth (X-API-Key + X-Signature, Base64 digest with timestamp) ---")
    for base in BASES:
        for path in PATHS_V2:
            ok, result = call_v2_style(base, path)
            status = "OK" if ok else "FAIL"
            print(f"  [{status}] {base}{path}")
            print(f"         {result[:200]}")
        print()


if __name__ == "__main__":
    main()
