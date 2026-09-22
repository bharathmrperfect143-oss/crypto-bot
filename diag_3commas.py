"""Diagnostic: try 3 different 3Commas endpoints to identify what's broken.

Usage: in workflow, run before reconcile_3commas.py to see what auth/perm state is.

Endpoints tested:
  1. /ver1/accounts   - basic API access (lists exchange accounts)
  2. /ver1/bots       - your bots list
  3. /ver1/deals      - active deals (what reconcile needs)
"""

import os
import sys
import json
import hmac
import hashlib
import urllib.request
import urllib.error


API_BASE = "https://api.3commas.io/public/api"


def call(path):
    """Try one endpoint. Returns a short diagnostic string."""
    full_uri = f"/public/api{path}"
    secret = os.environ.get("THREECOMMAS_API_SECRET", "").encode()
    api_key = os.environ.get("THREECOMMAS_API_KEY", "")
    if not secret or not api_key:
        return "MISSING_KEYS"
    sig = hmac.new(secret, full_uri.encode(), hashlib.sha256).hexdigest()
    url = API_BASE + path
    headers = {"Apikey": api_key, "Signature": sig,
               "User-Agent": "diag/1.0"}
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", errors="replace")
            preview = body[:120].replace("\n", " ")
            return f"HTTP {r.status}  body[:120]={preview!r}"
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")[:200]
        except Exception:
            pass
        return f"HTTP {e.code}  body={body!r}"
    except Exception as e:
        return f"ERROR: {type(e).__name__}: {e}"


def main():
    print("=" * 70)
    print("3Commas API diagnostic - tests 3 endpoints to identify auth/perm issues")
    print("=" * 70)
    endpoints = [
        ("/ver1/accounts", "basic auth test (lists exchange accounts)"),
        ("/ver1/bots?limit=10", "list your bots"),
        ("/ver1/deals?scope=active&limit=10", "active deals (reconcile needs this)"),
        ("/ver1/deals?limit=10", "all deals, no scope filter"),
    ]
    for path, desc in endpoints:
        print(f"\n[TEST] {desc}")
        print(f"  URL: {API_BASE}{path}")
        result = call(path)
        print(f"  RESULT: {result}")
        if "HTTP 200" in result and "{" in result:
            print(f"  STATUS: OK works")
        elif "HTTP 401" in result or "HTTP 403" in result:
            print(f"  STATUS: FAIL auth/permission denied")
        elif "HTTP 200" in result and ("<html" in result.lower() or "<!doctype" in result.lower()):
            print(f"  STATUS: FAIL returned HTML (likely auth/perms blocked)")
        else:
            print(f"  STATUS: ? unknown - see response above")


if __name__ == "__main__":
    main()
