"""WSDL contract checker for the OrderManagement service.

Modes:
  (default)                fetch the WSDL with basic auth, assert the 4
                           operations are advertised — used by make smoke-test
                           Stage 1.
  --expect-unauthorized    assert an unauthenticated fetch is rejected with
                           HTTP 401 (proves auth is enforced).
  --emit --out PATH        write the served WSDL to PATH (the golden contract
                           artifact behind `make contract-freeze`).

Security posture: the fetch target is restricted to an explicit host allowlist
(this checker exists to talk to the local SOAP service, nothing else), and
--emit is restricted to the service's own contract directory.
"""
import argparse
import base64
import hashlib
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

MARKERS = ("OrderManagement", "CreateOrder", "GetOrders", "GetOrderStatus", "UpdateOrderStatus")

# Only the HELIOS SOAP endpoints this tool is meant to check, by name.
ALLOWED_HOSTS = frozenset({"localhost", "127.0.0.1", "soap-service"})
CONTRACT_DIR = Path("/contract")
FALLBACK_CONTRACT_DIR = Path(__file__).resolve().parent / "contract"


def wsdl_url():
    raw = os.environ.get("SOAP_WSDL_URL", "http://localhost:8000/?wsdl")
    parsed = urlparse(raw)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"blocked scheme {parsed.scheme!r}; only http/https allowed")
    if parsed.hostname not in ALLOWED_HOSTS or parsed.username or parsed.password:
        raise ValueError(
            f"blocked host {parsed.hostname!r}; allowed hosts: {sorted(ALLOWED_HOSTS)}"
        )
    if parsed.path.rstrip("/") not in ("", "/?wsdl") and not parsed.query == "wsdl":
        raise ValueError(f"blocked path {parsed.path!r}; this tool only fetches the WSDL")
    return raw


def allowed_out_path(raw):
    candidate = Path(raw).resolve()
    for base in (CONTRACT_DIR, FALLBACK_CONTRACT_DIR):
        base = base.resolve()
        if candidate.parent == base and candidate.suffix == ".wsdl":
            return candidate
    raise ValueError(
        f"blocked --out {raw!r}; must be a .wsdl file directly inside the contract directory"
    )


def fetch(with_auth=True):
    request = urllib.request.Request(wsdl_url())
    request.add_header("User-Agent", "helios-contract-check/1.0")
    if with_auth:
        user = os.environ["SOAP_BASIC_AUTH_USER"]
        password = os.environ["SOAP_BASIC_AUTH_PASSWORD"]
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        request.add_header("Authorization", f"Basic {token}")
    with urllib.request.urlopen(request, timeout=15) as response:
        return response.read()


def check_markers(wsdl, where):
    missing = [marker for marker in MARKERS if marker not in wsdl]
    if missing:
        print(f"[contract] FAIL ({where}): missing {missing} in WSDL", file=sys.stderr)
        return False
    return True


def main():
    parser = argparse.ArgumentParser(description="Check the served OrderManagement WSDL.")
    parser.add_argument("--expect-unauthorized", action="store_true",
                        help="assert unauthenticated access is rejected with HTTP 401")
    parser.add_argument("--emit", action="store_true",
                        help="write the served WSDL to --out (golden contract artifact)")
    parser.add_argument("--out", default=None,
                        help=f"target .wsdl path (default: {FALLBACK_CONTRACT_DIR / 'OrderManagement.wsdl'})")
    args = parser.parse_args()
    out_path = args.out or str(FALLBACK_CONTRACT_DIR / "OrderManagement.wsdl")

    if args.expect_unauthorized:
        try:
            fetch(with_auth=False)
        except urllib.error.HTTPError as error:
            if error.code == 401:
                print("[contract] ok: unauthenticated WSDL request rejected with 401")
                return 0
            print(f"[contract] FAIL: expected 401, got HTTP {error.code}", file=sys.stderr)
            return 1
        print("[contract] FAIL: WSDL fetched without credentials — auth not enforced",
              file=sys.stderr)
        return 1

    try:
        wsdl = fetch(with_auth=True).decode()
    except urllib.error.HTTPError as error:
        print(f"[contract] FAIL: authenticated WSDL fetch got HTTP {error.code}", file=sys.stderr)
        return 1
    if not check_markers(wsdl, "served WSDL"):
        return 1

    if args.emit:
        target = allowed_out_path(out_path)
        target.write_text(wsdl, encoding="utf-8")
        digest = hashlib.sha256(wsdl.encode()).hexdigest()
        print(f"[contract] wrote {target} ({len(wsdl)} bytes, sha256 {digest[:16]}...)")
    else:
        print(f"[contract] ok: WSDL served with all operations ({len(wsdl)} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
