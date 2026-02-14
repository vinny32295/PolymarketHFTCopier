#!/usr/bin/env python3
"""
Smoke Test for Polymarket Copy Trader Bot
==========================================
Run this to verify your environment and connectivity step-by-step.
No trades will be executed — this is read-only.

Usage:
    python smoke_test.py                 # Run all checks
    python smoke_test.py --rpc URL       # Specify an RPC URL
    python smoke_test.py --address 0x... # Check a specific trader address
"""

import argparse
import json
import sys

PASS = "[PASS]"
FAIL = "[FAIL]"
SKIP = "[SKIP]"
INFO = "[INFO]"


def check_python_version():
    print(f"\n1. Python version: {sys.version}")
    if sys.version_info >= (3, 8):
        print(f"   {PASS} Python 3.8+ detected")
        return True
    else:
        print(f"   {FAIL} Python 3.8+ required")
        return False


def check_imports():
    print("\n2. Checking required packages...")
    all_ok = True

    # Core
    try:
        import tkinter
        print(f"   {PASS} tkinter")
    except ImportError:
        print(f"   {FAIL} tkinter — install python3-tk (apt) or ensure standard lib")
        all_ok = False

    # web3
    try:
        import web3
        print(f"   {PASS} web3 (version {web3.__version__})")
    except ImportError:
        print(f"   {FAIL} web3 — run: pip install web3")
        all_ok = False

    # requests
    try:
        import requests
        print(f"   {PASS} requests (version {requests.__version__})")
    except ImportError:
        print(f"   {FAIL} requests — run: pip install requests")
        all_ok = False

    # py-clob-client
    try:
        from py_clob_client.client import ClobClient
        print(f"   {PASS} py-clob-client")
    except ImportError:
        print(f"   {FAIL} py-clob-client — run: pip install py-clob-client")
        all_ok = False

    return all_ok


def check_config():
    print("\n3. Checking config.json...")
    try:
        with open("config.json", "r") as f:
            cfg = json.load(f)
        print(f"   {PASS} config.json loaded")
        print(f"   {INFO} RPC URL: {'set' if cfg.get('rpc_url') else 'empty'}")
        print(f"   {INFO} WS RPC URL: {'set' if cfg.get('ws_rpc_url') else 'empty'}")
        print(f"   {INFO} CLOB API enabled: {cfg.get('use_clob_api', True)}")
        print(f"   {INFO} CLOB API key: {'set' if cfg.get('clob_api_key') else 'empty'}")
        print(f"   {INFO} Copy percentage: {cfg.get('copy_percentage', 50)}%")
        print(f"   {INFO} Max trade: {cfg.get('max_trade_usdc', 100)} USDC")
        print(f"   {INFO} Dry run: {cfg.get('dry_run', False)}")
        n = len(cfg.get("watched_addresses", []))
        print(f"   {INFO} Watched addresses: {n}")
        return cfg
    except FileNotFoundError:
        print(f"   {INFO} config.json not found — will be created on first run")
        return {}
    except json.JSONDecodeError as e:
        print(f"   {FAIL} config.json is invalid JSON: {e}")
        return None


def check_rpc(rpc_url):
    print(f"\n4. Testing RPC connection: {rpc_url[:50]}...")
    try:
        from web3 import Web3
        from web3.middleware import ExtraDataToPOAMiddleware
        w3 = Web3(Web3.HTTPProvider(rpc_url))
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        if w3.is_connected():
            chain_id = w3.eth.chain_id
            block = w3.eth.block_number
            print(f"   {PASS} Connected — chain ID: {chain_id}, block: {block}")
            if chain_id != 137:
                print(f"   {FAIL} Expected Polygon mainnet (chain 137), got {chain_id}")
                return None
            return w3
        else:
            print(f"   {FAIL} Connection failed")
            return None
    except ImportError:
        print(f"   {SKIP} web3 not installed")
        return None
    except Exception as e:
        print(f"   {FAIL} {e}")
        return None


def check_gamma_api():
    print("\n5. Testing Gamma API (public, no auth)...")
    try:
        import requests
        resp = requests.get(
            "https://gamma-api.polymarket.com/markets",
            params={"limit": 1},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and len(data) > 0:
            market = data[0]
            print(f"   {PASS} Gamma API reachable")
            print(f"   {INFO} Sample market: {market.get('question', 'N/A')[:60]}...")
            return True
        else:
            print(f"   {FAIL} Unexpected response format")
            return False
    except ImportError:
        print(f"   {SKIP} requests not installed")
        return False
    except Exception as e:
        print(f"   {FAIL} {e}")
        return False


def check_clob_api():
    print("\n6. Testing CLOB API (public endpoints)...")
    try:
        import requests
        # /time is a simple public endpoint
        resp = requests.get("https://clob.polymarket.com/time", timeout=10)
        resp.raise_for_status()
        print(f"   {PASS} CLOB API reachable (server time: {resp.text.strip()})")
        return True
    except ImportError:
        print(f"   {SKIP} requests not installed")
        return False
    except Exception as e:
        print(f"   {FAIL} {e}")
        return False


def check_address_activity(address):
    print(f"\n7. Checking trade activity for {address[:10]}...{address[-6:]}...")
    try:
        import requests
        # Try Gamma API activity
        resp = requests.get(
            "https://gamma-api.polymarket.com/activity",
            params={"address": address.lower(), "limit": 5},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            trades = data if isinstance(data, list) else data.get("data", [])
            if trades:
                print(f"   {PASS} Found {len(trades)} recent activity record(s)")
                for t in trades[:3]:
                    print(f"   {INFO} {json.dumps(t, default=str)[:120]}...")
            else:
                print(f"   {INFO} No recent activity found via Gamma API")
        else:
            print(f"   {INFO} Gamma activity endpoint returned {resp.status_code}")

        # Also check CLOB trades endpoint (may require auth)
        resp2 = requests.get(
            "https://clob.polymarket.com/trades",
            params={"maker_address": address.lower(), "limit": 3},
            timeout=10,
        )
        if resp2.status_code == 200:
            print(f"   {PASS} CLOB /trades endpoint responded for this address")
        elif resp2.status_code == 401:
            print(f"   {INFO} CLOB /trades requires authentication (expected — use API creds)")
        else:
            print(f"   {INFO} CLOB /trades returned status {resp2.status_code}")

        return True
    except ImportError:
        print(f"   {SKIP} requests not installed")
        return False
    except Exception as e:
        print(f"   {FAIL} {e}")
        return False


def check_wallet_balance(w3, address):
    print(f"\n8. Checking on-chain balances for {address[:10]}...{address[-6:]}...")
    try:
        from web3 import Web3
        # MATIC balance
        balance = w3.eth.get_balance(Web3.to_checksum_address(address))
        matic = balance / 10**18
        print(f"   {INFO} MATIC balance: {matic:.4f}")

        # USDC balance
        usdc_abi = json.loads('[{"constant":true,"inputs":[{"name":"_owner","type":"address"}],'
                              '"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],'
                              '"type":"function"}]')
        usdc = w3.eth.contract(
            address=Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"),
            abi=usdc_abi,
        )
        usdc_raw = usdc.functions.balanceOf(Web3.to_checksum_address(address)).call()
        usdc_bal = usdc_raw / 10**6
        print(f"   {INFO} USDC balance: {usdc_bal:.2f}")
        return True
    except Exception as e:
        print(f"   {FAIL} {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Smoke test for Polymarket Copy Trader")
    parser.add_argument("--rpc", help="Polygon RPC URL to test")
    parser.add_argument("--address", help="Trader address to check activity for")
    args = parser.parse_args()

    print("=" * 60)
    print("  Polymarket Copy Trader — Smoke Test")
    print("=" * 60)

    results = []

    results.append(("Python version", check_python_version()))
    results.append(("Package imports", check_imports()))

    cfg = check_config()
    results.append(("Config file", cfg is not None))

    rpc_url = args.rpc or (cfg.get("rpc_url") if cfg else "")
    w3 = None
    if rpc_url:
        w3 = check_rpc(rpc_url)
        results.append(("RPC connection", w3 is not None))
    else:
        print(f"\n4. {SKIP} No RPC URL — pass --rpc URL to test")

    results.append(("Gamma API", check_gamma_api()))
    results.append(("CLOB API", check_clob_api()))

    address = args.address
    if not address and cfg and cfg.get("watched_addresses"):
        address = cfg["watched_addresses"][0]

    if address:
        results.append(("Address activity", check_address_activity(address)))
        if w3:
            results.append(("Wallet balance", check_wallet_balance(w3, address)))
    else:
        print(f"\n7. {SKIP} No address — pass --address 0x... to check activity")

    # Summary
    print("\n" + "=" * 60)
    print("  Summary")
    print("=" * 60)
    for name, ok in results:
        status = PASS if ok else FAIL
        print(f"  {status} {name}")

    failures = sum(1 for _, ok in results if not ok)
    if failures:
        print(f"\n  {failures} check(s) failed. Fix the above issues before running the bot.")
    else:
        print(f"\n  All checks passed! You're ready to run the bot.")
        print(f"\n  Recommended first steps:")
        print(f"    1. Enable dry-run mode in the GUI (or set dry_run: true in config.json)")
        print(f"    2. Add a known active trader address")
        print(f"    3. Start the bot and watch the logs")
        print(f"    4. Once you're happy, disable dry-run to go live")

    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
