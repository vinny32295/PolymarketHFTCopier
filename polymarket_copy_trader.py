#!/usr/bin/env python3
"""
Polymarket Copy Trader Bot
==========================
Monitors specified trader wallet addresses on Polymarket (Polygon network)
and automatically replicates their trades using the bot operator's wallet.

Features:
- Tkinter GUI for configuration and monitoring
- Real-time WebSocket-based transaction monitoring
- Polymarket CLOB API integration as primary data source
- On-chain fallback monitoring via Web3.py
- Configurable copy percentage and trade scaling
- Persistent JSON configuration
- Comprehensive logging (file, console, GUI)
"""

import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

# Tkinter is optional — only needed for the GUI, not for headless/server mode.
# Import is deferred so the bot can run on servers without Tk installed.
tk = None
ttk = None
scrolledtext = None
messagebox = None

def _import_tkinter():
    """Lazy-import tkinter modules. Called only when GUI mode is used."""
    global tk, ttk, scrolledtext, messagebox
    import tkinter as _tk
    from tkinter import ttk as _ttk, scrolledtext as _scrolledtext, messagebox as _messagebox
    tk = _tk
    ttk = _ttk
    scrolledtext = _scrolledtext
    messagebox = _messagebox

try:
    import requests
except ImportError:
    requests = None

try:
    from web3 import Web3
    from web3.middleware import ExtraDataToPOAMiddleware
except ImportError:
    Web3 = None

# Polymarket official Python CLOB client (for authenticated API access)
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import (
        ApiCreds, MarketOrderArgs, OpenOrderParams, OrderArgs, OrderType,
    )
    HAS_CLOB_SDK = True
except ImportError:
    ClobClient = None
    MarketOrderArgs = None
    OpenOrderParams = None
    HAS_CLOB_SDK = False

# ---------------------------------------------------------------------------
# Constants – Polymarket / Polygon addresses and ABIs
# ---------------------------------------------------------------------------

# Polymarket CTF Exchange (Conditional Token Framework) on Polygon
# Reference: https://docs.polymarket.com/
CTF_EXCHANGE_ADDRESS = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"

# Polymarket Neg Risk CTF Exchange
NEG_RISK_CTF_EXCHANGE_ADDRESS = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

# Neg Risk Adapter
NEG_RISK_ADAPTER_ADDRESS = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"

# USDC on Polygon (collateral token)
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

# Conditional Tokens contract (Gnosis)
CONDITIONAL_TOKENS_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

# Polymarket minimum order constraints
MIN_ORDER_SIZE_TOKENS = 5    # minimum outcome tokens per order
MIN_ORDER_NOTIONAL_USDC = 1.0  # minimum USDC notional for marketable orders

# Polymarket CLOB API base URL
CLOB_API_BASE = "https://clob.polymarket.com"

# Polymarket Gamma Markets API
GAMMA_API_BASE = "https://gamma-api.polymarket.com"

# Polymarket Data API (user activity, positions, trades)
DATA_API_BASE = "https://data-api.polymarket.com"

# Minimal ERC20 ABI for USDC balance/approval checks
ERC20_ABI = json.loads("""[
    {"constant":true,"inputs":[{"name":"_owner","type":"address"}],
     "name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],
     "type":"function"},
    {"constant":true,"inputs":[{"name":"_owner","type":"address"},
     {"name":"_spender","type":"address"}],
     "name":"allowance","outputs":[{"name":"","type":"uint256"}],
     "type":"function"},
    {"constant":false,"inputs":[{"name":"_spender","type":"address"},
     {"name":"_value","type":"uint256"}],
     "name":"approve","outputs":[{"name":"","type":"bool"}],
     "type":"function"}
]""")

# Minimal CTF Exchange ABI – covers the key order/trade functions we monitor
CTF_EXCHANGE_ABI = json.loads("""[
    {"inputs":[
        {"components":[
            {"name":"salt","type":"uint256"},
            {"name":"maker","type":"address"},
            {"name":"signer","type":"address"},
            {"name":"taker","type":"address"},
            {"name":"tokenId","type":"uint256"},
            {"name":"makerAmount","type":"uint256"},
            {"name":"takerAmount","type":"uint256"},
            {"name":"expiration","type":"uint256"},
            {"name":"nonce","type":"uint256"},
            {"name":"feeRateBps","type":"uint256"},
            {"name":"side","type":"uint8"},
            {"name":"signatureType","type":"uint8"}
        ],"name":"order","type":"tuple"},
        {"name":"signature","type":"bytes"}
    ],"name":"fillOrder","outputs":[],"stateMutability":"nonpayable","type":"function"},
    {"anonymous":false,"inputs":[
        {"indexed":true,"name":"orderHash","type":"bytes32"},
        {"indexed":true,"name":"maker","type":"address"},
        {"indexed":true,"name":"taker","type":"address"},
        {"indexed":false,"name":"makerAssetId","type":"uint256"},
        {"indexed":false,"name":"takerAssetId","type":"uint256"},
        {"indexed":false,"name":"makerAmountFilled","type":"uint256"},
        {"indexed":false,"name":"takerAmountFilled","type":"uint256"},
        {"indexed":false,"name":"fee","type":"uint256"}
    ],"name":"OrderFilled","type":"event"},
    {"anonymous":false,"inputs":[
        {"indexed":true,"name":"orderHash","type":"bytes32"},
        {"indexed":true,"name":"maker","type":"address"},
        {"indexed":true,"name":"taker","type":"address"},
        {"indexed":false,"name":"makerAssetId","type":"uint256"},
        {"indexed":false,"name":"takerAssetId","type":"uint256"},
        {"indexed":false,"name":"makerAmountFilled","type":"uint256"},
        {"indexed":false,"name":"takerAmountFilled","type":"uint256"}
    ],"name":"OrdersMatched","type":"event"}
]""")

# Default config file path
CONFIG_FILE = "config.json"
LOG_FILE = "bot.log"

# Default configuration template
DEFAULT_CONFIG = {
    "rpc_url": "",
    "ws_rpc_url": "",
    "private_key_file": ".private_key",
    "watched_addresses": [],
    "copy_percentage": 50,
    "max_trade_usdc": 100.0,
    "slippage_tolerance_bps": 100,
    "gas_multiplier": 1.2,
    "poll_interval_seconds": 15,
    "use_clob_api": True,
    "clob_api_key": "",
    "clob_api_secret": "",
    "clob_api_passphrase": "",
    "dry_run": False,
    "fixed_trade_usdc": 0.0,
    "order_ttl_seconds": 30,
}


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging(gui_handler=None):
    """Configure logging to file, console, and optionally a GUI handler."""
    logger = logging.getLogger("CopyTrader")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    # File handler
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # GUI handler (if provided)
    if gui_handler is not None:
        gui_handler.setLevel(logging.INFO)
        gui_handler.setFormatter(fmt)
        logger.addHandler(gui_handler)

    return logger


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------

def load_config(path=CONFIG_FILE):
    """Load configuration from JSON file, creating defaults if missing."""
    if os.path.exists(path):
        with open(path, "r") as f:
            cfg = json.load(f)
        # Merge any missing defaults
        for key, val in DEFAULT_CONFIG.items():
            cfg.setdefault(key, val)
        return cfg
    else:
        save_config(DEFAULT_CONFIG, path)
        return dict(DEFAULT_CONFIG)


def save_config(cfg, path=CONFIG_FILE):
    """Persist configuration to JSON file."""
    with open(path, "w") as f:
        json.dump(cfg, f, indent=2)


def load_private_key(cfg):
    """Load private key from the file specified in config."""
    pk_path = cfg.get("private_key_file", ".private_key")
    if not os.path.exists(pk_path):
        return ""
    with open(pk_path, "r") as f:
        key = f.read().strip()
    return key


def save_private_key(key, cfg):
    """Save private key to file (with restrictive permissions on Unix)."""
    pk_path = cfg.get("private_key_file", ".private_key")
    with open(pk_path, "w") as f:
        f.write(key.strip())
    try:
        os.chmod(pk_path, 0o600)
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Polymarket CLOB API client
# ---------------------------------------------------------------------------

class PolymarketCLOBClient:
    """Client for the Polymarket CLOB API with proper authentication.

    The CLOB API requires L2 authentication (API key + HMAC) for endpoints
    like GET /data/trades. API credentials are derived deterministically
    from the user's private key via the py-clob-client SDK.

    Public endpoints (markets, books, prices) need no auth.
    The Gamma API (gamma-api.polymarket.com) is fully public.

    Reference: https://docs.polymarket.com/
    """

    def __init__(self, cfg, private_key="", logger=None):
        self.cfg = cfg
        self.base_url = CLOB_API_BASE.rstrip("/")
        self.gamma_url = GAMMA_API_BASE.rstrip("/")
        self.data_url = DATA_API_BASE.rstrip("/")
        self.session = requests.Session() if requests else None
        self.logger = logger or logging.getLogger("CopyTrader")
        self._last_trade_ids = {}  # address -> set of seen trade IDs

        # Authenticated CLOB client (py-clob-client SDK)
        self.clob_sdk = None
        self._init_authenticated_client(cfg, private_key)

    def _init_authenticated_client(self, cfg, private_key):
        """Initialize the py-clob-client SDK with API credentials.

        If API credentials exist in config, use them directly.
        If only a private key is available, derive credentials automatically.
        """
        if not HAS_CLOB_SDK:
            self.logger.warning(
                "py-clob-client not installed – authenticated CLOB endpoints "
                "unavailable. Install with: pip install py-clob-client"
            )
            return

        api_key = cfg.get("clob_api_key", "")
        api_secret = cfg.get("clob_api_secret", "")
        api_passphrase = cfg.get("clob_api_passphrase", "")

        if api_key and api_secret and api_passphrase:
            # Use existing credentials
            try:
                self.clob_sdk = ClobClient(
                    host=CLOB_API_BASE,
                    chain_id=137,
                    key=private_key if private_key else None,
                    creds=ApiCreds(
                        api_key=api_key,
                        api_secret=api_secret,
                        api_passphrase=api_passphrase,
                    ),
                )
                self.logger.info("CLOB SDK initialized with stored API credentials")
                return
            except Exception as exc:
                self.logger.warning("Failed to init CLOB SDK with stored creds: %s", exc)

        if private_key:
            # Derive credentials from private key
            try:
                client = ClobClient(
                    host=CLOB_API_BASE,
                    chain_id=137,
                    key=private_key,
                )
                creds = client.create_or_derive_api_creds()
                self.logger.info("Derived CLOB API credentials from private key")

                # Store derived credentials in config for reuse
                cfg["clob_api_key"] = creds.api_key
                cfg["clob_api_secret"] = creds.api_secret
                cfg["clob_api_passphrase"] = creds.api_passphrase
                save_config(cfg)
                self.logger.info("Saved CLOB API credentials to config")

                # Reinitialize with full credentials
                self.clob_sdk = ClobClient(
                    host=CLOB_API_BASE,
                    chain_id=137,
                    key=private_key,
                    creds=creds,
                )
                self.logger.info("CLOB SDK fully initialized with derived credentials")
            except Exception as exc:
                self.logger.error("Failed to derive CLOB API credentials: %s", exc)

    def _get_public(self, url, params=None, retries=3, backoff=2):
        """HTTP GET for public (unauthenticated) endpoints with retries."""
        if self.session is None:
            self.logger.error("requests library not installed")
            return None
        for attempt in range(retries):
            try:
                resp = self.session.get(url, params=params, timeout=15)
                resp.raise_for_status()
                return resp.json()
            except Exception as exc:
                wait = backoff ** attempt
                self.logger.warning(
                    "API request failed (attempt %d/%d): %s – retrying in %ds",
                    attempt + 1, retries, exc, wait,
                )
                time.sleep(wait)
        return None

    def get_trades_for_address(self, address, limit=50):
        """Fetch recent trades for *address*.

        Uses the authenticated CLOB SDK if available, otherwise falls
        back to the public Gamma API activity endpoint.
        """
        # Primary: authenticated CLOB SDK get_trades
        if self.clob_sdk:
            try:
                data = self.clob_sdk.get_trades(
                    maker_address=address.lower(),
                    limit=limit,
                )
                if isinstance(data, list):
                    return data
                if hasattr(data, "__iter__"):
                    return list(data)
                return []
            except Exception as exc:
                self.logger.debug(
                    "CLOB SDK get_trades failed for %s: %s – trying Gamma fallback",
                    address[:10], exc,
                )

        # Fallback: Data API (public, no auth needed)
        url = f"{self.data_url}/activity"
        params = {"user": address.lower(), "limit": limit}
        data = self._get_public(url, params=params)
        if data is None:
            return []
        if isinstance(data, list):
            return data
        return data.get("data", data.get("history", []))

    def get_market_info(self, token_id=None, condition_id=None):
        """Fetch market details. Tries CLOB SDK then Gamma API."""
        if self.clob_sdk and token_id:
            try:
                return self.clob_sdk.get_market(token_id)
            except Exception:
                pass

        if condition_id:
            url = f"{self.gamma_url}/markets"
            params = {"condition_id": condition_id}
            data = self._get_public(url, params=params)
            if data and isinstance(data, list) and len(data) > 0:
                return data[0]
            return data
        return None

    def get_order_book(self, token_id):
        """Fetch the order book for a token (public endpoint)."""
        if self.clob_sdk:
            try:
                return self.clob_sdk.get_order_book(token_id)
            except Exception as exc:
                self.logger.debug("CLOB SDK get_order_book failed: %s", exc)

        url = f"{self.base_url}/book"
        params = {"token_id": token_id}
        return self._get_public(url, params=params)

    def get_last_trade_price(self, token_id):
        """Fetch last trade price (public endpoint, no auth needed)."""
        url = f"{self.base_url}/last-trade-price"
        params = {"token_id": token_id}
        data = self._get_public(url, params=params)
        if data:
            return float(data.get("price", 0))
        return None

    def place_order(self, token_id, side, size_usdc, price, neg_risk=False):
        """Place a Fill-or-Kill (FOK) market order on the Polymarket CLOB.

        FOK orders either fill immediately at the given price or are
        cancelled — no funds are left locked in open limit orders.

        Requires the py-clob-client SDK with valid API credentials.

        Args:
            token_id: The conditional token ID to trade.
            side: 'BUY' or 'SELL'.
            size_usdc: Trade size in USDC.
            price: Price per share (0.0–1.0 range).
            neg_risk: Whether the market uses the neg-risk framework.

        Returns:
            Order response dict, or None on failure.
        """
        if not self.clob_sdk:
            self.logger.error(
                "Cannot place order – CLOB SDK not initialized. "
                "Install py-clob-client and configure API credentials."
            )
            return None

        try:
            if price <= 0:
                self.logger.error("Invalid price %s – cannot place order", price)
                return None

            # Compute the minimum USDC needed to satisfy both Polymarket
            # constraints: ≥5 outcome tokens AND ≥$1 USDC notional.
            min_usdc_for_tokens = MIN_ORDER_SIZE_TOKENS * price
            effective_usdc = max(size_usdc, min_usdc_for_tokens,
                                MIN_ORDER_NOTIONAL_USDC)

            if effective_usdc > size_usdc:
                size_shares = round(effective_usdc / price, 2)
                self.logger.warning(
                    "Order size %.2f USDC (%.2f tokens) below minimums; "
                    "bumped to %.2f USDC (%.2f tokens) "
                    "(min tokens=%d, min notional=$%.0f)",
                    size_usdc, round(size_usdc / price, 2),
                    effective_usdc, size_shares,
                    MIN_ORDER_SIZE_TOKENS, MIN_ORDER_NOTIONAL_USDC,
                )
            actual_usdc = effective_usdc

            # Use FOK (Fill-or-Kill) market order — fills instantly or
            # gets cancelled, so no balance is locked in open orders.
            order_args = MarketOrderArgs(
                token_id=token_id,
                price=round(price, 2),
                amount=round(actual_usdc, 6),
                side=side.upper(),
                order_type=OrderType.FOK,
            )

            signed_order = self.clob_sdk.create_market_order(order_args)
            resp = self.clob_sdk.post_order(
                signed_order, orderType=OrderType.FOK,
            )

            self.logger.info(
                "FOK order placed: %s $%.2f @ %.4f for token %s — %s",
                side, actual_usdc, price, token_id[:16] + "...", resp,
            )
            return resp

        except Exception as exc:
            self.logger.error("Failed to place CLOB order: %s", exc, exc_info=True)
            return None

    # ------------------------------------------------------------------
    # Order monitoring helpers
    # ------------------------------------------------------------------

    def get_open_orders(self):
        """Fetch all open orders for the authenticated user.

        Returns a list of order dicts, or an empty list on failure.
        """
        if not self.clob_sdk:
            return []
        try:
            orders = self.clob_sdk.get_orders()
            if isinstance(orders, list):
                return orders
            if hasattr(orders, "__iter__"):
                return list(orders)
            return []
        except Exception as exc:
            self.logger.warning("Failed to fetch open orders: %s", exc)
            return []

    def get_order(self, order_id):
        """Fetch a single order by its ID.

        Returns the order dict, or None on failure.
        """
        if not self.clob_sdk:
            return None
        try:
            return self.clob_sdk.get_order(order_id)
        except Exception as exc:
            self.logger.warning("Failed to fetch order %s: %s", order_id, exc)
            return None

    def cancel_order(self, order_id):
        """Cancel a single open order.

        Returns the API response, or None on failure.
        """
        if not self.clob_sdk:
            return None
        try:
            resp = self.clob_sdk.cancel(order_id)
            self.logger.info("Cancelled order %s: %s", order_id, resp)
            return resp
        except Exception as exc:
            self.logger.warning("Failed to cancel order %s: %s", order_id, exc)
            return None

    def cancel_all_orders(self):
        """Cancel all open orders.

        Returns the API response, or None on failure.
        """
        if not self.clob_sdk:
            return None
        try:
            resp = self.clob_sdk.cancel_all()
            self.logger.info("Cancelled all orders: %s", resp)
            return resp
        except Exception as exc:
            self.logger.warning("Failed to cancel all orders: %s", exc)
            return None

    def get_new_trades(self, address):
        """Return only trades we have not seen before for *address*."""
        address = address.lower()
        all_trades = self.get_trades_for_address(address)
        seen = self._last_trade_ids.get(address, set())
        new_trades = []
        for trade in all_trades:
            tid = (
                trade.get("id")
                or trade.get("tradeID")
                or trade.get("hash")
                or trade.get("transactionHash", "")
            )
            if tid and tid not in seen:
                new_trades.append(trade)
                seen.add(tid)
        self._last_trade_ids[address] = seen
        return new_trades

    def seed_seen_trades(self, address):
        """Mark all current trades as 'seen' so we only act on future ones."""
        address = address.lower()
        all_trades = self.get_trades_for_address(address)
        seen = set()
        for trade in all_trades:
            tid = (
                trade.get("id")
                or trade.get("tradeID")
                or trade.get("hash")
                or trade.get("transactionHash", "")
            )
            if tid:
                seen.add(tid)
        self._last_trade_ids[address] = seen
        self.logger.info(
            "Seeded %d existing trades for %s", len(seen), address
        )


# ---------------------------------------------------------------------------
# On-chain monitor (Web3-based fallback)
# ---------------------------------------------------------------------------

class OnChainMonitor:
    """Monitors Polygon blocks for Polymarket trades by watched addresses.

    This class provides a fallback when the CLOB API is unavailable or as
    a complementary data source. It listens for OrderFilled / OrdersMatched
    events on the CTF Exchange contracts.
    """

    EXCHANGE_ADDRESSES = {
        CTF_EXCHANGE_ADDRESS.lower(),
        NEG_RISK_CTF_EXCHANGE_ADDRESS.lower(),
        NEG_RISK_ADAPTER_ADDRESS.lower(),
    }

    def __init__(self, w3, watched_addresses, logger=None):
        self.w3 = w3
        self.watched = {a.lower() for a in watched_addresses}
        self.logger = logger or logging.getLogger("CopyTrader")
        self._last_block = None

        # Build contract objects for event parsing
        self.ctf_exchange = self.w3.eth.contract(
            address=Web3.to_checksum_address(CTF_EXCHANGE_ADDRESS),
            abi=CTF_EXCHANGE_ABI,
        )

    def update_watched(self, addresses):
        self.watched = {a.lower() for a in addresses}

    def _scan_block(self, block_number):
        """Scan a single block for relevant Polymarket trades."""
        trades = []
        try:
            block = self.w3.eth.get_block(block_number, full_transactions=True)
        except Exception as exc:
            self.logger.error("Failed to fetch block %d: %s", block_number, exc)
            return trades

        for tx in block.get("transactions", []):
            sender = (tx.get("from") or "").lower()
            to_addr = (tx.get("to") or "").lower()

            # Check if the transaction involves a watched address
            # interacting with a Polymarket contract
            if sender not in self.watched:
                continue
            if to_addr not in self.EXCHANGE_ADDRESSES:
                continue

            self.logger.info(
                "On-chain: detected tx %s from watched address %s",
                tx["hash"].hex() if isinstance(tx["hash"], bytes) else tx["hash"],
                sender,
            )
            trades.append(self._parse_tx(tx))

        return [t for t in trades if t is not None]

    def _parse_tx(self, tx):
        """Attempt to decode a transaction's input data."""
        try:
            tx_hash = tx["hash"].hex() if isinstance(tx["hash"], bytes) else tx["hash"]
            input_data = tx.get("input", "0x")
            if isinstance(input_data, bytes):
                input_data = input_data.hex()
                if not input_data.startswith("0x"):
                    input_data = "0x" + input_data

            # Try to decode via the CTF Exchange ABI
            try:
                func, params = self.ctf_exchange.decode_function_input(input_data)
                return {
                    "source": "on-chain",
                    "tx_hash": tx_hash,
                    "from": tx["from"],
                    "function": func.fn_name,
                    "params": dict(params),
                    "block": tx.get("blockNumber"),
                }
            except Exception:
                # Could not decode – return raw info
                return {
                    "source": "on-chain",
                    "tx_hash": tx_hash,
                    "from": tx["from"],
                    "function": "unknown",
                    "raw_input": input_data[:200],
                    "block": tx.get("blockNumber"),
                }
        except Exception as exc:
            self.logger.debug("Failed to parse tx: %s", exc)
            return None

    def poll_new_blocks(self):
        """Check for new blocks since last poll and scan them."""
        try:
            current = self.w3.eth.block_number
        except Exception as exc:
            self.logger.error("Failed to get block number: %s", exc)
            return []

        if self._last_block is None:
            self._last_block = current
            self.logger.info("On-chain monitor starting at block %d", current)
            return []

        trades = []
        # Process at most 10 blocks per poll to avoid excessive RPC calls
        start = self._last_block + 1
        end = min(current, self._last_block + 10)
        for blk in range(start, end + 1):
            trades.extend(self._scan_block(blk))
        self._last_block = end
        return trades

    def get_event_logs(self, from_block, to_block):
        """Fetch OrderFilled event logs for watched maker addresses."""
        trades = []
        for addr in self.watched:
            try:
                logs = self.ctf_exchange.events.OrderFilled().get_logs(
                    fromBlock=from_block,
                    toBlock=to_block,
                    argument_filters={"maker": Web3.to_checksum_address(addr)},
                )
                for log in logs:
                    trades.append({
                        "source": "event-log",
                        "tx_hash": log.transactionHash.hex(),
                        "maker": log.args.get("maker", ""),
                        "taker": log.args.get("taker", ""),
                        "makerAssetId": str(log.args.get("makerAssetId", "")),
                        "takerAssetId": str(log.args.get("takerAssetId", "")),
                        "makerAmountFilled": str(log.args.get("makerAmountFilled", "")),
                        "takerAmountFilled": str(log.args.get("takerAmountFilled", "")),
                        "block": log.blockNumber,
                    })
            except Exception as exc:
                self.logger.debug("Event log query failed for %s: %s", addr, exc)
        return trades


# ---------------------------------------------------------------------------
# Trade Executor – replicates detected trades on-chain
# ---------------------------------------------------------------------------

class TradeExecutor:
    """Handles the execution of copy trades via the Polymarket CLOB API
    or directly on-chain.

    For the CLOB-based flow Polymarket requires signed orders submitted
    via their API. For simplicity this implementation focuses on the
    on-chain interaction path using the CTF Exchange contract, while also
    supporting CLOB order placement when possible.
    """

    def __init__(self, w3, private_key, cfg, clob_client=None, logger=None):
        self.w3 = w3
        self.private_key = private_key
        self.account = w3.eth.account.from_key(private_key)
        self.address = self.account.address
        self.cfg = cfg
        self.clob_client = clob_client  # PolymarketCLOBClient for order placement
        self.logger = logger or logging.getLogger("CopyTrader")
        self.copy_pct = Decimal(str(cfg.get("copy_percentage", 50))) / Decimal("100")
        self.max_trade = Decimal(str(cfg.get("max_trade_usdc", 100)))
        self.fixed_trade = Decimal(str(cfg.get("fixed_trade_usdc", 0)))
        self.gas_multiplier = cfg.get("gas_multiplier", 1.2)
        self.slippage_bps = cfg.get("slippage_tolerance_bps", 100)
        self._nonce_lock = threading.Lock()
        self._nonce = None

        # Balance cache – avoid hitting the RPC on every trade
        self._cached_balance = None
        self._balance_timestamp = 0

        # Open order tracking: order_id -> {placed_at, side, token_id, price, usdc}
        self._open_orders = {}
        self._order_ttl = cfg.get("order_ttl_seconds", 300)

        # Contract handles
        self.usdc = w3.eth.contract(
            address=Web3.to_checksum_address(USDC_ADDRESS), abi=ERC20_ABI
        )
        self.ctf_exchange = w3.eth.contract(
            address=Web3.to_checksum_address(CTF_EXCHANGE_ADDRESS),
            abi=CTF_EXCHANGE_ABI,
        )

    def _get_nonce(self):
        with self._nonce_lock:
            if self._nonce is None:
                self._nonce = self.w3.eth.get_transaction_count(self.address, "pending")
            else:
                self._nonce += 1
            return self._nonce

    def _reset_nonce(self):
        with self._nonce_lock:
            self._nonce = None

    def get_usdc_balance(self, max_age_seconds=300):
        """Return USDC balance as a Decimal (6 decimals).

        Results are cached for *max_age_seconds* (default 5 min) to reduce
        RPC load.  Pass ``max_age_seconds=0`` to force a fresh fetch.

        If the RPC call fails and a (possibly stale) cached value exists it
        is returned with a warning.  When no cached value is available the
        exception propagates.
        """
        now = time.time()
        if (
            self._cached_balance is not None
            and max_age_seconds > 0
            and now - self._balance_timestamp < max_age_seconds
        ):
            self.logger.debug("Using cached USDC balance")
            return self._cached_balance

        try:
            raw = self.usdc.functions.balanceOf(self.address).call()
            self._cached_balance = Decimal(raw) / Decimal("1000000")
            self._balance_timestamp = now
        except Exception as exc:
            if self._cached_balance is not None:
                self.logger.warning(
                    "USDC balance RPC failed (%s); using stale cached value", exc
                )
                return self._cached_balance
            raise
        return self._cached_balance

    def invalidate_balance_cache(self):
        """Clear the cached balance so the next call fetches fresh data."""
        self._cached_balance = None
        self._balance_timestamp = 0

    def get_matic_balance(self):
        """Return native MATIC/POL balance in ether."""
        raw = self.w3.eth.get_balance(self.address)
        return Decimal(raw) / Decimal("1000000000000000000")

    # ------------------------------------------------------------------
    # Open order monitoring
    # ------------------------------------------------------------------

    def track_order(self, order_id, side, token_id, price, usdc_amount):
        """Record an order so we can monitor its fill status later."""
        self._open_orders[order_id] = {
            "placed_at": time.time(),
            "side": side,
            "token_id": token_id,
            "price": price,
            "usdc": usdc_amount,
        }

    def monitor_open_orders(self):
        """Check tracked orders, log fills, and cancel stale ones.

        Called once per poll cycle from the main bot loop.  Returns a
        summary dict with counts: filled, cancelled, still_open.
        """
        if not self.clob_client or not self.clob_client.clob_sdk:
            return None
        if not self._open_orders:
            return None

        now = time.time()
        filled = 0
        cancelled = 0
        still_open = 0
        to_remove = []

        for order_id, info in list(self._open_orders.items()):
            order = self.clob_client.get_order(order_id)
            if order is None:
                # API error — skip, try again next cycle
                still_open += 1
                continue

            status = (order.get("status") or "").upper()

            if status in ("MATCHED", "FILLED"):
                self.logger.info(
                    "Order %s FILLED: %s $%.2f @ %.4f (token %s)",
                    order_id[:12] + "...",
                    info["side"],
                    info["usdc"],
                    info["price"],
                    info["token_id"][:16] + "...",
                )
                self.invalidate_balance_cache()
                filled += 1
                to_remove.append(order_id)

            elif status in ("CANCELLED", "EXPIRED"):
                self.logger.info(
                    "Order %s %s (was %s $%.2f @ %.4f)",
                    order_id[:12] + "...",
                    status,
                    info["side"],
                    info["usdc"],
                    info["price"],
                )
                to_remove.append(order_id)

            elif now - info["placed_at"] > self._order_ttl:
                # Stale order — cancel it
                self.logger.warning(
                    "Order %s stale (%.0fs old, TTL=%ds) — cancelling: "
                    "%s $%.2f @ %.4f",
                    order_id[:12] + "...",
                    now - info["placed_at"],
                    self._order_ttl,
                    info["side"],
                    info["usdc"],
                    info["price"],
                )
                self.clob_client.cancel_order(order_id)
                self.invalidate_balance_cache()
                cancelled += 1
                to_remove.append(order_id)
            else:
                age = int(now - info["placed_at"])
                self.logger.debug(
                    "Order %s still LIVE (%ds old): %s $%.2f @ %.4f",
                    order_id[:12] + "...",
                    age,
                    info["side"],
                    info["usdc"],
                    info["price"],
                )
                still_open += 1

        for oid in to_remove:
            self._open_orders.pop(oid, None)

        if filled or cancelled:
            self.logger.info(
                "Order monitor: %d filled, %d cancelled, %d still open",
                filled, cancelled, still_open,
            )

        return {"filled": filled, "cancelled": cancelled, "still_open": still_open}

    def compute_copy_amount(self, original_usdc_amount):
        """Return the copy amount in USDC.

        If fixed_trade_usdc > 0, use that flat amount for every trade.
        Otherwise scale the original by copy_percentage, capped to max_trade.
        When the balance is available, also capped to 95% of it.
        """
        if self.fixed_trade > 0:
            base = self.fixed_trade
        else:
            base = Decimal(str(original_usdc_amount)) * self.copy_pct
        amount = min(base, self.max_trade)
        try:
            balance = self.get_usdc_balance()
            amount = min(amount, balance * Decimal("0.95"))
        except Exception as exc:
            self.logger.warning(
                "Could not fetch USDC balance for cap check (%s); "
                "proceeding with max_trade cap only",
                exc,
            )
        return max(amount, Decimal("0"))

    def ensure_usdc_approval(self, spender, amount_raw):
        """Approve the spender for at least *amount_raw* USDC if needed."""
        spender = Web3.to_checksum_address(spender)
        current = self.usdc.functions.allowance(self.address, spender).call()
        if current >= amount_raw:
            return None  # Already approved

        self.logger.info("Approving USDC spend for %s ...", spender)
        tx = self.usdc.functions.approve(
            spender, 2**256 - 1  # max approval (common pattern)
        ).build_transaction(self._base_tx_params())
        return self._sign_and_send(tx)

    def _base_tx_params(self):
        """Common transaction parameters with EIP-1559 fees."""
        latest = self.w3.eth.get_block("latest")
        base_fee = latest.get("baseFeePerGas", 30_000_000_000)
        max_priority = self.w3.eth.max_priority_fee
        max_fee = int((base_fee * 2 + max_priority) * self.gas_multiplier)
        return {
            "from": self.address,
            "nonce": self._get_nonce(),
            "maxFeePerGas": max_fee,
            "maxPriorityFeePerGas": max_priority,
            "chainId": 137,  # Polygon mainnet
        }

    def _sign_and_send(self, tx, retries=3):
        """Sign and broadcast a transaction with retries."""
        for attempt in range(retries):
            try:
                # Estimate gas
                gas_est = self.w3.eth.estimate_gas(tx)
                tx["gas"] = int(gas_est * self.gas_multiplier)
                signed = self.w3.eth.account.sign_transaction(tx, self.private_key)
                tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
                self.logger.info("Tx sent: %s", tx_hash.hex())
                receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
                if receipt.status == 1:
                    self.logger.info("Tx confirmed: %s (block %d)", tx_hash.hex(), receipt.blockNumber)
                else:
                    self.logger.error("Tx reverted: %s", tx_hash.hex())
                    self._reset_nonce()
                return receipt
            except Exception as exc:
                self.logger.error(
                    "Tx send failed (attempt %d/%d): %s", attempt + 1, retries, exc
                )
                self._reset_nonce()
                time.sleep(2 ** attempt)
        return None

    def execute_copy_trade(self, trade_info):
        """Attempt to replicate a detected trade.

        *trade_info* is a dict with at least:
          - asset_id or tokenId: the conditional token ID
          - side: 'BUY' or 'SELL'
          - size or amount: the USDC notional
          - price: the price per share (0-1 range)
        """
        try:
            self.logger.debug("Raw trade_info: %s", json.dumps(trade_info, default=str)[:500])
            side = str(trade_info.get("side", "BUY")).upper()
            # CLOB API: prefer usdcSize (actual USDC spent), fall back to size/amount
            clob_size = (
                trade_info.get("usdcSize")
                or trade_info.get("size")
                or trade_info.get("amount")
            )
            if clob_size:
                original_usdc = Decimal(str(clob_size))
            else:
                raw = Decimal(str(trade_info.get("makerAmountFilled", "0")))
                original_usdc = raw / Decimal("1000000")
            self.logger.debug("Trade size parsing: raw=%s -> original_usdc=%s", clob_size or trade_info.get("makerAmountFilled"), original_usdc)
            if original_usdc <= 0:
                self.logger.warning("Skipping trade with zero/negative size")
                return None

            copy_amount = self.compute_copy_amount(original_usdc)
            if copy_amount <= Decimal("0.01"):
                self.logger.info("Computed copy amount too small (%.4f USDC), skipping", copy_amount)
                return None

            token_id = str(
                trade_info.get("asset")
                or trade_info.get("asset_id")
                or trade_info.get("tokenId")
                or trade_info.get("makerAssetId", "")
            )
            price = trade_info.get("price", 0.5)

            # --- Enforce Polymarket order minimums early ---
            # The CLOB API requires both ≥5 tokens AND ≥$1 USDC notional.
            # Compute the minimum viable USDC now so the balance cap and
            # allowance approval use the real order size (not the pre-bump
            # amount that place_order would silently inflate).
            price_f = float(price) if not isinstance(price, float) else price
            if price_f > 0:
                min_viable_usdc = max(
                    MIN_ORDER_SIZE_TOKENS * price_f,
                    MIN_ORDER_NOTIONAL_USDC,
                )
                if float(copy_amount) < min_viable_usdc:
                    # Check balance before committing to the bumped amount
                    try:
                        balance = float(self.get_usdc_balance())
                        if min_viable_usdc > balance * 0.95:
                            self.logger.info(
                                "Min viable order $%.2f exceeds 95%% of balance $%.2f, "
                                "skipping trade",
                                min_viable_usdc, balance,
                            )
                            return None
                    except Exception:
                        pass  # let it proceed; place_order will fail gracefully
                    self.logger.info(
                        "Bumping copy amount from $%.2f to min viable $%.2f "
                        "(price=%.4f, min_tokens=%d, min_notional=$%.0f)",
                        copy_amount, min_viable_usdc,
                        price_f, MIN_ORDER_SIZE_TOKENS, MIN_ORDER_NOTIONAL_USDC,
                    )
                    copy_amount = Decimal(str(round(min_viable_usdc, 6)))

            self.logger.info(
                "COPY TRADE: %s %.2f USDC of token %s (original: %.2f USDC, price: %s)",
                side, copy_amount, token_id[:16] + "..." if len(token_id) > 16 else token_id,
                original_usdc, price,
            )

            # --- DRY RUN: log but do not execute ---
            if self.cfg.get("dry_run", False):
                self.logger.info(
                    "[DRY RUN] Would %s %.2f USDC of token %s @ price %s",
                    side, copy_amount, token_id[:20], price,
                )
                return {
                    "status": "dry_run",
                    "side": side,
                    "amount_usdc": float(copy_amount),
                    "token_id": token_id,
                    "price": price,
                }

            # Ensure USDC approval on the CTF Exchange
            raw_amount = int(copy_amount * Decimal("1000000"))
            self.ensure_usdc_approval(CTF_EXCHANGE_ADDRESS, raw_amount)

            # Place order via the CLOB API (requires py-clob-client + API creds)
            if self.clob_client and self.clob_client.clob_sdk:
                # Apply slippage to price
                slippage_mult = Decimal(str(self.slippage_bps)) / Decimal("10000")
                if side == "BUY":
                    adjusted_price = float(Decimal(str(price)) * (Decimal("1") + slippage_mult))
                    adjusted_price = min(adjusted_price, 0.99)
                else:
                    adjusted_price = float(Decimal(str(price)) * (Decimal("1") - slippage_mult))
                    adjusted_price = max(adjusted_price, 0.01)

                result = self.clob_client.place_order(
                    token_id=token_id,
                    side=side,
                    size_usdc=float(copy_amount),
                    price=adjusted_price,
                )
                if result:
                    self.logger.info("Order submitted to CLOB: %s", result)
                    self.invalidate_balance_cache()

                    # Track the order for fill monitoring
                    order_id = None
                    if isinstance(result, dict):
                        order_id = (
                            result.get("orderID")
                            or result.get("id")
                            or result.get("order_id")
                        )
                    if order_id:
                        self.track_order(
                            order_id, side, token_id,
                            adjusted_price, float(copy_amount),
                        )

                    return {
                        "status": "submitted",
                        "side": side,
                        "amount_usdc": float(copy_amount),
                        "token_id": token_id,
                        "price": adjusted_price,
                        "clob_response": result,
                    }
                else:
                    self.logger.warning("CLOB order placement returned no result")
            else:
                self.logger.warning(
                    "CLOB SDK not available – trade detected but not executed. "
                    "Install py-clob-client and configure API credentials to enable."
                )

            return {
                "status": "detected_only",
                "side": side,
                "amount_usdc": float(copy_amount),
                "token_id": token_id,
                "price": price,
            }

        except Exception as exc:
            self.logger.error("Failed to execute copy trade: %s", exc, exc_info=True)
            return None


# ---------------------------------------------------------------------------
# Core bot engine (runs in a background thread)
# ---------------------------------------------------------------------------

class CopyTraderBot:
    """Orchestrates the monitoring loop and trade execution."""

    def __init__(self, cfg, logger):
        self.cfg = cfg
        self.logger = logger
        self.running = False
        self._thread = None

        # Web3 connection (lazy init)
        self.w3 = None
        self.on_chain_monitor = None
        self.executor = None
        self.clob_client = None

    def _init_web3(self):
        """Initialize Web3 provider from config."""
        if Web3 is None:
            self.logger.error("web3 library not installed – on-chain features disabled")
            return False

        rpc_url = self.cfg.get("rpc_url", "")
        ws_url = self.cfg.get("ws_rpc_url", "")

        if ws_url:
            try:
                from web3 import Web3 as W3
                # web3.py 6.x+: WebsocketProvider was removed; try WebSocketProvider
                _WsProvider = getattr(W3, "WebSocketProvider", None) or getattr(W3, "WebsocketProvider", None)
                if _WsProvider is None:
                    from web3.providers import WebSocketProvider as _WsProvider
                self.w3 = W3(_WsProvider(ws_url))
                self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
                if self.w3.is_connected():
                    self.logger.info("Connected via WebSocket: %s", ws_url[:40] + "...")
                    return True
            except Exception as exc:
                self.logger.warning("WebSocket connection failed: %s", exc)

        if rpc_url:
            try:
                self.w3 = Web3(Web3.HTTPProvider(rpc_url))
                self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
                if self.w3.is_connected():
                    self.logger.info("Connected via HTTP RPC: %s", rpc_url[:40] + "...")
                    return True
            except Exception as exc:
                self.logger.warning("HTTP RPC connection failed: %s", exc)

        self.logger.error("No valid RPC connection could be established")
        return False

    def _init_executor(self):
        pk = load_private_key(self.cfg)
        if not pk:
            self.logger.error("No private key configured – cannot execute trades")
            return False
        try:
            self.executor = TradeExecutor(
                self.w3, pk, self.cfg,
                clob_client=self.clob_client,
                logger=self.logger,
            )
            self.logger.info("Executor initialized – wallet: %s", self.executor.address)
        except Exception as exc:
            self.logger.error("Failed to initialize executor: %s", exc)
            return False
        # Balance check is informational — don't let it block the executor
        try:
            balance = self.executor.get_usdc_balance()
            matic = self.executor.get_matic_balance()
            self.logger.info("Balances – USDC: %.2f | MATIC: %.4f", balance, matic)
        except Exception as exc:
            self.logger.warning(
                "Could not fetch startup balances (will retry later): %s", exc
            )
        return True

    def _init_clob(self):
        if not self.cfg.get("use_clob_api", True):
            return
        if requests is None:
            self.logger.warning("requests library not installed – CLOB API disabled")
            return
        pk = load_private_key(self.cfg)
        self.clob_client = PolymarketCLOBClient(
            cfg=self.cfg, private_key=pk, logger=self.logger,
        )
        # Cancel any stale open orders from a previous session so balance
        # isn't locked up.  With FOK orders this shouldn't happen going
        # forward, but cleans up legacy GTC orders.
        try:
            resp = self.clob_client.cancel_all_orders()
            if resp:
                self.logger.info("Startup: cancelled all open orders: %s", resp)
        except Exception as exc:
            self.logger.warning("Startup: cancel-all failed (non-fatal): %s", exc)

        # Seed existing trades so we don't copy old history
        watched = self.cfg.get("watched_addresses", [])
        for i, addr in enumerate(watched):
            self.clob_client.seed_seen_trades(addr)
            if i < len(watched) - 1:
                time.sleep(0.5)  # throttle API calls between addresses

    def start(self):
        if self.running:
            self.logger.warning("Bot is already running")
            return
        self.running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self.logger.info("Bot started")

    def stop(self):
        self.running = False
        self.logger.info("Bot stop requested")

    def _run_loop(self):
        """Main monitoring loop."""
        # Initialise connections – CLOB first so executor can use it
        self._init_clob()

        web3_ok = self._init_web3()
        if web3_ok:
            self._init_executor()
            watched = self.cfg.get("watched_addresses", [])
            self.on_chain_monitor = OnChainMonitor(self.w3, watched, self.logger)

        poll_interval = self.cfg.get("poll_interval_seconds", 15)
        self.logger.info(
            "Monitoring %d address(es), poll interval %ds",
            len(self.cfg.get("watched_addresses", [])),
            poll_interval,
        )

        while self.running:
            try:
                new_trades = []

                # --- CLOB API polling (primary) ---
                if self.clob_client:
                    for addr in self.cfg.get("watched_addresses", []):
                        api_trades = self.clob_client.get_new_trades(addr)
                        if api_trades:
                            self.logger.info(
                                "CLOB API: %d new trade(s) from %s",
                                len(api_trades), addr[:10] + "...",
                            )
                            new_trades.extend(api_trades)

                # --- On-chain polling (fallback) ---
                if self.on_chain_monitor and web3_ok:
                    self.on_chain_monitor.update_watched(
                        self.cfg.get("watched_addresses", [])
                    )
                    chain_trades = self.on_chain_monitor.poll_new_blocks()
                    if chain_trades:
                        self.logger.info(
                            "On-chain: %d trade(s) detected", len(chain_trades)
                        )
                        new_trades.extend(chain_trades)

                # --- Execute copies ---
                for trade in new_trades:
                    if self.executor:
                        self.executor.execute_copy_trade(trade)
                    else:
                        self.logger.warning(
                            "Trade detected but executor not ready: %s",
                            json.dumps(trade, default=str)[:200],
                        )

                # --- Monitor open orders (fill status / stale cancellation) ---
                if self.executor:
                    try:
                        self.executor.monitor_open_orders()
                    except Exception as mon_exc:
                        self.logger.debug(
                            "Order monitor error: %s", mon_exc,
                        )

            except Exception as exc:
                self.logger.error("Error in monitoring loop: %s", exc, exc_info=True)

            # Sleep with early-exit check
            for _ in range(int(poll_interval)):
                if not self.running:
                    break
                time.sleep(1)

        self.logger.info("Bot stopped")


# ---------------------------------------------------------------------------
# GUI Text Handler for logging
# ---------------------------------------------------------------------------

class TextHandler(logging.Handler):
    """Logging handler that writes to a Tkinter ScrolledText widget."""

    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record) + "\n"
        # Schedule GUI update on the main thread
        self.text_widget.after(0, self._append, msg)

    def _append(self, msg):
        self.text_widget.configure(state="normal")
        self.text_widget.insert(tk.END, msg)
        self.text_widget.see(tk.END)
        self.text_widget.configure(state="disabled")


# ---------------------------------------------------------------------------
# Tkinter GUI Application
# ---------------------------------------------------------------------------

class CopyTraderGUI:
    """Main application window."""

    def __init__(self):
        _import_tkinter()

        self.cfg = load_config()
        self.bot = None
        self.logger = None

        self.root = tk.Tk()
        self.root.title("Polymarket Copy Trader Bot")
        self.root.geometry("900x720")
        self.root.minsize(700, 550)

        self._build_ui()

        # Set up logging with GUI handler
        gui_handler = TextHandler(self.log_area)
        self.logger = setup_logging(gui_handler)
        self.logger.info("Application started")
        self._load_fields_from_config()

    # ---- UI construction ----

    def _build_ui(self):
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)

        # Tab 1: Configuration
        config_frame = ttk.Frame(notebook, padding=10)
        notebook.add(config_frame, text="Configuration")
        self._build_config_tab(config_frame)

        # Tab 2: Watched Addresses
        addr_frame = ttk.Frame(notebook, padding=10)
        notebook.add(addr_frame, text="Watched Addresses")
        self._build_address_tab(addr_frame)

        # Tab 3: Log / Status
        log_frame = ttk.Frame(notebook, padding=10)
        notebook.add(log_frame, text="Log")
        self._build_log_tab(log_frame)

        # Bottom control bar
        ctrl = ttk.Frame(self.root, padding=5)
        ctrl.pack(fill=tk.X)
        self.start_btn = ttk.Button(ctrl, text="Start Bot", command=self._start_bot)
        self.start_btn.pack(side=tk.LEFT, padx=5)
        self.stop_btn = ttk.Button(
            ctrl, text="Stop Bot", command=self._stop_bot, state=tk.DISABLED
        )
        self.stop_btn.pack(side=tk.LEFT, padx=5)
        self.save_btn = ttk.Button(ctrl, text="Save Config", command=self._save_config)
        self.save_btn.pack(side=tk.RIGHT, padx=5)
        self.status_var = tk.StringVar(value="Status: Idle")
        ttk.Label(ctrl, textvariable=self.status_var).pack(side=tk.RIGHT, padx=10)

    def _build_config_tab(self, parent):
        # RPC URL
        row = 0
        ttk.Label(parent, text="HTTP RPC URL:").grid(row=row, column=0, sticky=tk.W, pady=3)
        self.rpc_entry = ttk.Entry(parent, width=70)
        self.rpc_entry.grid(row=row, column=1, columnspan=2, sticky=tk.EW, pady=3)

        row += 1
        ttk.Label(parent, text="WebSocket RPC URL:").grid(row=row, column=0, sticky=tk.W, pady=3)
        self.ws_rpc_entry = ttk.Entry(parent, width=70)
        self.ws_rpc_entry.grid(row=row, column=1, columnspan=2, sticky=tk.EW, pady=3)

        # Private key
        row += 1
        ttk.Label(parent, text="Private Key:").grid(row=row, column=0, sticky=tk.W, pady=3)
        self.pk_entry = ttk.Entry(parent, width=70, show="*")
        self.pk_entry.grid(row=row, column=1, sticky=tk.EW, pady=3)
        ttk.Button(parent, text="Show/Hide", command=self._toggle_pk).grid(
            row=row, column=2, padx=5
        )

        row += 1
        ttk.Label(
            parent,
            text="⚠ WARNING: Your private key controls your funds. "
                 "Never share it. It is stored locally in a restricted file.",
            foreground="red",
            wraplength=600,
        ).grid(row=row, column=0, columnspan=3, sticky=tk.W, pady=3)

        # Copy percentage
        row += 1
        ttk.Label(parent, text="Copy Percentage (%):").grid(row=row, column=0, sticky=tk.W, pady=3)
        self.copy_pct_var = tk.IntVar(value=50)
        pct_frame = ttk.Frame(parent)
        pct_frame.grid(row=row, column=1, sticky=tk.W, pady=3)
        self.copy_pct_scale = ttk.Scale(
            pct_frame, from_=1, to=100, variable=self.copy_pct_var, orient=tk.HORIZONTAL, length=250,
        )
        self.copy_pct_scale.pack(side=tk.LEFT)
        self.copy_pct_label = ttk.Label(pct_frame, textvariable=self.copy_pct_var, width=4)
        self.copy_pct_label.pack(side=tk.LEFT, padx=5)

        # Max trade size
        row += 1
        ttk.Label(parent, text="Max Trade (USDC):").grid(row=row, column=0, sticky=tk.W, pady=3)
        self.max_trade_entry = ttk.Entry(parent, width=20)
        self.max_trade_entry.grid(row=row, column=1, sticky=tk.W, pady=3)

        # Fixed trade size
        row += 1
        ttk.Label(parent, text="Fixed Trade (USDC):").grid(row=row, column=0, sticky=tk.W, pady=3)
        fixed_frame = ttk.Frame(parent)
        fixed_frame.grid(row=row, column=1, sticky=tk.W, pady=3)
        self.fixed_trade_entry = ttk.Entry(fixed_frame, width=10)
        self.fixed_trade_entry.pack(side=tk.LEFT)
        ttk.Label(fixed_frame, text="(0 = use % scaling)").pack(side=tk.LEFT, padx=5)

        # Slippage
        row += 1
        ttk.Label(parent, text="Slippage Tolerance (bps):").grid(row=row, column=0, sticky=tk.W, pady=3)
        self.slippage_entry = ttk.Entry(parent, width=20)
        self.slippage_entry.grid(row=row, column=1, sticky=tk.W, pady=3)

        # Poll interval
        row += 1
        ttk.Label(parent, text="Poll Interval (seconds):").grid(row=row, column=0, sticky=tk.W, pady=3)
        self.poll_entry = ttk.Entry(parent, width=20)
        self.poll_entry.grid(row=row, column=1, sticky=tk.W, pady=3)

        # Use CLOB API checkbox
        row += 1
        self.use_clob_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            parent, text="Use Polymarket CLOB API (recommended)", variable=self.use_clob_var
        ).grid(row=row, column=0, columnspan=2, sticky=tk.W, pady=3)

        # Dry-run mode checkbox
        row += 1
        self.dry_run_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            parent, text="Dry Run Mode (detect trades but do NOT execute)",
            variable=self.dry_run_var
        ).grid(row=row, column=0, columnspan=2, sticky=tk.W, pady=3)

        # --- CLOB API Credentials ---
        row += 1
        ttk.Separator(parent, orient=tk.HORIZONTAL).grid(
            row=row, column=0, columnspan=3, sticky=tk.EW, pady=8
        )

        row += 1
        ttk.Label(
            parent, text="CLOB API Credentials (auto-derived from private key, or enter manually):",
            font=("TkDefaultFont", 9, "bold"),
        ).grid(row=row, column=0, columnspan=3, sticky=tk.W, pady=3)

        row += 1
        ttk.Label(parent, text="API Key:").grid(row=row, column=0, sticky=tk.W, pady=2)
        self.api_key_entry = ttk.Entry(parent, width=70)
        self.api_key_entry.grid(row=row, column=1, columnspan=2, sticky=tk.EW, pady=2)

        row += 1
        ttk.Label(parent, text="API Secret:").grid(row=row, column=0, sticky=tk.W, pady=2)
        self.api_secret_entry = ttk.Entry(parent, width=70, show="*")
        self.api_secret_entry.grid(row=row, column=1, columnspan=2, sticky=tk.EW, pady=2)

        row += 1
        ttk.Label(parent, text="API Passphrase:").grid(row=row, column=0, sticky=tk.W, pady=2)
        self.api_passphrase_entry = ttk.Entry(parent, width=70, show="*")
        self.api_passphrase_entry.grid(row=row, column=1, columnspan=2, sticky=tk.EW, pady=2)

        row += 1
        self.derive_btn = ttk.Button(
            parent, text="Derive Credentials from Private Key",
            command=self._derive_api_creds,
        )
        self.derive_btn.grid(row=row, column=1, sticky=tk.W, pady=3)

        row += 1
        ttk.Label(
            parent,
            text="Credentials are derived deterministically from your private key. "
                 "They will be auto-generated on first bot start if left blank.",
            foreground="gray",
            wraplength=600,
        ).grid(row=row, column=0, columnspan=3, sticky=tk.W, pady=2)

        parent.columnconfigure(1, weight=1)

    def _build_address_tab(self, parent):
        ttk.Label(parent, text="Watched Trader Addresses:").pack(anchor=tk.W)

        list_frame = ttk.Frame(parent)
        list_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        self.addr_listbox = tk.Listbox(list_frame, height=10, font=("Courier", 10))
        scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.addr_listbox.yview)
        self.addr_listbox.configure(yscrollcommand=scrollbar.set)
        self.addr_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        entry_frame = ttk.Frame(parent)
        entry_frame.pack(fill=tk.X, pady=5)
        ttk.Label(entry_frame, text="Address:").pack(side=tk.LEFT)
        self.new_addr_entry = ttk.Entry(entry_frame, width=50, font=("Courier", 10))
        self.new_addr_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)

        btn_frame = ttk.Frame(parent)
        btn_frame.pack(fill=tk.X)
        ttk.Button(btn_frame, text="Add", command=self._add_address).pack(side=tk.LEFT, padx=3)
        ttk.Button(btn_frame, text="Remove Selected", command=self._remove_address).pack(
            side=tk.LEFT, padx=3
        )
        ttk.Button(btn_frame, text="Clear All", command=self._clear_addresses).pack(
            side=tk.LEFT, padx=3
        )

    def _build_log_tab(self, parent):
        self.log_area = scrolledtext.ScrolledText(
            parent, state="disabled", wrap=tk.WORD, font=("Courier", 9), height=25
        )
        self.log_area.pack(fill=tk.BOTH, expand=True)
        ttk.Button(parent, text="Clear Log", command=self._clear_log).pack(anchor=tk.E, pady=3)

    # ---- Field load/save ----

    def _load_fields_from_config(self):
        self.rpc_entry.insert(0, self.cfg.get("rpc_url", ""))
        self.ws_rpc_entry.insert(0, self.cfg.get("ws_rpc_url", ""))
        self.copy_pct_var.set(self.cfg.get("copy_percentage", 50))
        self.max_trade_entry.insert(0, str(self.cfg.get("max_trade_usdc", 100)))
        self.fixed_trade_entry.insert(0, str(self.cfg.get("fixed_trade_usdc", 0)))
        self.slippage_entry.insert(0, str(self.cfg.get("slippage_tolerance_bps", 100)))
        self.poll_entry.insert(0, str(self.cfg.get("poll_interval_seconds", 15)))
        self.use_clob_var.set(self.cfg.get("use_clob_api", True))
        self.dry_run_var.set(self.cfg.get("dry_run", False))
        # API credentials
        self.api_key_entry.insert(0, self.cfg.get("clob_api_key", ""))
        self.api_secret_entry.insert(0, self.cfg.get("clob_api_secret", ""))
        self.api_passphrase_entry.insert(0, self.cfg.get("clob_api_passphrase", ""))
        pk = load_private_key(self.cfg)
        if pk:
            self.pk_entry.insert(0, pk)
        for addr in self.cfg.get("watched_addresses", []):
            self.addr_listbox.insert(tk.END, addr)

    def _read_fields_to_config(self):
        self.cfg["rpc_url"] = self.rpc_entry.get().strip()
        self.cfg["ws_rpc_url"] = self.ws_rpc_entry.get().strip()
        self.cfg["copy_percentage"] = self.copy_pct_var.get()
        self.cfg["use_clob_api"] = self.use_clob_var.get()
        self.cfg["dry_run"] = self.dry_run_var.get()
        # API credentials
        self.cfg["clob_api_key"] = self.api_key_entry.get().strip()
        self.cfg["clob_api_secret"] = self.api_secret_entry.get().strip()
        self.cfg["clob_api_passphrase"] = self.api_passphrase_entry.get().strip()
        try:
            self.cfg["max_trade_usdc"] = float(self.max_trade_entry.get().strip())
        except ValueError:
            pass
        try:
            self.cfg["fixed_trade_usdc"] = float(self.fixed_trade_entry.get().strip())
        except ValueError:
            pass
        try:
            self.cfg["slippage_tolerance_bps"] = int(self.slippage_entry.get().strip())
        except ValueError:
            pass
        try:
            self.cfg["poll_interval_seconds"] = int(self.poll_entry.get().strip())
        except ValueError:
            pass
        self.cfg["watched_addresses"] = list(self.addr_listbox.get(0, tk.END))

    # ---- Button handlers ----

    def _toggle_pk(self):
        current = self.pk_entry.cget("show")
        self.pk_entry.configure(show="" if current == "*" else "*")

    def _derive_api_creds(self):
        """Derive Polymarket CLOB API credentials from the private key."""
        pk = self.pk_entry.get().strip()
        if not pk:
            messagebox.showwarning("No Key", "Enter your private key first.")
            return
        if not HAS_CLOB_SDK:
            messagebox.showerror(
                "Missing SDK",
                "py-clob-client is not installed.\n\n"
                "Run: pip install py-clob-client",
            )
            return
        try:
            client = ClobClient(host=CLOB_API_BASE, chain_id=137, key=pk)
            creds = client.create_or_derive_api_creds()

            # Populate the GUI fields
            self.api_key_entry.delete(0, tk.END)
            self.api_key_entry.insert(0, creds.api_key)
            self.api_secret_entry.delete(0, tk.END)
            self.api_secret_entry.insert(0, creds.api_secret)
            self.api_passphrase_entry.delete(0, tk.END)
            self.api_passphrase_entry.insert(0, creds.api_passphrase)

            self.logger.info("CLOB API credentials derived successfully")
            messagebox.showinfo("Success", "API credentials derived and populated.")
        except Exception as exc:
            self.logger.error("Failed to derive API credentials: %s", exc)
            messagebox.showerror("Error", f"Failed to derive credentials:\n{exc}")

    def _add_address(self):
        addr = self.new_addr_entry.get().strip()
        if not addr:
            return
        # Basic validation
        if not addr.startswith("0x") or len(addr) != 42:
            messagebox.showwarning("Invalid Address", "Please enter a valid Ethereum/Polygon address (0x... 42 chars).")
            return
        if addr.lower() in [a.lower() for a in self.addr_listbox.get(0, tk.END)]:
            messagebox.showinfo("Duplicate", "This address is already in the list.")
            return
        self.addr_listbox.insert(tk.END, addr)
        self.new_addr_entry.delete(0, tk.END)

    def _remove_address(self):
        sel = self.addr_listbox.curselection()
        if sel:
            self.addr_listbox.delete(sel[0])

    def _clear_addresses(self):
        self.addr_listbox.delete(0, tk.END)

    def _clear_log(self):
        self.log_area.configure(state="normal")
        self.log_area.delete("1.0", tk.END)
        self.log_area.configure(state="disabled")

    def _save_config(self):
        self._read_fields_to_config()
        pk = self.pk_entry.get().strip()
        if pk:
            save_private_key(pk, self.cfg)
        save_config(self.cfg)
        self.logger.info("Configuration saved")

    def _start_bot(self):
        self._save_config()
        if not self.cfg.get("watched_addresses"):
            messagebox.showwarning("No Addresses", "Add at least one trader address to watch.")
            return
        if not self.cfg.get("rpc_url") and not self.cfg.get("ws_rpc_url"):
            if not self.cfg.get("use_clob_api"):
                messagebox.showwarning(
                    "No RPC", "Provide an RPC URL or enable the CLOB API."
                )
                return

        self.bot = CopyTraderBot(self.cfg, self.logger)
        self.bot.start()
        self.start_btn.configure(state=tk.DISABLED)
        self.stop_btn.configure(state=tk.NORMAL)
        self.status_var.set("Status: Running")

    def _stop_bot(self):
        if self.bot:
            self.bot.stop()
        self.start_btn.configure(state=tk.NORMAL)
        self.stop_btn.configure(state=tk.DISABLED)
        self.status_var.set("Status: Stopped")

    def run(self):
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.mainloop()

    def _on_close(self):
        if self.bot and self.bot.running:
            self.bot.stop()
        self.root.destroy()


# ---------------------------------------------------------------------------
# Headless health-check server (for cloud PaaS deployments)
# ---------------------------------------------------------------------------

class HealthCheckServer:
    """Minimal HTTP server on $PORT (default 8080) that responds 200 OK.

    DigitalOcean App Platform (and similar PaaS) send periodic health
    checks. Without a listening port, the deployment is marked unhealthy.
    """

    def __init__(self, bot, logger, port=None):
        self.bot = bot
        self.logger = logger
        self.port = int(port or os.environ.get("PORT", 8080))
        self._thread = None

    def start(self):
        from http.server import HTTPServer, BaseHTTPRequestHandler

        bot_ref = self.bot
        logger_ref = self.logger

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                status = "running" if bot_ref and bot_ref.running else "stopped"
                body = json.dumps({"status": status}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, fmt, *args):
                pass  # suppress default access logs

        server = HTTPServer(("0.0.0.0", self.port), Handler)
        self._thread = threading.Thread(target=server.serve_forever, daemon=True)
        self._thread.start()
        self.logger.info("Health-check server listening on port %d", self.port)


# ---------------------------------------------------------------------------
# Headless (CLI) runner — no Tkinter, for server deployments
# ---------------------------------------------------------------------------

def run_headless():
    """Run the bot in headless mode using config.json + env vars."""
    logger = setup_logging()
    cfg = load_config()

    # Allow env-var overrides so secrets stay out of config.json on the server
    if os.environ.get("RPC_URL"):
        cfg["rpc_url"] = os.environ["RPC_URL"]
    if os.environ.get("WS_RPC_URL"):
        cfg["ws_rpc_url"] = os.environ["WS_RPC_URL"]
    if os.environ.get("PRIVATE_KEY"):
        save_private_key(os.environ["PRIVATE_KEY"], cfg)
    if os.environ.get("WATCHED_ADDRESSES"):
        cfg["watched_addresses"] = [
            a.strip() for a in os.environ["WATCHED_ADDRESSES"].split(",") if a.strip()
        ]
    if os.environ.get("COPY_PERCENTAGE"):
        cfg["copy_percentage"] = int(os.environ["COPY_PERCENTAGE"])
    if os.environ.get("MAX_TRADE_USDC"):
        cfg["max_trade_usdc"] = float(os.environ["MAX_TRADE_USDC"])
    if os.environ.get("DRY_RUN"):
        cfg["dry_run"] = os.environ["DRY_RUN"].lower() in ("1", "true", "yes")
    if os.environ.get("CLOB_API_KEY"):
        cfg["clob_api_key"] = os.environ["CLOB_API_KEY"]
    if os.environ.get("CLOB_API_SECRET"):
        cfg["clob_api_secret"] = os.environ["CLOB_API_SECRET"]
    if os.environ.get("CLOB_API_PASSPHRASE"):
        cfg["clob_api_passphrase"] = os.environ["CLOB_API_PASSPHRASE"]

    if not cfg.get("watched_addresses"):
        logger.error("No watched addresses configured. Set WATCHED_ADDRESSES env var or edit config.json.")
        sys.exit(1)

    logger.info("=== Polymarket Copy Trader — Headless Mode ===")
    logger.info("Watched addresses: %s", cfg["watched_addresses"])
    logger.info("Copy %%: %s | Max trade: %s USDC | Dry run: %s",
                cfg.get("copy_percentage"), cfg.get("max_trade_usdc"), cfg.get("dry_run", False))

    bot = CopyTraderBot(cfg, logger)

    # Start health-check HTTP server for App Platform
    health = HealthCheckServer(bot, logger)
    health.start()

    # Start the bot (runs in a background thread)
    bot.start()

    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        bot.stop()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    headless = "--headless" in sys.argv

    # Auto-detect: if no DISPLAY / no Tkinter, fall back to headless
    if not headless:
        display = os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY")
        if not display and sys.platform != "win32" and sys.platform != "darwin":
            headless = True

    if headless:
        run_headless()
    else:
        app = CopyTraderGUI()
        app.run()


if __name__ == "__main__":
    main()


# ---------------------------------------------------------------------------
# USAGE INSTRUCTIONS
# ---------------------------------------------------------------------------
"""
=============================================================================
POLYMARKET COPY TRADER BOT — USAGE INSTRUCTIONS
=============================================================================

1. REQUIRED INSTALLATIONS
   ----------------------
   pip install web3 requests py-clob-client

   Optional (for .env support):
   pip install python-dotenv

   Python 3.8+ is required. Tkinter is included with standard Python
   installations on most platforms.

   The py-clob-client package is the official Polymarket Python SDK.
   It handles API authentication (HMAC signing) and order creation.

2. CONFIGURATION
   -------------
   On first run the bot creates a config.json with default values.
   You can also edit it manually:

   {
     "rpc_url": "https://polygon-rpc.com",
     "ws_rpc_url": "wss://polygon-bor-rpc.publicnode.com",
     "private_key_file": ".private_key",
     "watched_addresses": [
       "0xABC...123"
     ],
     "copy_percentage": 50,
     "max_trade_usdc": 100.0,
     "slippage_tolerance_bps": 100,
     "gas_multiplier": 1.2,
     "poll_interval_seconds": 15,
     "use_clob_api": true,
     "clob_api_key": "",
     "clob_api_secret": "",
     "clob_api_passphrase": ""
   }

   Recommended Polygon RPC providers:
   - Alchemy:  https://alchemy.com  (free tier available)
   - Infura:   https://infura.io
   - QuickNode: https://quicknode.com
   - Public:   https://polygon-rpc.com (rate limited)

3. API CREDENTIALS & AUTHENTICATION
   ---------------------------------
   Polymarket's CLOB API requires L2 authentication (API key + HMAC)
   for trade history and order placement endpoints.

   Credentials are derived DETERMINISTICALLY from your wallet private key:

   Option A – Automatic (recommended):
     Enter your private key in the GUI, then click
     "Derive Credentials from Private Key". The API key, secret, and
     passphrase will be generated and saved to config.json.

   Option B – Automatic on first start:
     If no credentials are saved but a private key is present, the bot
     will derive them automatically when started.

   Option C – Manual:
     Use the py-clob-client SDK directly:
       from py_clob_client.client import ClobClient
       client = ClobClient("https://clob.polymarket.com", 137, key="0x...")
       creds = client.create_or_derive_api_creds()
       print(creds)
     Then paste apiKey, secret, passphrase into the GUI or config.json.

   IMPORTANT: Each wallet can only have ONE active API key at a time.
   Calling create_or_derive_api_creds() is safe to repeat — it returns
   the same key deterministically without invalidating it.

4. PRIVATE KEY SETUP
   -----------------
   Enter your private key in the GUI or save it to the file specified
   by "private_key_file" in config.json (default: .private_key).

   The file is created with 0600 permissions (owner-only read/write).

   ⚠  SECURITY WARNINGS:
   • NEVER share your private key with anyone.
   • NEVER commit .private_key or config.json with keys to version control.
   • Consider using a dedicated hot wallet with limited funds.
   • This bot has FULL control over the wallet whose key you provide.
   • Run on a secure, trusted machine only.
   • Use a hardware wallet or multisig for large holdings.

5. RUNNING THE BOT
   ----------------
   python polymarket_copy_trader.py

   The Tkinter GUI will open. From there you can:
   - Configure RPC endpoints and your private key
   - Derive or enter CLOB API credentials
   - Add trader wallet addresses to monitor
   - Adjust copy percentage (1–100%) and max trade size
   - Start/Stop the monitoring bot
   - View real-time logs in the Log tab

6. HOW IT WORKS
   -------------
   The bot uses two complementary data sources:

   a) Polymarket CLOB API (primary, recommended):
      Uses authenticated requests via py-clob-client to fetch trades
      for watched addresses. Orders are placed via the CLOB API using
      EIP-712 signed order payloads.

   b) On-chain monitoring (fallback):
      Scans new Polygon blocks for transactions from watched addresses
      to Polymarket's CTF Exchange contracts. Requires a valid RPC URL.

   When a new trade is detected, the bot:
   1. Computes the copy amount (original size x copy percentage)
   2. Caps it to max_trade_usdc and 95% of available USDC balance
   3. Ensures USDC approval on the exchange contract
   4. Places the order via the CLOB API (if SDK + credentials are available)
   5. Logs the result

6. LOGGING
   -------
   - Console: INFO level and above
   - bot.log: DEBUG level (full detail)
   - GUI Log tab: INFO level and above

7. DISCLAIMER
   ----------
   This software is provided for EDUCATIONAL PURPOSES ONLY. Trading
   on prediction markets involves substantial risk of loss. The authors
   are not responsible for any financial losses incurred through the
   use of this software. Use at your own risk. Ensure compliance with
   all applicable laws and regulations in your jurisdiction.
=============================================================================
"""
