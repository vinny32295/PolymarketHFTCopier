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

# Neg Risk Adapter (acts as oracle on ConditionalTokens for Neg Risk markets)
NEG_RISK_ADAPTER_ADDRESS = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"

# UMA CTF Adapter V2 on Polygon (oracle for standard/non-Neg-Risk markets)
UMA_CTF_ADAPTER_ADDRESS = "0x6A9D222616C90FcA5754cd1333cFD9b7fb6a4F74"

# USDC on Polygon (collateral token)
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

# Conditional Tokens contract (Gnosis)
CONDITIONAL_TOKENS_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

# Polymarket minimum order constraints
MIN_ORDER_SIZE_TOKENS = 5    # minimum outcome tokens per order
MIN_ORDER_NOTIONAL_USDC = 1.05  # slightly above $1 to stay above min after fees

# Low-balance pause/resume thresholds (USDC)
# When balance drops below the pause threshold the bot stops placing new
# trades and waits for open orders to settle.  Trading resumes once the
# balance recovers to at least the resume threshold.
LOW_BALANCE_PAUSE_THRESHOLD = Decimal("1.05")   # can't even fill the smallest order

# Auto-exit thresholds for open positions
TAKE_PROFIT_PRICE = Decimal("0.99")   # sell when token price reaches 99c (near-certain outcome)
STOP_LOSS_PCT = Decimal("0.50")       # sell when price drops to 50% of entry price

# Polymarket CLOB API base URL
CLOB_API_BASE = "https://clob.polymarket.com"

# Polymarket Gamma Markets API
GAMMA_API_BASE = "https://gamma-api.polymarket.com"

# Polymarket Data API (user activity, positions, trades)
DATA_API_BASE = "https://data-api.polymarket.com"

# Positions persistence file — stores redemption params so they
# survive restarts and don't depend on the Gamma API at redeem time.
POSITIONS_FILE = "positions.json"

# Polymarket Proxy Wallet Factory on Polygon
# Deploys lightweight proxy wallets for Polymarket users; each EOA has
# at most one proxy.  The factory exposes a mapping to look up the proxy
# address from the owner EOA.
PROXY_FACTORY_ADDRESS = "0xaB45c5A4B0c941a2F231C04C3f49182e1A254052"

# Minimal Proxy Factory ABI – multiple getter names tried in order;
# also includes the Deploy event for event-log-based discovery.
PROXY_FACTORY_ABI = json.loads("""[
    {"constant":true,
     "inputs":[{"name":"","type":"address"}],
     "name":"getProxy",
     "outputs":[{"name":"","type":"address"}],
     "type":"function"},
    {"constant":true,
     "inputs":[{"name":"","type":"address"}],
     "name":"proxies",
     "outputs":[{"name":"","type":"address"}],
     "type":"function"},
    {"constant":true,
     "inputs":[{"name":"","type":"address"}],
     "name":"proxyFor",
     "outputs":[{"name":"","type":"address"}],
     "type":"function"},
    {"anonymous":false,
     "inputs":[
        {"indexed":true,"name":"deployer","type":"address"},
        {"indexed":false,"name":"proxy","type":"address"}],
     "name":"Deploy",
     "type":"event"}
]""")

# Minimal Proxy Wallet ABI – execute(address,uint256,bytes) forwards a
# call through the proxy.  Only the owner EOA may call this.
PROXY_WALLET_ABI = json.loads("""[
    {"constant":false,
     "inputs":[
        {"name":"to","type":"address"},
        {"name":"value","type":"uint256"},
        {"name":"data","type":"bytes"}],
     "name":"execute",
     "outputs":[{"name":"success","type":"bool"},
                {"name":"returnData","type":"bytes"}],
     "type":"function"}
]""")

# Polymarket Safe Proxy Factory on Polygon (Gnosis Safe based).
# Newer accounts use Safe wallets instead of lightweight proxies.
SAFE_PROXY_FACTORY_ADDRESS = "0xaacFeEa03eb1561C4e67d661e40682Bd20e3541b"

# Minimal Gnosis Safe ABI – enough to execute arbitrary calls through
# a 1-of-1 multisig Safe owned by our EOA.
GNOSIS_SAFE_ABI = json.loads("""[
    {"constant":false,
     "inputs":[
        {"name":"to","type":"address"},
        {"name":"value","type":"uint256"},
        {"name":"data","type":"bytes"},
        {"name":"operation","type":"uint8"},
        {"name":"safeTxGas","type":"uint256"},
        {"name":"baseGas","type":"uint256"},
        {"name":"gasPrice","type":"uint256"},
        {"name":"gasToken","type":"address"},
        {"name":"refundReceiver","type":"address"},
        {"name":"signatures","type":"bytes"}],
     "name":"execTransaction",
     "outputs":[{"name":"success","type":"bool"}],
     "type":"function"},
    {"constant":true,
     "inputs":[
        {"name":"to","type":"address"},
        {"name":"value","type":"uint256"},
        {"name":"data","type":"bytes"},
        {"name":"operation","type":"uint8"},
        {"name":"safeTxGas","type":"uint256"},
        {"name":"baseGas","type":"uint256"},
        {"name":"gasPrice","type":"uint256"},
        {"name":"gasToken","type":"address"},
        {"name":"refundReceiver","type":"address"},
        {"name":"_nonce","type":"uint256"}],
     "name":"getTransactionHash",
     "outputs":[{"name":"","type":"bytes32"}],
     "type":"function"},
    {"constant":true,
     "inputs":[],
     "name":"nonce",
     "outputs":[{"name":"","type":"uint256"}],
     "type":"function"}
]""")

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
     "type":"function"},
    {"constant":false,"inputs":[{"name":"_to","type":"address"},
     {"name":"_value","type":"uint256"}],
     "name":"transfer","outputs":[{"name":"","type":"bool"}],
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

# Minimal Conditional Tokens (ERC1155) ABI – for checking balances &
# redeeming resolved positions back to USDC.
CONDITIONAL_TOKENS_ABI = json.loads("""[
    {"constant":true,"inputs":[{"name":"owner","type":"address"},
     {"name":"id","type":"uint256"}],
     "name":"balanceOf","outputs":[{"name":"","type":"uint256"}],
     "type":"function"},
    {"constant":true,"inputs":[{"name":"conditionId","type":"bytes32"}],
     "name":"payoutDenominator",
     "outputs":[{"name":"","type":"uint256"}],
     "type":"function"},
    {"constant":true,"inputs":[
        {"name":"oracle","type":"address"},
        {"name":"questionId","type":"bytes32"},
        {"name":"outcomeSlotCount","type":"uint256"}],
     "name":"getConditionId",
     "outputs":[{"name":"","type":"bytes32"}],
     "stateMutability":"pure",
     "type":"function"},
    {"constant":false,"inputs":[
        {"name":"collateralToken","type":"address"},
        {"name":"parentCollectionId","type":"bytes32"},
        {"name":"conditionId","type":"bytes32"},
        {"name":"indexSets","type":"uint256[]"}],
     "name":"redeemPositions","outputs":[],
     "type":"function"}
]""")

# Neg Risk Adapter ABI — handles wrapped conditional tokens for Neg Risk
# markets.  Unlike the standard ConditionalTokens.redeemPositions, the
# adapter's redeemPositions takes only (conditionId, indexSets) because the
# collateral token and parent collection are managed internally.
NEG_RISK_ADAPTER_ABI = json.loads("""[
    {"constant":true,"inputs":[{"name":"owner","type":"address"},
     {"name":"id","type":"uint256"}],
     "name":"balanceOf","outputs":[{"name":"","type":"uint256"}],
     "type":"function"},
    {"constant":false,"inputs":[
        {"name":"conditionId","type":"bytes32"},
        {"name":"indexSets","type":"uint256[]"}],
     "name":"redeemPositions","outputs":[],
     "type":"function"}
]""")

# How often to check for redeemable (settled) positions (seconds)
REDEEM_CHECK_INTERVAL_SECONDS = 300  # 5 minutes

# When paused for low balance, check for settlements much more
# aggressively so we can redeem USDC and resume trading quickly.
PAUSED_REDEEM_INTERVAL_SECONDS = 30  # every 30 s while paused

# How often to run the full portfolio scan (more expensive than the
# periodic check_and_redeem_settled because it queries the Data API
# for the wallet's complete trade history).
PORTFOLIO_SCAN_INTERVAL_SECONDS = 900  # 15 minutes

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
    "order_ttl_seconds": 30,
    "resume_threshold_usdc": 5.0,
    "auto_redeem_settled": True,
    "proxy_redeem": True,
    "proxy_withdraw": True,
    "proxy_address": "",
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


def _encode_abi(contract, fn_name, args):
    """Encode ABI call data, compatible with web3.py v6 and v7+.

    web3.py v7 renamed ``contract.encodeABI`` to ``contract.encode_abi``
    and changed the keyword from ``fn_name`` to ``abi_element_identifier``.
    """
    # web3.py v7+
    if hasattr(contract, "encode_abi"):
        return contract.encode_abi(abi_element_identifier=fn_name, args=args)
    # web3.py v6 (legacy)
    return contract.encodeABI(fn_name=fn_name, args=args)


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
        self._token_to_condition = {}  # token_id -> condition_id from activity
        self._token_to_slug = {}  # token_id -> market slug from activity

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

    def _normalise_market(self, market):
        """Ensure market data is a plain dict."""
        if market and hasattr(market, "__dict__") and not isinstance(market, dict):
            return vars(market)
        return market

    def get_market_by_token(self, token_id):
        """Look up market info by CLOB token ID.

        Returns a market dict with fields like condition_id, closed,
        active, question, etc.  Returns None on failure.

        Tries multiple strategies in order:
        1. Gamma API with ``clob_token_ids``
        2. CLOB SDK ``get_market(condition_id)`` (from activity cache)
        3. Gamma API with ``condition_id`` (from activity cache)
        4. CLOB SDK ``get_market(token_id)`` directly (some deployments
           resolve either kind of ID at the ``/markets/<id>`` endpoint)
        5. Gamma API with ``slug`` (from activity cache)
        """
        short = token_id[:16] + "..."
        gamma_url = f"{self.gamma_url}/markets"

        # ---- Strategy 1: Gamma API with clob_token_ids ----
        data = self._get_public(gamma_url, params={"clob_token_ids": token_id})
        if data and isinstance(data, list) and len(data) > 0:
            return data[0]

        # Log once per run what the Gamma API actually returned
        if not getattr(self, "_gamma_diag_logged", False):
            self._gamma_diag_logged = True
            self.logger.info(
                "Gamma API /markets?clob_token_ids=... returned: %s (type=%s)",
                repr(data)[:200] if data is not None else "None",
                type(data).__name__,
            )

        # ---- Strategy 2: CLOB SDK with condition_id from activity ----
        cached_cid = self._token_to_condition.get(token_id)
        if cached_cid and self.clob_sdk:
            try:
                market = self.clob_sdk.get_market(cached_cid)
                if market:
                    self.logger.debug("Resolved %s via CLOB SDK (cid)", short)
                    return self._normalise_market(market)
            except Exception:
                pass

        # ---- Strategy 3: Gamma API with condition_id ----
        if cached_cid:
            data = self._get_public(gamma_url, params={"condition_id": cached_cid})
            if data and isinstance(data, list) and len(data) > 0:
                self.logger.debug("Resolved %s via Gamma condition_id", short)
                return data[0]

        # ---- Strategy 4: CLOB SDK get_market(token_id) directly ----
        if self.clob_sdk:
            try:
                market = self.clob_sdk.get_market(token_id)
                if market:
                    self.logger.debug("Resolved %s via CLOB SDK (token_id)", short)
                    return self._normalise_market(market)
            except Exception:
                pass

        # ---- Strategy 5: Gamma API with slug from activity ----
        cached_slug = self._token_to_slug.get(token_id)
        if cached_slug:
            data = self._get_public(gamma_url, params={"slug": cached_slug})
            if data and isinstance(data, list) and len(data) > 0:
                self.logger.debug("Resolved %s via Gamma slug", short)
                return data[0]

        return None

    def get_wallet_token_ids(self, address, limit=200):
        """Discover all unique token IDs the wallet has ever traded.

        Queries the Data API activity feed and extracts token IDs from
        each trade record.  Returns a set of token-ID strings.
        """
        url = f"{self.data_url}/activity"
        params = {"user": address.lower(), "limit": limit}
        data = self._get_public(url, params=params)
        if not data:
            return set()

        trades = data if isinstance(data, list) else data.get(
            "data", data.get("history", [])
        )
        token_ids = set()
        logged_sample = False
        for trade in trades:
            # Log the first trade record's keys for diagnostics
            if not logged_sample and isinstance(trade, dict):
                self.logger.debug(
                    "Activity record sample keys: %s", sorted(trade.keys()),
                )
                logged_sample = True
            tid = (
                trade.get("asset")
                or trade.get("asset_id")
                or trade.get("tokenId")
                or trade.get("token_id")
                or trade.get("makerAssetId")
            )
            if tid:
                tid = str(tid)
                token_ids.add(tid)
                # Cache condition_id from activity for CLOB SDK fallback
                cid = trade.get("conditionId") or trade.get("condition_id")
                if cid and tid not in self._token_to_condition:
                    self._token_to_condition[tid] = str(cid)
                # Also try slug for Gamma API fallback
                slug = trade.get("slug") or trade.get("market_slug")
                if slug and tid not in self._token_to_slug:
                    self._token_to_slug[tid] = str(slug)

        if self._token_to_condition:
            self.logger.info(
                "Cached %d condition_id(s) from activity data",
                len(self._token_to_condition),
            )
        return token_ids

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
        """Place a GTC (Good-Till-Cancelled) limit order on the Polymarket CLOB.

        The order rests on the book until it fills or is explicitly
        cancelled.  The bot's order monitor tracks open orders and
        cancels stale ones periodically.

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
                if side.upper() == "SELL":
                    # SELL orders: never bump beyond what we actually hold.
                    self.logger.warning(
                        "SELL size $%.2f below minimum $%.2f — skipping "
                        "(cannot inflate sell beyond held tokens)",
                        size_usdc, effective_usdc,
                    )
                    return None
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

            rounded_price = round(price, 2)
            if rounded_price <= 0:
                self.logger.error("Price rounds to zero – cannot place order")
                return None
            size_tokens = round(actual_usdc / rounded_price, 2)

            limit_args = OrderArgs(
                price=rounded_price,
                size=size_tokens,
                side=side.upper(),
                token_id=token_id,
            )
            signed_order = self.clob_sdk.create_order(limit_args)
            resp = self.clob_sdk.post_order(
                signed_order, orderType=OrderType.GTC,
            )
            self.logger.info(
                "GTC order placed: %s $%.2f @ %.4f for token %s — %s",
                side, actual_usdc, price, token_id[:16] + "...", resp,
            )
            return resp

        except Exception as exc:
            exc_msg = str(exc).lower()
            if "does not exist" in exc_msg:
                self.logger.warning(
                    "Orderbook does not exist for token %s — market likely "
                    "resolved. Needs on-chain redemption, not CLOB sale.",
                    token_id[:16] + "...",
                )
                return {"error": "orderbook_dead"}
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

        # Cached proxy wallet address (discovered once, reused)
        self._proxy_address = None
        self._proxy_discovery_done = False
        self._proxy_is_safe = False  # True when proxy is a Gnosis Safe

        # Position tracker: token_id -> {tokens, entry_price, neg_risk,
        #   condition_id, collateral_token, parent_collection_id, index_sets}
        # Stores all redemption parameters at trade time so we never need
        # to look them up again via the Gamma API.
        self._positions = self._load_positions()

        # Contract handles
        self.usdc = w3.eth.contract(
            address=Web3.to_checksum_address(USDC_ADDRESS), abi=ERC20_ABI
        )
        self.ctf_exchange = w3.eth.contract(
            address=Web3.to_checksum_address(CTF_EXCHANGE_ADDRESS),
            abi=CTF_EXCHANGE_ABI,
        )
        self.conditional_tokens = w3.eth.contract(
            address=Web3.to_checksum_address(CONDITIONAL_TOKENS_ADDRESS),
            abi=CONDITIONAL_TOKENS_ABI,
        )
        self.neg_risk_adapter = w3.eth.contract(
            address=Web3.to_checksum_address(NEG_RISK_ADAPTER_ADDRESS),
            abi=NEG_RISK_ADAPTER_ABI,
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

    # ------------------------------------------------------------------
    # Position persistence
    # ------------------------------------------------------------------

    @property
    def _positions_file(self):
        return self.cfg.get("positions_file", POSITIONS_FILE)

    def _load_positions(self):
        """Load positions from disk.  Returns empty dict on failure."""
        try:
            with open(self._positions_file, "r") as fh:
                raw = json.load(fh)
            positions = {}
            for tid, entry in raw.items():
                positions[tid] = {
                    "tokens": Decimal(str(entry.get("tokens", 0))),
                    "entry_price": Decimal(str(entry.get("entry_price", 0))),
                    "neg_risk": entry.get("neg_risk", False),
                    "condition_id": entry.get("condition_id"),
                    "collateral_token": entry.get("collateral_token", USDC_ADDRESS),
                    "parent_collection_id": entry.get(
                        "parent_collection_id", "0x" + "00" * 32,
                    ),
                    "index_sets": entry.get("index_sets", [1, 2]),
                }
            self.logger.info(
                "Loaded %d position(s) from %s", len(positions), self._positions_file,
            )
            return positions
        except FileNotFoundError:
            return {}
        except Exception as exc:
            self.logger.warning("Could not load positions file: %s", exc)
            return {}

    def _save_positions(self):
        """Persist current positions to disk (atomic write)."""
        try:
            serialisable = {}
            for tid, pos in self._positions.items():
                serialisable[tid] = {
                    "tokens": str(pos["tokens"]),
                    "entry_price": str(pos["entry_price"]),
                    "neg_risk": pos.get("neg_risk", False),
                    "condition_id": pos.get("condition_id"),
                    "collateral_token": pos.get("collateral_token", USDC_ADDRESS),
                    "parent_collection_id": pos.get(
                        "parent_collection_id", "0x" + "00" * 32,
                    ),
                    "index_sets": pos.get("index_sets", [1, 2]),
                }
            pf = self._positions_file
            tmp = pf + ".tmp"
            with open(tmp, "w") as fh:
                json.dump(serialisable, fh, indent=2)
            os.replace(tmp, pf)
        except Exception as exc:
            self.logger.warning("Could not save positions: %s", exc)

    def get_usdc_balance(self, max_age_seconds=15):
        """Return USDC balance as a Decimal (6 decimals).

        Results are cached for *max_age_seconds* (default 15 s — aligned
        with the poll interval) to avoid stale reads after deposits.
        Pass ``max_age_seconds=0`` to force a fresh fetch.

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

    # ------------------------------------------------------------------
    # Auto-exit: take-profit & stop-loss
    # ------------------------------------------------------------------

    def check_exit_conditions(self):
        """Scan all open positions and sell any that hit exit thresholds.

        - **Take-profit**: current price >= 0.99  (near-certain outcome)
        - **Stop-loss**:   current price <= 50% of entry price

        Requires a CLOB client to fetch live prices and place sell orders.
        Returns a list of sell results (one per exited position), or an
        empty list when there is nothing to do.
        """
        if not self.clob_client:
            return []
        if not self._positions:
            return []

        results = []
        # Iterate over a snapshot so we can mutate _positions safely
        for token_id, pos in list(self._positions.items()):
            tokens = pos["tokens"]
            entry_price = pos["entry_price"]
            if tokens <= 0:
                continue

            current_price = self.clob_client.get_last_trade_price(token_id)
            if current_price is None:
                continue
            current_price_d = Decimal(str(current_price))

            reason = None
            if current_price_d >= TAKE_PROFIT_PRICE:
                reason = "take-profit"
            elif entry_price > 0 and current_price_d <= entry_price * STOP_LOSS_PCT:
                reason = "stop-loss"

            if reason is None:
                continue

            sell_usdc = float(tokens * current_price_d)
            self.logger.warning(
                "AUTO-EXIT (%s): selling %.2f tokens of %s "
                "(entry=%.4f, current=%.4f, value=$%.2f)",
                reason,
                tokens,
                token_id[:16] + "..." if len(token_id) > 16 else token_id,
                entry_price,
                current_price_d,
                sell_usdc,
            )

            if self.cfg.get("dry_run", False):
                self.logger.info(
                    "[DRY RUN] Would auto-exit %s: SELL $%.2f of token %s",
                    reason, sell_usdc, token_id[:20],
                )
                results.append({
                    "status": "dry_run",
                    "reason": reason,
                    "side": "SELL",
                    "amount_usdc": sell_usdc,
                    "token_id": token_id,
                    "price": float(current_price_d),
                })
                continue

            # Place a SELL order for the full position
            slippage_mult = Decimal(str(self.slippage_bps)) / Decimal("10000")
            adjusted_price = float(current_price_d * (Decimal("1") - slippage_mult))
            adjusted_price = max(adjusted_price, 0.01)

            result = self.clob_client.place_order(
                token_id=token_id,
                side="SELL",
                size_usdc=sell_usdc,
                price=adjusted_price,
            )

            # --- Dead orderbook → try on-chain redemption instead ---
            if isinstance(result, dict) and result.get("error") == "orderbook_dead":
                redeemed = self._try_onchain_redeem(token_id, pos, reason)
                if redeemed:
                    results.append(redeemed)
                continue

            if result:
                self.logger.info(
                    "Auto-exit %s order submitted: %s", reason, result,
                )
                self.invalidate_balance_cache()
                # Clear the position
                del self._positions[token_id]
                self._save_positions()

                order_id = None
                if isinstance(result, dict):
                    order_id = (
                        result.get("orderID")
                        or result.get("id")
                        or result.get("order_id")
                    )
                if order_id:
                    self.track_order(
                        order_id, "SELL", token_id,
                        adjusted_price, sell_usdc,
                    )

                results.append({
                    "status": "submitted",
                    "reason": reason,
                    "side": "SELL",
                    "amount_usdc": sell_usdc,
                    "token_id": token_id,
                    "price": adjusted_price,
                    "clob_response": result,
                })
            else:
                # If the sell value is below the exchange minimum, this
                # position is dust and will never be sellable.  Remove it
                # so we don't retry every cycle.
                min_sell_usdc = max(
                    float(MIN_ORDER_SIZE_TOKENS) * adjusted_price,
                    float(MIN_ORDER_NOTIONAL_USDC),
                )
                if sell_usdc < min_sell_usdc:
                    self.logger.info(
                        "Position %s is dust ($%.2f < min $%.2f) — "
                        "removing from tracking",
                        token_id[:16] + "...", sell_usdc, min_sell_usdc,
                    )
                    del self._positions[token_id]
                    self._save_positions()
                else:
                    self.logger.warning(
                        "Auto-exit %s order failed for token %s",
                        reason, token_id[:16] + "...",
                    )

        return results

    def _try_onchain_redeem(self, token_id, pos, reason):
        """Attempt on-chain redemption for a position whose orderbook is dead.

        When the CLOB returns "orderbook does not exist", the market has
        resolved and the tokens can only be redeemed on-chain — not sold.

        Returns a result dict on success, or None.
        """
        condition_id = pos.get("condition_id")
        neg_risk = pos.get("neg_risk", False)

        if not condition_id:
            # Try the Gamma API as a last resort
            try:
                market = self.clob_client.get_market_by_token(token_id)
                if market:
                    condition_id = market.get("condition_id")
                    neg_risk = self._is_neg_risk_market(market)
            except Exception:
                pass

        if not condition_id:
            self.logger.warning(
                "Cannot redeem token %s — no condition_id available. "
                "Removing stale position from tracking.",
                token_id[:16] + "...",
            )
            self._positions.pop(token_id, None)
            self._save_positions()
            return None

        # Check if actually resolved on-chain
        try:
            resolved_cid, payout_denom = self._resolve_condition_id(
                condition_id, neg_risk=neg_risk,
            )
        except Exception as exc:
            self.logger.warning(
                "On-chain resolution check failed for token %s: %s",
                token_id[:16] + "...", exc,
            )
            # Remove from tracking to stop retry spam
            self._positions.pop(token_id, None)
            self._save_positions()
            return None

        if payout_denom == 0:
            self.logger.info(
                "Market not resolved on-chain for token %s — "
                "orderbook dead but not settled yet. Removing from tracking.",
                token_id[:16] + "...",
            )
            self._positions.pop(token_id, None)
            self._save_positions()
            return None

        # Market IS resolved — redeem on-chain!
        self.logger.info(
            "REDEEM (auto-exit %s): orderbook dead, redeeming on-chain — "
            "token %s, conditionId=%s, neg_risk=%s",
            reason, token_id[:16] + "...",
            resolved_cid[:16] + "...", neg_risk,
        )

        if self.cfg.get("dry_run", False):
            self.logger.info(
                "[DRY RUN] Would redeem token %s on-chain", token_id[:16] + "...",
            )
            return {"status": "dry_run", "reason": reason, "token_id": token_id}

        try:
            tx = self._build_redeem_tx(resolved_cid, neg_risk=neg_risk)
            receipt = self._sign_and_send(tx)
            if receipt and receipt.status == 1:
                self.logger.info(
                    "Redemption OK (auto-exit %s): tx %s — USDC returned",
                    reason, receipt.transactionHash.hex(),
                )
                self.invalidate_balance_cache()
                self._positions.pop(token_id, None)
                self._save_positions()
                return {
                    "status": "redeemed",
                    "reason": reason,
                    "token_id": token_id,
                    "tx_hash": receipt.transactionHash.hex(),
                }
            else:
                self.logger.warning(
                    "Redemption tx failed for token %s — removing from tracking",
                    token_id[:16] + "...",
                )
                self._positions.pop(token_id, None)
                self._save_positions()
        except Exception as exc:
            self.logger.warning(
                "On-chain redeem failed for token %s: %s — "
                "removing from tracking",
                token_id[:16] + "...", exc,
            )
            self._positions.pop(token_id, None)
            self._save_positions()

        return None

    # ------------------------------------------------------------------
    # Portfolio scan & auto-redeem settled positions
    # ------------------------------------------------------------------

    def scan_and_redeem_portfolio(self):
        """Discover ALL wallet positions and redeem resolved ones.

        Queries the Data API for the wallet's full trade history, checks
        on-chain balances for every token ID ever traded, and:

        * **Resolved markets** (``payoutDenominator > 0`` on-chain) →
          calls ``redeemPositions`` to convert winning tokens back to USDC.
        * **Active markets** → seeds ``_positions`` so the periodic
          ``check_and_redeem_settled`` can monitor them going forward.

        **On-chain resolution is the authoritative signal.**  The Gamma
        API's ``closed``/``active`` flags are NOT required — the oracle's
        ``payoutDenominator`` is checked for every position that has a
        non-zero token balance.  This handles the common case where
        Polymarket's UI/API still shows a position as "active" after
        the underlying market has resolved.

        Called at startup and also periodically to catch newly resolved
        markets.
        """
        if not self.clob_client:
            self.logger.info("Portfolio scan skipped — no CLOB client")
            return []
        if not self.cfg.get("auto_redeem_settled", True):
            return []

        self.logger.info("Scanning wallet portfolio for redeemable positions...")

        # 1. Discover token IDs from trade history (EOA + proxy)
        token_ids = self.clob_client.get_wallet_token_ids(self.address)

        # Also check the proxy wallet's trade history to catch tokens
        # that might have been traded through it.
        proxy_addr = self.cfg.get("proxy_address", "")
        if proxy_addr and proxy_addr.startswith("0x") and len(proxy_addr) == 42:
            proxy_tokens = self.clob_client.get_wallet_token_ids(proxy_addr)
            if proxy_tokens:
                token_ids = token_ids | proxy_tokens

        # Also include any token IDs from _positions that may not appear
        # in the API trade history (e.g. markets removed from the API).
        if self._positions:
            pos_tokens = set(self._positions.keys())
            added = pos_tokens - token_ids
            if added:
                self.logger.info(
                    "Adding %d token(s) from positions.json not in trade history",
                    len(added),
                )
            token_ids = token_ids | pos_tokens

        if not token_ids:
            self.logger.info("No trade history found for wallet — portfolio scan done")
            return []

        self.logger.info(
            "Found %d unique token IDs in wallet trade history", len(token_ids),
        )

        results = []
        active_seeded = 0
        zero_balance_count = 0
        no_market_count = 0
        error_count = 0
        unresolved_no_price = 0

        for token_id in token_ids:
            try:
                # 2. Look up market info — first check persisted positions,
                #    then fall back to API lookup.
                existing_pos = self._positions.get(token_id)
                condition_id = existing_pos.get("condition_id") if existing_pos else None
                neg_risk = existing_pos.get("neg_risk", False) if existing_pos else False

                if not condition_id:
                    market = self.clob_client.get_market_by_token(token_id)
                    if not market:
                        no_market_count += 1
                        self.logger.debug(
                            "No market info found for token %s", token_id[:16] + "...",
                        )
                        continue

                    condition_id = market.get("condition_id")
                    if not condition_id:
                        self.logger.debug(
                            "No condition_id for token %s (%s)",
                            token_id[:16] + "...",
                            market.get("question", "?")[:40],
                        )
                        no_market_count += 1
                        continue

                    neg_risk = self._is_neg_risk_market(market)
                else:
                    market = None  # already have what we need

                # 3. Check on-chain balance (correct contract for market type)
                ct_balance = self._get_token_balance(
                    self.address, token_id, neg_risk=neg_risk,
                )

                if ct_balance == 0:
                    zero_balance_count += 1
                    continue

                # 4. On-chain resolution check (authoritative).
                #    Tries the API's condition_id directly, then derives
                #    the real CTF conditionId using known oracle addresses.
                resolved_cid, payout_denom = self._resolve_condition_id(
                    condition_id, neg_risk=neg_risk,
                )

                if payout_denom == 0:
                    # Not resolved on-chain — seed as active position
                    if token_id not in self._positions:
                        tokens = Decimal(ct_balance) / Decimal("1000000")
                        price = self.clob_client.get_last_trade_price(token_id)
                        entry_price = Decimal(str(price)) if price and price > 0 else Decimal("0")
                        self._positions[token_id] = {
                            "tokens": tokens,
                            "entry_price": entry_price,
                            "neg_risk": neg_risk,
                            "condition_id": condition_id,
                            "collateral_token": USDC_ADDRESS,
                            "parent_collection_id": "0x" + "00" * 32,
                            "index_sets": [1, 2],
                        }
                        self._save_positions()
                        active_seeded += 1
                        if entry_price > 0:
                            self.logger.info(
                                "Discovered active position: %s (%.2f tokens @ $%.4f, neg_risk=%s)",
                                token_id[:16] + "...", tokens, price, neg_risk,
                            )
                        else:
                            unresolved_no_price += 1
                            self.logger.info(
                                "Discovered active position (no price available): %s "
                                "(%.2f tokens, balance=%d, neg_risk=%s, question=%s)",
                                token_id[:16] + "...", tokens, ct_balance, neg_risk,
                                market.get("question", "?")[:50],
                            )
                    continue

                # 5. Market is resolved on-chain — redeem!
                #    Use resolved_cid (may differ from the API's condition_id
                #    if the API returned a questionId rather than the derived
                #    CTF conditionId).
                question = market.get("question", "unknown")
                self.logger.info(
                    "REDEEM: Resolved position — token %s, "
                    "question: %s, balance: %d, neg_risk: %s",
                    token_id[:16] + "...", question[:60], ct_balance, neg_risk,
                )

                if self.cfg.get("dry_run", False):
                    self.logger.info(
                        "[DRY RUN] Would redeem for condition %s",
                        resolved_cid[:16] + "...",
                    )
                    results.append({
                        "status": "dry_run",
                        "token_id": token_id,
                        "balance": ct_balance,
                    })
                    continue

                tx = self._build_redeem_tx(resolved_cid, neg_risk=neg_risk)

                receipt = self._sign_and_send(tx)
                if receipt and receipt.status == 1:
                    self.logger.info(
                        "Redemption OK: tx %s — USDC returned to wallet",
                        receipt.transactionHash.hex(),
                    )
                    self.invalidate_balance_cache()
                    # Remove from position tracker if it was there
                    self._positions.pop(token_id, None)
                    self._save_positions()
                    results.append({
                        "status": "redeemed",
                        "token_id": token_id,
                        "balance": ct_balance,
                        "tx_hash": receipt.transactionHash.hex(),
                    })
                else:
                    self.logger.warning(
                        "Redemption tx failed for token %s",
                        token_id[:16] + "...",
                    )
                    results.append({
                        "status": "failed",
                        "token_id": token_id,
                    })

            except Exception as exc:
                error_count += 1
                self.logger.info(
                    "Error scanning token %s: %s", token_id[:16] + "...", exc,
                )

        redeemed = sum(1 for r in results if r["status"] == "redeemed")
        self.logger.info(
            "Portfolio scan complete: %d token(s) scanned — "
            "%d zero-balance, %d no-market-info, %d error(s), "
            "%d resolved (%d redeemed), %d active seeded (%d without price)",
            len(token_ids), zero_balance_count, no_market_count,
            error_count, len(results), redeemed, active_seeded,
            unresolved_no_price,
        )
        return results

    def _is_neg_risk_market(self, market):
        """Return True if the market uses the Neg Risk framework."""
        neg = market.get("neg_risk")
        if isinstance(neg, bool):
            return neg
        if isinstance(neg, str):
            return neg.lower() in ("true", "1", "yes")
        # Also check neg_risk_market_id presence as a secondary signal
        return bool(market.get("neg_risk_market_id"))

    def _derive_condition_id(self, oracle_address, question_id_hex):
        """Compute the CTF conditionId on-chain via getConditionId.

        conditionId = keccak256(abi.encodePacked(oracle, questionId, outcomeSlotCount))

        Uses the ConditionalTokens contract's ``getConditionId`` pure
        function so we don't need a local keccak library.
        """
        question_bytes = bytes.fromhex(question_id_hex.replace("0x", ""))
        return self.conditional_tokens.functions.getConditionId(
            Web3.to_checksum_address(oracle_address),
            question_bytes,
            2,  # outcomeSlotCount for binary markets
        ).call()

    def _resolve_condition_id(self, api_condition_id, neg_risk=False):
        """Find the correct conditionId that has payoutDenominator > 0.

        The Gamma API's ``condition_id`` field may be the raw questionId
        rather than the derived CTF conditionId.  For Neg Risk markets
        the oracle is the NegRiskAdapter; for standard markets it's the
        UMA CTF Adapter.  This method tries the API value directly,
        then falls back to deriving the conditionId with known oracles.

        Returns ``(conditionId_hex, payout_denom)`` — the conditionId
        string (with ``0x`` prefix) that resolved on-chain and its
        payout denominator.  Returns ``(api_condition_id, 0)`` if
        nothing resolved.
        """
        api_cond_bytes = bytes.fromhex(api_condition_id.replace("0x", ""))

        # 1. Try the API's value directly
        try:
            pd = self.conditional_tokens.functions.payoutDenominator(
                api_cond_bytes
            ).call()
            if pd > 0:
                return api_condition_id, pd
        except Exception:
            pass

        # 2. Derive conditionId with known oracle addresses.
        #    The API's condition_id may be a questionId that needs to be
        #    hashed with the oracle address to get the real CTF conditionId.
        oracles = []
        if neg_risk:
            oracles.append(("NegRiskAdapter", NEG_RISK_ADAPTER_ADDRESS))
        oracles.append(("UMA_CTF_Adapter", UMA_CTF_ADAPTER_ADDRESS))
        if not neg_risk:
            oracles.append(("NegRiskAdapter", NEG_RISK_ADAPTER_ADDRESS))

        for label, oracle_addr in oracles:
            try:
                derived = self._derive_condition_id(oracle_addr, api_condition_id)
                pd = self.conditional_tokens.functions.payoutDenominator(
                    derived
                ).call()
                if pd > 0:
                    derived_hex = "0x" + (
                        derived.hex() if isinstance(derived, bytes)
                        else derived.replace("0x", "")
                    )
                    self.logger.info(
                        "Resolved conditionId via %s oracle: API gave %s, "
                        "derived %s (payoutDenom=%d)",
                        label,
                        api_condition_id[:16] + "...",
                        derived_hex[:16] + "...",
                        pd,
                    )
                    return derived_hex, pd
            except Exception:
                pass

        return api_condition_id, 0

    def _get_token_balance(self, owner, token_id, neg_risk=False):
        """Return the on-chain token balance, checking the right contract.

        For standard markets, tokens live on the ConditionalTokens contract.
        For Neg Risk markets, tokens are wrapped by the NegRiskAdapter.
        Falls back to the other contract if the primary returns 0.
        """
        tid = int(token_id)
        addr = Web3.to_checksum_address(owner)
        if neg_risk:
            # Neg Risk: check adapter first, then conditional tokens
            try:
                bal = self.neg_risk_adapter.functions.balanceOf(addr, tid).call()
                if bal > 0:
                    return bal
            except Exception:
                pass
            try:
                return self.conditional_tokens.functions.balanceOf(addr, tid).call()
            except Exception:
                return 0
        else:
            # Standard: check conditional tokens first, then adapter as fallback
            try:
                bal = self.conditional_tokens.functions.balanceOf(addr, tid).call()
                if bal > 0:
                    return bal
            except Exception:
                pass
            try:
                return self.neg_risk_adapter.functions.balanceOf(addr, tid).call()
            except Exception:
                return 0

    def _build_redeem_tx(self, condition_id, neg_risk=False):
        """Build a redeemPositions transaction for the correct contract.

        Standard markets: ConditionalTokens.redeemPositions(collateral, parent, cond, indexSets)
        Neg Risk markets: NegRiskAdapter.redeemPositions(cond, indexSets)
        """
        cond_bytes = bytes.fromhex(condition_id.replace("0x", ""))
        if neg_risk:
            return self.neg_risk_adapter.functions.redeemPositions(
                cond_bytes,
                [1, 2],
            ).build_transaction(self._base_tx_params())
        else:
            return self.conditional_tokens.functions.redeemPositions(
                Web3.to_checksum_address(USDC_ADDRESS),
                b"\x00" * 32,       # parentCollectionId (root)
                cond_bytes,
                [1, 2],             # both binary outcomes
            ).build_transaction(self._base_tx_params())

    def check_and_redeem_settled(self):
        """Scan positions for resolved markets and redeem tokens back to USDC.

        Uses **on-chain resolution as the primary signal**.  For each tracked
        position:

        1. Looks up the market via Gamma API to get the condition_id and
           neg_risk flag.
        2. Checks on-chain ``payoutDenominator`` — if non-zero the oracle
           has reported the outcome and the market IS resolved, regardless
           of what the API's ``closed``/``active`` flags say.
        3. Checks the wallet's ERC-1155 balance of the conditional token.
        4. Calls ``redeemPositions`` on the appropriate contract.

        This avoids the problem where Polymarket's API still shows a
        position as "active" even though the market has already resolved
        on-chain — a common situation that previously blocked redemption.

        Returns a list of result dicts (one per redeemed position).
        """
        if not self.clob_client:
            return []
        if not self._positions:
            return []
        if not self.cfg.get("auto_redeem_settled", True):
            return []

        results = []
        for token_id, pos in list(self._positions.items()):
            if pos["tokens"] <= 0:
                continue

            try:
                # 1. Use persisted condition_id if available; fall back to API
                condition_id = pos.get("condition_id")
                neg_risk = pos.get("neg_risk", False)

                if not condition_id:
                    market = self.clob_client.get_market_by_token(token_id)
                    if not market:
                        continue
                    condition_id = market.get("condition_id")
                    if not condition_id:
                        self.logger.debug(
                            "Market for token %s has no condition_id, skipping",
                            token_id[:16] + "...",
                        )
                        continue
                    neg_risk = self._is_neg_risk_market(market)
                    # Backfill redemption params into the position
                    pos["condition_id"] = condition_id
                    pos["neg_risk"] = neg_risk
                    self._save_positions()

                # 2. On-chain resolution check.  Tries the API's
                #    condition_id directly, then derives the real CTF
                #    conditionId using known oracle addresses.
                resolved_cid, payout_denom = self._resolve_condition_id(
                    condition_id, neg_risk=neg_risk,
                )
                if payout_denom == 0:
                    continue

                # 3. Check on-chain balance of the conditional token
                ct_balance = self._get_token_balance(
                    self.address, token_id, neg_risk=neg_risk,
                )
                if ct_balance == 0:
                    self.logger.info(
                        "Market resolved but no on-chain tokens for %s — "
                        "clearing stale position",
                        token_id[:16] + "...",
                    )
                    del self._positions[token_id]
                    self._save_positions()
                    continue

                self.logger.info(
                    "REDEEM: Market resolved for token %s (condition %s, "
                    "neg_risk=%s) — redeeming %d conditional tokens",
                    token_id[:16] + "...",
                    resolved_cid[:16] + "...",
                    neg_risk,
                    ct_balance,
                )

                # 4. Dry-run guard
                if self.cfg.get("dry_run", False):
                    self.logger.info(
                        "[DRY RUN] Would redeem positions for condition %s",
                        resolved_cid[:16] + "...",
                    )
                    results.append({
                        "status": "dry_run",
                        "condition_id": resolved_cid,
                        "token_id": token_id,
                        "balance": ct_balance,
                    })
                    continue

                # 5. Build and send the redeemPositions transaction
                tx = self._build_redeem_tx(resolved_cid, neg_risk=neg_risk)

                receipt = self._sign_and_send(tx)
                if receipt and receipt.status == 1:
                    self.logger.info(
                        "Redemption confirmed: tx %s — USDC returned to wallet",
                        receipt.transactionHash.hex(),
                    )
                    self.invalidate_balance_cache()
                    del self._positions[token_id]
                    self._save_positions()
                    results.append({
                        "status": "redeemed",
                        "condition_id": condition_id,
                        "token_id": token_id,
                        "balance": ct_balance,
                        "tx_hash": receipt.transactionHash.hex(),
                    })
                else:
                    self.logger.warning(
                        "Redemption tx failed for condition %s",
                        condition_id[:16] + "...",
                    )
                    results.append({
                        "status": "failed",
                        "condition_id": condition_id,
                        "token_id": token_id,
                    })

            except Exception as exc:
                self.logger.warning(
                    "Error checking redemption for token %s: %s",
                    token_id[:16] + "...",
                    exc,
                )

        if results:
            redeemed = sum(1 for r in results if r["status"] == "redeemed")
            self.logger.info(
                "Redemption check complete: %d redeemed out of %d candidates",
                redeemed, len(results),
            )
        return results

    # ------------------------------------------------------------------
    # Proxy wallet discovery, redemption & withdrawal
    # ------------------------------------------------------------------

    def _get_proxy_from_profile_api(self):
        """Try to discover proxy address via Polymarket's public profile API.

        Queries the Gamma API profile endpoint which maps an EOA to its
        associated proxy/contract wallet.  No authentication needed.

        Returns the proxy address as a checksummed string, or ``None``.
        """
        if not requests:
            return None

        address_variants = [
            self.address.lower(),
            Web3.to_checksum_address(self.address),
        ]
        proxy_keys = (
            "proxyAddress", "proxy_address", "proxy", "proxyWallet",
            "proxy_wallet", "contractAddress", "contract_address",
            "polyAddress", "poly_address",
        )

        for addr_form in address_variants:
            for base in (GAMMA_API_BASE, DATA_API_BASE):
                url = f"{base}/profiles/{addr_form}"
                try:
                    resp = requests.get(url, timeout=10)
                    if resp.status_code != 200:
                        continue
                    data = resp.json()
                    if not isinstance(data, dict):
                        continue
                    for key in proxy_keys:
                        val = data.get(key)
                        if (
                            val
                            and isinstance(val, str)
                            and val.startswith("0x")
                            and len(val) == 42
                        ):
                            checksummed = Web3.to_checksum_address(val)
                            zero = "0x" + "0" * 40
                            if checksummed != zero:
                                self.logger.info(
                                    "Discovered proxy via profile API "
                                    "(%s): %s",
                                    base.split("//")[1].split(".")[0],
                                    checksummed,
                                )
                                return checksummed
                except Exception as exc:
                    self.logger.debug(
                        "Profile API probe %s failed: %s", url[:60], exc,
                    )
        return None

    def _get_proxy_from_polygonscan(self):
        """Try to discover proxy address via Polygonscan's free API.

        Searches the EOA's outbound transaction history for calls to the
        known proxy factory contracts.  When such a transaction is found,
        checks its internal transactions for the created proxy address.

        Returns the proxy address as a checksummed string, or ``None``.
        """
        if not requests:
            return None

        api_key = self.cfg.get("polygonscan_api_key", "")
        api_url = "https://api.polygonscan.com/api"
        factories = {
            PROXY_FACTORY_ADDRESS.lower(): False,
            SAFE_PROXY_FACTORY_ADDRESS.lower(): True,
        }
        zero = "0x" + "0" * 40

        try:
            params = {
                "module": "account",
                "action": "txlist",
                "address": self.address,
                "startblock": 0,
                "endblock": 99999999,
                "sort": "asc",
            }
            if api_key:
                params["apikey"] = api_key
            resp = requests.get(api_url, params=params, timeout=20)
            data = resp.json()
            if data.get("status") != "1" or not data.get("result"):
                return None

            for tx in data["result"]:
                to_addr = (tx.get("to") or "").lower()
                if to_addr not in factories:
                    continue
                tx_hash = tx.get("hash")
                if not tx_hash:
                    continue

                # Found a factory call — look for the created contract
                # in internal transactions
                int_params = {
                    "module": "account",
                    "action": "txlistinternal",
                    "txhash": tx_hash,
                }
                if api_key:
                    int_params["apikey"] = api_key
                int_resp = requests.get(
                    api_url, params=int_params, timeout=15,
                )
                int_data = int_resp.json()
                if int_data.get("status") == "1" and int_data.get("result"):
                    for itx in int_data["result"]:
                        ca = itx.get("contractAddress", "")
                        if ca and ca != zero:
                            addr = Web3.to_checksum_address(ca)
                            is_safe = factories[to_addr]
                            self.logger.info(
                                "Discovered %s proxy via Polygonscan: "
                                "%s (tx %s)",
                                "Safe" if is_safe else "legacy",
                                addr, tx_hash[:16] + "...",
                            )
                            return addr

                # Also check receipt logs as fallback
                try:
                    receipt = self.w3.eth.get_transaction_receipt(tx_hash)
                    for log in receipt.get("logs", []):
                        topics = log.get("topics", [])
                        if len(topics) >= 2:
                            eoa_topic = "0x" + self.address[2:].lower().zfill(64)
                            if topics[1].hex().lower() == eoa_topic.lower():
                                raw_data = log.get("data", b"")
                                raw_hex = (
                                    raw_data.hex()
                                    if isinstance(raw_data, bytes)
                                    else raw_data.replace("0x", "")
                                )
                                if len(raw_hex) >= 40:
                                    addr = Web3.to_checksum_address(
                                        "0x" + raw_hex[-40:]
                                    )
                                    self.logger.info(
                                        "Discovered proxy from factory tx "
                                        "receipt: %s", addr,
                                    )
                                    return addr
                except Exception:
                    pass

        except Exception as exc:
            self.logger.debug("Polygonscan proxy search failed: %s", exc)

        return None

    def _search_factory_events(self, factory_addr, is_safe=False):
        """Search a factory contract's event logs for a proxy deployed by this EOA.

        Uses specific event signatures for efficiency:
        - Legacy factory: ``Deploy(address,address)`` — deployer indexed, proxy in data.
        - Safe factory:   ``ProxyCreation(address,address)`` — proxy in data, singleton in data.
          Also tries the 1-arg variant ``ProxyCreation(address)``.

        Falls back to a broad topic-1 scan if signature-specific queries miss.

        Returns the proxy address as a checksummed string, or ``None``.
        """
        eoa_padded = "0x" + self.address[2:].lower().zfill(64)

        chunk_size = 5_000_000
        label = "Safe" if is_safe else "legacy"
        checksummed_factory = Web3.to_checksum_address(factory_addr)

        # Build a list of event signatures to try (most-specific first)
        if is_safe:
            event_sigs = [
                Web3.keccak(text="ProxyCreation(address,address)").hex(),
                Web3.keccak(text="ProxyCreation(address)").hex(),
            ]
        else:
            event_sigs = [
                Web3.keccak(text="Deploy(address,address)").hex(),
            ]

        # Approximate deployment blocks on Polygon:
        #   Legacy Proxy Factory: ~20M    Safe Proxy Factory: ~40M
        # Use a conservative start to avoid missing early deployments.
        start_block = 20_000_000

        try:
            latest_block = self.w3.eth.block_number
            self.logger.info(
                "Searching %s factory (%s) event logs for EOA %s "
                "(blocks %d–%d)...",
                label, factory_addr[:12] + "...", self.address,
                start_block, latest_block,
            )

            # --- Pass 1: signature-specific queries ---
            for sig in event_sigs:
                for from_blk in range(start_block, latest_block + 1, chunk_size):
                    to_blk = min(from_blk + chunk_size - 1, latest_block)
                    try:
                        logs = self.w3.eth.get_logs({
                            "address": checksummed_factory,
                            "fromBlock": from_blk,
                            "toBlock": to_blk,
                            "topics": [sig, eoa_padded],
                        })
                        addr = self._extract_proxy_from_logs(logs, label)
                        if addr:
                            return addr
                    except Exception as exc:
                        self.logger.debug(
                            "%s sig-scan chunk %d–%d failed: %s",
                            label, from_blk, to_blk, exc,
                        )

            # --- Pass 2: broad scan (topic0=any, topic1=EOA) ---
            self.logger.info(
                "%s signature-specific search found nothing — "
                "trying broad topic scan...", label,
            )
            for from_blk in range(start_block, latest_block + 1, chunk_size):
                to_blk = min(from_blk + chunk_size - 1, latest_block)
                try:
                    logs = self.w3.eth.get_logs({
                        "address": checksummed_factory,
                        "fromBlock": from_blk,
                        "toBlock": to_blk,
                        "topics": [None, eoa_padded],
                    })
                    addr = self._extract_proxy_from_logs(logs, label)
                    if addr:
                        return addr
                except Exception as exc:
                    self.logger.debug(
                        "%s broad-scan chunk %d–%d failed: %s",
                        label, from_blk, to_blk, exc,
                    )

        except Exception as exc:
            self.logger.info(
                "%s factory event search failed: %s", label, exc,
            )
        return None

    @staticmethod
    def _extract_proxy_from_logs(logs, label=""):
        """Extract a proxy address from a list of event logs.

        Checks both ``log['data']`` (non-indexed params) and
        ``log['topics'][2]`` (third indexed topic) to handle different
        factory event formats.

        Returns the checksummed proxy address, or ``None``.
        """
        if not logs:
            return None
        last_log = logs[-1]

        # Try topics[2] first (common: Deploy(indexed owner, indexed proxy))
        topics = last_log.get("topics", [])
        if len(topics) >= 3:
            raw_topic = topics[2]
            if isinstance(raw_topic, bytes):
                raw_topic = raw_topic.hex()
            else:
                raw_topic = raw_topic.replace("0x", "")
            if len(raw_topic) >= 40:
                return Web3.to_checksum_address("0x" + raw_topic[-40:])

        # Fall back to data field (non-indexed param)
        raw_data = last_log.get("data", b"")
        if isinstance(raw_data, bytes):
            raw_hex = raw_data.hex()
        else:
            raw_hex = raw_data.replace("0x", "")
        if len(raw_hex) >= 40:
            return Web3.to_checksum_address("0x" + raw_hex[-40:])

        return None

    def discover_proxy_wallet(self):
        """Discover the Polymarket proxy wallet address for this EOA.

        Tries multiple methods in order of reliability:
        1. Config override (``proxy_address`` in config / ``PROXY_ADDRESS`` env).
        2. Cached result from a previous call.
        3. Polymarket profile API (Gamma / Data API).
        4. Legacy Proxy Factory — view functions then event logs.
        5. Safe Proxy Factory — event logs (``ProxyCreation``).
        6. Polygonscan API — search EOA tx history for factory calls.

        Sets ``self._proxy_is_safe`` when the wallet comes from the Safe factory.

        Returns the proxy address as a checksummed string, or ``None``.
        """
        # 0. Return cached result if we already discovered it
        if self._proxy_discovery_done:
            return self._proxy_address

        zero_addr = "0x" + "0" * 40

        # 1. Manual config override — most reliable
        cfg_addr = self.cfg.get("proxy_address", "")
        env_addr = os.environ.get("PROXY_ADDRESS", "").strip()
        if env_addr and env_addr.startswith("0x") and len(env_addr) == 42:
            cfg_addr = env_addr
        if cfg_addr and cfg_addr.startswith("0x") and len(cfg_addr) == 42:
            addr = Web3.to_checksum_address(cfg_addr)
            self.logger.info("Using configured proxy address: %s", addr)
            # Detect if it's a Safe by checking if execTransaction exists
            self._proxy_is_safe = self._check_is_safe(addr)
            self._proxy_address = addr
            self._proxy_discovery_done = True
            return addr

        self.logger.info(
            "No proxy_address in config (got %r) — trying auto-discovery...",
            self.cfg.get("proxy_address", ""),
        )

        # 2. Polymarket profile API — fast, no auth needed
        self.logger.info(
            "Attempting proxy discovery via Polymarket profile API..."
        )
        api_result = self._get_proxy_from_profile_api()
        if api_result:
            self._proxy_is_safe = self._check_is_safe(api_result)
            self._proxy_address = api_result
            self._proxy_discovery_done = True
            return api_result

        # ------ Legacy Proxy Factory ------
        factory_addr = Web3.to_checksum_address(PROXY_FACTORY_ADDRESS)
        factory = self.w3.eth.contract(
            address=factory_addr, abi=PROXY_FACTORY_ABI,
        )

        # 3. Try legacy factory view functions
        for fn_name in ("getProxy", "proxies", "proxyFor"):
            try:
                fn = getattr(factory.functions, fn_name)
                result = fn(self.address).call()
                if result and result != zero_addr:
                    addr = Web3.to_checksum_address(result)
                    self.logger.info(
                        "Discovered proxy wallet via factory.%s: %s",
                        fn_name, addr,
                    )
                    self._proxy_is_safe = False
                    self._proxy_address = addr
                    self._proxy_discovery_done = True
                    return addr
                self.logger.info(
                    "Factory.%s returned zero address — trying next method",
                    fn_name,
                )
            except Exception as exc:
                self.logger.info(
                    "Factory.%s call failed (%s) — trying next method",
                    fn_name, exc,
                )

        # 4. Search legacy factory event logs
        result = self._search_factory_events(PROXY_FACTORY_ADDRESS, is_safe=False)
        if result:
            self._proxy_is_safe = False
            self._proxy_address = result
            self._proxy_discovery_done = True
            return result

        # ------ Safe Proxy Factory ------
        # 5. Search Safe factory event logs (ProxyCreation events)
        self.logger.info(
            "Legacy factory had no results. Checking Safe Proxy Factory...",
        )
        result = self._search_factory_events(
            SAFE_PROXY_FACTORY_ADDRESS, is_safe=True,
        )
        if result:
            self._proxy_is_safe = True
            self._proxy_address = result
            self._proxy_discovery_done = True
            self.logger.info(
                "Wallet is a Gnosis Safe — will use execTransaction for proxy ops",
            )
            return result

        # ------ Polygonscan API fallback ------
        # 6. Search EOA's tx history on Polygonscan for factory calls
        self.logger.info(
            "On-chain factory searches failed. "
            "Trying Polygonscan API fallback...",
        )
        scan_result = self._get_proxy_from_polygonscan()
        if scan_result:
            self._proxy_is_safe = self._check_is_safe(scan_result)
            self._proxy_address = scan_result
            self._proxy_discovery_done = True
            return scan_result

        self.logger.warning(
            "Could not discover proxy wallet for EOA %s. "
            "Set PROXY_ADDRESS env var or proxy_address in config.json "
            "to provide it manually.",
            self.address,
        )
        self._proxy_discovery_done = True  # don't retry every 5 min
        return None

    def _check_is_safe(self, address):
        """Probe whether *address* is a Gnosis Safe by calling ``nonce()``."""
        try:
            safe = self.w3.eth.contract(
                address=Web3.to_checksum_address(address),
                abi=GNOSIS_SAFE_ABI,
            )
            safe.functions.nonce().call()
            self.logger.info(
                "Address %s responds to nonce() — treating as Gnosis Safe",
                address[:12] + "...",
            )
            return True
        except Exception:
            return False

    def get_proxy_usdc_balance(self, proxy_address):
        """Return the USDC balance held by *proxy_address* (Decimal, 6 dp)."""
        raw = self.usdc.functions.balanceOf(
            Web3.to_checksum_address(proxy_address)
        ).call()
        return Decimal(raw) / Decimal("1000000")

    def get_proxy_token_balance(self, proxy_address, token_id):
        """Return the conditional-token balance for *token_id* in the proxy."""
        return self.conditional_tokens.functions.balanceOf(
            Web3.to_checksum_address(proxy_address), int(token_id)
        ).call()

    def _execute_via_proxy(self, proxy_address, to, call_data):
        """Send a transaction through the proxy wallet.

        Dispatches to the appropriate execution method based on whether the
        wallet is a Gnosis Safe or a legacy Polymarket proxy.

        Returns the transaction receipt, or ``None`` on failure.
        """
        if self._proxy_is_safe:
            return self._execute_via_safe(proxy_address, to, call_data)
        return self._execute_via_legacy_proxy(proxy_address, to, call_data)

    def _execute_via_legacy_proxy(self, proxy_address, to, call_data):
        """Execute through a legacy Polymarket proxy wallet."""
        proxy = self.w3.eth.contract(
            address=Web3.to_checksum_address(proxy_address),
            abi=PROXY_WALLET_ABI,
        )
        tx = proxy.functions.execute(
            Web3.to_checksum_address(to),
            0,  # value — no native token transfer
            call_data,
        ).build_transaction(self._base_tx_params())
        return self._sign_and_send(tx)

    def _execute_via_safe(self, safe_address, to, call_data):
        """Execute a transaction through a Gnosis Safe (1-of-1 multisig).

        Builds the Safe transaction hash, signs it with this EOA's key,
        and calls ``execTransaction`` on the Safe contract.
        """
        safe_addr = Web3.to_checksum_address(safe_address)
        to_addr = Web3.to_checksum_address(to)
        safe = self.w3.eth.contract(address=safe_addr, abi=GNOSIS_SAFE_ABI)

        zero_addr = Web3.to_checksum_address("0x" + "0" * 40)
        safe_nonce = safe.functions.nonce().call()

        # Build the Safe transaction hash
        safe_tx_hash = safe.functions.getTransactionHash(
            to_addr,        # to
            0,              # value
            call_data,      # data
            0,              # operation (Call)
            0,              # safeTxGas
            0,              # baseGas
            0,              # gasPrice
            zero_addr,      # gasToken
            zero_addr,      # refundReceiver
            safe_nonce,     # _nonce
        ).call()

        # Sign the hash with our EOA private key
        signed = self.w3.eth.account.signHash(safe_tx_hash, self.private_key)
        # Encode signature as r + s + v (65 bytes)
        signature = (
            signed.r.to_bytes(32, "big")
            + signed.s.to_bytes(32, "big")
            + signed.v.to_bytes(1, "big")
        )

        # Build and send the execTransaction call
        tx = safe.functions.execTransaction(
            to_addr,        # to
            0,              # value
            call_data,      # data
            0,              # operation (Call)
            0,              # safeTxGas
            0,              # baseGas
            0,              # gasPrice
            zero_addr,      # gasToken
            zero_addr,      # refundReceiver
            signature,      # signatures
        ).build_transaction(self._base_tx_params())
        return self._sign_and_send(tx)

    def redeem_via_proxy(self, proxy_address, condition_id, neg_risk=False):
        """Redeem resolved positions held in the proxy wallet.

        Encodes a ``redeemPositions`` call and forwards it through the
        proxy's ``execute`` method.  For Neg Risk markets the call goes
        to the NegRiskAdapter; for standard markets it goes to the
        ConditionalTokens contract.
        """
        cond_bytes = bytes.fromhex(condition_id.replace("0x", ""))
        if neg_risk:
            call_data = _encode_abi(
                self.neg_risk_adapter,
                fn_name="redeemPositions",
                args=[cond_bytes, [1, 2]],
            )
            target = NEG_RISK_ADAPTER_ADDRESS
        else:
            call_data = _encode_abi(
                self.conditional_tokens,
                fn_name="redeemPositions",
                args=[
                    Web3.to_checksum_address(USDC_ADDRESS),
                    b"\x00" * 32,       # parentCollectionId (root)
                    cond_bytes,          # conditionId
                    [1, 2],             # both binary outcomes
                ],
            )
            target = CONDITIONAL_TOKENS_ADDRESS
        return self._execute_via_proxy(
            proxy_address, target, call_data,
        )

    def withdraw_usdc_from_proxy(self, proxy_address, amount=None):
        """Transfer USDC from the proxy wallet to this EOA.

        If *amount* is ``None``, the entire proxy USDC balance is withdrawn.
        *amount* should be a ``Decimal`` in human-readable units (e.g. 150.0).

        Returns the transaction receipt, or ``None`` on failure.
        """
        if amount is None:
            amount = self.get_proxy_usdc_balance(proxy_address)
        if amount <= 0:
            self.logger.debug("No USDC to withdraw from proxy %s", proxy_address)
            return None

        raw_amount = int(amount * Decimal("1000000"))
        call_data = _encode_abi(
            self.usdc,
            fn_name="transfer",
            args=[self.address, raw_amount],
        )
        self.logger.info(
            "Withdrawing $%.2f USDC from proxy %s → EOA %s",
            amount, proxy_address[:12] + "...", self.address[:12] + "...",
        )
        return self._execute_via_proxy(proxy_address, USDC_ADDRESS, call_data)

    def scan_and_redeem_proxy_portfolio(self):
        """Discover the proxy wallet, redeem settled positions, and withdraw USDC.

        Mirrors ``scan_and_redeem_portfolio`` but operates on the proxy
        wallet instead of the EOA directly.  Steps:

        1. Discover proxy address from the factory.
        2. For each token ID in the wallet's trade history, check if the
           proxy holds a balance.
        3. If the market is resolved → redeem via proxy.
        4. If ``proxy_withdraw`` is enabled → transfer USDC to EOA.

        Returns a list of result dicts.
        """
        if not self.cfg.get("proxy_redeem", True):
            return []
        if not self.clob_client:
            self.logger.info("Proxy scan skipped — no CLOB client")
            return []

        proxy_address = self.discover_proxy_wallet()
        if not proxy_address:
            self.logger.info("No proxy wallet found for this EOA — skipping proxy scan")
            return []

        # Report proxy balances
        try:
            proxy_usdc = self.get_proxy_usdc_balance(proxy_address)
            self.logger.info("Proxy %s USDC balance: $%.2f", proxy_address[:12] + "...", proxy_usdc)
        except Exception as exc:
            self.logger.warning("Could not fetch proxy USDC balance: %s", exc)
            proxy_usdc = Decimal("0")

        # Discover token IDs from trade history
        token_ids = self.clob_client.get_wallet_token_ids(self.address)
        if not token_ids:
            self.logger.info("No trade history — proxy redemption scan done")
            # Still attempt withdrawal if there's USDC sitting in the proxy
            if proxy_usdc > 0 and self.cfg.get("proxy_withdraw", True):
                return self._maybe_withdraw_proxy_usdc(proxy_address)
            return []

        self.logger.info(
            "Scanning %d token IDs for proxy-held positions...", len(token_ids),
        )

        results = []
        for token_id in token_ids:
            try:
                # Use persisted condition_id if available; fall back to API
                existing_pos = self._positions.get(token_id)
                condition_id = existing_pos.get("condition_id") if existing_pos else None
                neg_risk = existing_pos.get("neg_risk", False) if existing_pos else False

                if not condition_id:
                    market = self.clob_client.get_market_by_token(token_id)
                    if not market:
                        continue
                    condition_id = market.get("condition_id")
                    if not condition_id:
                        continue
                    neg_risk = self._is_neg_risk_market(market)

                # Check token balance on the correct contract for the proxy
                ct_balance = self._get_token_balance(
                    proxy_address, token_id, neg_risk=neg_risk,
                )
                if ct_balance == 0:
                    continue

                # On-chain resolution check — tries the API's condition_id
                # directly, then derives the real CTF conditionId.
                resolved_cid, payout_denom = self._resolve_condition_id(
                    condition_id, neg_risk=neg_risk,
                )
                if payout_denom == 0:
                    self.logger.info(
                        "Proxy holds active position: token %s, balance %d, neg_risk=%s",
                        token_id[:16] + "...", ct_balance, neg_risk,
                    )
                    continue

                question = market.get("question", "unknown")
                self.logger.info(
                    "PROXY REDEEM: Resolved position — token %s, "
                    "question: %s, proxy balance: %d, neg_risk: %s",
                    token_id[:16] + "...", question[:60], ct_balance, neg_risk,
                )

                if self.cfg.get("dry_run", False):
                    self.logger.info(
                        "[DRY RUN] Would redeem proxy position for condition %s",
                        resolved_cid[:16] + "...",
                    )
                    results.append({
                        "status": "dry_run",
                        "source": "proxy",
                        "token_id": token_id,
                        "balance": ct_balance,
                    })
                    continue

                receipt = self.redeem_via_proxy(
                    proxy_address, resolved_cid, neg_risk=neg_risk,
                )
                if receipt and receipt.status == 1:
                    self.logger.info(
                        "Proxy redemption OK: tx %s — USDC now in proxy",
                        receipt.transactionHash.hex(),
                    )
                    results.append({
                        "status": "redeemed",
                        "source": "proxy",
                        "token_id": token_id,
                        "balance": ct_balance,
                        "tx_hash": receipt.transactionHash.hex(),
                    })
                else:
                    self.logger.warning(
                        "Proxy redemption tx failed for token %s",
                        token_id[:16] + "...",
                    )
                    results.append({
                        "status": "failed",
                        "source": "proxy",
                        "token_id": token_id,
                    })

            except Exception as exc:
                self.logger.info(
                    "Error scanning proxy token %s: %s", token_id[:16] + "...", exc,
                )

        redeemed = sum(1 for r in results if r["status"] == "redeemed")
        self.logger.info(
            "Proxy scan complete: %d resolved position(s) processed, %d redeemed",
            len(results), redeemed,
        )

        # Withdraw any USDC accumulated in the proxy back to the EOA
        if self.cfg.get("proxy_withdraw", True) and not self.cfg.get("dry_run", False):
            withdraw_results = self._maybe_withdraw_proxy_usdc(proxy_address)
            results.extend(withdraw_results)

        return results

    def _maybe_withdraw_proxy_usdc(self, proxy_address):
        """Withdraw all USDC from the proxy to the EOA if balance > 0."""
        results = []
        try:
            balance = self.get_proxy_usdc_balance(proxy_address)
            if balance <= 0:
                return results

            if self.cfg.get("dry_run", False):
                self.logger.info(
                    "[DRY RUN] Would withdraw $%.2f USDC from proxy %s",
                    balance, proxy_address[:12] + "...",
                )
                results.append({
                    "status": "dry_run",
                    "source": "proxy_withdrawal",
                    "amount_usdc": float(balance),
                })
                return results

            receipt = self.withdraw_usdc_from_proxy(proxy_address)
            if receipt and receipt.status == 1:
                self.logger.info(
                    "Proxy USDC withdrawal OK: $%.2f → EOA, tx %s",
                    balance, receipt.transactionHash.hex(),
                )
                self.invalidate_balance_cache()
                results.append({
                    "status": "withdrawn",
                    "source": "proxy_withdrawal",
                    "amount_usdc": float(balance),
                    "tx_hash": receipt.transactionHash.hex(),
                })
            else:
                self.logger.warning("Proxy USDC withdrawal tx failed")
                results.append({
                    "status": "failed",
                    "source": "proxy_withdrawal",
                    "amount_usdc": float(balance),
                })
        except Exception as exc:
            self.logger.warning("Proxy USDC withdrawal error: %s", exc)
        return results

    def compute_copy_amount(self, original_usdc_amount):
        """Return the copy amount in USDC.

        Scales the original trade by copy_percentage, capped to max_trade.
        When the balance is available, also capped to 95% of it.
        """
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

            # Detect whether this is a Neg Risk market and capture the
            # condition_id — both are needed for correct exchange approval
            # and later redemption.
            neg_risk = False
            condition_id = None
            market = None
            if self.clob_client:
                try:
                    market = self.clob_client.get_market_by_token(token_id)
                    if market:
                        neg_risk = self._is_neg_risk_market(market)
                        condition_id = market.get("condition_id")
                except Exception:
                    pass

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

            # --- SELL guard: only sell tokens we actually hold ---
            if side == "SELL":
                pos = self._positions.get(token_id)
                held = pos["tokens"] if pos else Decimal("0")
                if held <= 0:
                    self.logger.info(
                        "SELL skipped – no position in token %s",
                        token_id[:16] + "...",
                    )
                    return None
                # Cap sell to what we own (in USDC terms at current price)
                if price_f > 0:
                    held_usdc = held * Decimal(str(price_f))
                    if copy_amount > held_usdc:
                        self.logger.info(
                            "SELL capped from $%.2f to $%.2f (held %.2f tokens)",
                            copy_amount, held_usdc, held,
                        )
                        copy_amount = held_usdc

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

            # Ensure USDC approval on the correct exchange.
            # Neg Risk markets use a different exchange contract.
            raw_amount = int(copy_amount * Decimal("1000000"))
            self.ensure_usdc_approval(CTF_EXCHANGE_ADDRESS, raw_amount)
            if neg_risk:
                self.ensure_usdc_approval(
                    NEG_RISK_CTF_EXCHANGE_ADDRESS, raw_amount,
                )

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

                    # Update position tracker — include all redemption params
                    tokens = (
                        copy_amount / Decimal(str(adjusted_price))
                        if adjusted_price > 0
                        else Decimal("0")
                    )
                    redeem_params = {
                        "neg_risk": neg_risk,
                        "condition_id": condition_id,
                        "collateral_token": USDC_ADDRESS,
                        "parent_collection_id": "0x" + "00" * 32,
                        "index_sets": [1, 2],
                    }
                    if side == "BUY":
                        pos = self._positions.get(token_id)
                        if pos:
                            # Weighted-average entry price
                            old_tokens = pos["tokens"]
                            old_cost = old_tokens * pos["entry_price"]
                            new_cost = tokens * Decimal(str(adjusted_price))
                            total_tokens = old_tokens + tokens
                            avg_price = (
                                (old_cost + new_cost) / total_tokens
                                if total_tokens > 0
                                else Decimal(str(adjusted_price))
                            )
                            pos["tokens"] = total_tokens
                            pos["entry_price"] = avg_price
                            pos.update(redeem_params)
                        else:
                            self._positions[token_id] = {
                                "tokens": tokens,
                                "entry_price": Decimal(str(adjusted_price)),
                                **redeem_params,
                            }
                    elif side == "SELL":
                        pos = self._positions.get(token_id)
                        if pos:
                            pos["tokens"] = max(pos["tokens"] - tokens, Decimal("0"))
                            # Remove position entirely if fully closed
                            if pos["tokens"] <= 0:
                                del self._positions[token_id]

                    # Log redemption parameters for every trade
                    self.logger.info(
                        "Redemption params for token %s: "
                        "conditionId=%s, collateralToken=%s, "
                        "parentCollectionId=0x00..00, indexSets=[1,2], "
                        "neg_risk=%s",
                        token_id[:16] + "...",
                        condition_id[:16] + "..." if condition_id else "UNKNOWN",
                        USDC_ADDRESS,
                        neg_risk,
                    )

                    cur_tokens = (
                        self._positions[token_id]["tokens"]
                        if token_id in self._positions
                        else Decimal("0")
                    )
                    self.logger.info(
                        "Position updated: token %s now %.2f tokens",
                        token_id[:16] + "...",
                        cur_tokens,
                    )

                    # Persist to disk so redemption params survive restarts
                    self._save_positions()

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
        self._session_start = None

        # Low-balance pause state.  When the USDC balance is too low to
        # place any order the bot stops copying new trades and waits for
        # open positions to settle.  Trading resumes once the balance
        # reaches resume_threshold_usdc (default $5, configurable).
        self._paused_low_balance = False

        # Trade history for the current session (for the stop report)
        self._trade_history = []

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
        # isn't locked up in GTC orders that never filled.
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
        self._session_start = datetime.now()
        self._trade_history = []
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self.logger.info("Bot started")

    def stop(self):
        self.running = False
        self.logger.info("Bot stop requested")
        # Report is generated at the end of _run_loop after the while
        # loop exits, so positions and balance are still accessible.

    def _run_loop(self):
        """Main monitoring loop."""
        # Initialise connections – CLOB first so executor can use it
        self._init_clob()

        web3_ok = self._init_web3()
        if web3_ok:
            self._init_executor()
            watched = self.cfg.get("watched_addresses", [])
            self.on_chain_monitor = OnChainMonitor(self.w3, watched, self.logger)

            # ---- Startup redemption (runs BEFORE copy-trading begins) ----
            self.logger.info("Startup: redeeming settled positions before monitoring...")

            if self.executor and self.cfg.get("auto_redeem_settled", True):
                try:
                    self.executor.scan_and_redeem_portfolio()
                except Exception as scan_exc:
                    self.logger.warning(
                        "Startup portfolio scan failed (non-fatal): %s",
                        scan_exc,
                    )

            if self.executor and self.cfg.get("proxy_redeem", True):
                try:
                    self.executor.scan_and_redeem_proxy_portfolio()
                except Exception as proxy_exc:
                    self.logger.warning(
                        "Startup proxy scan failed (non-fatal): %s",
                        proxy_exc,
                    )

            # Report balance after redemptions so user knows what's available
            if self.executor:
                try:
                    balance = self.executor.get_usdc_balance(max_age_seconds=0)
                    self.logger.info(
                        "Startup redemption complete — USDC balance: $%.2f",
                        balance,
                    )
                except Exception:
                    pass

        poll_interval = self.cfg.get("poll_interval_seconds", 15)
        self.logger.info(
            "Monitoring %d address(es), poll interval %ds",
            len(self.cfg.get("watched_addresses", [])),
            poll_interval,
        )

        resume_threshold = Decimal(
            str(self.cfg.get("resume_threshold_usdc", 5))
        )
        _last_pause_log = 0  # timestamp of last "still paused" INFO log
        _last_redeem_check = 0  # timestamp of last settled-position redemption scan
        _last_proxy_check = 0   # timestamp of last proxy wallet redemption scan
        _last_portfolio_scan = 0  # timestamp of last full portfolio scan

        while self.running:
            try:
                # --- Low-balance pause / resume check ---
                if self.executor:
                    try:
                        balance = self.executor.get_usdc_balance(max_age_seconds=0)
                        if self._paused_low_balance:
                            if balance >= resume_threshold:
                                self._paused_low_balance = False
                                self.logger.info(
                                    "Balance recovered to $%.2f (>= $%s) "
                                    "— resuming trading",
                                    balance, resume_threshold,
                                )
                            else:
                                # Log at INFO only every 5 minutes to
                                # avoid flooding the logs while idle.
                                now = time.time()
                                if now - _last_pause_log >= 300:
                                    self.logger.info(
                                        "Paused (low balance $%.2f, need $%s "
                                        "to resume) — waiting for settlements",
                                        balance, resume_threshold,
                                    )
                                    _last_pause_log = now
                        elif balance < LOW_BALANCE_PAUSE_THRESHOLD:
                            self._paused_low_balance = True
                            _last_pause_log = time.time()
                            self.logger.warning(
                                "Balance $%.2f below $%s — pausing new trades. "
                                "Will check for settlements every %ds and "
                                "resume when balance reaches $%s.",
                                balance,
                                LOW_BALANCE_PAUSE_THRESHOLD,
                                PAUSED_REDEEM_INTERVAL_SECONDS,
                                resume_threshold,
                            )
                    except Exception as bal_exc:
                        self.logger.debug(
                            "Balance check failed: %s", bal_exc,
                        )

                # --- Monitor open orders regardless of pause state ---
                # Orders must still be tracked so they can settle and
                # free up balance for the resume check.
                if self.executor:
                    try:
                        self.executor.monitor_open_orders()
                    except Exception as mon_exc:
                        self.logger.debug(
                            "Order monitor error: %s", mon_exc,
                        )

                # --- Auto-exit positions at take-profit / stop-loss ---
                # Runs even while paused so we protect existing positions.
                if self.executor:
                    try:
                        self.executor.check_exit_conditions()
                    except Exception as exit_exc:
                        self.logger.debug(
                            "Exit condition check error: %s", exit_exc,
                        )

                # --- Auto-redeem settled positions back to USDC ---
                # When paused (low balance) we check every 30 s so we
                # can redeem as trades resolve and resume quickly.
                # Otherwise checks every 5 min.
                now = time.time()
                redeem_interval = (
                    PAUSED_REDEEM_INTERVAL_SECONDS
                    if self._paused_low_balance
                    else REDEEM_CHECK_INTERVAL_SECONDS
                )
                _did_redeem = False
                if (
                    self.executor
                    and self.cfg.get("auto_redeem_settled", True)
                    and now - _last_redeem_check >= redeem_interval
                ):
                    _last_redeem_check = now
                    try:
                        redeemed = self.executor.check_and_redeem_settled()
                        if redeemed:
                            _did_redeem = True
                            self.logger.info(
                                "Auto-redeem: %d position(s) redeemed",
                                len(redeemed),
                            )
                    except Exception as redeem_exc:
                        self.logger.debug(
                            "Redemption check error: %s", redeem_exc,
                        )

                # --- Full portfolio scan (periodic) ---
                # Re-scans the wallet's complete trade history to catch
                # positions that resolved since the last check.  More
                # expensive than check_and_redeem_settled (which only
                # checks _positions dict) but catches positions that
                # were missed or not tracked.
                # When paused, runs every 15 min to sweep for anything
                # the lightweight check missed.
                if (
                    self.executor
                    and self.cfg.get("auto_redeem_settled", True)
                    and now - _last_portfolio_scan >= PORTFOLIO_SCAN_INTERVAL_SECONDS
                ):
                    _last_portfolio_scan = now
                    try:
                        scan_results = self.executor.scan_and_redeem_portfolio()
                        if scan_results:
                            _did_redeem = True
                            self.logger.info(
                                "Portfolio scan: %d position(s) processed",
                                len(scan_results),
                            )
                    except Exception as scan_exc:
                        self.logger.debug(
                            "Portfolio scan error: %s", scan_exc,
                        )

                # --- Proxy wallet redemption & withdrawal ---
                # Uses the same accelerated cadence while paused.
                proxy_interval = (
                    PAUSED_REDEEM_INTERVAL_SECONDS
                    if self._paused_low_balance
                    else REDEEM_CHECK_INTERVAL_SECONDS
                )
                if (
                    self.executor
                    and self.cfg.get("proxy_redeem", True)
                    and now - _last_proxy_check >= proxy_interval
                ):
                    _last_proxy_check = now
                    try:
                        proxy_results = self.executor.scan_and_redeem_proxy_portfolio()
                        if proxy_results:
                            _did_redeem = True
                            self.logger.info(
                                "Proxy check: %d result(s) processed",
                                len(proxy_results),
                            )
                    except Exception as proxy_exc:
                        self.logger.debug(
                            "Proxy redemption check error: %s", proxy_exc,
                        )

                # If we just redeemed while paused, immediately re-check
                # balance so we can resume without waiting another cycle.
                if _did_redeem and self._paused_low_balance and self.executor:
                    try:
                        self.executor.invalidate_balance_cache()
                        balance = self.executor.get_usdc_balance(max_age_seconds=0)
                        if balance >= resume_threshold:
                            self._paused_low_balance = False
                            self.logger.info(
                                "Redemption recovered funds — balance $%.2f "
                                "(>= $%s), resuming trading",
                                balance, resume_threshold,
                            )
                    except Exception:
                        pass  # will be rechecked next cycle

                # --- Skip trade detection & execution while paused ---
                if self._paused_low_balance:
                    pass  # just wait for balance to recover
                else:
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
                                "On-chain: %d trade(s) detected",
                                len(chain_trades),
                            )
                            new_trades.extend(chain_trades)

                    # --- Execute copies ---
                    for trade in new_trades:
                        if self.executor:
                            result = self.executor.execute_copy_trade(trade)
                            if result and isinstance(result, dict):
                                result["timestamp"] = datetime.now().isoformat()
                                self._trade_history.append(result)
                        else:
                            self.logger.warning(
                                "Trade detected but executor not ready: %s",
                                json.dumps(trade, default=str)[:200],
                            )

            except Exception as exc:
                self.logger.error("Error in monitoring loop: %s", exc, exc_info=True)

            # Sleep with early-exit check
            for _ in range(int(poll_interval)):
                if not self.running:
                    break
                time.sleep(1)

        self._generate_stop_report()
        self.logger.info("Bot stopped")

    def _generate_stop_report(self):
        """Write a session log file and a P/L report with redemption params.

        Creates two files in the working directory:
        - ``session_<timestamp>.log``  — copy of the bot log
        - ``session_<timestamp>_report.json`` — structured P/L + positions
        """
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        session_end = datetime.now()

        # ---- Build P/L report ----
        buy_total = sum(
            t.get("amount_usdc", 0)
            for t in self._trade_history
            if t.get("side") == "BUY" and t.get("status") == "submitted"
        )
        sell_total = sum(
            t.get("amount_usdc", 0)
            for t in self._trade_history
            if t.get("side") == "SELL" and t.get("status") == "submitted"
        )

        # Current balance
        current_balance = None
        if self.executor:
            try:
                current_balance = float(self.executor.get_usdc_balance(max_age_seconds=0))
            except Exception:
                pass

        # Collect open positions with full redemption params
        positions = {}
        if self.executor and hasattr(self.executor, "_positions"):
            for token_id, pos in self.executor._positions.items():
                tokens = pos.get("tokens", 0)
                if isinstance(tokens, Decimal):
                    tokens = float(tokens)
                entry_price = pos.get("entry_price", 0)
                if isinstance(entry_price, Decimal):
                    entry_price = float(entry_price)
                positions[token_id] = {
                    "tokens": tokens,
                    "entry_price": entry_price,
                    "cost_basis_usdc": round(tokens * entry_price, 6),
                    "redemption_params": {
                        "conditionId": pos.get("condition_id", "UNKNOWN"),
                        "collateralToken": pos.get("collateral_token", USDC_ADDRESS),
                        "parentCollectionId": pos.get(
                            "parent_collection_id", "0x" + "00" * 32,
                        ),
                        "indexSets": pos.get("index_sets", [1, 2]),
                        "neg_risk": pos.get("neg_risk", False),
                    },
                }

        report = {
            "session_start": (
                self._session_start.isoformat() if self._session_start else None
            ),
            "session_end": session_end.isoformat(),
            "usdc_balance_at_stop": current_balance,
            "total_trades": len(self._trade_history),
            "total_bought_usdc": round(buy_total, 6),
            "total_sold_usdc": round(sell_total, 6),
            "net_spent_usdc": round(buy_total - sell_total, 6),
            "trade_history": self._trade_history,
            "open_positions": positions,
        }

        # ---- Write report JSON ----
        report_path = f"session_{ts}_report.json"
        try:
            with open(report_path, "w") as f:
                json.dump(report, f, indent=2, default=str)
            self.logger.info("P/L report saved: %s", report_path)
        except Exception as exc:
            self.logger.warning("Failed to save P/L report: %s", exc)

        # ---- Write session log ----
        log_path = f"session_{ts}.log"
        try:
            src = "bot.log"
            if os.path.exists(src):
                import shutil
                shutil.copy2(src, log_path)
                self.logger.info("Session log saved: %s", log_path)
        except Exception as exc:
            self.logger.warning("Failed to save session log: %s", exc)

        # ---- Log summary to console ----
        self.logger.info(
            "Session summary: %d trades | bought $%.2f | sold $%.2f | "
            "net spent $%.2f | %d open position(s)",
            len(self._trade_history), buy_total, sell_total,
            buy_total - sell_total, len(positions),
        )
        if positions:
            self.logger.info(
                "Open positions with redemption params saved to %s",
                report_path,
            )


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
        max_frame = ttk.Frame(parent)
        max_frame.grid(row=row, column=1, sticky=tk.W, pady=3)
        self.max_trade_entry = ttk.Entry(max_frame, width=10)
        self.max_trade_entry.pack(side=tk.LEFT)
        ttk.Label(max_frame, text="(caps each copy trade)").pack(side=tk.LEFT, padx=5)

        # Slippage
        row += 1
        ttk.Label(parent, text="Slippage Tolerance (bps):").grid(row=row, column=0, sticky=tk.W, pady=3)
        self.slippage_entry = ttk.Entry(parent, width=20)
        self.slippage_entry.grid(row=row, column=1, sticky=tk.W, pady=3)

        # Resume threshold
        row += 1
        ttk.Label(parent, text="Resume Threshold (USDC):").grid(row=row, column=0, sticky=tk.W, pady=3)
        resume_frame = ttk.Frame(parent)
        resume_frame.grid(row=row, column=1, sticky=tk.W, pady=3)
        self.resume_threshold_entry = ttk.Entry(resume_frame, width=10)
        self.resume_threshold_entry.pack(side=tk.LEFT)
        ttk.Label(resume_frame, text="(min balance to resume after pause)").pack(side=tk.LEFT, padx=5)

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

        # Auto-redeem settled positions checkbox
        row += 1
        self.auto_redeem_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            parent,
            text="Auto-redeem settled positions (convert winning tokens back to USDC)",
            variable=self.auto_redeem_var,
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
        self.slippage_entry.insert(0, str(self.cfg.get("slippage_tolerance_bps", 100)))
        self.resume_threshold_entry.insert(0, str(self.cfg.get("resume_threshold_usdc", 5)))
        self.poll_entry.insert(0, str(self.cfg.get("poll_interval_seconds", 15)))
        self.use_clob_var.set(self.cfg.get("use_clob_api", True))
        self.dry_run_var.set(self.cfg.get("dry_run", False))
        self.auto_redeem_var.set(self.cfg.get("auto_redeem_settled", True))
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
        self.cfg["auto_redeem_settled"] = self.auto_redeem_var.get()
        # API credentials
        self.cfg["clob_api_key"] = self.api_key_entry.get().strip()
        self.cfg["clob_api_secret"] = self.api_secret_entry.get().strip()
        self.cfg["clob_api_passphrase"] = self.api_passphrase_entry.get().strip()
        try:
            self.cfg["max_trade_usdc"] = float(self.max_trade_entry.get().strip())
        except ValueError:
            pass
        try:
            self.cfg["slippage_tolerance_bps"] = int(self.slippage_entry.get().strip())
        except ValueError:
            pass
        try:
            self.cfg["resume_threshold_usdc"] = float(self.resume_threshold_entry.get().strip())
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
    if os.environ.get("RESUME_THRESHOLD_USDC"):
        cfg["resume_threshold_usdc"] = float(os.environ["RESUME_THRESHOLD_USDC"])
    if os.environ.get("DRY_RUN"):
        cfg["dry_run"] = os.environ["DRY_RUN"].lower() in ("1", "true", "yes")
    if os.environ.get("CLOB_API_KEY"):
        cfg["clob_api_key"] = os.environ["CLOB_API_KEY"]
    if os.environ.get("CLOB_API_SECRET"):
        cfg["clob_api_secret"] = os.environ["CLOB_API_SECRET"]
    if os.environ.get("CLOB_API_PASSPHRASE"):
        cfg["clob_api_passphrase"] = os.environ["CLOB_API_PASSPHRASE"]
    if os.environ.get("AUTO_REDEEM_SETTLED"):
        cfg["auto_redeem_settled"] = os.environ["AUTO_REDEEM_SETTLED"].lower() in ("1", "true", "yes")
    if os.environ.get("PROXY_REDEEM"):
        cfg["proxy_redeem"] = os.environ["PROXY_REDEEM"].lower() in ("1", "true", "yes")
    if os.environ.get("PROXY_WITHDRAW"):
        cfg["proxy_withdraw"] = os.environ["PROXY_WITHDRAW"].lower() in ("1", "true", "yes")
    if os.environ.get("PROXY_ADDRESS"):
        cfg["proxy_address"] = os.environ["PROXY_ADDRESS"].strip()

    if not cfg.get("watched_addresses"):
        logger.error("No watched addresses configured. Set WATCHED_ADDRESSES env var or edit config.json.")
        sys.exit(1)

    logger.info("=== Polymarket Copy Trader — Headless Mode ===")
    logger.info("Watched addresses: %s", cfg["watched_addresses"])
    logger.info("Copy %%: %s | Max trade: %s USDC | Resume threshold: $%s | Dry run: %s | Auto-redeem: %s",
                cfg.get("copy_percentage"), cfg.get("max_trade_usdc"),
                cfg.get("resume_threshold_usdc", 5), cfg.get("dry_run", False),
                cfg.get("auto_redeem_settled", True))
    logger.info("Proxy redeem: %s | Proxy withdraw: %s",
                cfg.get("proxy_redeem", True), cfg.get("proxy_withdraw", True))

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
