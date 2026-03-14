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
from concurrent.futures import ThreadPoolExecutor
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

# websocket-client for real-time eth_subscribe monitoring
try:
    import websocket as _ws_lib
    HAS_WS_CLIENT = True
except ImportError:
    _ws_lib = None
    HAS_WS_CLIENT = False


# ---------------------------------------------------------------------------
# _SyncWebSocketProvider – synchronous WebSocket JSON-RPC for web3.py 7.x
# ---------------------------------------------------------------------------
# web3.py 7.x removed synchronous WebSocket support; the built-in
# WebSocketProvider only works with AsyncWeb3.  This thin provider uses the
# ``websocket-client`` library to keep a persistent WebSocket connection open
# and exposes the synchronous ``make_request`` / ``is_connected`` interface
# that the regular (sync) ``Web3`` class requires.  All RPC traffic goes over
# the WebSocket instead of HTTP, giving lower latency for high-frequency
# polling and transaction submission.
# ---------------------------------------------------------------------------

_SyncWebSocketProvider = None  # populated below if deps are available

if _ws_lib is not None and Web3 is not None:
    try:
        from web3.providers.base import BaseProvider as _WsBaseProvider

        class _SyncWebSocketProvider(_WsBaseProvider):  # type: ignore[no-redef]
            """Synchronous WebSocket JSON-RPC provider (websocket-client)."""

            def __init__(self, ws_url, timeout=30):
                super().__init__()
                self._ws_url = ws_url
                self._timeout = timeout
                self._ws = None
                self._lock = threading.Lock()
                self._rid = 0
                self._connect()

            # -- connection management ----------------------------------

            def _connect(self):
                if self._ws is not None:
                    try:
                        self._ws.close()
                    except Exception:
                        pass
                ws = _ws_lib.WebSocket()
                ws.settimeout(self._timeout)
                ws.connect(self._ws_url)
                self._ws = ws

            # -- BaseProvider interface ---------------------------------

            def make_request(self, method, params):
                with self._lock:
                    self._rid += 1
                    request = {
                        "jsonrpc": "2.0",
                        "method": str(method),
                        "params": params if params is not None else [],
                        "id": self._rid,
                    }
                    last_exc = None
                    for attempt in range(3):
                        try:
                            self._ws.send(json.dumps(request))
                            return json.loads(self._ws.recv())
                        except (
                            _ws_lib.WebSocketConnectionClosedException,
                            ConnectionError,
                            BrokenPipeError,
                            OSError,
                        ) as exc:
                            last_exc = exc
                            if attempt < 2:
                                try:
                                    self._connect()
                                except Exception:
                                    pass
                    raise last_exc  # type: ignore[misc]

            def is_connected(self, show_traceback=False):
                try:
                    return self._ws is not None and self._ws.connected
                except Exception:
                    return False

    except ImportError:
        pass  # web3 installed without expected base class – leave as None


# ---------------------------------------------------------------------------
# Constants – Polymarket / Polygon addresses and ABIs
# ---------------------------------------------------------------------------

VERSION = "1.2.0"

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
TAKE_PROFIT_PRICE = Decimal("0.99")   # sell near top instead of waiting for resolution
STOP_LOSS_PCT = Decimal("0")          # disabled — follow whale's hold-to-resolution strategy

# Polymarket CLOB API base URL
CLOB_API_BASE = "https://clob.polymarket.com"

# Polymarket Gamma Markets API
GAMMA_API_BASE = "https://gamma-api.polymarket.com"

# Polymarket Data API (user activity, positions, trades)
DATA_API_BASE = "https://data-api.polymarket.com"

# Positions persistence file — stores redemption params so they
# survive restarts and don't depend on the Gamma API at redeem time.
POSITIONS_FILE = "positions.json"
TRADE_HISTORY_FILE = "trade_history.json"
SESSION_TRADES_FILE = "session_trades.json"

# Module-level lock protecting concurrent read-modify-write of
# trade_history.json from multiple threads (executor, arb, martingale).
_TRADE_HISTORY_LOCK = threading.Lock()
STRATEGY_SUMMARY_FILE = "strategy_summary.json"
MARTINGALE_HISTORY_FILE = "martingale_history.json"
MISSED_WINDOWS_FILE = "missed_windows.json"
_MISSED_WINDOWS_LOCK = threading.Lock()
MARTINGALE_SUMMARY_FILE = "martingale_summary.json"
MARTINGALE_EVENTS_FILE = "martingale_events.json"
_MARTINGALE_EVENTS_LOCK = threading.Lock()
USER_CONFIG_FILE = "config.json"  # persists RPC URLs, proxy address, etc.

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
        {"name":"","type":"bytes32"},
        {"name":"","type":"uint256"}],
     "name":"payoutNumerators",
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
     "type":"function"},
    {"constant":false,"inputs":[
        {"name":"operator","type":"address"},
        {"name":"approved","type":"bool"}],
     "name":"setApprovalForAll","outputs":[],
     "type":"function"},
    {"constant":true,"inputs":[
        {"name":"owner","type":"address"},
        {"name":"operator","type":"address"}],
     "name":"isApprovedForAll",
     "outputs":[{"name":"","type":"bool"}],
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
     "type":"function"},
    {"constant":false,"inputs":[
        {"name":"operator","type":"address"},
        {"name":"approved","type":"bool"}],
     "name":"setApprovalForAll","outputs":[],
     "type":"function"},
    {"constant":true,"inputs":[
        {"name":"owner","type":"address"},
        {"name":"operator","type":"address"}],
     "name":"isApprovedForAll",
     "outputs":[{"name":"","type":"bool"}],
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

LOG_FILE = "bot.log"

# Default configuration template
DEFAULT_CONFIG = {
    "rpc_url": "",
    "ws_rpc_url": "",
    "private_key_file": ".private_key",
    "watched_addresses": [],
    "copy_percentage": 50,
    "max_trade_usdc": 100.0,
    "slippage_tolerance_bps": 50,
    "gas_multiplier": 1.2,
    "poll_interval_seconds": 5,
    "use_clob_api": True,
    "clob_api_key": "",
    "clob_api_secret": "",
    "clob_api_passphrase": "",
    "dry_run": False,
    "order_ttl_seconds": 60,
    "resume_threshold_usdc": 5.0,
    "take_profit_price": 0.99,
    "take_profit_pct": 0,
    "stop_loss_pct": 0,
    "exit_check_seconds": 5,
    "exit_mode": "whale",
    "auto_redeem_settled": True,
    "proxy_redeem": True,
    "proxy_withdraw": True,
    "proxy_address": "",
    "webhook_url": "",
    "telegram_bot_token": "",
    "telegram_chat_id": "",
    "max_price_deviation_pct": 3,
    "max_loss_usdc": 0,
    "trade_max_age_seconds": 20,
    # --- Arbitrage mode (binary market spread capture) ---
    "arb_enabled": False,
    "arb_condition_ids": [],          # condition IDs of binary markets to monitor
    "arb_dynamic_slug": "",           # DEPRECATED — single slug (kept for backward compat)
    "arb_dynamic_window": 300,        # DEPRECATED — window for single slug (kept for compat)
    "arb_dynamic_slugs": [],          # list of {"slug": str, "window": int, "format": str}
                                      #   format: "timestamp" (default) or "hourly"
                                      #   e.g. [{"slug": "btc-updown-15m", "window": 900},
                                      #         {"slug": "ethereum-up-or-down", "window": 3600, "format": "hourly"}]
    "arb_min_edge_pct": 1.0,         # minimum spread % to trigger (e.g. 1.0 = 1%)
    "arb_size_usdc": 10.0,           # USDC to spend per side of each arb trade
    "arb_max_positions": 5,           # max simultaneous arb positions
    "arb_poll_seconds": 1,            # how often to scan orderbooks
    "arb_log_interval": 5,            # seconds between INFO-level arb scan logs
    # --- Martingale mode (double-on-loss binary market betting) ---
    "martingale_enabled": False,
    "martingale_direction": "Up",      # "Up" or "Down"
    "martingale_start_bet": 5.0,       # starting bet size in USDC
    "martingale_max_bet": 0,           # max bet cap in USDC (0 = no limit)
    "martingale_max_streak": 0,        # stop after N consecutive losses (0 = no limit)
    "martingale_streak_reset": True,   # reset bet to start_bet when streak recovery completes
    "martingale_hard_reset_streak": 0, # at streak Y, just reset to start_bet and keep going (0 = disabled)
    "martingale_recovery_candles": 5,   # number of candles to evaluate for recovery
    "martingale_recovery_green": 3,     # how many of those candles must be green to resume
    "martingale_recovery_interval": 300, # candle interval in seconds for recovery sampling
    "martingale_post_recovery_confirm": True,  # require confirmation candles on every bet after recovery
    "martingale_streak_confirm_at": 7,    # require trend confirmation before betting at this streak level (0 = disabled)
    "martingale_streak_confirm_green": 2, # green candles needed out of confirm_total
    "martingale_streak_confirm_total": 3, # total candles to sample for confirmation
    "martingale_slug_base": "btc-updown-5m",  # slug prefix for the market
    "martingale_window": 300,          # window size in seconds (300 = 5 min)
    "martingale_poll_seconds": 10,     # how often to check for resolution
    "martingale_price_min": 0.40,      # min ask price to accept (lower bound of buy range)
    "martingale_price_max": 0.55,      # max ask price to accept (upper bound of buy range)
    "martingale_max_entry_seconds": 60, # only bet in the first N seconds of a window
    # --- Multi-strategy martingale (overrides flat keys above when set) ---
    # List of strategy dicts, each with keys: name, slug_base, window,
    # direction, start_bet, max_bet, max_streak, price_min, price_max,
    # max_entry_seconds, poll_seconds.  Runs one independent bot per entry.
    # Example:
    #   [{"name": "BTC 5m", "slug_base": "btc-updown-5m", "window": 300,
    #     "start_bet": 1.0, "direction": "Up"},
    #    {"name": "BTC 15m", "slug_base": "btc-updown-15m", "window": 900,
    #     "start_bet": 5.0, "direction": "Up"}]
    "martingale_strategies": [],
}

# Keys from DEFAULT_CONFIG that are worth persisting across restarts.
# Sensitive secrets (private key, API keys) are excluded.
_PERSISTENT_CONFIG_KEYS = [
    "rpc_url", "ws_rpc_url", "proxy_address",
    "watched_addresses", "copy_percentage", "max_trade_usdc",
    "slippage_tolerance_bps", "poll_interval_seconds", "order_ttl_seconds",
    "resume_threshold_usdc", "take_profit_price", "take_profit_pct",
    "stop_loss_pct", "max_loss_usdc", "exit_check_seconds", "exit_mode",
    "dry_run", "auto_redeem_settled", "proxy_redeem", "proxy_withdraw",
    "max_price_deviation_pct", "trade_max_age_seconds",
    "arb_enabled", "arb_condition_ids", "arb_dynamic_slug",
    "arb_dynamic_window", "arb_dynamic_slugs", "arb_min_edge_pct",
    "arb_size_usdc", "arb_max_positions", "arb_poll_seconds",
    "webhook_url",
    "telegram_bot_token", "telegram_chat_id",
    "martingale_enabled", "martingale_direction", "martingale_start_bet",
    "martingale_max_bet", "martingale_max_streak",
    "martingale_slug_base", "martingale_window", "martingale_poll_seconds",
    "martingale_price_min", "martingale_price_max", "martingale_max_entry_seconds",
    "martingale_recovery_candles", "martingale_recovery_green",
    "martingale_recovery_interval",
    "martingale_strategies",
]


def load_user_config():
    """Load persisted settings from config.json, returning a dict.

    Only returns keys that are in _PERSISTENT_CONFIG_KEYS.
    Returns an empty dict if the file doesn't exist or is corrupt.
    """
    try:
        with open(USER_CONFIG_FILE, "r") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            return {}
        return {k: v for k, v in data.items() if k in _PERSISTENT_CONFIG_KEYS}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def save_user_config(cfg):
    """Persist user settings to config.json (only safe, non-secret keys)."""
    data = {k: cfg[k] for k in _PERSISTENT_CONFIG_KEYS if k in cfg}
    try:
        tmp = USER_CONFIG_FILE + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(data, fh, indent=2, default=str)
        os.replace(tmp, USER_CONFIG_FILE)
    except OSError:
        pass


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
# Webhook notifications (Discord / Slack / generic)
# ---------------------------------------------------------------------------

def send_webhook(url, message, logger=None):
    """Fire-and-forget a webhook notification.

    Supports Discord and Slack webhook URLs natively (sends the appropriate
    JSON payload).  For other URLs, sends ``{"text": message}``.

    Runs in a daemon thread so it never blocks the bot loop.
    """
    if not url or not requests:
        return

    def _post():
        try:
            if "discord" in url:
                payload = {"content": message[:2000]}
            else:
                # Slack and generic webhooks
                payload = {"text": message[:4000]}
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code >= 400 and logger:
                logger.debug("Webhook returned %d: %s", resp.status_code, resp.text[:200])
        except Exception as exc:
            if logger:
                logger.debug("Webhook failed: %s", exc)

    threading.Thread(target=_post, daemon=True).start()


# ---------------------------------------------------------------------------
# Telegram notifications & command bot
# ---------------------------------------------------------------------------

TELEGRAM_API = "https://api.telegram.org/bot{token}"


def _append_missed_window(record, logger=None):
    """Append a missed-window record to missed_windows.json (thread-safe)."""
    with _MISSED_WINDOWS_LOCK:
        try:
            with open(MISSED_WINDOWS_FILE, "r") as fh:
                history = json.load(fh)
        except (FileNotFoundError, json.JSONDecodeError):
            history = []
        history.append(record)
        try:
            tmp = MISSED_WINDOWS_FILE + ".tmp"
            with open(tmp, "w") as fh:
                json.dump(history, fh, indent=2, default=str)
            os.replace(tmp, MISSED_WINDOWS_FILE)
        except OSError as exc:
            if logger:
                logger.debug("Could not save missed_windows.json: %s", exc)


def _load_missed_windows():
    """Load all missed-window records from missed_windows.json."""
    try:
        with open(MISSED_WINDOWS_FILE, "r") as fh:
            return json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def send_telegram(token, chat_id, message, logger=None, parse_mode=None):
    """Fire-and-forget a Telegram message.

    Runs in a daemon thread so it never blocks the bot loop.
    """
    if not token or not chat_id or not requests:
        return

    def _post():
        try:
            url = TELEGRAM_API.format(token=token) + "/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": message[:4096],
            }
            if parse_mode:
                payload["parse_mode"] = parse_mode
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code >= 400 and logger:
                logger.debug(
                    "Telegram returned %d: %s",
                    resp.status_code, resp.text[:200],
                )
        except Exception as exc:
            if logger:
                logger.debug("Telegram send failed: %s", exc)

    threading.Thread(target=_post, daemon=True).start()


def send_telegram_photo(token, chat_id, photo_bytes, caption=None, logger=None):
    """Fire-and-forget a photo to Telegram.

    *photo_bytes* should be a bytes object (e.g. PNG image).
    Runs in a daemon thread so it never blocks the bot loop.
    """
    if not token or not chat_id or not requests:
        return

    def _post():
        try:
            url = TELEGRAM_API.format(token=token) + "/sendPhoto"
            files = {"photo": ("chart.png", photo_bytes, "image/png")}
            data = {"chat_id": chat_id}
            if caption:
                data["caption"] = caption[:1024]
            resp = requests.post(url, data=data, files=files, timeout=30)
            if resp.status_code >= 400 and logger:
                logger.debug(
                    "Telegram sendPhoto returned %d: %s",
                    resp.status_code, resp.text[:200],
                )
        except Exception as exc:
            if logger:
                logger.debug("Telegram sendPhoto failed: %s", exc)

    threading.Thread(target=_post, daemon=True).start()


class TelegramCommandBot:
    """Long-polls the Telegram Bot API for commands and replies with bot data.

    Supported commands:
        /balance   — current USDC & MATIC balances
        /positions — open positions with floating P&L
        /trades    — recent trade history (last 10)
        /stats     — session W/L record, win %, total P&L, ROI
        /status    — bot running state, session P&L, uptime
        /help      — list available commands
    """

    def __init__(self, token, chat_id, bot_ref, logger):
        self.token = token
        self.chat_id = str(chat_id)
        self.bot = bot_ref          # CopyTraderBot instance
        self.logger = logger
        self._stop_event = threading.Event()
        self._thread = None
        self._last_update_id = 0

    def start(self):
        if not self._verify_token():
            return  # token invalid — don't start polling
        self._delete_webhook()
        self._flush_old_updates()
        self._register_commands()
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()
        self.logger.info("Telegram command bot started (chat_id=%s)", self.chat_id)
        self.send("Commands active — type /help for available commands.")

    def _verify_token(self):
        """Call getMe to verify the bot token is valid."""
        try:
            url = TELEGRAM_API.format(token=self.token) + "/getMe"
            resp = requests.get(url, timeout=10)
            data = resp.json()
            if resp.status_code == 200 and data.get("ok"):
                bot_user = data.get("result", {})
                self.logger.info(
                    "Telegram bot verified: @%s (id=%s)",
                    bot_user.get("username", "?"), bot_user.get("id", "?"),
                )
                return True
            else:
                self.logger.error(
                    "Telegram bot token INVALID — getMe returned %d: %s",
                    resp.status_code, resp.text[:200],
                )
                return False
        except Exception as exc:
            self.logger.error("Telegram bot token check failed: %s", exc)
            return False

    def _delete_webhook(self):
        """Remove any active webhook so getUpdates polling works.

        The Telegram Bot API refuses to deliver updates via getUpdates
        while a webhook is set — returning 409 Conflict instead.  This
        silently breaks command handling while notifications (sendMessage)
        continue to work normally, making the issue hard to diagnose.
        """
        try:
            url = TELEGRAM_API.format(token=self.token) + "/deleteWebhook"
            resp = requests.post(url, timeout=10)
            if resp.status_code == 200 and resp.json().get("ok"):
                self.logger.info("Telegram webhook cleared (getUpdates enabled)")
            else:
                self.logger.warning(
                    "deleteWebhook returned unexpected response: %s", resp.text[:200],
                )
        except Exception as exc:
            self.logger.warning("Could not delete Telegram webhook: %s", exc)

    def _flush_old_updates(self):
        """Consume all pending updates so the poll loop starts fresh.

        Without this, stale updates from a previous session (or a
        competing consumer) can desync ``_last_update_id`` and cause
        the bot to either replay old commands or silently miss new ones.
        """
        try:
            base = TELEGRAM_API.format(token=self.token)
            resp = requests.get(
                base + "/getUpdates", params={"offset": -1, "timeout": 0},
                timeout=10,
            )
            if resp.status_code == 200:
                results = resp.json().get("result", [])
                if results:
                    self._last_update_id = results[-1]["update_id"]
                    self.logger.info(
                        "Telegram: flushed %d stale update(s), "
                        "resuming from update_id %d",
                        len(results), self._last_update_id,
                    )
                else:
                    self.logger.info("Telegram: no stale updates to flush")
        except Exception as exc:
            self.logger.warning("Telegram flush failed: %s", exc)

    def _register_commands(self):
        """Register all commands with Telegram via setMyCommands so they
        appear in the '/' command menu and are recognized by the client."""
        commands = [
            ("balance", "Current USDC & MATIC balances"),
            ("positions", "Open positions with floating P&L"),
            ("trades", "Recent trade history (last 10)"),
            ("stats", "Stats by timeframe (1D 7D 30D ALL)"),
            ("strategies", "Martingale strategy status"),
            ("status", "Bot state, session P&L, uptime"),
            ("chart", "Equity P&L chart (add 'session' for current)"),
            ("missed", "Missed windows (1D 7D 30D ALL)"),
            ("stop", "Stop the bot gracefully"),
            ("pause", "Pause trading (keep monitoring)"),
            ("resume", "Resume trading after pause"),
            ("kill", "Emergency kill switch"),
            ("toggle_arb", "Toggle arb mode on/off"),
            ("toggle_martingale", "Toggle martingale on/off"),
            ("toggle_copy", "Toggle copy trading on/off"),
            ("dry_run", "Toggle dry run mode on/off"),
            ("set_max_bet", "Set max bet size in USDC"),
            ("set_copy_pct", "Set copy percentage (0-100)"),
            ("set_max_trade", "Set max trade size in USDC"),
            ("set_min_edge", "Set minimum arb edge %"),
            ("set_max_loss", "Set max loss threshold in USDC"),
            ("set_exit", "Set exit strategy (whale|auto)"),
            ("sell", "Force sell position(s)"),
            ("reset_martingale", "Reset streak & bet size"),
            ("redeem", "Scan & redeem resolved positions"),
            ("help", "List available commands"),
        ]
        try:
            url = TELEGRAM_API.format(token=self.token) + "/setMyCommands"
            payload = {
                "commands": [
                    {"command": cmd, "description": desc}
                    for cmd, desc in commands
                ]
            }
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code == 200 and resp.json().get("ok"):
                self.logger.info("Telegram commands registered (%d commands)", len(commands))
            else:
                self.logger.warning("Failed to register Telegram commands: %s", resp.text)
        except Exception as exc:
            self.logger.warning("Could not register Telegram commands: %s", exc)

    def stop(self):
        self._stop_event.set()
        self.logger.info("Telegram command bot stopped")

    def send(self, text, parse_mode=None):
        """Send a message to the configured chat."""
        send_telegram(self.token, self.chat_id, text, self.logger, parse_mode)

    # -- polling loop -------------------------------------------------------

    def _poll_loop(self):
        base = TELEGRAM_API.format(token=self.token)
        _poll_err_count = 0
        _first_success = True
        while not self._stop_event.is_set():
            try:
                resp = requests.get(
                    base + "/getUpdates",
                    params={
                        "offset": self._last_update_id + 1,
                        "timeout": 5,
                    },
                    timeout=10,
                )
                if resp.status_code != 200:
                    _poll_err_count += 1
                    if _poll_err_count <= 3:
                        self.logger.warning(
                            "Telegram getUpdates HTTP %d: %s",
                            resp.status_code, resp.text[:300],
                        )
                    self._stop_event.wait(5)
                    continue
                _poll_err_count = 0
                if _first_success:
                    self.logger.info("Telegram poll loop active — listening for commands")
                    _first_success = False
                data = resp.json()
                for update in data.get("result", []):
                    self._last_update_id = update["update_id"]
                    # Accept both normal messages and edited messages
                    msg = update.get("message") or update.get("edited_message") or {}
                    # Only respond to our configured chat
                    chat_id = str(msg.get("chat", {}).get("id", ""))
                    if chat_id != self.chat_id:
                        continue
                    text = (msg.get("text") or "").strip()
                    if text.startswith("/"):
                        self._handle_command(text)
            except requests.exceptions.Timeout:
                continue
            except Exception as exc:
                self.logger.warning("Telegram poll error: %s", exc)
                self._stop_event.wait(5)

    # -- command handlers ---------------------------------------------------

    def _handle_command(self, text):
        parts = text.split()
        cmd = parts[0].lower().split("@")[0]  # strip @botname
        args = parts[1:]
        handlers = {
            "/balance": self._cmd_balance,
            "/positions": self._cmd_positions,
            "/trades": self._cmd_trades,
            "/stats": self._cmd_stats,
            "/strategies": self._cmd_strategies,
            "/status": self._cmd_status,
            "/chart": self._cmd_chart,
            "/missed": self._cmd_missed,
            "/help": self._cmd_help,
            "/start": self._cmd_help,
            # -- Control commands --
            "/stop": self._cmd_stop,
            "/pause": self._cmd_pause,
            "/resume": self._cmd_resume,
            "/kill": self._cmd_kill,
            # -- Toggles --
            "/toggle_arb": self._cmd_toggle_arb,
            "/toggle_martingale": self._cmd_toggle_martingale,
            "/toggle_copy": self._cmd_toggle_copy,
            "/dry_run": self._cmd_dry_run,
            # -- Parameter tuning --
            "/set_max_bet": self._cmd_set_max_bet,
            "/set_copy_pct": self._cmd_set_copy_pct,
            "/set_max_trade": self._cmd_set_max_trade,
            "/set_min_edge": self._cmd_set_min_edge,
            "/set_max_loss": self._cmd_set_max_loss,
            "/set_exit": self._cmd_set_exit,
            # -- Position management --
            "/sell": self._cmd_sell,
            "/reset_martingale": self._cmd_reset_martingale,
            "/redeem": self._cmd_redeem,
        }
        handler = handlers.get(cmd)
        if handler:
            try:
                handler(args)
            except Exception as exc:
                self.send(f"Error: {exc}")
        else:
            self.send(f"Unknown command: {cmd}\nType /help for available commands.")

    def _get_executor(self):
        if self.bot and hasattr(self.bot, "executor") and self.bot.executor:
            return self.bot.executor
        return None

    def _cmd_balance(self, args=None):
        executor = self._get_executor()
        if not executor:
            self.send("Bot not running — no balance data available.")
            return
        try:
            usdc = executor.get_usdc_balance(max_age_seconds=15)
        except Exception:
            usdc = None
        try:
            matic = executor.get_matic_balance()
        except Exception:
            matic = None

        lines = ["BALANCE"]
        lines.append(f"  USDC:  ${usdc:,.2f}" if usdc is not None else "  USDC:  unavailable")
        lines.append(f"  MATIC: {matic:,.4f}" if matic is not None else "  MATIC: unavailable")

        # Proxy balance
        proxy = self.bot.cfg.get("proxy_address", "")
        if proxy and proxy.startswith("0x") and len(proxy) == 42:
            try:
                proxy_usdc = executor.get_proxy_usdc_balance(proxy)
                lines.append(f"  Proxy: ${proxy_usdc:,.2f}")
            except Exception:
                pass

        self.send("\n".join(lines))

    def _cmd_positions(self, args=None):
        executor = self._get_executor()
        if not executor:
            self.send("Bot not running — no position data available.")
            return

        positions = dict(executor._positions)
        # Collect arb positions
        arb_mon = getattr(self.bot, "_arb_monitor", None)
        arb_positions = dict(arb_mon._active_positions) if arb_mon else {}
        # Collect martingale active bets
        mart_mgr = getattr(self.bot, "_martingale_mgr", None)
        mart_bets = []
        if mart_mgr:
            for mb in mart_mgr.bots:
                if mb._active_bet:
                    mart_bets.append((mb.strategy_name, mb._active_bet))

        total_count = len(positions) + len(arb_positions) + len(mart_bets)
        if total_count == 0:
            self.send("No open positions.")
            return

        lines = [f"OPEN POSITIONS ({total_count})"]
        total_cost = Decimal("0")
        total_value = Decimal("0")

        # --- Copy / direct positions ---
        for token_id, pos in positions.items():
            tokens = pos.get("tokens", Decimal("0"))
            entry_price = pos.get("entry_price", Decimal("0"))
            if tokens <= 0:
                continue
            market_name = pos.get("market_name") or (token_id[:16] + "...")
            cost_basis = tokens * entry_price
            total_cost += cost_basis

            # Try to get current price
            cur_price = None
            if self.bot.clob_client:
                try:
                    cur_price = self.bot.clob_client.get_last_trade_price(token_id)
                except Exception:
                    pass

            if cur_price is not None:
                cur_price_d = Decimal(str(cur_price))
                cur_value = tokens * cur_price_d
                total_value += cur_value
                float_pnl = cur_value - cost_basis
                pnl_sign = "+" if float_pnl >= 0 else ""
                lines.append(
                    f"\n  {market_name[:40]}\n"
                    f"    {tokens:.1f} shares @ ${entry_price:.4f}\n"
                    f"    Now ${cur_price_d:.4f} | P/L {pnl_sign}${float_pnl:.2f}"
                )
            else:
                total_value += cost_basis
                lines.append(
                    f"\n  {market_name[:40]}\n"
                    f"    {tokens:.1f} shares @ ${entry_price:.4f}"
                )

        # --- Arbitrage positions ---
        arb_total_profit = Decimal("0")
        for _cid, apos in arb_positions.items():
            a_cost = Decimal(str(apos.get("total_cost", 0)))
            a_profit = Decimal(str(apos.get("locked_profit", 0)))
            a_yes = Decimal(str(apos.get("yes_shares", 0)))
            a_no = Decimal(str(apos.get("no_shares", 0)))
            a_matched = min(a_yes, a_no)
            a_question = apos.get("question", "Unknown")
            is_partial = apos.get("partial", False)
            total_cost += a_cost
            arb_total_profit += a_profit
            tag = "ARB" if not is_partial else "ARB!"
            edge = apos.get("edge_pct", 0)
            lines.append(
                f"\n  [{tag}] {a_question[:36]}\n"
                f"    {a_matched:.1f} matched | cost ${a_cost:.2f}\n"
                f"    Locked P/L +${a_profit:.4f} (edge {edge:.1f}%)"
            )

        # --- Martingale active bets ---
        for strat_name, bet in mart_bets:
            m_cost = Decimal(str(bet.get("cost", 0)))
            m_shares = Decimal(str(bet.get("shares", 0)))
            direction = bet.get("direction", "?")
            question = bet.get("question", "?")
            total_cost += m_cost
            entry_p = m_cost / m_shares if m_shares > 0 else Decimal("0")
            lines.append(
                f"\n  [MART-{direction}] {question[:34]}\n"
                f"    {m_shares:.1f} shares @ ${entry_p:.4f}\n"
                f"    [{strat_name}] pending resolution"
            )

        total_pnl = total_value - total_cost + arb_total_profit
        pnl_sign = "+" if total_pnl >= 0 else ""
        lines.append(f"\nTotal: cost ${total_cost:.2f} | value ${total_value:.2f} | {pnl_sign}${total_pnl:.2f}")
        self.send("\n".join(lines))

    def _cmd_trades(self, args=None):
        # Try session trades first, then trade_history.json
        trades = []
        if self.bot:
            trades = list(getattr(self.bot, "_trade_history", []))
        if not trades:
            try:
                history_file = self.bot.cfg.get("trade_history_file", "trade_history.json") if self.bot else "trade_history.json"
                with open(history_file, "r") as fh:
                    trades = json.load(fh)
            except (FileNotFoundError, json.JSONDecodeError):
                pass

        if not trades:
            self.send("No trade history available.")
            return

        recent = trades[-10:]  # last 10
        lines = [f"RECENT TRADES (last {len(recent)} of {len(trades)})"]
        for t in reversed(recent):
            # Three record types:
            #  1) Copy trade result (in-memory): has side, amount_usdc, price
            #  2) Closed/resolved position (_log_closed_trade): has market,
            #     entry_price, cost_basis_usdc, outcome
            #  3) Martingale (_log_bet): like #2 but reason="martingale"
            has_entry = "entry_price" in t or "cost_basis_usdc" in t
            if has_entry:
                # Types 2 & 3: resolved positions and martingale
                outcome = t.get("outcome", "")
                label = t.get("market", outcome or "closed")
                amount = t.get("cost_basis_usdc", 0)
                price = t.get("entry_price", 0)
                ts = t.get("closed_at", "")
            else:
                # Type 1: in-memory copy trade result
                label = t.get("side", "?")
                amount = t.get("amount_usdc", t.get("cost", 0))
                price = t.get("price", 0)
                ts = t.get("timestamp", t.get("ts", ""))

            pnl = t.get("pnl_usdc", "")
            line = f"  {label} ${float(amount):,.2f} @ ${float(price):.4f}"
            if pnl:
                line += f" | P/L ${float(pnl):+.2f}"
            slip = t.get("slippage") or {}
            vs_fair = slip.get("vs_fair", 0)
            if vs_fair:
                line += f" | slip {vs_fair:+.4f}"
            if ts:
                ts_short = str(ts).split("T")[-1][:8] if "T" in str(ts) else str(ts)[-8:]
                line += f" [{ts_short}]"
            lines.append(line)

        self.send("\n".join(lines))

    @staticmethod
    def _bucket_stats(records):
        """Compute W/L/sold/cost/proceeds/pnl for a list of trade records."""
        wins = sum(1 for r in records if r.get("outcome") in ("won", "win"))
        losses = sum(1 for r in records if r.get("outcome") in ("lost", "loss"))
        sold = sum(1 for r in records if r.get("outcome") == "sold")
        cost = sum(r.get("cost_basis_usdc", 0) for r in records)
        proceeds = sum(r.get("proceeds_usdc", 0) for r in records)
        pnl = proceeds - cost
        decided = wins + losses
        win_pct = (wins / decided * 100) if decided > 0 else 0
        roi = (pnl / cost * 100) if cost > 0 else 0
        return {
            "n": len(records), "wins": wins, "losses": losses, "sold": sold,
            "cost": cost, "proceeds": proceeds, "pnl": pnl,
            "win_pct": win_pct, "roi": roi,
        }

    def _load_session_history(self):
        """Load trade_history.json filtered to the current session."""
        history = []
        try:
            history_file = (
                self.bot.cfg.get("trade_history_file", TRADE_HISTORY_FILE)
                if self.bot else TRADE_HISTORY_FILE
            )
            with open(history_file, "r") as fh:
                history = json.load(fh)
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        session_start = None
        if self.bot:
            ss = getattr(self.bot, "_session_start", None)
            if ss:
                session_start = ss.isoformat() if hasattr(ss, "isoformat") else str(ss)
        if session_start:
            history = [
                r for r in history
                if r.get("closed_at", "") >= session_start
            ]
        return history

    def _load_history_with_timeframe(self, timeframe=None):
        """Load trade_history.json with optional timeframe filter.

        Supported timeframes: '1D' (today), '7D', '30D', 'ALL', 'session' (default).
        Returns (filtered_history, label) tuple.
        """
        history = []
        try:
            history_file = (
                self.bot.cfg.get("trade_history_file", TRADE_HISTORY_FILE)
                if self.bot else TRADE_HISTORY_FILE
            )
            with open(history_file, "r") as fh:
                history = json.load(fh)
        except (FileNotFoundError, json.JSONDecodeError):
            pass

        if not timeframe or timeframe.lower() == "session":
            # Default: current session only
            session_start = None
            if self.bot:
                ss = getattr(self.bot, "_session_start", None)
                if ss:
                    session_start = ss.isoformat() if hasattr(ss, "isoformat") else str(ss)
            if session_start:
                history = [
                    r for r in history
                    if r.get("closed_at", "") >= session_start
                ]
            return history, "SESSION"

        tf = timeframe.upper()
        if tf == "ALL":
            return history, "ALL TIME"

        # Rolling window filtering using LOCAL time (matching how
        # trade timestamps are stored via datetime.now().isoformat()).
        from datetime import timedelta
        now = datetime.now()
        if tf == "1D":
            cutoff = now - timedelta(hours=24)
            label = "LAST 24 HOURS"
        elif tf == "7D":
            cutoff = now - timedelta(days=7)
            label = "LAST 7 DAYS"
        elif tf == "30D":
            cutoff = now - timedelta(days=30)
            label = "LAST 30 DAYS"
        else:
            # Unknown — fall back to session
            session_start = None
            if self.bot:
                ss = getattr(self.bot, "_session_start", None)
                if ss:
                    session_start = ss.isoformat() if hasattr(ss, "isoformat") else str(ss)
            if session_start:
                history = [
                    r for r in history
                    if r.get("closed_at", "") >= session_start
                ]
            return history, "SESSION"

        # Use naive isoformat (no +00:00 suffix) to match trade timestamps
        cutoff_str = cutoff.isoformat()
        history = [
            r for r in history
            if r.get("closed_at", "") >= cutoff_str
        ]
        return history, label

    def _cmd_stats(self, args=None):
        """Statistics: bets placed, W/L, win %, total return,
        plus per-strategy breakdown for martingale.

        Usage: /stats [1D|7D|30D|ALL|session]
        Defaults to current session if no timeframe given.
        """
        timeframe = args[0] if args else None
        history, label = self._load_history_with_timeframe(timeframe)

        if not history:
            self.send(f"No trades for {label}.")
            return

        # -- Session runtime --
        runtime_str = ""
        if self.bot:
            start = getattr(self.bot, "_session_start", None)
            if start:
                delta = datetime.now() - start
                total_s = int(delta.total_seconds())
                hours, rem = divmod(total_s, 3600)
                mins, secs = divmod(rem, 60)
                if hours > 0:
                    runtime_str = f"{hours}h {mins}m {secs}s"
                else:
                    runtime_str = f"{mins}m {secs}s"

        # -- Overall totals --
        t = self._bucket_stats(history)
        header = f"STATS — {label}"
        if runtime_str:
            header += f"  (runtime: {runtime_str})"
        lines = [
            header,
            f"  Bets placed: {t['n']}",
            f"  Won: {t['wins']}  |  Lost: {t['losses']}"
            + (f"  |  Sold: {t['sold']}" if t["sold"] else ""),
            f"  Win rate: {t['win_pct']:.1f}%",
            "",
            f"  Deployed: ${t['cost']:,.2f}",
            f"  Returned: ${t['proceeds']:,.2f}",
            f"  Net P&L: ${t['pnl']:+,.2f}",
            f"  ROI: {t['roi']:+.1f}%",
        ]

        # -- Split by source: positions, arb, martingale --
        mart_trades = [r for r in history if r.get("reason") == "martingale"]
        arb_trades = [r for r in history
                      if r.get("reason", "").startswith("arb")]
        pos_trades = [r for r in history
                      if r.get("reason") != "martingale"
                      and not r.get("reason", "").startswith("arb")]

        # Determine label: only say "COPY TRADING" if copy trading is on
        copy_active = bool(
            self.bot and self.bot.cfg.get("watched_addresses")
        )
        pos_label = "COPY TRADING" if copy_active else "POSITIONS"

        # Always show per-source breakdown so users can see what drove P&L
        if pos_trades:
            c = self._bucket_stats(pos_trades)
            lines += [
                "",
                pos_label,
                f"  {c['n']} trades  |  W/L {c['wins']}/{c['losses']}"
                f" ({c['win_pct']:.0f}%)  |  P&L ${c['pnl']:+,.2f}",
            ]
        if arb_trades:
            a = self._bucket_stats(arb_trades)
            lines += [
                "",
                "ARBITRAGE",
                f"  {a['n']} trades  |  P&L ${a['pnl']:+,.2f}",
            ]

        # -- Per-strategy martingale breakdown --
        if mart_trades:
            # Group by strategy name from trade records
            strat_buckets = {}
            for r in mart_trades:
                details = r.get("martingale_details") or {}
                name = details.get("strategy", "default")
                strat_buckets.setdefault(name, []).append(r)

            lines += ["", "MARTINGALE"]
            for name, records in strat_buckets.items():
                s = self._bucket_stats(records)
                sl = MartingaleBot._aggregate_slippage(records)
                slip_tag = ""
                if sl["avg_fill_price"] > 0:
                    slip_tag = f"  |  avg fill ${sl['avg_fill_price']:.3f} slip ${sl['total_slippage_usdc']:+,.2f}"
                lines.append(
                    f"  [{name}]  {s['n']} bets  |  "
                    f"W/L {s['wins']}/{s['losses']} ({s['win_pct']:.0f}%)  |  "
                    f"P&L ${s['pnl']:+,.2f}  |  "
                    f"deployed ${s['cost']:,.2f}{slip_tag}"
                )

            # Live bot state (current bet, streak)
            mgr = getattr(self.bot, "_martingale_mgr", None) if self.bot else None
            if mgr:
                lines.append("")
                lines.append("LIVE STATE")
                for mb in mgr.bots:
                    active = "ACTIVE" if mb._active_bet else "waiting"
                    lines.append(
                        f"  [{mb.strategy_name}] "
                        f"next=${mb.current_bet:.2f}  |  "
                        f"streak={mb.consecutive_losses}  |  "
                        f"{active}"
                    )

        # -- Slippage summary for session --
        slippage_records = [r for r in history if r.get("slippage")]
        if slippage_records:
            sl = MartingaleBot._aggregate_slippage(slippage_records)
            lines += [
                "",
                "SLIPPAGE (vs $0.50 fair — positive = good)",
                f"  Bets tracked: {sl['bets_with_slippage_data']}",
                f"  Total slippage: ${sl['total_slippage_usdc']:+,.4f}",
                f"  Avg slippage: {sl['avg_slippage']:+.4f}/sh",
                f"  Avg fill price: ${sl['avg_fill_price']:.4f}",
                f"  Best fill: {sl['best_fill']:+.4f}/sh",
                f"  Worst fill: {sl['worst_fill']:+.4f}/sh",
            ]

        self.send("\n".join(lines))

    def _cmd_strategies(self, args=None):
        """Per-strategy diagnostic view — shows why each martingale
        strategy is or isn't betting."""
        mgr = getattr(self.bot, "_martingale_mgr", None) if self.bot else None
        if not mgr or not mgr.bots:
            self.send("No martingale strategies configured.")
            return

        lines = ["STRATEGY DIAGNOSTICS"]
        for mb in mgr.bots:
            window = int(mb._scfg("window", "martingale_window", 300))
            slug_base = mb._scfg("slug_base", "martingale_slug_base", "?")
            now = int(time.time())
            window_ts = mb._get_window_ts(window)
            secs_in = now - window_ts
            next_slug = f"{slug_base}-{window_ts}"

            lines.append("")
            lines.append(f"[{mb.strategy_name}]")
            lines.append(f"  slug: {next_slug}")
            lines.append(f"  window: {window}s  |  {secs_in}s in")
            lines.append(
                f"  dir: {mb.direction}  |  bet: ${mb.current_bet:.2f}  |  "
                f"streak: {mb.consecutive_losses}"
            )
            lines.append(f"  P&L: ${mb.session_pnl:+,.2f}")

            # Counters
            lines.append(
                f"  windows: {mb._windows_attempted} bet  |  "
                f"{mb._windows_no_market} no-market"
            )

            # Current state
            if mb._active_bet:
                bet = mb._active_bet
                time_left = bet["window_end"] - now
                lines.append(
                    f"  ACTIVE: {bet['direction']} ${bet['cost']:.2f} "
                    f"@ ${bet.get('fill_price', 0):.4f}  |  "
                    f"resolves in {max(time_left, 0)}s"
                )
            elif mb._skip_reason:
                lines.append(f"  BLOCKED: {mb._skip_reason}")
            elif mb._stop_event.is_set():
                lines.append("  STOPPED")
            else:
                lines.append("  waiting for next window")

        self.send("\n".join(lines))

    def _cmd_status(self, args=None):
        running = self.bot.running if self.bot else False
        lines = [f"BOT STATUS: {'RUNNING' if running else 'STOPPED'}"]

        if running and self.bot:
            # Uptime
            start = getattr(self.bot, "_session_start", None)
            if start:
                delta = datetime.now() - start
                hours, rem = divmod(int(delta.total_seconds()), 3600)
                minutes = rem // 60
                lines.append(f"  Uptime: {hours}h {minutes}m")

            # Session trades
            trade_count = len(getattr(self.bot, "_trade_history", []))
            lines.append(f"  Session trades: {trade_count}")

            # Modes
            modes = []
            if self.bot.cfg.get("watched_addresses"):
                modes.append(f"Copy ({len(self.bot.cfg['watched_addresses'])} addr)")
            if getattr(self.bot, "_arb_monitor", None):
                modes.append("Arbitrage")
            mgr = getattr(self.bot, "_martingale_mgr", None)
            if mgr:
                for mg in mgr.bots:
                    status_extra = ""
                    if mg._streak_paused:
                        favorable = sum(
                            1 for c in mg._recovery_candles
                            if c.get("favorable", c.get("green"))
                        )
                        total = len(mg._recovery_candles)
                        n_needed = int(mg._scfg(
                            "recovery_green", "martingale_recovery_green", 3))
                        n_candles = int(mg._scfg(
                            "recovery_candles", "martingale_recovery_candles", 5))
                        paused_mins = ""
                        if mg._streak_paused_at:
                            delta = datetime.now() - mg._streak_paused_at
                            paused_mins = f", paused {int(delta.total_seconds() // 60)}m"
                        status_extra = (
                            f" [PAUSED — recovery {favorable}/{total} "
                            f"favorable, need {n_needed}/{n_candles}{paused_mins}]"
                        )
                    lines.append(
                        f"  Martingale [{mg.strategy_name}]: "
                        f"streak={mg.consecutive_losses}, "
                        f"bet=${mg.current_bet:.2f}, "
                        f"P/L=${mg.session_pnl:+.2f}{status_extra}"
                    )
                modes.append(f"Martingale ({len(mgr.bots)} strat)")
            lines.append(f"  Modes: {', '.join(modes) if modes else 'none'}")

            # Dry run
            if self.bot.cfg.get("dry_run"):
                lines.append("  DRY RUN MODE")

        self.send("\n".join(lines))

    def _cmd_missed(self, args=None):
        """Analyze windows missed due to price out of range / other reasons.

        Usage: /missed [1D|7D|30D|ALL|session]
        Defaults to current session if no timeframe given.
        """
        # Load from persistent file (historical) for non-session queries
        timeframe = args[0] if args else None
        tf = (timeframe or "").upper()

        if tf in ("1D", "7D", "30D", "ALL"):
            all_missed = _load_missed_windows()
            if tf != "ALL":
                from datetime import timedelta
                now = datetime.now()
                if tf == "1D":
                    cutoff = now.replace(hour=0, minute=0, second=0, microsecond=0)
                elif tf == "7D":
                    cutoff = (now - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
                else:  # 30D
                    cutoff = (now - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
                cutoff_str = cutoff.isoformat()
                all_missed = [m for m in all_missed if m.get("ts", "") >= cutoff_str]
            label = {"1D": "TODAY", "7D": "LAST 7 DAYS", "30D": "LAST 30 DAYS", "ALL": "ALL TIME"}[tf]
        else:
            # Default: current session from in-memory bot data
            mart_mgr = getattr(self.bot, "_martingale_mgr", None) if self.bot else None
            if not mart_mgr:
                self.send("Martingale not active — no missed window data.")
                return
            all_missed = []
            for mb in mart_mgr.bots:
                for m in mb._missed_windows:
                    all_missed.append(dict(m))
            label = "SESSION"

        if not all_missed:
            self.send(f"No missed windows for {label}.")
            return

        all_missed.sort(key=lambda r: r.get("ts", ""))

        # --- Breakdown by reason ---
        reason_counts = {}
        for m in all_missed:
            reason = m.get("reason", "unknown")
            # Normalize reasons to clean buckets
            if "outside" in reason:
                reason = "price outside range"
            elif "thin book" in reason:
                reason = "thin book"
            elif "too late" in reason:
                reason = "too late"
            elif "low balance" in reason:
                reason = "low balance"
            elif "market not found" in reason:
                reason = "market not found"
            elif "FOK rejected" in reason:
                reason = "FOK rejected"
            elif "order failed" in reason:
                reason = "order failed"
            elif "no asks" in reason:
                reason = "no asks"
            reason_counts[reason] = reason_counts.get(reason, 0) + 1

        # --- By streak level at time of miss ---
        streak_at_miss = {}
        for m in all_missed:
            s = m.get("streak", 0)
            streak_at_miss[s] = streak_at_miss.get(s, 0) + 1

        # --- Estimate capital at risk from missed windows ---
        total_missed_exposure = sum(m.get("bet_would_be", 0) for m in all_missed)

        # --- Consecutive missed windows ---
        # Group by strategy and find the worst streak of consecutive misses
        by_strat = {}
        for m in all_missed:
            s = m.get("strategy", "default")
            by_strat.setdefault(s, []).append(m)

        max_consec = 0
        for strat, misses in by_strat.items():
            wts = sorted(set(m.get("window_ts", 0) for m in misses))
            if len(wts) < 2:
                max_consec = max(max_consec, len(wts))
                continue
            # Infer window size from gaps
            gaps = [wts[i+1] - wts[i] for i in range(len(wts)-1)]
            window_s = min(gaps) if gaps else 300
            consec = 1
            best = 1
            for i in range(1, len(wts)):
                if wts[i] - wts[i-1] == window_s:
                    consec += 1
                    best = max(best, consec)
                else:
                    consec = 1
            max_consec = max(max_consec, best)

        # --- Build message ---
        lines = [f"MISSED WINDOWS — {label} ({len(all_missed)} total)"]

        lines.append("\nBy Reason:")
        for reason, cnt in sorted(reason_counts.items(), key=lambda x: -x[1]):
            pct = cnt / len(all_missed) * 100
            lines.append(f"  {reason}: {cnt} ({pct:.0f}%)")

        lines.append("\nBy Streak at Time of Miss:")
        for streak, cnt in sorted(streak_at_miss.items()):
            label = f"streak {streak}" if streak > 0 else "no streak"
            lines.append(f"  {label}: {cnt} missed")

        # --- Gap Up / Gap Down analysis ---
        gap_ups = [m for m in all_missed if m.get("gap") == "gap_up"]
        gap_downs = [m for m in all_missed if m.get("gap") == "gap_down"]
        price_misses = gap_ups + gap_downs
        non_price = len(all_missed) - len(price_misses)

        if price_misses:
            lines.append("\nGap Analysis (price misses):")
            if gap_ups:
                up_prices = [m["ask_price"] for m in gap_ups if m.get("ask_price")]
                up_exposure = sum(m.get("bet_would_be", 0) for m in gap_ups)
                lines.append(
                    f"  GAP UP (too high): {len(gap_ups)}"
                    f" ({len(gap_ups) / len(all_missed) * 100:.0f}%)"
                )
                if up_prices:
                    lines.append(
                        f"    prices: ${min(up_prices):.4f} – "
                        f"${max(up_prices):.4f} "
                        f"(avg ${sum(up_prices)/len(up_prices):.4f})"
                    )
                    lines.append(f"    exposure missed: ${up_exposure:,.2f}")
            if gap_downs:
                dn_prices = [m["ask_price"] for m in gap_downs if m.get("ask_price")]
                dn_exposure = sum(m.get("bet_would_be", 0) for m in gap_downs)
                lines.append(
                    f"  GAP DOWN (too low): {len(gap_downs)}"
                    f" ({len(gap_downs) / len(all_missed) * 100:.0f}%)"
                )
                if dn_prices:
                    lines.append(
                        f"    prices: ${min(dn_prices):.4f} – "
                        f"${max(dn_prices):.4f} "
                        f"(avg ${sum(dn_prices)/len(dn_prices):.4f})"
                    )
                    lines.append(f"    exposure missed: ${dn_exposure:,.2f}")
            if non_price > 0:
                lines.append(f"  Other (non-price): {non_price}")

        # --- All ask prices at time of miss ---
        all_prices = [m.get("ask_price") for m in all_missed if m.get("ask_price")]
        if all_prices:
            lines.append("\nAsk Price at Miss (all):")
            lines.append(
                f"  min ${min(all_prices):.4f}  |  "
                f"avg ${sum(all_prices)/len(all_prices):.4f}  |  "
                f"max ${max(all_prices):.4f}"
            )

        lines.append(f"\nMax Consecutive Misses: {max_consec}")
        lines.append(f"Total Exposure Missed: ${total_missed_exposure:,.2f}")

        # Time range
        first = all_missed[0].get("ts", "?")[:16].replace("T", " ")
        last = all_missed[-1].get("ts", "?")[:16].replace("T", " ")
        lines.append(f"\nPeriod: {first} → {last}")

        self.send("\n".join(lines))

    @staticmethod
    def _parse_date_range(text):
        """Parse a date range string like '2/27-2/28' or '02/27-02/28'.

        Returns (start_date, end_date) as datetime.date objects, or None
        if the text doesn't match a date range pattern.  The year defaults
        to the current year.  End-date is *inclusive* (the filter will
        include the entire end day).
        """
        import re
        m = re.match(r"^(\d{1,2})/(\d{1,2})\s*-\s*(\d{1,2})/(\d{1,2})$", text.strip())
        if not m:
            return None
        try:
            now = datetime.now()
            start = datetime(now.year, int(m.group(1)), int(m.group(2))).date()
            end = datetime(now.year, int(m.group(3)), int(m.group(4))).date()
            return start, end
        except ValueError:
            return None

    def _cmd_chart(self, args=None):
        """Generate a cumulative P&L equity chart and send it as a photo."""
        try:
            import matplotlib
            matplotlib.use("Agg")  # headless backend
            import matplotlib.pyplot as plt
            import matplotlib.dates as mdates
            import io
        except ImportError:
            self.send("Chart unavailable — matplotlib not installed.\nRun: pip install matplotlib")
            return

        # Load trade history
        try:
            with open(TRADE_HISTORY_FILE, "r") as fh:
                history = json.load(fh)
        except (FileNotFoundError, json.JSONDecodeError):
            history = []

        if not history:
            self.send("No trade data yet — chart unavailable.")
            return

        history.sort(key=lambda r: r.get("closed_at", ""))

        # --- Filters: /chart session  OR  /chart M/D-M/D ---
        filter_label = None
        session_only = args and args[0].lower() == "session"
        date_range = None
        if args and not session_only:
            date_range = self._parse_date_range(" ".join(args))

        if session_only and self.bot:
            ss = getattr(self.bot, "_session_start", None)
            if ss:
                session_start = ss.isoformat() if hasattr(ss, "isoformat") else str(ss)
                history = [r for r in history if r.get("closed_at", "") >= session_start]
        elif date_range:
            start_date, end_date = date_range
            # Include full end day (up to 23:59:59)
            start_iso = datetime.combine(start_date, datetime.min.time()).isoformat()
            end_iso = datetime.combine(end_date, datetime.max.time()).isoformat()
            history = [
                r for r in history
                if start_iso <= r.get("closed_at", "") <= end_iso
            ]
            filter_label = f"{start_date.strftime('%m/%d')}–{end_date.strftime('%m/%d')}"

        if not history:
            msg = "No trades in current session." if session_only else "No trades in the selected date range."
            self.send(msg)
            return

        # Build cumulative P&L series
        timestamps = []
        cum_pnl = []
        running = 0.0
        for rec in history:
            running += rec.get("pnl_usdc", 0)
            ts_str = rec.get("closed_at", "")
            try:
                dt = datetime.fromisoformat(ts_str)
            except (ValueError, TypeError):
                continue
            timestamps.append(dt)
            cum_pnl.append(round(running, 2))

        if len(timestamps) < 2:
            self.send("Not enough data points for a chart.")
            return

        # Generate chart
        fig, ax = plt.subplots(figsize=(10, 5))
        fig.patch.set_facecolor("#1e1e1e")
        ax.set_facecolor("#1e1e1e")

        final_pnl = cum_pnl[-1]
        line_color = "#00cc44" if final_pnl >= 0 else "#cc4444"
        fill_color = "#0a3d0a" if final_pnl >= 0 else "#3d0a0a"

        ax.plot(timestamps, cum_pnl, color=line_color, linewidth=2)
        ax.fill_between(timestamps, cum_pnl, 0, color=fill_color, alpha=0.5)
        ax.axhline(y=0, color="#666666", linewidth=0.8)

        # --- Annotate key data points (high, low, latest) ---
        max_pnl = max(cum_pnl)
        min_pnl = min(cum_pnl)
        max_idx = cum_pnl.index(max_pnl)
        min_idx = cum_pnl.index(min_pnl)
        pnl_range = max(abs(max_pnl - min_pnl), 1)

        annotated_indices = set()

        # High point
        if max_pnl != 0:
            ax.annotate(
                f"High ${max_pnl:+,.2f}\n{timestamps[max_idx].strftime('%m/%d %H:%M')}",
                xy=(timestamps[max_idx], max_pnl),
                xytext=(0, 12), textcoords="offset points",
                fontsize=7, color="#00ff88", fontweight="bold",
                ha="center", va="bottom",
                arrowprops=dict(arrowstyle="-", color="#00ff88", lw=0.8),
            )
            ax.plot(timestamps[max_idx], max_pnl, "o", color="#00ff88", markersize=5, zorder=5)
            annotated_indices.add(max_idx)

        # Low point
        if min_pnl != 0 and min_idx != max_idx:
            ax.annotate(
                f"Low ${min_pnl:+,.2f}\n{timestamps[min_idx].strftime('%m/%d %H:%M')}",
                xy=(timestamps[min_idx], min_pnl),
                xytext=(0, -12), textcoords="offset points",
                fontsize=7, color="#ff6666", fontweight="bold",
                ha="center", va="top",
                arrowprops=dict(arrowstyle="-", color="#ff6666", lw=0.8),
            )
            ax.plot(timestamps[min_idx], min_pnl, "o", color="#ff6666", markersize=5, zorder=5)
            annotated_indices.add(min_idx)

        # Latest point
        last_idx = len(cum_pnl) - 1
        if last_idx not in annotated_indices:
            last_color = "#00ff88" if final_pnl >= 0 else "#ff6666"
            ax.annotate(
                f"Now ${final_pnl:+,.2f}",
                xy=(timestamps[last_idx], final_pnl),
                xytext=(8, 0), textcoords="offset points",
                fontsize=7, color=last_color, fontweight="bold",
                ha="left", va="center",
            )
            ax.plot(timestamps[last_idx], final_pnl, "o", color=last_color, markersize=5, zorder=5)
            annotated_indices.add(last_idx)

        # --- Interval labels along the curve ---
        n_pts = len(cum_pnl)
        if n_pts > 10:
            interval = max(n_pts // 8, 1)
            for i in range(interval, n_pts - interval // 2, interval):
                if i in annotated_indices:
                    continue
                # Alternate above/below to avoid overlap
                offset_y = 10 if cum_pnl[i] >= 0 else -10
                va = "bottom" if offset_y > 0 else "top"
                ax.annotate(
                    f"${cum_pnl[i]:+,.2f}",
                    xy=(timestamps[i], cum_pnl[i]),
                    xytext=(0, offset_y), textcoords="offset points",
                    fontsize=6, color="#aaaaaa", ha="center", va=va,
                )
                ax.plot(timestamps[i], cum_pnl[i], "o", color="#888888", markersize=3, zorder=4)

        ax.set_title(
            f"Cumulative P&L: ${final_pnl:+,.2f}  ({len(cum_pnl)} trades)",
            color=line_color, fontsize=13, fontweight="bold",
        )
        ax.set_ylabel("P&L ($)", color="#aaaaaa", fontsize=10)
        ax.tick_params(colors="#aaaaaa", labelsize=8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_color("#444444")
        ax.spines["bottom"].set_color("#444444")
        ax.grid(axis="y", color="#333333", linestyle="--", linewidth=0.5)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d %H:%M"))
        fig.autofmt_xdate(rotation=30, ha="right")
        plt.tight_layout()

        # Render to PNG bytes
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=150, facecolor=fig.get_facecolor())
        plt.close(fig)
        buf.seek(0)
        png_bytes = buf.read()

        range_str = filter_label or f"{timestamps[0].strftime('%m/%d %H:%M')} → {timestamps[-1].strftime('%m/%d %H:%M')}"
        caption = f"Equity Chart | {range_str} | P&L ${final_pnl:+,.2f}"
        send_telegram_photo(self.token, self.chat_id, png_bytes, caption=caption, logger=self.logger)

    def _cmd_help(self, args=None):
        self.send(
            "Polymarket Bot Commands:\n"
            "\n"
            "INFO\n"
            "/balance — USDC & MATIC balances\n"
            "/positions — open positions with P/L\n"
            "/trades — recent trade history\n"
            "/stats [1D|7D|30D|ALL] — W/L, win %, P&L, ROI\n"
            "/strategies — per-strategy diagnostics\n"
            "/status — bot state, uptime, session info\n"
            "/chart — equity P&L chart\n"
            "   /chart session — current session only\n"
            "   /chart 2/27-2/28 — filter by date range\n"
            "/missed [1D|7D|30D|ALL] — missed windows analysis\n"
            "\n"
            "CONTROL\n"
            "/stop — graceful shutdown\n"
            "/pause — pause new trades (keeps exits running)\n"
            "/resume — resume after pause\n"
            "/kill — emergency kill switch\n"
            "\n"
            "TOGGLES\n"
            "/toggle_arb on|off\n"
            "/toggle_martingale on|off\n"
            "/toggle_copy on|off\n"
            "/dry_run on|off\n"
            "\n"
            "PARAMETERS\n"
            "/set_max_bet <usdc>\n"
            "/set_copy_pct <0-100>\n"
            "/set_max_trade <usdc>\n"
            "/set_min_edge <pct>\n"
            "/set_max_loss <usdc>\n"
            "/set_exit whale|auto\n"
            "\n"
            "ACTIONS\n"
            "/sell <token_id|all> — force sell position(s)\n"
            "/reset_martingale [strategy] — reset streak & bet\n"
            "/redeem — scan & redeem resolved positions\n"
            "/help — this message"
        )

    # -------------------------------------------------------------------
    # Control commands
    # -------------------------------------------------------------------

    def _cmd_stop(self, args=None):
        """Graceful shutdown — stops all subsystems, lets active bets settle."""
        if not self.bot:
            self.send("No bot instance.")
            return
        if not self.bot.running:
            self.send("Bot is already stopped.")
            return
        self.send("Stopping bot gracefully...")
        self.bot.stop()

    def _cmd_pause(self, args=None):
        """Pause new trade detection without full shutdown."""
        if not self.bot:
            self.send("No bot instance.")
            return
        if not self.bot.running:
            self.send("Bot is not running.")
            return
        if self.bot._paused_low_balance:
            self.send("Already paused.")
            return
        self.bot._paused_low_balance = True
        self.logger.info("MANUAL PAUSE via Telegram")
        self.send(
            "PAUSED — no new trades will be placed.\n"
            "Exit checks & redemptions still running.\n"
            "Use /resume to continue."
        )

    def _cmd_resume(self, args=None):
        """Resume trading after a manual pause."""
        if not self.bot:
            self.send("No bot instance.")
            return
        if not self.bot.running:
            self.send("Bot is not running.")
            return
        if not self.bot._paused_low_balance:
            self.send("Bot is not paused.")
            return
        self.bot._paused_low_balance = False
        self.logger.info("MANUAL RESUME via Telegram")
        self.send("RESUMED — trading is active.")

    def _cmd_kill(self, args=None):
        """Emergency kill switch — stops everything immediately."""
        if not self.bot:
            self.send("No bot instance.")
            return
        executor = self._get_executor()
        if executor:
            executor.kill_switch_triggered = True
        self.send("KILL SWITCH ACTIVATED — stopping all trading.")
        self.logger.critical("KILL SWITCH triggered via Telegram")
        self.bot.stop()

    # -------------------------------------------------------------------
    # Toggle commands
    # -------------------------------------------------------------------

    def _cmd_toggle_arb(self, args):
        """Toggle arbitrage on/off.  Usage: /toggle_arb on|off"""
        if not self.bot:
            self.send("No bot instance.")
            return
        if not args:
            cur = self.bot.cfg.get("arb_enabled", False)
            self.send(f"Arbitrage is {'ON' if cur else 'OFF'}.\nUsage: /toggle_arb on|off")
            return
        val = args[0].lower()
        if val not in ("on", "off"):
            self.send("Usage: /toggle_arb on|off")
            return
        new_val = val == "on"
        self.bot.cfg["arb_enabled"] = new_val
        save_user_config(self.bot.cfg)
        self.send(f"Arbitrage {'ENABLED' if new_val else 'DISABLED'}.")

    def _cmd_toggle_martingale(self, args):
        """Toggle martingale on/off.  Usage: /toggle_martingale on|off"""
        if not self.bot:
            self.send("No bot instance.")
            return
        if not args:
            cur = self.bot.cfg.get("martingale_enabled", False)
            self.send(
                f"Martingale is {'ON' if cur else 'OFF'}.\n"
                "Usage: /toggle_martingale on|off"
            )
            return
        val = args[0].lower()
        if val not in ("on", "off"):
            self.send("Usage: /toggle_martingale on|off")
            return
        new_val = val == "on"
        self.bot.cfg["martingale_enabled"] = new_val
        save_user_config(self.bot.cfg)
        self.send(f"Martingale {'ENABLED' if new_val else 'DISABLED'}.")

    def _cmd_toggle_copy(self, args):
        """Toggle copy trading on/off.  Usage: /toggle_copy on|off

        Stores watched_addresses in _watched_addresses_backup when
        toggling off so they can be restored with /toggle_copy on.
        """
        if not self.bot:
            self.send("No bot instance.")
            return
        if not args:
            cur = bool(self.bot.cfg.get("watched_addresses"))
            self.send(
                f"Copy trading is {'ON' if cur else 'OFF'}.\n"
                "Usage: /toggle_copy on|off"
            )
            return
        val = args[0].lower()
        if val not in ("on", "off"):
            self.send("Usage: /toggle_copy on|off")
            return
        if val == "off":
            addrs = self.bot.cfg.get("watched_addresses", [])
            if addrs:
                self.bot._watched_addresses_backup = list(addrs)
                self.bot.cfg["watched_addresses"] = []
                save_user_config(self.bot.cfg)
                self.send(
                    f"Copy trading DISABLED ({len(addrs)} address(es) backed up).\n"
                    "Use /toggle_copy on to restore."
                )
            else:
                self.send("Copy trading is already off.")
        else:
            backup = getattr(self.bot, "_watched_addresses_backup", None)
            current = self.bot.cfg.get("watched_addresses", [])
            if current:
                self.send(
                    f"Copy trading already on ({len(current)} address(es))."
                )
            elif backup:
                self.bot.cfg["watched_addresses"] = backup
                save_user_config(self.bot.cfg)
                self.send(
                    f"Copy trading ENABLED — restored {len(backup)} address(es)."
                )
            else:
                self.send(
                    "No addresses to restore. Add watched_addresses "
                    "to config.json manually."
                )

    def _cmd_dry_run(self, args):
        """Toggle dry-run mode.  Usage: /dry_run on|off"""
        if not self.bot:
            self.send("No bot instance.")
            return
        if not args:
            cur = self.bot.cfg.get("dry_run", False)
            self.send(f"Dry run is {'ON' if cur else 'OFF'}.\nUsage: /dry_run on|off")
            return
        val = args[0].lower()
        if val not in ("on", "off"):
            self.send("Usage: /dry_run on|off")
            return
        new_val = val == "on"
        self.bot.cfg["dry_run"] = new_val
        save_user_config(self.bot.cfg)
        self.send(f"Dry run {'ENABLED' if new_val else 'DISABLED'}.")

    # -------------------------------------------------------------------
    # Parameter tuning commands
    # -------------------------------------------------------------------

    def _cmd_set_max_bet(self, args):
        """Set martingale max bet cap.  Usage: /set_max_bet <usdc>"""
        if not self.bot:
            self.send("No bot instance.")
            return
        if not args:
            cur = self.bot.cfg.get("martingale_max_bet", 0)
            self.send(
                f"Current max bet: ${cur:.2f} (0 = no limit)\n"
                "Usage: /set_max_bet <usdc>"
            )
            return
        try:
            val = float(args[0])
        except ValueError:
            self.send("Invalid number. Usage: /set_max_bet <usdc>")
            return
        if val < 0:
            self.send("Max bet cannot be negative.")
            return
        self.bot.cfg["martingale_max_bet"] = val
        # Also update per-strategy dicts so _scfg picks it up immediately
        mgr = getattr(self.bot, "_martingale_mgr", None)
        if mgr:
            for mb in mgr.bots:
                mb.strategy["max_bet"] = val
        save_user_config(self.bot.cfg)
        label = f"${val:.2f}" if val > 0 else "unlimited"
        self.send(f"Martingale max bet set to {label}.")

    def _cmd_set_copy_pct(self, args):
        """Set copy trade percentage.  Usage: /set_copy_pct <0-100>"""
        if not self.bot:
            self.send("No bot instance.")
            return
        if not args:
            cur = self.bot.cfg.get("copy_percentage", 50)
            self.send(f"Current copy %: {cur}%\nUsage: /set_copy_pct <0-100>")
            return
        try:
            val = float(args[0])
        except ValueError:
            self.send("Invalid number. Usage: /set_copy_pct <0-100>")
            return
        if val < 0 or val > 100:
            self.send("Value must be between 0 and 100.")
            return
        self.bot.cfg["copy_percentage"] = val
        save_user_config(self.bot.cfg)
        executor = self._get_executor()
        if executor:
            executor.copy_pct = Decimal(str(val)) / Decimal("100")
        self.send(f"Copy percentage set to {val:.0f}%.")

    def _cmd_set_max_trade(self, args):
        """Set max trade size in USDC.  Usage: /set_max_trade <usdc>"""
        if not self.bot:
            self.send("No bot instance.")
            return
        if not args:
            cur = self.bot.cfg.get("max_trade_usdc", 100)
            self.send(f"Current max trade: ${cur:.2f}\nUsage: /set_max_trade <usdc>")
            return
        try:
            val = float(args[0])
        except ValueError:
            self.send("Invalid number. Usage: /set_max_trade <usdc>")
            return
        if val <= 0:
            self.send("Max trade must be positive.")
            return
        self.bot.cfg["max_trade_usdc"] = val
        save_user_config(self.bot.cfg)
        executor = self._get_executor()
        if executor:
            executor.max_trade = Decimal(str(val))
        self.send(f"Max trade size set to ${val:.2f}.")

    def _cmd_set_min_edge(self, args):
        """Set arb minimum edge %.  Usage: /set_min_edge <pct>"""
        if not self.bot:
            self.send("No bot instance.")
            return
        if not args:
            cur = self.bot.cfg.get("arb_min_edge_pct", 1.0)
            self.send(f"Current min edge: {cur}%\nUsage: /set_min_edge <pct>")
            return
        try:
            val = float(args[0])
        except ValueError:
            self.send("Invalid number. Usage: /set_min_edge <pct>")
            return
        if val < 0:
            self.send("Min edge cannot be negative.")
            return
        self.bot.cfg["arb_min_edge_pct"] = val
        save_user_config(self.bot.cfg)
        self.send(f"Arb min edge set to {val:.2f}%.")

    def _cmd_set_max_loss(self, args):
        """Set session max loss for kill switch.  Usage: /set_max_loss <usdc>"""
        if not self.bot:
            self.send("No bot instance.")
            return
        if not args:
            cur = self.bot.cfg.get("max_loss_usdc", 0)
            self.send(
                f"Current max loss: ${cur:.2f} (0 = disabled)\n"
                "Usage: /set_max_loss <usdc>"
            )
            return
        try:
            val = float(args[0])
        except ValueError:
            self.send("Invalid number. Usage: /set_max_loss <usdc>")
            return
        if val < 0:
            self.send("Max loss cannot be negative.")
            return
        self.bot.cfg["max_loss_usdc"] = val
        save_user_config(self.bot.cfg)
        label = f"${val:.2f}" if val > 0 else "disabled"
        self.send(f"Kill switch max loss set to {label}.")

    def _cmd_set_exit(self, args):
        """Set exit mode.  Usage: /set_exit whale|auto"""
        if not self.bot:
            self.send("No bot instance.")
            return
        if not args:
            cur = self.bot.cfg.get("exit_mode", "whale")
            self.send(f"Current exit mode: {cur}\nUsage: /set_exit whale|auto")
            return
        val = args[0].lower()
        if val not in ("whale", "auto"):
            self.send("Usage: /set_exit whale|auto")
            return
        self.bot.cfg["exit_mode"] = val
        save_user_config(self.bot.cfg)
        desc = (
            "whale (hold until resolution/whale sell)"
            if val == "whale" else
            "auto (take-profit & stop-loss active)"
        )
        self.send(f"Exit mode set to {desc}.")

    # -------------------------------------------------------------------
    # Position management commands
    # -------------------------------------------------------------------

    def _cmd_sell(self, args):
        """Force sell a position.  Usage: /sell <token_id|all>"""
        executor = self._get_executor()
        if not executor:
            self.send("Bot not running — cannot sell.")
            return
        if not args:
            self.send("Usage: /sell <token_id|all>\nUse /positions to see token IDs.")
            return
        if self.bot.cfg.get("dry_run", False):
            self.send("Cannot sell in dry-run mode.")
            return
        target = args[0].lower()
        positions = dict(executor._positions)
        if not positions:
            self.send("No open positions.")
            return

        if target == "all":
            self.send(f"Force selling {len(positions)} position(s)...")
            sold = 0
            for token_id, pos in list(positions.items()):
                ok = self._force_sell_position(executor, token_id, pos)
                if ok:
                    sold += 1
            self.send(f"Sold {sold}/{len(positions)} positions.")
        else:
            # Match by full token_id or prefix
            match = None
            for tid in positions:
                if tid == target or tid.startswith(target):
                    match = tid
                    break
            if not match:
                self.send(f"No position found matching '{target}'.")
                return
            pos = positions[match]
            market = pos.get("market_name") or match[:16] + "..."
            self.send(f"Force selling {market}...")
            ok = self._force_sell_position(executor, match, pos)
            if ok:
                self.send(f"Sell submitted for {market}.")
            else:
                self.send(f"Failed to sell {market}. Check logs.")

    def _force_sell_position(self, executor, token_id, pos):
        """Place a market sell for a single position. Returns True on success."""
        # Never sell martingale-owned tokens — they resolve on-chain.
        if (token_id in executor._martingale_token_ids
                or self.bot.cfg.get("martingale_enabled", False)):
            self.logger.warning(
                "FORCE SELL BLOCKED (martingale): refusing to sell %s",
                token_id[:16] + "...",
            )
            self.send(
                f"Cannot sell {pos.get('market_name') or token_id[:16]}... "
                f"— owned by martingale strategy (resolves on-chain)."
            )
            return False
        tokens = pos.get("tokens", Decimal("0"))
        if tokens <= 0:
            return False
        try:
            price = executor.clob_client.get_last_trade_price(token_id)
            if price is None:
                price = float(pos.get("entry_price", 0.5))
            price_d = Decimal(str(price))
            sell_usdc = float(tokens * price_d)
            slippage_mult = Decimal(str(executor.slippage_bps)) / Decimal("10000")
            adjusted = float(price_d * (Decimal("1") - slippage_mult))
            adjusted = max(adjusted, 0.01)
            neg_risk = pos.get("neg_risk", False)
            executor.ensure_ct_approval(neg_risk=neg_risk)
            result = executor.clob_client.place_order(
                token_id=token_id,
                side="SELL",
                size_usdc=sell_usdc,
                price=adjusted,
            )
            if result:
                executor._log_closed_trade(
                    token_id, pos.get("entry_price", 0), price_d,
                    tokens, "manual_sell",
                    market=pos.get("market_name"),
                )
                if token_id in executor._positions:
                    del executor._positions[token_id]
                executor._save_positions()
                executor.invalidate_balance_cache()
                return True
        except Exception as exc:
            self.logger.error("Force sell failed for %s: %s", token_id[:16], exc)
        return False

    def _cmd_reset_martingale(self, args):
        """Reset martingale streak & bet.  Usage: /reset_martingale [strategy]"""
        if not self.bot:
            self.send("No bot instance.")
            return
        mgr = getattr(self.bot, "_martingale_mgr", None)
        if not mgr or not mgr.bots:
            self.send("No martingale strategies running.")
            return
        if not args:
            # Reset all strategies
            for mb in mgr.bots:
                mb.reset_state()
            self.send(
                f"Reset {len(mgr.bots)} martingale strateg"
                f"{'y' if len(mgr.bots) == 1 else 'ies'}."
            )
            return
        # Reset a specific strategy by name
        target = " ".join(args).lower()
        matched = None
        for mb in mgr.bots:
            if mb.strategy_name.lower() == target:
                matched = mb
                break
        if not matched:
            names = ", ".join(mb.strategy_name for mb in mgr.bots)
            self.send(f"Strategy '{target}' not found.\nAvailable: {names}")
            return
        matched.reset_state()
        self.send(
            f"Reset [{matched.strategy_name}] — "
            f"bet=${matched.current_bet:.2f}, streak=0."
        )

    def _cmd_redeem(self, args=None):
        """Scan for old resolved positions and redeem to EOA wallet."""
        executor = self._get_executor()
        if not executor:
            self.send("Bot not running — cannot redeem.")
            return
        self.send("Scanning for redeemable positions...")
        # Run the full portfolio scan in a background thread to avoid
        # blocking the Telegram polling loop.
        def _do_redeem():
            try:
                results = executor.scan_and_redeem_portfolio()
                # Also try proxy wallet if configured
                proxy_results = []
                if self.bot.cfg.get("proxy_redeem", False):
                    try:
                        proxy_results = executor.scan_and_redeem_proxy_portfolio()
                    except Exception as exc:
                        self.logger.error("Proxy redeem scan failed: %s", exc)
                eoa_count = len(results) if results else 0
                proxy_count = len(proxy_results) if proxy_results else 0
                total = eoa_count + proxy_count
                if total > 0:
                    lines = [f"Redeemed {total} position(s):"]
                    for r in (results or []):
                        pnl = r.get("pnl_usdc", 0)
                        market = r.get("market", r.get("token_id", "?")[:20])
                        lines.append(f"  {market}: ${pnl:+,.2f}")
                    if proxy_count:
                        lines.append(f"  + {proxy_count} proxy position(s)")
                    self.send("\n".join(lines))
                else:
                    self.send("No redeemable positions found.")
            except Exception as exc:
                self.send(f"Redeem scan failed: {exc}")
        threading.Thread(target=_do_redeem, daemon=True).start()


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
        self._private_key = private_key  # stored for on-chain approvals
        self.base_url = CLOB_API_BASE.rstrip("/")
        self.gamma_url = GAMMA_API_BASE.rstrip("/")
        self.data_url = DATA_API_BASE.rstrip("/")
        self.session = requests.Session() if requests else None
        self.logger = logger or logging.getLogger("CopyTrader")
        self._last_trade_ids = {}  # address -> set of seen trade IDs
        self._token_to_condition = {}  # token_id -> condition_id from activity
        self._token_to_neg_risk = {}  # token_id -> neg_risk flag from activity/API
        self._token_to_slug = {}  # token_id -> market slug from activity
        self._token_to_avg_entry = {}  # token_id -> VWAP entry price from activity

        # Lazy-init web3 for on-chain approvals (populated by ensure_usdc_approval)
        self._w3 = None
        self._account = None

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
                self.logger.info("Derived CLOB API credentials (stored in memory)")

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

    # ------------------------------------------------------------------
    # On-chain USDC approval (lazy web3 init)
    # ------------------------------------------------------------------

    def _ensure_w3(self):
        """Lazily initialize a web3 connection for on-chain approvals."""
        if self._w3 is not None:
            return True
        if Web3 is None:
            self.logger.debug("web3 not installed — cannot approve on-chain")
            return False
        if not self._private_key:
            self.logger.debug("No private key — cannot approve on-chain")
            return False
        rpc_url = self.cfg.get("rpc_url", "")
        if not rpc_url:
            self.logger.debug("No rpc_url configured — cannot approve on-chain")
            return False
        try:
            self._w3 = Web3(Web3.HTTPProvider(rpc_url))
            self._w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            self._account = self._w3.eth.account.from_key(self._private_key)
            return True
        except Exception as exc:
            self.logger.warning("Failed to init web3 for approvals: %s", exc)
            self._w3 = None
            return False

    def ensure_usdc_approval(self, spender, amount_raw):
        """Approve *spender* to transfer USDC on behalf of the wallet.

        Uses a lazy-initialized web3 connection from ``rpc_url`` in config.
        This allows the CLOB client to handle approvals even when no
        TradeExecutor is available.

        Returns the tx receipt on new approval, ``True`` if already approved,
        or raises ``RuntimeError`` on failure so callers can distinguish
        "approval succeeded" from "approval could not be set".
        """
        if not self._ensure_w3():
            raise RuntimeError(
                "Web3 not available — configure rpc_url in config.json "
                "for on-chain USDC approvals"
            )
        try:
            spender = Web3.to_checksum_address(spender)
            usdc = self._w3.eth.contract(
                address=Web3.to_checksum_address(USDC_ADDRESS), abi=ERC20_ABI,
            )
            current = usdc.functions.allowance(
                self._account.address, spender,
            ).call()
            if current >= amount_raw:
                return True  # already approved

            self.logger.info("Approving USDC spend for %s ...", spender)
            nonce = self._w3.eth.get_transaction_count(
                self._account.address, "pending",
            )
            tx = usdc.functions.approve(
                spender, 2**256 - 1,  # max approval
            ).build_transaction({
                "chainId": 137,
                "from": self._account.address,
                "nonce": nonce,
                "gas": 80_000,
                "maxFeePerGas": self._w3.to_wei(50, "gwei"),
                "maxPriorityFeePerGas": self._w3.to_wei(30, "gwei"),
            })
            signed = self._account.sign_transaction(tx)
            tx_hash = self._w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = self._w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
            self.logger.info(
                "USDC approval tx confirmed: %s (status=%s)",
                receipt.transactionHash.hex(), receipt.status,
            )
            return receipt
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(f"ensure_usdc_approval failed: {exc}") from exc

    def ensure_ct_approval(self, neg_risk=False):
        """Approve CTF Exchange to transfer conditional tokens (ERC1155)."""
        if not self._ensure_w3():
            return None
        try:
            ctf_address = Web3.to_checksum_address(CONDITIONAL_TOKENS_ADDRESS)
            ctf = self._w3.eth.contract(address=ctf_address, abi=CONDITIONAL_TOKENS_ABI)
            exchange = (
                NEG_RISK_CTF_EXCHANGE_ADDRESS if neg_risk
                else CTF_EXCHANGE_ADDRESS
            )
            exchange = Web3.to_checksum_address(exchange)
            if ctf.functions.isApprovedForAll(
                self._account.address, exchange,
            ).call():
                return None  # already approved

            self.logger.info("Approving CTF for %s ...", exchange)
            nonce = self._w3.eth.get_transaction_count(
                self._account.address, "pending",
            )
            tx = ctf.functions.setApprovalForAll(
                exchange, True,
            ).build_transaction({
                "chainId": 137,
                "from": self._account.address,
                "nonce": nonce,
                "gas": 80_000,
                "maxFeePerGas": self._w3.to_wei(50, "gwei"),
                "maxPriorityFeePerGas": self._w3.to_wei(30, "gwei"),
            })
            signed = self._account.sign_transaction(tx)
            tx_hash = self._w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = self._w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
            self.logger.info(
                "CTF approval tx confirmed: %s (status=%s)",
                receipt.transactionHash.hex(), receipt.status,
            )
            return receipt
        except Exception as exc:
            self.logger.warning("ensure_ct_approval failed: %s", exc)
            return None

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

        Also computes the volume-weighted average BUY price for each
        token and caches it in ``_token_to_avg_entry`` so that
        discovered positions can use the user's actual cost basis
        instead of the market's last trade price.
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
        # Accumulators for VWAP: token_id -> [total_cost, total_shares]
        buy_accum = {}
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

                # Accumulate buy-side cost basis for VWAP entry price
                side = str(
                    trade.get("side") or trade.get("type") or ""
                ).upper()
                if side == "BUY":
                    try:
                        price = float(trade.get("price", 0))
                        shares = float(
                            trade.get("size")
                            or trade.get("amount")
                            or trade.get("usdcSize")
                            or 0
                        )
                        if price > 0 and shares > 0:
                            acc = buy_accum.setdefault(tid, [0.0, 0.0])
                            acc[0] += price * shares  # total cost
                            acc[1] += shares           # total shares
                    except (ValueError, TypeError):
                        pass

        # Compute VWAP entry prices from accumulated buy trades
        for tid, (total_cost, total_shares) in buy_accum.items():
            if total_shares > 0:
                self._token_to_avg_entry[tid] = total_cost / total_shares

        if self._token_to_avg_entry:
            self.logger.info(
                "Cached %d avg entry price(s) from activity data",
                len(self._token_to_avg_entry),
            )
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
                book = self.clob_sdk.get_order_book(token_id)
                # SDK may return an OrderBookSummary object instead of a dict;
                # normalise to dict so callers can use .get("bids") etc.
                if book and not isinstance(book, dict):
                    def _entry(e):
                        if isinstance(e, dict):
                            return e
                        return {
                            "price": str(getattr(e, "price", "0")),
                            "size": str(getattr(e, "size", "0")),
                        }
                    return {
                        "bids": [_entry(b) for b in (getattr(book, "bids", []) or [])],
                        "asks": [_entry(a) for a in (getattr(book, "asks", []) or [])],
                    }
                return book
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

    def place_order(self, token_id, side, size_usdc, price, neg_risk=False,
                    use_fok=False, max_retry_price=None):
        """Place an order on the Polymarket CLOB.

        By default places a GTC (Good-Till-Cancelled) limit order.  When
        *use_fok* is True, places a FOK (Fill-or-Kill) market order that
        fills instantly or is cancelled — avoiding stale limit orders that
        sit on the book.  If the FOK is rejected (common with precision
        issues), falls back to GTC automatically.

        Args:
            token_id: The conditional token ID to trade.
            side: 'BUY' or 'SELL'.
            size_usdc: Trade size in USDC.
            price: Price per share (0.0–1.0 range).
            neg_risk: Whether the market uses the neg-risk framework.
            use_fok: If True, try FOK first for instant fill.
            max_retry_price: If set, caps the FOK retry price.  Used by
                arb orders to prevent the retry from accepting a fill
                price that destroys the arb edge.  If the refreshed ask
                exceeds this cap, the retry is aborted.

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

            # Round price to 2 decimals first — the CLOB API uses this
            # rounded value, so all minimum calculations must be based on it
            # to avoid rounding mismatches (e.g. computing 5.00 tokens with
            # the raw price but the API seeing 4.96 after rounding).
            rounded_price = round(price, 2)
            if rounded_price <= 0:
                self.logger.error("Price rounds to zero – cannot place order")
                return None

            # Compute the minimum USDC needed to satisfy both Polymarket
            # constraints: ≥5 outcome tokens AND ≥$1 USDC notional.
            min_usdc_for_tokens = MIN_ORDER_SIZE_TOKENS * rounded_price
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
                size_shares = round(effective_usdc / rounded_price, 2)
                self.logger.warning(
                    "Order size %.2f USDC (%.2f tokens) below minimums; "
                    "bumped to %.2f USDC (%.2f tokens) "
                    "(min tokens=%d, min notional=$%.0f)",
                    size_usdc, round(size_usdc / rounded_price, 2),
                    effective_usdc, size_shares,
                    MIN_ORDER_SIZE_TOKENS, MIN_ORDER_NOTIONAL_USDC,
                )
            actual_usdc = effective_usdc

            size_tokens = round(actual_usdc / rounded_price, 2)

            # Safety net: if rounding still drops below the minimum token
            # count, ceil up to exactly MIN_ORDER_SIZE_TOKENS.
            if side.upper() == "BUY" and size_tokens < MIN_ORDER_SIZE_TOKENS:
                size_tokens = float(MIN_ORDER_SIZE_TOKENS)

            # --- FOK (Fill-or-Kill) attempt for instant fills ---
            if use_fok and MarketOrderArgs is not None:
                try:
                    if side.upper() == "BUY":
                        fok_args = MarketOrderArgs(
                            token_id=token_id,
                            amount=round(actual_usdc, 2),
                            price=rounded_price,
                            side=side.upper(),
                        )
                        signed_fok = self.clob_sdk.create_market_order(fok_args)
                    else:
                        # SELL FOK: amount is in shares, not USDC
                        fok_args = MarketOrderArgs(
                            token_id=token_id,
                            amount=round(size_tokens, 2),
                            price=rounded_price,
                            side=side.upper(),
                        )
                        signed_fok = self.clob_sdk.create_market_order(fok_args)
                    resp = self.clob_sdk.post_order(
                        signed_fok, orderType=OrderType.FOK,
                    )
                    self.logger.info(
                        "FOK order filled: %s $%.2f @ %.4f for token %s — %s",
                        side, actual_usdc, price, token_id[:16] + "...", resp,
                    )
                    return resp
                except Exception as fok_exc:
                    # Classify: network error (order may have been matched
                    # on the exchange but response lost) vs clean API
                    # rejection (order definitely not matched).
                    _exc_s = str(fok_exc)
                    _is_net = (
                        getattr(fok_exc, 'status_code', -1) is None
                        or "Request exception" in _exc_s
                        or "timeout" in _exc_s.lower()
                        or "connection" in _exc_s.lower()
                        or isinstance(fok_exc, (
                            ConnectionError, TimeoutError, OSError))
                    )
                    if _is_net:
                        # Network error — do NOT retry; the original order
                        # may have been matched.  Let the caller check for
                        # a phantom fill before re-submitting.
                        self.logger.warning(
                            "FOK network error (%s) — NOT retrying "
                            "(order may have been matched)",
                            fok_exc,
                        )
                        return {
                            "status": "network_error",
                            "reason": str(fok_exc),
                            "token_id": token_id,
                            "side": side.upper(),
                            "amount": round(actual_usdc, 2),
                            "price": rounded_price,
                        }
                    # Clean API rejection — safe to retry with refreshed
                    # price (the order was definitely not matched).
                    self.logger.warning(
                        "FOK rejected (%s) — retrying with refreshed price",
                        fok_exc,
                    )
                    try:
                        retry_book = self.get_order_book(token_id)
                        retry_bids = (retry_book or {}).get("bids") or []
                        retry_asks = (retry_book or {}).get("asks") or []
                        retry_bid = float(retry_bids[0].get("price", 0)) if retry_bids else 0
                        retry_ask = float(retry_asks[0].get("price", 0)) if retry_asks else 0
                        if side.upper() == "BUY" and retry_ask > 0:
                            # Hard ceiling: never pay more than 5% above our
                            # original price.  The old 0.99 fallback was
                            # effectively a market buy that could fill at any
                            # ask on the book — causing massive overpays.
                            inherent_cap = round(rounded_price * 1.05, 2)
                            price_cap = min(
                                inherent_cap,
                                max_retry_price or inherent_cap,
                            )
                            retry_price = round(min(retry_ask * 1.005, price_cap), 2)
                            if retry_price > price_cap:
                                self.logger.warning(
                                    "FOK retry: refreshed ask $%.4f exceeds "
                                    "price cap $%.4f (original $%.4f) — aborting",
                                    retry_ask, price_cap, rounded_price,
                                )
                                return {"status": "fok_rejected",
                                        "reason": "retry_price_exceeds_cap"}
                        elif side.upper() == "SELL" and retry_bid > 0:
                            inherent_floor = round(rounded_price * 0.95, 2)
                            price_floor = max(
                                inherent_floor,
                                max_retry_price or inherent_floor,
                            )
                            retry_price = round(max(retry_bid * 0.995, price_floor, 0.01), 2)
                            if retry_price < price_floor:
                                self.logger.warning(
                                    "FOK retry: refreshed bid $%.4f below "
                                    "price floor $%.4f — aborting",
                                    retry_bid, price_floor,
                                )
                                return {"status": "fok_rejected",
                                        "reason": "retry_price_below_floor"}
                        else:
                            self.logger.warning("FOK retry: no orderbook — aborting")
                            return {"status": "fok_rejected", "reason": str(fok_exc)}

                        retry_usdc = round(actual_usdc, 2)
                        retry_tokens = round(actual_usdc / retry_price, 2) if retry_price > 0 else 0
                        if side.upper() == "BUY":
                            fok_args2 = MarketOrderArgs(
                                token_id=token_id,
                                amount=retry_usdc,
                                price=retry_price,
                                side=side.upper(),
                            )
                        else:
                            fok_args2 = MarketOrderArgs(
                                token_id=token_id,
                                amount=round(retry_tokens, 2),
                                price=retry_price,
                                side=side.upper(),
                            )
                        signed_retry = self.clob_sdk.create_market_order(fok_args2)
                        resp2 = self.clob_sdk.post_order(
                            signed_retry, orderType=OrderType.FOK,
                        )
                        self.logger.info(
                            "FOK retry filled: %s $%.2f @ %.4f (was %.4f) for token %s — %s",
                            side, actual_usdc, retry_price, price,
                            token_id[:16] + "...", resp2,
                        )
                        return resp2
                    except Exception as retry_exc:
                        _rexc_s = str(retry_exc)
                        _is_net2 = (
                            getattr(retry_exc, 'status_code', -1) is None
                            or "Request exception" in _rexc_s
                            or "timeout" in _rexc_s.lower()
                            or "connection" in _rexc_s.lower()
                            or isinstance(retry_exc, (
                                ConnectionError, TimeoutError, OSError))
                        )
                        if _is_net2:
                            self.logger.warning(
                                "FOK retry network error (%s) — aborting "
                                "(order may have been matched)",
                                retry_exc,
                            )
                            return {
                                "status": "network_error",
                                "reason": str(retry_exc),
                                "token_id": token_id,
                                "side": side.upper(),
                                "amount": retry_usdc,
                                "price": retry_price,
                            }
                        self.logger.warning(
                            "FOK retry also rejected (%s) — aborting",
                            retry_exc,
                        )
                        return {"status": "fok_rejected", "reason": str(retry_exc)}

            # --- GTC (Good-Till-Cancelled) limit order ---
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
        """Return only trades we have not seen before for *address*.

        Trades older than ``trade_max_age_seconds`` (default 30) are
        automatically discarded so the bot never executes at a stale price.
        """
        address = address.lower()
        all_trades = self.get_trades_for_address(address)
        seen = self._last_trade_ids.get(address, set())
        max_age = self.cfg.get("trade_max_age_seconds", 30)
        now = time.time()
        new_trades = []
        for trade in all_trades:
            tid = (
                trade.get("id")
                or trade.get("tradeID")
                or trade.get("hash")
                or trade.get("transactionHash", "")
            )
            if tid and tid not in seen:
                seen.add(tid)

                # --- Staleness filter ---
                # Try to extract the trade timestamp so we can skip trades
                # that are too old by the time we detect them.
                trade_ts = (
                    trade.get("matchTime")          # CLOB API epoch-second
                    or trade.get("timestamp")
                    or trade.get("createdAt")
                    or trade.get("blockTimestamp")
                )
                if trade_ts is not None:
                    try:
                        ts_val = float(trade_ts)
                        # If value looks like epoch-millis, convert
                        if ts_val > 1e12:
                            ts_val = ts_val / 1000.0
                        age = now - ts_val
                        if age > max_age:
                            self.logger.info(
                                "STALE TRADE: skipping trade %s (age=%.1fs > "
                                "max=%ds) for %s",
                                str(tid)[:16], age, max_age, address[:10],
                            )
                            continue
                        # Annotate trade with detection latency for logging
                        trade["_detection_latency_s"] = round(age, 2)
                    except (ValueError, TypeError):
                        pass  # unparseable timestamp — proceed anyway

                new_trades.append(trade)
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

    def _rpc_with_retry(self, fn, description="RPC call", retries=3):
        """Execute *fn()* with exponential-backoff retries on connection errors."""
        for attempt in range(retries + 1):
            try:
                return fn()
            except (ConnectionError, OSError) as exc:
                if attempt < retries:
                    delay = 2 ** attempt
                    self.logger.warning(
                        "%s failed (attempt %d/%d): %s — retrying in %ds",
                        description, attempt + 1, retries + 1, exc, delay,
                    )
                    time.sleep(delay)
                else:
                    raise

    def _scan_block(self, block_number):
        """Scan a single block for relevant Polymarket trades."""
        trades = []
        try:
            block = self._rpc_with_retry(
                lambda: self.w3.eth.get_block(block_number, full_transactions=True),
                description="get_block(%d)" % block_number,
            )
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
            current = self._rpc_with_retry(
                lambda: self.w3.eth.block_number,
                description="get_block_number",
            )
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
# WebSocket Monitor – real-time whale trade detection via eth_subscribe
# ---------------------------------------------------------------------------

class WebSocketMonitor(threading.Thread):
    """Real-time whale trade detection via Polygon WebSocket eth_subscribe.

    Subscribes to OrderFilled and OrdersMatched events on the CTF Exchange
    contracts, filtered by watched whale addresses (as both maker and taker).
    When a matching event arrives, signals the main loop to immediately poll
    the CLOB API for structured trade details instead of waiting for the next
    poll interval.

    Requires:
      - A Polygon WebSocket RPC URL (e.g. Alchemy, Infura, QuickNode)
      - The ``websocket-client`` package (``pip install websocket-client``)

    Falls back gracefully if websocket-client is not installed.
    """

    RECONNECT_DELAYS = [1, 2, 4, 8, 16, 30]  # seconds, capped at 30
    PING_INTERVAL = 30  # seconds between keepalive pings
    DATA_TIMEOUT = 300  # seconds without any message before reconnecting

    def __init__(self, ws_url, watched_addresses, wake_event, logger=None):
        super().__init__(daemon=True, name="WebSocketMonitor")
        self.ws_url = ws_url
        self.watched = {a.lower().replace("0x", "") for a in watched_addresses}
        self.wake_event = wake_event  # threading.Event to wake main loop
        self.logger = logger or logging.getLogger("CopyTrader")
        self.running = True
        self._ws = None
        self._reconnect_count = 0
        self.last_event_time = 0.0
        self.connected = False

        # Compute event topic hashes (keccak256 of canonical signatures)
        if Web3 is not None:
            self._order_filled_topic = Web3.keccak(
                text="OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"
            ).hex()
            self._orders_matched_topic = Web3.keccak(
                text="OrdersMatched(bytes32,address,address,uint256,uint256,uint256,uint256)"
            ).hex()
        else:
            # Precomputed fallbacks (Polygon mainnet)
            self._order_filled_topic = (
                "0x4b9f2d36e1b4c93de62cc077b00b1a91d84b6c31b4a14e012718571f"
                "3100b2257"
            )
            self._orders_matched_topic = (
                "0x1234567890abcdef"  # placeholder, Web3 should always be available
            )

    def update_watched(self, addresses):
        """Update the set of whale addresses to monitor (thread-safe)."""
        self.watched = {a.lower().replace("0x", "") for a in addresses}

    def stop(self):
        """Signal the monitor to shut down."""
        self.running = False
        ws = self._ws
        if ws:
            try:
                ws.close()
            except Exception:
                pass

    def run(self):
        """Main thread loop — connect, subscribe, listen, reconnect."""
        if not HAS_WS_CLIENT:
            self.logger.warning(
                "websocket-client not installed — WebSocket monitoring disabled. "
                "Install with: pip install websocket-client"
            )
            return

        if Web3 is None:
            self.logger.warning(
                "web3 not installed — WebSocket monitoring disabled."
            )
            return

        self.logger.info(
            "WebSocket monitor starting — %s (watching %d address(es))",
            self.ws_url[:50] + "...", len(self.watched),
        )

        while self.running:
            try:
                self._connect_and_listen()
            except Exception as exc:
                self.connected = False
                if not self.running:
                    break
                idx = min(self._reconnect_count, len(self.RECONNECT_DELAYS) - 1)
                delay = self.RECONNECT_DELAYS[idx]
                self.logger.warning(
                    "WebSocket disconnected: %s — reconnecting in %ds (attempt %d)",
                    exc, delay, self._reconnect_count + 1,
                )
                self._reconnect_count += 1
                # Sleep in small steps so we can exit quickly on stop()
                elapsed = 0.0
                while elapsed < delay and self.running:
                    time.sleep(min(0.5, delay - elapsed))
                    elapsed += 0.5

        self.connected = False
        self.logger.info("WebSocket monitor stopped")

    def _connect_and_listen(self):
        """Connect to the Polygon WebSocket RPC, subscribe, and listen."""
        ws = _ws_lib.WebSocket()
        ws.settimeout(self.PING_INTERVAL + 10)
        ws.connect(self.ws_url)
        self._ws = ws
        self.connected = True
        self._reconnect_count = 0
        self.logger.info("WebSocket connected to %s", self.ws_url[:50] + "...")

        # Subscribe to CTF Exchange events for whale addresses
        self._subscribe(ws)

        # Listen for incoming events
        last_ping = time.monotonic()
        last_data = time.monotonic()

        while self.running:
            try:
                raw = ws.recv()
                if not raw:
                    continue
                last_data = time.monotonic()

                msg = json.loads(raw)

                # Subscription confirmation
                if "result" in msg and "id" in msg:
                    self.logger.debug(
                        "WebSocket subscription confirmed: id=%s sub=%s",
                        msg["id"], msg["result"],
                    )
                    continue

                # Subscription event
                if msg.get("method") == "eth_subscription":
                    self._handle_event(msg["params"]["result"])

            except _ws_lib.WebSocketTimeoutException:
                pass  # normal timeout, send ping below
            except _ws_lib.WebSocketConnectionClosedException:
                raise ConnectionError("WebSocket connection closed by server")

            # Keepalive ping
            now = time.monotonic()
            if now - last_ping >= self.PING_INTERVAL:
                try:
                    ws.ping()
                except Exception:
                    raise ConnectionError("WebSocket ping failed")
                last_ping = now

            # Reconnect if no data for too long (server went silent)
            if now - last_data > self.DATA_TIMEOUT:
                raise ConnectionError(
                    "No data received for %ds — reconnecting" % self.DATA_TIMEOUT
                )

    def _subscribe(self, ws):
        """Send eth_subscribe requests for OrderFilled/OrdersMatched events."""
        whale_topics = ["0x" + addr.zfill(64) for addr in self.watched]

        if not whale_topics:
            self.logger.warning("No whale addresses configured for WebSocket monitoring")
            return

        exchanges = [
            CTF_EXCHANGE_ADDRESS,
            NEG_RISK_CTF_EXCHANGE_ADDRESS,
        ]
        event_topics = [
            self._order_filled_topic,
            self._orders_matched_topic,
        ]

        # Subscription 1: whale as MAKER (topic index 2)
        ws.send(json.dumps({
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": ["logs", {
                "address": exchanges,
                "topics": [
                    event_topics,   # topic0: either event type
                    None,           # topic1: any orderHash
                    whale_topics,   # topic2: maker is a watched whale
                ],
            }],
            "id": 1,
        }))

        # Subscription 2: whale as TAKER (topic index 3)
        ws.send(json.dumps({
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": ["logs", {
                "address": exchanges,
                "topics": [
                    event_topics,   # topic0: either event type
                    None,           # topic1: any orderHash
                    None,           # topic2: any maker
                    whale_topics,   # topic3: taker is a watched whale
                ],
            }],
            "id": 2,
        }))

        self.logger.info(
            "WebSocket subscribed: %d event type(s) × %d exchange(s) × %d whale(s) "
            "(maker + taker)",
            len(event_topics), len(exchanges), len(whale_topics),
        )

    def _handle_event(self, log_entry):
        """Process an incoming OrderFilled/OrdersMatched log event."""
        topics = log_entry.get("topics", [])
        tx_hash = log_entry.get("transactionHash", "")
        block_hex = log_entry.get("blockNumber", "0x0")
        block_num = int(block_hex, 16) if isinstance(block_hex, str) else block_hex

        # Identify which whale address matched (topic2=maker, topic3=taker)
        whale_addr = None
        whale_role = None
        for idx, label in [(2, "maker"), (3, "taker")]:
            if idx < len(topics) and topics[idx]:
                addr_hex = topics[idx][-40:].lower()
                if addr_hex in self.watched:
                    whale_addr = "0x" + addr_hex
                    whale_role = label
                    break

        event_name = "OrderFilled"
        if len(topics) > 0 and topics[0] == self._orders_matched_topic:
            event_name = "OrdersMatched"

        self.logger.info(
            "WS EVENT: %s — whale %s (%s) — block %d — tx %s",
            event_name,
            whale_addr[:12] + "..." if whale_addr else "unknown",
            whale_role or "?",
            block_num,
            tx_hash[:18] + "..." if tx_hash else "?",
        )

        self.last_event_time = time.time()

        # Wake the main loop to immediately poll CLOB API for full trade details
        self.wake_event.set()


# ---------------------------------------------------------------------------
# Arbitrage Monitor – binary market spread capture
# ---------------------------------------------------------------------------

class ArbitrageMonitor(threading.Thread):
    """Scans binary Polymarket markets for risk-free spread opportunities.

    A binary market has exactly two outcome tokens whose payouts sum to $1.
    When the combined best-ask price of both outcomes drops below $1, buying
    both sides locks in a guaranteed profit equal to ($1 - total_cost) per
    share once the market resolves.

    Example:  Up = $0.48, Down = $0.50  ->  cost = $0.98, profit = $0.02/share (2%).

    The monitor:
      1. Resolves each configured ``condition_id`` to its pair of token IDs
         via the Gamma API.
      2. Polls orderbooks for both tokens on a fast interval.
      3. When the combined best-ask drops below the configured edge threshold,
         places simultaneous FOK BUY orders for both sides.
      4. Tracks active arb positions to avoid exceeding ``arb_max_positions``.
    """

    ARB_POSITIONS_FILE = "arb_positions.json"

    def __init__(self, clob_client, cfg, logger=None):
        super().__init__(daemon=True, name="ArbitrageMonitor")
        self.clob_client = clob_client
        self.cfg = cfg
        self.logger = logger or logging.getLogger("CopyTrader")
        self._stop_event = threading.Event()

        # Lock protects _markets from concurrent access between
        # _resolve_dynamic_slugs() and _scan_cycle().
        self._market_lock = threading.Lock()

        # condition_id -> {"yes_token": str, "no_token": str, "question": str, ...}
        self._markets = {}
        # condition_id -> {"yes_fill": {...}, "no_fill": {...}, "cost": float, "ts": str}
        self._active_positions = {}
        self._load_positions()

        # Per-slug state: slug_base -> {"window_key": str, "failed_key": str}
        # Tracks the last resolved window and last failed window for each
        # configured dynamic slug so they rotate independently.
        self._slug_states = {}

        # Per-market log-throttle timestamps so multi-market scanning
        # doesn't spam logs.
        self._edge_log_ts = {}   # cid -> last log time
        self._noask_log_ts = {}  # cid -> last log time

        # Callbacks wired up by CopyTraderBot after construction.
        # notify_callback(message) — sends webhook notification.
        # log_trade_callback(record) — appends to trade_history.json.
        self.notify_callback = None
        self.log_trade_callback = None

    # -- persistence --------------------------------------------------------

    def _load_positions(self):
        try:
            with open(self.ARB_POSITIONS_FILE, "r") as fh:
                self._active_positions = json.load(fh)
        except (FileNotFoundError, json.JSONDecodeError):
            self._active_positions = {}

    def _save_positions(self):
        try:
            tmp = self.ARB_POSITIONS_FILE + ".tmp"
            with open(tmp, "w") as fh:
                json.dump(self._active_positions, fh, indent=2, default=str)
            os.replace(tmp, self.ARB_POSITIONS_FILE)
        except Exception as exc:
            self.logger.debug("Could not save arb positions: %s", exc)

    def _log_arb_trade(self, position, is_partial=False):
        """Log a completed arb trade to trade_history.json.

        Creates a single record per arb with both legs' data and the
        correct locked P&L based on actual fill amounts.
        """
        if not self.log_trade_callback:
            return

        question = position.get("question", "Unknown")
        yes_shares = position.get("yes_shares", 0)
        no_shares = position.get("no_shares", 0)
        total_cost = position.get("total_cost", 0)
        locked_profit = position.get("locked_profit", 0)
        edge_pct = position.get("edge_pct", 0)
        matched_shares = min(yes_shares, no_shares)

        # For a complete arb, the proceeds are guaranteed at resolution:
        # each matched pair of shares pays out $1.00.
        if is_partial:
            outcome = "partial"
            reason = "arb_partial"
            # Partial arbs have unknown proceeds
            proceeds = 0
            pnl = -total_cost  # worst-case until resolution
        else:
            outcome = "arb"
            reason = "arb_executed"
            proceeds = matched_shares  # $1 per matched pair at resolution
            pnl = locked_profit

        # Extract per-leg details for the record
        yes_fill = position.get("yes_fill") or {}
        no_fill = position.get("no_fill") or {}

        record = {
            "closed_at": position.get("ts", datetime.now().isoformat()),
            "token_id": "arb",
            "market": question,
            "shares": matched_shares,
            "entry_price": round(total_cost / matched_shares, 6) if matched_shares > 0 else 0,
            "exit_price": 1.0 if not is_partial else 0,
            "cost_basis_usdc": round(total_cost, 6),
            "proceeds_usdc": round(proceeds, 6),
            "pnl_usdc": round(pnl, 6),
            "outcome": outcome,
            "reason": reason,
            "arb_details": {
                "yes_shares": yes_shares,
                "no_shares": no_shares,
                "yes_cost": round(
                    float(yes_fill.get("makingAmount", 0))
                    if isinstance(yes_fill, dict) else 0, 4,
                ),
                "no_cost": round(
                    float(no_fill.get("makingAmount", 0))
                    if isinstance(no_fill, dict) else 0, 4,
                ),
                "edge_pct": edge_pct,
                "locked_profit": locked_profit,
            },
        }

        try:
            self.log_trade_callback(record)
        except Exception as exc:
            self.logger.debug("Could not log arb trade to history: %s", exc)

    def _notify_arb(self, message):
        """Send webhook notification for arb events."""
        if self.notify_callback:
            try:
                self.notify_callback(message)
            except Exception:
                pass

    # -- market resolution --------------------------------------------------

    def _resolve_markets(self):
        """Resolve each condition_id to its pair of outcome token IDs."""
        condition_ids = self.cfg.get("arb_condition_ids", [])
        if not condition_ids:
            self.logger.warning("Arbitrage enabled but no condition IDs configured")
            return

        for cid in condition_ids:
            if cid in self._markets:
                continue  # already resolved
            try:
                market = self.clob_client.get_market_info(condition_id=cid)
                if not market:
                    self.logger.warning("Arb: could not fetch market for condition %s", cid[:20])
                    continue

                tokens = market.get("tokens") or []
                if len(tokens) < 2:
                    self.logger.warning(
                        "Arb: market %s has %d tokens (need 2) — skipping",
                        cid[:20], len(tokens),
                    )
                    continue

                # Map outcomes.  Polymarket uses "Yes"/"No" or custom labels.
                # We just need the two token IDs — call them "yes" and "no"
                # regardless of the actual outcome name.
                t0 = tokens[0]
                t1 = tokens[1]
                tid0 = t0.get("token_id") or t0.get("tokenId") or ""
                tid1 = t1.get("token_id") or t1.get("tokenId") or ""
                label0 = t0.get("outcome", "A")
                label1 = t1.get("outcome", "B")
                question = market.get("question", cid[:30])

                self._markets[cid] = {
                    "yes_token": tid0,
                    "no_token": tid1,
                    "yes_label": label0,
                    "no_label": label1,
                    "question": question,
                }
                self.logger.info(
                    "Arb: resolved market '%s' — %s=%s... / %s=%s...",
                    question[:50], label0, tid0[:12], label1, tid1[:12],
                )
            except Exception as exc:
                self.logger.warning("Arb: failed to resolve condition %s: %s", cid[:20], exc)

    # -- dynamic slug discovery ---------------------------------------------

    @staticmethod
    def _get_window_ts(window):
        """Return the current window-start unix timestamp for a given window size."""
        window = max(int(window), 30)
        now = int(time.time())
        return now - (now % window)

    def _get_effective_slugs(self):
        """Return the list of dynamic slug entries from config.

        Supports both the new ``arb_dynamic_slugs`` list format and the
        legacy single ``arb_dynamic_slug`` string.  The legacy format is
        automatically converted so callers always get a uniform list of
        ``{"slug": str, "window": int, "format": str}`` dicts.
        """
        slugs = self.cfg.get("arb_dynamic_slugs") or []
        if slugs:
            # Normalise entries: ensure each has "format" defaulting to "timestamp"
            normalised = []
            for entry in slugs:
                if isinstance(entry, str):
                    entry = {"slug": entry}
                if isinstance(entry, dict) and entry.get("slug", "").strip():
                    normalised.append({
                        "slug": entry["slug"].strip(),
                        "window": int(entry.get("window", 300)),
                        "format": entry.get("format", "timestamp"),
                    })
            return normalised

        # Backward compat: single slug string -> list of one
        single = self.cfg.get("arb_dynamic_slug", "").strip()
        if single:
            return [{
                "slug": single,
                "window": int(self.cfg.get("arb_dynamic_window", 300)),
                "format": "timestamp",
            }]
        return []

    def _generate_slug(self, slug_entry):
        """Generate the full event slug for the current window.

        For ``"timestamp"`` format (default):
            ``{base_slug}-{unix_window_start}``

        For ``"hourly"`` format:
            ``{base_slug}-{month}-{day}-{hour}{am/pm}-et``
            Uses US Eastern time with proper DST handling.
        """
        base = slug_entry["slug"]
        window = slug_entry["window"]
        fmt = slug_entry.get("format", "timestamp")

        if fmt == "hourly":
            return self._generate_hourly_slug(base, window)

        # Default: timestamp format
        window_ts = self._get_window_ts(window)
        return f"{base}-{window_ts}"

    @staticmethod
    def _generate_hourly_slug(base, window):
        """Generate a human-readable hourly slug in US Eastern time.

        Pattern: ``{base}-{month}-{day}-{hour}{am/pm}-et``
        Example: ``ethereum-up-or-down-february-16-9pm-et``
        """
        try:
            from zoneinfo import ZoneInfo
            et = ZoneInfo("America/New_York")
        except (ImportError, KeyError):
            from datetime import timedelta as _td
            et = timezone(_td(hours=-5))

        now_et = datetime.now(et)
        # Truncate to the current window boundary.  For hourly markets
        # the window is typically 3600 but we honour whatever is configured.
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        ts = int((now_et.astimezone(timezone.utc) - epoch).total_seconds())
        window_start_ts = ts - (ts % max(window, 60))
        window_start_utc = datetime.fromtimestamp(window_start_ts, tz=timezone.utc)
        window_start_et = window_start_utc.astimezone(et)

        month_name = window_start_et.strftime("%B").lower()
        day = window_start_et.day
        hour_12 = int(window_start_et.strftime("%I"))
        ampm = window_start_et.strftime("%p").lower()
        return f"{base}-{month_name}-{day}-{hour_12}{ampm}-et"

    def _window_key_for(self, slug_entry):
        """Return a comparable key representing the current window for a slug.

        For timestamp slugs this is the unix timestamp (int).
        For hourly slugs this is the generated slug suffix (str).
        Either way, when the key changes the window has rotated.
        """
        return self._generate_slug(slug_entry)

    def _resolve_dynamic_slugs(self):
        """Discover current rotating markets for all configured dynamic slugs.

        Unlike the legacy single-slug method, this **merges** newly discovered
        markets into ``self._markets`` (keyed by condition_id) and removes
        stale markets whose window has rotated.

        Each market entry is tagged with ``_slug_key`` (the base slug) and
        ``_resolved_slug`` (the full generated slug at discovery time) so
        that ``_scan_cycle()`` can perform per-market staleness checks.
        """
        slug_entries = self._get_effective_slugs()
        if not slug_entries:
            return

        any_resolved = False
        new_markets = {}

        for entry in slug_entries:
            slug_base = entry["slug"]
            current_key = self._window_key_for(entry)

            state = self._slug_states.get(slug_base, {})
            prev_key = state.get("window_key")

            # Same window, already resolved — carry forward existing markets
            if current_key == prev_key:
                with self._market_lock:
                    for cid, mkt in self._markets.items():
                        if mkt.get("_slug_key") == slug_base:
                            new_markets[cid] = mkt
                continue

            # Already failed for this exact window — skip
            if current_key == state.get("failed_key"):
                # Still carry forward any existing markets from this slug
                # (they may be from a previous successful resolution)
                with self._market_lock:
                    for cid, mkt in self._markets.items():
                        if mkt.get("_slug_key") == slug_base:
                            new_markets[cid] = mkt
                continue

            full_slug = current_key  # _window_key_for returns the full slug
            self.logger.info(
                "Arb dynamic: discovering market for slug '%s'", full_slug,
            )

            resolved = self._fetch_and_parse_slug(
                full_slug, slug_base, entry,
            )
            if resolved:
                cid, mkt_data = resolved
                new_markets[cid] = mkt_data
                self._slug_states[slug_base] = {
                    "window_key": current_key, "failed_key": "",
                }
                any_resolved = True
            else:
                self._slug_states[slug_base] = {
                    "window_key": state.get("window_key", ""),
                    "failed_key": current_key,
                }
                # Carry forward existing markets from this slug
                with self._market_lock:
                    for cid, mkt in self._markets.items():
                        if mkt.get("_slug_key") == slug_base:
                            new_markets[cid] = mkt

        # Also carry forward any static (non-dynamic) markets.
        with self._market_lock:
            for cid, mkt in self._markets.items():
                if not mkt.get("_slug_key"):
                    new_markets[cid] = mkt

        # Atomically swap the entire markets dict.
        with self._market_lock:
            self._markets = new_markets

    def _fetch_and_parse_slug(self, full_slug, slug_base, slug_entry):
        """Fetch a single event slug from the Gamma API and parse it.

        Returns ``(condition_id, market_dict)`` on success, or ``None``.
        """
        try:
            url = f"{GAMMA_API_BASE}/events"
            data = self.clob_client._get_public(url, params={"slug": full_slug})

            event = None
            if isinstance(data, list) and data:
                event = data[0]
            elif isinstance(data, dict):
                event = data

            if not event:
                self.logger.warning(
                    "Arb dynamic: no event found for slug '%s' — "
                    "market may not be open yet", full_slug,
                )
                return None

            markets = event.get("markets") or []
            if not markets:
                self.logger.warning(
                    "Arb dynamic: event '%s' has no markets", full_slug,
                )
                return None

            market = markets[0]
            cid = market.get("condition_id") or market.get("conditionId") or ""
            if not cid:
                self.logger.warning(
                    "Arb dynamic: market in event '%s' has no condition_id",
                    full_slug,
                )
                return None

            # --- Parse token IDs ---
            token_ids = []
            outcome_labels = []

            raw_clob = market.get("clobTokenIds") or ""
            if isinstance(raw_clob, str) and raw_clob.strip():
                try:
                    parsed = json.loads(raw_clob)
                    if isinstance(parsed, list):
                        token_ids = [str(t) for t in parsed]
                except (json.JSONDecodeError, TypeError):
                    pass
            elif isinstance(raw_clob, list):
                token_ids = [str(t) for t in raw_clob]

            raw_outcomes = market.get("outcomes") or ""
            if isinstance(raw_outcomes, str) and raw_outcomes.strip():
                try:
                    parsed = json.loads(raw_outcomes)
                    if isinstance(parsed, list):
                        outcome_labels = [str(o) for o in parsed]
                except (json.JSONDecodeError, TypeError):
                    pass
            elif isinstance(raw_outcomes, list):
                outcome_labels = [str(o) for o in raw_outcomes]

            # Fallback: try native "tokens" array (CLOB API format)
            if len(token_ids) < 2:
                tokens = market.get("tokens") or []
                if len(tokens) >= 2:
                    token_ids = [
                        tokens[0].get("token_id") or tokens[0].get("tokenId") or "",
                        tokens[1].get("token_id") or tokens[1].get("tokenId") or "",
                    ]
                    outcome_labels = [
                        tokens[0].get("outcome", "A"),
                        tokens[1].get("outcome", "B"),
                    ]

            if len(token_ids) < 2 or not token_ids[0] or not token_ids[1]:
                self.logger.warning(
                    "Arb dynamic: market %s has insufficient token data "
                    "(clobTokenIds=%r, tokens=%r) — skipping",
                    cid[:20],
                    market.get("clobTokenIds", ""),
                    len(market.get("tokens") or []),
                )
                return None

            if len(outcome_labels) < 2:
                outcome_labels = ["A", "B"]

            tid0, tid1 = token_ids[0], token_ids[1]
            label0, label1 = outcome_labels[0], outcome_labels[1]
            question = market.get("question") or event.get("title") or full_slug

            self.logger.info(
                "Arb dynamic: resolved '%s' — %s=%s... / %s=%s... (slug %s)",
                question[:50], label0, tid0[:12], label1, tid1[:12], full_slug,
            )

            return cid, {
                "yes_token": tid0,
                "no_token": tid1,
                "yes_label": label0,
                "no_label": label1,
                "question": question,
                # Multi-slug metadata for per-market staleness checks
                "_slug_key": slug_base,
                "_slug_entry": slug_entry,
                "_resolved_slug": full_slug,
            }

        except Exception as exc:
            self.logger.warning(
                "Arb dynamic: failed to resolve slug '%s': %s", full_slug, exc,
            )
            return None

    # -- core loop ----------------------------------------------------------

    def run(self):
        slug_entries = self._get_effective_slugs()
        static_cids = self.cfg.get("arb_condition_ids", [])

        if slug_entries:
            for entry in slug_entries:
                self.logger.info(
                    "Arbitrage monitor: dynamic slug '%s' "
                    "(window %ds, format %s)",
                    entry["slug"], entry["window"], entry.get("format", "timestamp"),
                )
            self._resolve_dynamic_slugs()
        else:
            self.logger.info(
                "Arbitrage monitor starting — %d market(s) configured",
                len(static_cids),
            )
            self._resolve_markets()

        if not self._markets and not slug_entries:
            self.logger.error("Arb: no markets could be resolved — monitor stopping")
            return

        poll_interval = max(self.cfg.get("arb_poll_seconds", 2), 0.5)

        while not self._stop_event.is_set():
            # Runtime toggle: if arb_enabled is turned off, stop gracefully
            if not self.cfg.get("arb_enabled", True):
                self.logger.info(
                    "Arbitrage monitor: arb_enabled toggled off — stopping"
                )
                break

            try:
                # For dynamic slugs, re-resolve when any window rotates
                if slug_entries:
                    self._resolve_dynamic_slugs()

                if self._markets:
                    self._scan_cycle()
            except Exception as exc:
                self.logger.error("Arb scan error: %s", exc, exc_info=True)
            self._stop_event.wait(timeout=poll_interval)

        self.logger.info("Arbitrage monitor stopped")

    def stop(self):
        self._stop_event.set()

    # -- scan & execute -----------------------------------------------------

    def _scan_cycle(self):
        """One pass: check every tracked market for an arb opportunity."""
        min_edge_pct = self.cfg.get("arb_min_edge_pct", 1.0)
        arb_size = self.cfg.get("arb_size_usdc", 10.0)
        max_positions = self.cfg.get("arb_max_positions", 5)
        dry_run = self.cfg.get("dry_run", False)

        # Snapshot markets under lock so we iterate a stable copy.
        with self._market_lock:
            markets_snapshot = dict(self._markets)

        # Capture per-market window keys at scan start for staleness checks.
        window_keys_at_start = {}
        for cid, mkt in markets_snapshot.items():
            entry = mkt.get("_slug_entry")
            if entry:
                window_keys_at_start[cid] = self._window_key_for(entry)

        for cid, mkt in markets_snapshot.items():
            # Respect max positions
            if len(self._active_positions) >= max_positions:
                break

            # Skip if we already have an active arb on this market
            if cid in self._active_positions:
                continue

            yes_token = mkt["yes_token"]
            no_token = mkt["no_token"]

            # Fetch orderbooks for both sides in parallel to cut latency
            try:
                with ThreadPoolExecutor(max_workers=2) as pool:
                    fut_yes = pool.submit(self.clob_client.get_order_book, yes_token)
                    fut_no = pool.submit(self.clob_client.get_order_book, no_token)
                    book_yes = fut_yes.result(timeout=5)
                    book_no = fut_no.result(timeout=5)
            except Exception as exc:
                self.logger.debug("Arb: orderbook fetch failed for %s: %s", cid[:16], exc)
                continue

            if not book_yes or not book_no:
                continue

            asks_yes = book_yes.get("asks") or []
            asks_no = book_no.get("asks") or []

            if not asks_yes or not asks_no:
                now = time.time()
                last_t = self._noask_log_ts.get(cid, 0)
                if now - last_t >= 60:
                    self._noask_log_ts[cid] = now
                    self.logger.info(
                        "Arb scan: %s — no asks on %s side (Yes asks: %d, No asks: %d)",
                        mkt["question"][:40],
                        "Yes" if not asks_yes else "No",
                        len(asks_yes), len(asks_no),
                    )
                continue

            # Sort asks by price ascending so asks[0] is the *cheapest*
            # offer.  The CLOB API does not guarantee sort order, so
            # without this we may pick a stale $0.99 resting order
            # instead of the tightest available ask.
            asks_yes = sorted(asks_yes, key=lambda e: float(e.get("price", "0")))
            asks_no = sorted(asks_no, key=lambda e: float(e.get("price", "0")))

            best_ask_yes = float(asks_yes[0].get("price", 0))
            best_ask_no = float(asks_no[0].get("price", 0))
            avail_yes = float(asks_yes[0].get("size", 0))
            avail_no = float(asks_no[0].get("size", 0))

            self.logger.debug(
                "Arb orderbook: %s — Yes top3 asks: %s / No top3 asks: %s",
                mkt["question"][:30],
                [(float(a.get("price", 0)), float(a.get("size", 0))) for a in asks_yes[:3]],
                [(float(a.get("price", 0)), float(a.get("size", 0))) for a in asks_no[:3]],
            )

            if best_ask_yes <= 0 or best_ask_no <= 0:
                continue

            combined = best_ask_yes + best_ask_no
            edge = 1.0 - combined  # positive = profitable
            edge_pct = edge * 100.0

            if edge_pct < min_edge_pct:
                # Log at INFO periodically so user can see the monitor is
                # actively scanning and what the current spread looks like.
                log_interval = self.cfg.get("arb_log_interval", 5)
                now = time.time()
                last_t = self._edge_log_ts.get(cid, 0)
                if now - last_t >= log_interval:
                    self._edge_log_ts[cid] = now
                    self.logger.info(
                        "Arb scan: %s — %s=$%.3f + %s=$%.3f = $%.4f "
                        "(edge %.2f%% < min %.2f%%, no trade)",
                        mkt["question"][:40],
                        mkt["yes_label"], best_ask_yes,
                        mkt["no_label"], best_ask_no,
                        combined, edge_pct, min_edge_pct,
                    )
                continue

            # --- OPPORTUNITY FOUND ---
            # Calculate how many shares we can buy (limited by available
            # liquidity on both sides and our configured size).
            max_shares_by_budget = arb_size / max(best_ask_yes, best_ask_no)
            target_shares = min(max_shares_by_budget, avail_yes, avail_no)
            est_cost = round(target_shares * (best_ask_yes + best_ask_no), 2)
            est_profit = round(target_shares - est_cost, 4)

            self.logger.info(
                "ARB OPPORTUNITY: %s — %s=$%.3f + %s=$%.3f = $%.4f "
                "(edge %.2f%%, ~%.0f shares, est cost $%.2f, est profit $%.4f)",
                mkt["question"][:50],
                mkt["yes_label"], best_ask_yes,
                mkt["no_label"], best_ask_no,
                combined, edge_pct, target_shares, est_cost, est_profit,
            )

            if dry_run:
                self.logger.info("ARB DRY RUN — would buy both sides (skipping)")
                continue

            # Guard: abort if this market's window boundary has passed since
            # we started this scan cycle.  The condition_id and token IDs
            # from our snapshot belong to the *previous* window and
            # Polymarket may have already created the next market.
            slug_entry = mkt.get("_slug_entry")
            if slug_entry and cid in window_keys_at_start:
                current_wk = self._window_key_for(slug_entry)
                if current_wk != window_keys_at_start[cid]:
                    self.logger.warning(
                        "ARB SKIPPED: window rotated during scan — "
                        "aborting stale order for %s",
                        mkt["question"][:40],
                    )
                    continue

            # --- Execute first leg ---
            # Cap the FOK retry price to preserve the arb edge.
            # Max acceptable price for this leg = $1 - other_side_ask - min_margin.
            # This prevents the retry from accepting a fill that destroys the arb.
            max_retry_yes = round(1.0 - best_ask_no - 0.005, 2)
            yes_result, yes_shares, yes_cost = self._place_arb_leg(
                yes_token, best_ask_yes, target_shares, mkt["yes_label"],
                max_retry_price=max(max_retry_yes, 0.01),
            )

            if not yes_result:
                self.logger.warning(
                    "ARB ABORTED: first leg (%s) failed for %s",
                    mkt["yes_label"], mkt["question"][:40],
                )
                continue

            # Guard between legs: if window rotated after first leg, do NOT
            # place the second leg (it would target a different market).
            if slug_entry and cid in window_keys_at_start:
                current_wk = self._window_key_for(slug_entry)
                if current_wk != window_keys_at_start[cid]:
                    self.logger.warning(
                        "ARB ABORTED BETWEEN LEGS: window rotated after %s leg "
                        "filled (%.2f shares @ $%.2f) — skipping %s leg",
                        mkt["yes_label"], yes_shares, yes_cost,
                        mkt["no_label"],
                    )
                    self._active_positions[cid] = {
                        "question": mkt["question"],
                        "yes_fill": yes_result,
                        "no_fill": None,
                        "total_cost": yes_cost,
                        "locked_profit": 0,
                        "edge_pct": 0,
                        "partial": True,
                        "ts": datetime.now().isoformat(),
                    }
                    self._save_positions()
                    continue

            # --- Verify arb viability after first leg ---
            # The first leg may have filled at a different price than
            # expected.  Check that the actual cost per share + current
            # best ask for the other side still yields a profit.
            yes_eff_price = yes_cost / yes_shares if yes_shares > 0 else best_ask_yes
            try:
                fresh_book_no = self.clob_client.get_order_book(no_token)
                fresh_asks_no = sorted(
                    (fresh_book_no or {}).get("asks") or [],
                    key=lambda e: float(e.get("price", "0")),
                )
                fresh_ask_no = float(fresh_asks_no[0].get("price", 0)) if fresh_asks_no else 0
                fresh_avail_no = float(fresh_asks_no[0].get("size", 0)) if fresh_asks_no else 0
            except Exception:
                fresh_ask_no = best_ask_no
                fresh_avail_no = avail_no

            if fresh_ask_no <= 0:
                self.logger.warning(
                    "ARB ABORTED: no asks on %s side after first leg fill",
                    mkt["no_label"],
                )
                self._active_positions[cid] = {
                    "question": mkt["question"],
                    "yes_fill": yes_result, "no_fill": None,
                    "total_cost": yes_cost, "locked_profit": 0,
                    "edge_pct": 0, "partial": True,
                    "ts": datetime.now().isoformat(),
                }
                self._save_positions()
                continue

            real_combined = yes_eff_price + fresh_ask_no
            if real_combined >= 1.0:
                self.logger.warning(
                    "ARB ABORTED: after first leg, combined cost $%.4f >= $1 "
                    "(%s eff=$%.4f + %s ask=$%.4f) — no longer profitable",
                    real_combined,
                    mkt["yes_label"], yes_eff_price,
                    mkt["no_label"], fresh_ask_no,
                )
                self._active_positions[cid] = {
                    "question": mkt["question"],
                    "yes_fill": yes_result, "no_fill": None,
                    "total_cost": yes_cost, "locked_profit": 0,
                    "edge_pct": 0, "partial": True,
                    "ts": datetime.now().isoformat(),
                }
                self._save_positions()
                continue

            # --- Execute second leg ---
            # CRITICAL: use the ACTUAL share count from the first leg
            # so both sides have equal shares.  Also cap to available
            # liquidity on the second side.
            no_target_shares = min(yes_shares, fresh_avail_no)
            if no_target_shares < yes_shares * 0.95:
                self.logger.warning(
                    "ARB ABORTED: insufficient %s liquidity "
                    "(need %.2f shares, avail %.2f) for equal-share arb",
                    mkt["no_label"], yes_shares, fresh_avail_no,
                )
                self._active_positions[cid] = {
                    "question": mkt["question"],
                    "yes_fill": yes_result, "no_fill": None,
                    "total_cost": yes_cost, "locked_profit": 0,
                    "edge_pct": 0, "partial": True,
                    "ts": datetime.now().isoformat(),
                }
                self._save_positions()
                continue

            max_retry_no = round(1.0 - yes_eff_price - 0.005, 2)
            no_result, no_shares, no_cost = self._place_arb_leg(
                no_token, fresh_ask_no, no_target_shares, mkt["no_label"],
                max_retry_price=max(max_retry_no, 0.01),
            )

            if yes_result and no_result:
                actual_total_cost = yes_cost + no_cost
                actual_min_shares = min(yes_shares, no_shares)
                actual_profit = round(actual_min_shares - actual_total_cost, 4)
                actual_edge = round(
                    (1.0 - actual_total_cost / actual_min_shares) * 100
                    if actual_min_shares > 0 else 0, 2,
                )
                self._active_positions[cid] = {
                    "question": mkt["question"],
                    "yes_fill": yes_result,
                    "no_fill": no_result,
                    "yes_shares": yes_shares,
                    "no_shares": no_shares,
                    "total_cost": actual_total_cost,
                    "locked_profit": actual_profit,
                    "edge_pct": actual_edge,
                    "ts": datetime.now().isoformat(),
                }
                self._save_positions()
                self.logger.info(
                    "ARB EXECUTED: %s — %s=%.2f shares, %s=%.2f shares, "
                    "cost $%.2f, locked profit $%.4f (%.2f%%)",
                    mkt["question"][:50],
                    mkt["yes_label"], yes_shares,
                    mkt["no_label"], no_shares,
                    actual_total_cost, actual_profit, actual_edge,
                )
                if abs(yes_shares - no_shares) > 0.01:
                    self.logger.warning(
                        "ARB WARNING: share mismatch — %s=%.4f vs %s=%.4f "
                        "(diff=%.4f, %.2f unhedged shares)",
                        mkt["yes_label"], yes_shares,
                        mkt["no_label"], no_shares,
                        abs(yes_shares - no_shares),
                        abs(yes_shares - no_shares),
                    )
                # Log to trade history and send webhook
                self._log_arb_trade(self._active_positions[cid])
                self._notify_arb(
                    "ARB EXECUTED: %s — %s=%.2f, %s=%.2f shares, "
                    "cost $%.2f, locked profit $%.4f (%.2f%%)"
                    % (
                        mkt["question"][:50],
                        mkt["yes_label"], yes_shares,
                        mkt["no_label"], no_shares,
                        actual_total_cost, actual_profit, actual_edge,
                    )
                )
            else:
                self.logger.warning(
                    "ARB PARTIAL FILL: %s — %s=%s, %s=%s",
                    mkt["question"][:40],
                    mkt["yes_label"],
                    "%.2f shares" % yes_shares if yes_result else "FAILED",
                    mkt["no_label"],
                    "%.2f shares" % no_shares if no_result else "FAILED",
                )
                partial_pos = {
                    "question": mkt["question"],
                    "yes_fill": yes_result,
                    "no_fill": no_result,
                    "yes_shares": yes_shares if yes_result else 0,
                    "no_shares": no_shares if no_result else 0,
                    "total_cost": (yes_cost if yes_result else 0) + (no_cost if no_result else 0),
                    "locked_profit": 0,
                    "edge_pct": round(edge_pct, 4),
                    "partial": True,
                    "ts": datetime.now().isoformat(),
                }
                self._active_positions[cid] = partial_pos
                self._save_positions()
                self._log_arb_trade(partial_pos, is_partial=True)
                self._notify_arb(
                    "ARB PARTIAL FILL: %s — %s=%s, %s=%s (NEEDS ATTENTION)"
                    % (
                        mkt["question"][:40],
                        mkt["yes_label"],
                        "%.2f shares" % yes_shares if yes_result else "FAILED",
                        mkt["no_label"],
                        "%.2f shares" % no_shares if no_result else "FAILED",
                    )
                )

    def _place_arb_leg(self, token_id, price, target_shares, label,
                       max_retry_price=None):
        """Place a single FOK buy order for one side of the arb.

        Args:
            token_id: Token to buy.
            price: Expected price per share (best ask at scan time).
            target_shares: Number of shares to acquire.
            label: Human-readable label for logging (e.g. "Up").
            max_retry_price: If set, caps the FOK retry price to this
                value.  For arb orders this prevents the retry from
                accepting a fill price that destroys the arb edge.

        Returns:
            ``(result_dict, actual_shares, actual_cost)`` on success,
            ``(None, 0, 0)`` on failure.
        """
        size_usdc = round(target_shares * price, 2)
        # Send the initial FOK at the max arb-viable price instead of
        # the exact best ask.  The CLOB fills at the best available
        # price up to this limit, so small price movements between the
        # orderbook fetch and the order submission won't cause a
        # rejection.  Using the exact best ask (old behaviour) meant
        # *any* movement resulted in a FOK rejection.
        fok_price = price
        if max_retry_price and max_retry_price > price:
            fok_price = max_retry_price
        try:
            result = self.clob_client.place_order(
                token_id=token_id,
                side="BUY",
                size_usdc=size_usdc,
                price=fok_price,
                use_fok=True,
                max_retry_price=max_retry_price,
            )
            # Guard against FOK rejections that return a truthy dict like
            # {"status": "fok_rejected", ...} or {"error": ...} — these
            # are *not* successful fills and must not be treated as such.
            if isinstance(result, dict) and (
                result.get("status") == "fok_rejected"
                or result.get("error")
            ):
                self.logger.warning(
                    "Arb leg NOT filled (%s): %s", label, result,
                )
                return None, 0.0, 0.0

            # Extract actual fill amounts from the CLOB response.
            # For BUY: takingAmount = shares received, makingAmount = USDC spent.
            actual_shares = 0.0
            actual_cost = 0.0
            if isinstance(result, dict):
                try:
                    actual_shares = float(result.get("takingAmount", 0))
                except (ValueError, TypeError):
                    pass
                try:
                    actual_cost = float(result.get("makingAmount", 0))
                except (ValueError, TypeError):
                    pass
            # Fallback: if response doesn't have these fields, use our
            # planned values as a best-effort estimate.
            if actual_shares <= 0:
                actual_shares = target_shares
            if actual_cost <= 0:
                actual_cost = size_usdc

            if result:
                eff_price = actual_cost / actual_shares if actual_shares > 0 else price
                self.logger.info(
                    "Arb leg filled: %s — %.2f shares @ $%.4f eff "
                    "(planned %.2f @ $%.3f, cost $%.2f) — %s",
                    label, actual_shares, eff_price,
                    target_shares, price, actual_cost,
                    str(result)[:100],
                )
            return result, actual_shares, actual_cost
        except Exception as exc:
            self.logger.error("Arb leg FAILED (%s): %s", label, exc)
            return None, 0.0, 0.0

    # -- public helpers (used by GUI / dashboard) ---------------------------

    @property
    def active_position_count(self):
        return len(self._active_positions)

    def get_status_summary(self):
        """Return a one-line status string for the dashboard."""
        n_markets = len(self._markets)
        n_pos = len(self._active_positions)
        total_locked = sum(
            p.get("locked_profit", 0) for p in self._active_positions.values()
        )
        return (
            f"Markets: {n_markets} | Active arbs: {n_pos} | "
            f"Locked profit: ${total_locked:,.4f}"
        )


# ---------------------------------------------------------------------------
# Martingale Bot – double-on-loss betting on 5-min BTC binary markets
# ---------------------------------------------------------------------------

class MartingaleBot(threading.Thread):
    """Martingale strategy on binary Polymarket markets.

    Places a directional bet (Up or Down) on a rotating binary market.
    If the bet wins, resets to the starting bet.  If it loses, doubles
    the bet for the next window.

    State is persisted to a JSON file so it survives restarts.

    When *strategy* is supplied (a dict with keys like ``name``,
    ``slug_base``, ``start_bet``, etc.) the bot uses those values
    instead of the flat ``martingale_*`` keys in *cfg*.  This allows
    multiple MartingaleBot instances to run concurrently with different
    parameters (e.g. $1 on 5-min markets, $5 on 15-min markets).
    """

    STATE_FILE = "martingale_state.json"

    def __init__(self, clob_client, cfg, logger=None, strategy=None,
                 executor=None):
        self.strategy = strategy or {}
        self.strategy_name = self.strategy.get("name", "default")

        # Sanitise name for file / thread naming (alphanum + dash/underscore)
        safe_name = "".join(
            c if c.isalnum() or c in "-_" else "_"
            for c in self.strategy_name.lower()
        )

        thread_name = (
            f"MartingaleBot-{safe_name}" if strategy else "MartingaleBot"
        )
        super().__init__(daemon=True, name=thread_name)

        self.clob_client = clob_client
        # Validate executor type — must be a TradeExecutor (or at least have
        # ensure_usdc_approval).  A misconfiguration can pass the clob_client
        # as executor, which silently breaks on-chain approval calls.
        if executor is not None and not hasattr(executor, "ensure_usdc_approval"):
            _log = logger or logging.getLogger("martingale")
            _log.error(
                "MARTINGALE: executor is %s (expected TradeExecutor) — "
                "on-chain approval/balance checks will be disabled. "
                "Ensure an RPC URL is configured so the TradeExecutor "
                "can be initialized.",
                type(executor).__name__,
            )
            executor = None
        self.executor = executor  # TradeExecutor for on-chain ops (approval, balance)
        self.cfg = cfg
        self.logger = logger or logging.getLogger("martingale")
        self._stop_event = threading.Event()

        # Per-strategy state file — avoids collisions when running
        # multiple strategies simultaneously.
        if strategy:
            self.STATE_FILE = f"martingale_state_{safe_name}.json"

        # State — strategy dict overrides flat config keys
        self.start_bet = float(self._scfg("start_bet", "martingale_start_bet", 5.0))
        self.current_bet = self.start_bet
        self.direction = self._scfg("direction", "martingale_direction", "Up")
        self.consecutive_losses = 0
        self.session_pnl = 0.0
        self._active_bet = None
        self._bet_history = []
        self._last_window_ts = 0
        self._next_window_cache = None  # pre-fetched market for next window
        self._skip_reason = None        # last reason a window was skipped
        self._skip_price = None         # ask price when last skip occurred
        self._skip_gap = None           # "gap_up", "gap_down", or None
        self._miss_recorded_for_ts = 0  # last window_ts we recorded a miss for (dedup)
        self._fok_retry_count = 0       # FOK rejection retries within current window
        self._fok_retry_window_ts = 0   # window_ts the retry counter belongs to
        self._fok_net_err_count = 0     # FOK network errors within current window
        self._fok_net_err_window_ts = 0 # window_ts the net-error counter belongs to
        self._windows_attempted = 0     # windows where we tried to bet
        self._windows_no_market = 0     # market slug not found on Gamma API
        self._missed_windows = []       # windows skipped due to price/book/balance

        # Streak-pause recovery state
        self._streak_paused = False     # True when max_streak hit, waiting for recovery
        self._streak_paused_at = None   # datetime when pause started
        self._recovery_candles = []     # list of {"open": px, "close": px, "green": bool}
        self._recovery_candle_open = None   # price at start of current candle
        self._recovery_candle_ts = 0    # epoch when current candle opened

        # Streak-level trend confirmation (e.g. 2/3 candles green before betting at streak 7)
        self._streak_confirming = False        # True when waiting for trend confirmation
        self._streak_confirmed = False         # One-shot flag: confirmation just passed, skip re-entry gate
        self._streak_confirm_candles = []      # list of {"open": px, "close": px, "green": bool}
        self._streak_confirm_candle_open = None
        self._streak_confirm_candle_ts = 0

        # Post-recovery confirmation mode — after recovery from streak pause,
        # require confirmation candles before EVERY bet until a win or hard reset.
        self._post_recovery_mode = False
        self._post_recovery_losses = 0  # losses since recovery; confirmation kicks in after 2

        # Timing metrics
        self._bet_placed_at = None          # time.time() when bet was placed
        self._window_open_at = None         # time.time() when we first saw the new window
        self._last_heartbeat = 0            # time.time() of last periodic summary log
        self._heartbeat_interval = 300      # seconds between heartbeat logs
        self._session_wins = 0              # win count this session
        self._session_losses = 0            # loss count this session
        self._total_resolution_time = 0.0   # cumulative resolution wait seconds
        self._resolution_count = 0          # number of resolved bets (for avg calc)
        self._fok_network_errors = 0        # FOK network errors this session
        self._fok_rejections = 0            # FOK clean rejections this session
        self._phantom_fills = 0             # phantom fills detected this session

        # Callbacks (wired by CopyTraderBot)
        self.notify_callback = None
        self.log_trade_callback = None

        # Validate slug_base vs window — auto-correct common misconfigs
        self._validate_slug_window()

        self._load_state()

    # -- strategy-aware config helper ----------------------------------------

    def _scfg(self, strategy_key, config_key=None, default=None):
        """Return a config value, preferring the per-strategy dict.

        Lookup order:
        1. ``self.strategy[strategy_key]``
        2. ``self.cfg[config_key]``   (flat martingale_* key)
        3. *default*
        """
        val = self.strategy.get(strategy_key)
        if val is not None:
            return val
        if config_key:
            return self.cfg.get(config_key, default)
        return default

    def _do_usdc_approval(self, spender, amount_raw, neg_risk=False):
        """Try USDC approval via executor, then fall back to clob_client.

        Returns True only when approval is confirmed (on-chain allowance
        is sufficient).  Returns False with a WARNING log when approval
        could not be verified.
        """
        # Build ordered list of approvers to try
        approvers = []
        if self.executor and hasattr(self.executor, "ensure_usdc_approval"):
            approvers.append(("TradeExecutor", self.executor))
        if self.clob_client and hasattr(self.clob_client, "ensure_usdc_approval"):
            approvers.append(("PolymarketCLOBClient", self.clob_client))

        if not approvers:
            self.logger.warning(
                "MARTINGALE: no approver available — neither executor nor "
                "clob_client has ensure_usdc_approval.  Configure rpc_url "
                "in config.json to enable on-chain approvals."
            )
            return False

        last_err = None
        for name, approver in approvers:
            try:
                approver.ensure_usdc_approval(spender, amount_raw)
                if neg_risk:
                    approver.ensure_usdc_approval(
                        NEG_RISK_CTF_EXCHANGE_ADDRESS, amount_raw,
                    )
                return True
            except Exception as exc:
                last_err = exc
                self.logger.warning(
                    "MARTINGALE: approval via %s failed (%s)", name, exc,
                )

        self.logger.warning(
            "MARTINGALE: all approval attempts failed — last error: %s. "
            "Orders will likely fail with 'not enough balance / allowance'.",
            last_err,
        )
        return False

    def _diagnose_allowance(self, spender, amount_raw):
        """Check on-chain USDC balance & allowance for diagnostics.

        Returns a human-readable string describing the wallet state so
        the log message is actionable.
        """
        # Try via executor first, then clob_client
        w3 = None
        account = None
        for src_name, src in [("executor", self.executor),
                              ("clob_client", self.clob_client)]:
            if src is None:
                continue
            _w3 = getattr(src, "_w3", None) or getattr(src, "w3", None)
            _acct = getattr(src, "_account", None) or getattr(src, "account", None)
            if _w3 and _acct:
                w3, account = _w3, _acct
                break
            # For clob_client, try to init web3 on the fly
            if hasattr(src, "_ensure_w3"):
                try:
                    src._ensure_w3()
                    _w3 = getattr(src, "_w3", None)
                    _acct = getattr(src, "_account", None)
                    if _w3 and _acct:
                        w3, account = _w3, _acct
                        break
                except Exception:
                    pass

        if not w3 or not account:
            return ("Cannot diagnose — no Web3 connection. "
                    "Set rpc_url in config.json.")

        try:
            if Web3 is None:
                return "Cannot diagnose — web3 package not installed."
            usdc = w3.eth.contract(
                address=Web3.to_checksum_address(USDC_ADDRESS), abi=ERC20_ABI,
            )
            balance = usdc.functions.balanceOf(account.address).call()
            allowance = usdc.functions.allowance(
                account.address, Web3.to_checksum_address(spender),
            ).call()
            bal_usdc = balance / 1_000_000
            allow_usdc = allowance / 1_000_000
            need_usdc = amount_raw / 1_000_000
            parts = []
            if balance < amount_raw:
                parts.append(
                    f"BALANCE TOO LOW: ${bal_usdc:.2f} USDC "
                    f"(need ${need_usdc:.2f})"
                )
            else:
                parts.append(f"Balance OK: ${bal_usdc:.2f} USDC")
            if allowance < amount_raw:
                parts.append(
                    f"ALLOWANCE TOO LOW: ${allow_usdc:.2f} approved "
                    f"(need ${need_usdc:.2f}) for {spender[:10]}..."
                )
            else:
                parts.append(f"Allowance OK: ${allow_usdc:.2f}")
            return " | ".join(parts)
        except Exception as exc:
            return f"Diagnosis failed: {exc}"

    # -- config validation --------------------------------------------------

    # Map common timeframe suffixes in slug_base → expected window seconds
    _SLUG_TIMEFRAME_MAP = {
        "1m": 60, "2m": 120, "3m": 180, "5m": 300, "10m": 600,
        "15m": 900, "30m": 1800, "1h": 3600,
    }

    def _validate_slug_window(self):
        """Warn (and auto-correct) when slug_base implies a different
        window than what is configured.

        E.g. slug_base='btc-updown-15m' clearly implies a 900s window,
        but a user might forget to change window from the default 300.
        """
        slug_base = self._scfg("slug_base", "martingale_slug_base", "")
        window = int(self._scfg("window", "martingale_window", 300))

        # Try to infer expected window from slug_base suffix
        inferred = None
        for suffix, seconds in self._SLUG_TIMEFRAME_MAP.items():
            if slug_base.endswith(f"-{suffix}") or slug_base.endswith(f"_{suffix}"):
                inferred = seconds
                break

        if inferred and inferred != window:
            self.logger.warning(
                "MARTINGALE [%s] CONFIG MISMATCH: slug_base '%s' implies "
                "window=%ds but config has window=%ds — auto-correcting to %ds. "
                "Please fix your strategy config.",
                self.strategy_name, slug_base, inferred, window, inferred,
            )
            # Auto-correct in the strategy dict so all downstream code
            # uses the right window.
            self.strategy["window"] = inferred

    # -- structured event logging -------------------------------------------

    def _log_event(self, event_type, **kwargs):
        """Append a structured event to the martingale events log.

        Each event is a JSON object with a timestamp, strategy name,
        event type, and type-specific fields.  This provides a complete,
        machine-parseable audit trail of all martingale activity.
        """
        event = {
            "ts": datetime.now().isoformat(),
            "epoch": int(time.time()),
            "strategy": self.strategy_name,
            "event": event_type,
            "streak": self.consecutive_losses,
            "current_bet": self.current_bet,
            "session_pnl": round(self.session_pnl, 6),
        }
        event.update(kwargs)

        try:
            with _MARTINGALE_EVENTS_LOCK:
                try:
                    with open(MARTINGALE_EVENTS_FILE, "r") as fh:
                        events = json.load(fh)
                except (FileNotFoundError, json.JSONDecodeError):
                    events = []
                events.append(event)
                # Cap at 2000 events to prevent unbounded growth
                if len(events) > 2000:
                    events = events[-2000:]
                tmp = MARTINGALE_EVENTS_FILE + ".tmp"
                with open(tmp, "w") as fh:
                    json.dump(events, fh, indent=2, default=str)
                os.replace(tmp, MARTINGALE_EVENTS_FILE)
        except OSError as exc:
            self.logger.debug("Could not write martingale event: %s", exc)

    def _log_heartbeat(self):
        """Periodically log a summary of bot health and performance.

        Called each cycle; only emits a log entry every
        ``_heartbeat_interval`` seconds.
        """
        now = time.time()
        if now - self._last_heartbeat < self._heartbeat_interval:
            return
        self._last_heartbeat = now

        avg_res = (
            round(self._total_resolution_time / self._resolution_count, 1)
            if self._resolution_count > 0 else 0
        )
        win_rate = (
            round(self._session_wins / (self._session_wins + self._session_losses) * 100, 1)
            if (self._session_wins + self._session_losses) > 0 else 0
        )

        status = "PAUSED" if self._streak_paused else (
            "CONFIRMING" if self._streak_confirming else (
                "IN_POSITION" if self._active_bet else "HUNTING"
            )
        )
        if self._post_recovery_mode and not self._streak_paused:
            status += " [POST-RECOVERY]"

        recovery_info = ""
        recovery_event_data = {}
        if self._streak_paused:
            favorable = sum(
                1 for c in self._recovery_candles
                if c.get("favorable", c.get("green"))
            )
            total = len(self._recovery_candles)
            n_needed = int(self._scfg(
                "recovery_green", "martingale_recovery_green", 3))
            n_candles = int(self._scfg(
                "recovery_candles", "martingale_recovery_candles", 5))
            paused_mins = ""
            if self._streak_paused_at:
                delta = datetime.now() - self._streak_paused_at
                paused_mins = f", paused {int(delta.total_seconds() // 60)}m"
            recovery_info = (
                f" | recovery={favorable}/{total} favorable "
                f"(need {n_needed}/{n_candles}){paused_mins}"
            )
            recovery_event_data = {
                "recovery_favorable": favorable,
                "recovery_total": total,
                "recovery_needed_green": n_needed,
                "recovery_needed_candles": n_candles,
            }

        self.logger.info(
            "MARTINGALE [%s] HEARTBEAT: status=%s | W/L=%d/%d (%.1f%%) | "
            "P&L=$%.4f | bet=$%.2f | streak=%d | avg_resolution=%.1fs | "
            "windows_attempted=%d | missed=%d | "
            "fok_errors=%d | fok_rejects=%d | phantom_fills=%d%s",
            self.strategy_name, status, self._session_wins,
            self._session_losses, win_rate, self.session_pnl,
            self.current_bet, self.consecutive_losses, avg_res,
            self._windows_attempted, len(self._missed_windows),
            self._fok_network_errors, self._fok_rejections,
            self._phantom_fills, recovery_info,
        )

        self._log_event("heartbeat",
            status=status,
            session_wins=self._session_wins,
            session_losses=self._session_losses,
            win_rate_pct=win_rate,
            avg_resolution_seconds=avg_res,
            windows_attempted=self._windows_attempted,
            windows_missed=len(self._missed_windows),
            fok_network_errors=self._fok_network_errors,
            fok_rejections=self._fok_rejections,
            phantom_fills=self._phantom_fills,
            **recovery_event_data,
        )

    # -- persistence --------------------------------------------------------

    def _save_state(self):
        state = {
            "current_bet": self.current_bet,
            "consecutive_losses": self.consecutive_losses,
            "session_pnl": self.session_pnl,
            "direction": self.direction,
            "active_bet": self._active_bet,
            "last_window_ts": self._last_window_ts,
            "missed_windows": self._missed_windows[-500:],  # cap at 500
            "streak_paused": self._streak_paused,
            "streak_paused_at": self._streak_paused_at.isoformat() if self._streak_paused_at else None,
            "recovery_candles": self._recovery_candles[-20:],
            "streak_confirming": self._streak_confirming,
            "streak_confirm_candles": self._streak_confirm_candles[-10:],
            "post_recovery_mode": self._post_recovery_mode,
            "post_recovery_losses": self._post_recovery_losses,
        }
        try:
            tmp = self.STATE_FILE + ".tmp"
            with open(tmp, "w") as fh:
                json.dump(state, fh, indent=2, default=str)
            os.replace(tmp, self.STATE_FILE)
        except OSError as exc:
            self.logger.debug("Could not save martingale state: %s", exc)

    def _load_state(self):
        try:
            with open(self.STATE_FILE, "r") as fh:
                state = json.load(fh)
            self.current_bet = float(state.get("current_bet", self.start_bet))
            self.consecutive_losses = int(state.get("consecutive_losses", 0))
            # Consistency guard: current_bet must match start_bet × 2^streak.
            # A stale state file can leave these out of sync (e.g. $48 at
            # streak 0), causing wildly wrong bet sizes.
            expected_bet = round(self.start_bet * (2 ** self.consecutive_losses), 2)
            max_bet = float(self._scfg("max_bet", "martingale_max_bet", 0))
            if max_bet > 0:
                expected_bet = min(expected_bet, max_bet)
            if abs(self.current_bet - expected_bet) > 0.01:
                self.logger.warning(
                    "MARTINGALE [%s]: state mismatch — loaded bet=$%.2f but "
                    "expected $%.2f (start=$%.2f × 2^%d) — correcting",
                    self.strategy_name, self.current_bet, expected_bet,
                    self.start_bet, self.consecutive_losses,
                )
                self.current_bet = expected_bet
                self._save_state()
            self.session_pnl = float(state.get("session_pnl", 0.0))
            self.direction = state.get("direction", self.direction)
            self._active_bet = state.get("active_bet")
            # Re-register the token with the executor so the portfolio
            # scan / auto-exit logic knows not to touch it.
            if self._active_bet and self.executor:
                tid = self._active_bet.get("token_id")
                if tid:
                    self.executor._martingale_token_ids.add(tid)
            self._last_window_ts = int(state.get("last_window_ts", 0))
            self._missed_windows = state.get("missed_windows", [])
            self._streak_paused = bool(state.get("streak_paused", False))
            paused_at_str = state.get("streak_paused_at")
            if paused_at_str:
                try:
                    self._streak_paused_at = datetime.fromisoformat(paused_at_str)
                except (ValueError, TypeError):
                    self._streak_paused_at = None
            self._recovery_candles = state.get("recovery_candles", [])
            self._streak_confirming = bool(state.get("streak_confirming", False))
            self._streak_confirm_candles = state.get("streak_confirm_candles", [])
            self._post_recovery_mode = bool(state.get("post_recovery_mode", False))
            self._post_recovery_losses = int(state.get("post_recovery_losses", 0))

            # On restart while confirming, reset candle sampling so we
            # start fresh with current market prices.
            if self._streak_confirming:
                self._streak_confirm_candle_open = None
                self._streak_confirm_candle_ts = 0
                n_total = int(self._scfg(
                    "streak_confirm_total", "martingale_streak_confirm_total", 3))
                n_green = int(self._scfg(
                    "streak_confirm_green", "martingale_streak_confirm_green", 2))
                # Prune stale confirm candles
                interval = int(self._scfg(
                    "recovery_interval", "martingale_recovery_interval", 300))
                cutoff = int(time.time()) - (n_total * interval)
                self._streak_confirm_candles = [
                    c for c in self._streak_confirm_candles
                    if c.get("ts", 0) >= cutoff
                ]
                green = sum(1 for c in self._streak_confirm_candles if c["green"])
                self.logger.info(
                    "MARTINGALE [%s]: streak confirmation on restart: "
                    "%d/%d candles (%d green, need %d/%d)",
                    self.strategy_name, len(self._streak_confirm_candles),
                    n_total, green, n_green, n_total,
                )

            # On restart while paused, prune recovery candles that are
            # outside the lookback window so the bot evaluates recent
            # market conditions.  Candles within the last N*interval
            # seconds are kept — the bot only needs to fill the gap
            # rather than re-collecting all N from scratch.
            if self._streak_paused and self._recovery_candles:
                interval = int(self._scfg(
                    "recovery_interval", "martingale_recovery_interval", 300))
                n_candles = int(self._scfg(
                    "recovery_candles", "martingale_recovery_candles", 5))
                cutoff = int(time.time()) - (n_candles * interval)
                before = len(self._recovery_candles)
                self._recovery_candles = [
                    c for c in self._recovery_candles if c.get("ts", 0) >= cutoff
                ]
                pruned = before - len(self._recovery_candles)
                # Reset open candle so we start sampling fresh
                self._recovery_candle_open = None
                self._recovery_candle_ts = 0
                if pruned:
                    self.logger.info(
                        "MARTINGALE [%s]: pruned %d stale recovery candle(s), "
                        "kept %d recent — need %d more",
                        self.strategy_name, pruned,
                        len(self._recovery_candles),
                        n_candles - len(self._recovery_candles),
                    )
                    self._save_state()
                favorable = sum(
                    1 for c in self._recovery_candles
                    if c["green"]
                )
                self.logger.info(
                    "MARTINGALE [%s]: recovery status on restart: "
                    "%d/%d candles (%d favorable, need %d)",
                    self.strategy_name, len(self._recovery_candles),
                    n_candles, favorable,
                    int(self._scfg(
                        "recovery_green", "martingale_recovery_green", 3)),
                )

            self.logger.info(
                "Loaded martingale state: bet=$%.2f, streak=%d, pnl=$%.4f, "
                "dir=%s, last_window_ts=%d, missed=%d%s",
                self.current_bet, self.consecutive_losses,
                self.session_pnl, self.direction, self._last_window_ts,
                len(self._missed_windows),
                " [PAUSED — waiting for recovery]" if self._streak_paused
                else " [CONFIRMING — waiting for trend]" if self._streak_confirming
                else "",
            )

            # Discard stale active bets from a previous session.
            # If the bet's window ended more than 2 windows ago, it is
            # from an old run that was stopped before resolution.  Do NOT
            # treat it as a timeout loss — just drop it.
            if self._active_bet:
                window = int(self._scfg("window", "martingale_window", 300))
                now = int(time.time())
                window_end = int(self._active_bet.get("window_end", 0))
                if window_end and now > window_end + window * 2:
                    self.logger.warning(
                        "MARTINGALE: discarding stale active bet from "
                        "window ending %d (%ds ago) — will NOT count as loss",
                        window_end, now - window_end,
                    )
                    self._active_bet = None
                    self._save_state()

        except (FileNotFoundError, json.JSONDecodeError, OSError):
            pass

    def reset_state(self):
        """Wipe persisted state and reset to defaults."""
        self.current_bet = self.start_bet
        self.consecutive_losses = 0
        self.session_pnl = 0.0
        self._active_bet = None
        self._last_window_ts = 0
        self._fok_retry_count = 0
        self._fok_retry_window_ts = 0
        self._streak_paused = False
        self._streak_paused_at = None
        self._recovery_candles = []
        self._recovery_candle_open = None
        self._recovery_candle_ts = 0
        self._recovery_pause_window_ts = 0
        self._recovery_last_logged_ts = 0
        self._post_recovery_mode = False
        self._post_recovery_losses = 0
        try:
            os.remove(self.STATE_FILE)
        except OSError:
            pass
        self.logger.info(
            "MARTINGALE: state reset — bet=$%.2f, streak=0", self.start_bet,
        )

    # -- slug / timing helpers ----------------------------------------------

    @staticmethod
    def _get_window_ts(window):
        """Return the current window-start unix timestamp."""
        window = max(int(window), 30)
        now = int(time.time())
        return now - (now % window)

    def _generate_slug(self):
        base = self._scfg("slug_base", "martingale_slug_base", "btc-updown-5m")
        window = int(self._scfg("window", "martingale_window", 300))
        window_ts = self._get_window_ts(window)
        return f"{base}-{window_ts}"

    def _get_window_end(self):
        window = int(self._scfg("window", "martingale_window", 300))
        return self._get_window_ts(window) + window

    # -- market fetching ----------------------------------------------------

    def _fetch_market(self, slug):
        """Fetch a binary market from the Gamma API by event slug."""
        try:
            url = f"{GAMMA_API_BASE}/events"
            data = self.clob_client._get_public(url, params={"slug": slug})

            event = None
            if isinstance(data, list) and data:
                event = data[0]
            elif isinstance(data, dict):
                event = data

            if not event:
                return None

            markets = event.get("markets") or []
            if not markets:
                return None

            market = markets[0]
            cid = market.get("condition_id")

            # Parse token IDs
            token_ids = []
            raw_clob = market.get("clobTokenIds")
            if isinstance(raw_clob, str):
                try:
                    token_ids = json.loads(raw_clob)
                except (json.JSONDecodeError, TypeError):
                    pass
            elif isinstance(raw_clob, list):
                token_ids = raw_clob

            # Parse outcomes
            outcomes = []
            raw_out = market.get("outcomes")
            if isinstance(raw_out, str):
                try:
                    outcomes = json.loads(raw_out)
                except (json.JSONDecodeError, TypeError):
                    pass
            elif isinstance(raw_out, list):
                outcomes = raw_out

            # Fallback: tokens array
            if len(token_ids) < 2:
                tokens = market.get("tokens") or []
                if len(tokens) >= 2:
                    token_ids = [
                        t.get("token_id") or t.get("tokenId") for t in tokens
                    ]
                    if not outcomes:
                        outcomes = [t.get("outcome") for t in tokens]

            if len(token_ids) < 2 or len(outcomes) < 2:
                return None

            # Map Up/Down to indices — default to first=Up, second=Down
            up_idx, down_idx = 0, 1
            for i, o in enumerate(outcomes):
                if o and str(o).lower() == "up":
                    up_idx = i
                elif o and str(o).lower() == "down":
                    down_idx = i

            # Detect neg_risk — needed for correct exchange approval
            neg_risk = False
            neg = market.get("neg_risk")
            if isinstance(neg, bool):
                neg_risk = neg
            elif isinstance(neg, str):
                neg_risk = neg.lower() in ("true", "1", "yes")
            if not neg_risk:
                neg_risk = bool(market.get("neg_risk_market_id"))

            return {
                "condition_id": cid,
                "up_token": token_ids[up_idx],
                "down_token": token_ids[down_idx],
                "question": market.get("question", slug),
                "neg_risk": neg_risk,
            }
        except Exception as exc:
            self.logger.warning("Failed to fetch market '%s': %s", slug, exc)
            return None

    def _get_best_ask(self, token_id):
        """Return (price, size) of the best ask for *token_id*."""
        try:
            book = self.clob_client.get_order_book(token_id)
            asks = book.get("asks") or []
            if not asks:
                return None, 0.0
            asks = sorted(asks, key=lambda e: float(e.get("price", "0")))
            return float(asks[0]["price"]), float(asks[0].get("size", 0))
        except Exception:
            return None, 0.0

    def _get_book_depth(self, token_id, max_price):
        """Return total shares available on the ask side up to *max_price*.

        Aggregates across all price levels so we know the true fillable
        depth for a FOK/market order, not just top-of-book.
        """
        try:
            book = self.clob_client.get_order_book(token_id)
            asks = book.get("asks") or []
            total = 0.0
            for level in asks:
                px = float(level.get("price", "0"))
                if px <= max_price:
                    total += float(level.get("size", 0))
            return total
        except Exception:
            return 0.0

    # -- resolution detection -----------------------------------------------

    def _check_resolution(self, condition_id, direction):
        """Poll Gamma API to see if a market has resolved.

        Returns ``(resolved, won)`` where *won* is ``True`` if our
        direction won, ``False`` if it lost, or ``None`` if not yet resolved.
        """
        try:
            url = f"{GAMMA_API_BASE}/markets"
            data = self.clob_client._get_public(
                url, params={"condition_id": condition_id},
            )
            market = None
            if isinstance(data, list) and data:
                market = data[0]
            elif isinstance(data, dict):
                market = data

            if not market:
                self.logger.info(
                    "MARTINGALE RESOLUTION: no market data for %s",
                    condition_id[:16] + "...",
                )
                return False, None

            # Check closure flag
            closed = market.get("closed", False)
            active = market.get("active", True)

            # Parse outcome prices (needed for both API and orderbook paths)
            raw_prices = market.get("outcomePrices")
            raw_outcomes = market.get("outcomes")

            outcome_prices = None
            if raw_prices:
                if isinstance(raw_prices, str):
                    try:
                        outcome_prices = [float(x) for x in json.loads(raw_prices)]
                    except (json.JSONDecodeError, TypeError, ValueError):
                        try:
                            outcome_prices = [
                                float(x.strip()) for x in raw_prices.split(",")
                            ]
                        except (ValueError, AttributeError):
                            pass
                elif isinstance(raw_prices, list):
                    try:
                        outcome_prices = [float(x) for x in raw_prices]
                    except (ValueError, TypeError):
                        pass

            outcomes = None
            if raw_outcomes:
                if isinstance(raw_outcomes, str):
                    try:
                        outcomes = json.loads(raw_outcomes)
                    except (json.JSONDecodeError, TypeError):
                        outcomes = [x.strip() for x in raw_outcomes.split(",")]
                elif isinstance(raw_outcomes, list):
                    outcomes = raw_outcomes
            if not outcomes:
                outcomes = ["Up", "Down"]

            if not closed and active:
                self.logger.info(
                    "MARTINGALE RESOLUTION: market not closed yet "
                    "(closed=%s, active=%s, prices=%s)",
                    closed, active, outcome_prices,
                )
                return False, None

            if not outcome_prices or len(outcome_prices) < 2:
                self.logger.info(
                    "MARTINGALE RESOLUTION: no outcome prices yet "
                    "(closed=%s, prices=%s)",
                    closed, outcome_prices,
                )
                return False, None

            # Map direction to outcome index.  Gamma API outcomes are
            # typically ["Yes","No"] while our direction is "Up"/"Down".
            # "Up" maps to index 0 (Yes), "Down" maps to index 1 (No).
            dir_idx = None
            for i, o in enumerate(outcomes):
                ol = str(o).lower()
                dl = str(direction).lower()
                if ol == dl or (dl == "up" and ol == "yes") or (dl == "down" and ol == "no"):
                    dir_idx = i
                    break
            if dir_idx is None:
                # Fallback: Up=0, Down=1
                dir_idx = 0 if str(direction).lower() == "up" else 1

            # Need a clear winner (one price near 1.0)
            if max(outcome_prices) < 0.9:
                # If the market is closed/inactive AND our side's price
                # is near zero, detect a loss without waiting for 1.0.
                if (not active or closed):
                    if outcome_prices[dir_idx] <= 0.05:
                        self.logger.info(
                            "MARTINGALE RESOLVED (early): %s price=%.4f "
                            "≤ 0.05 on closed market — treating as LOSS",
                            direction, outcome_prices[dir_idx],
                        )
                        return True, False
                    # Opposite side near zero → we won
                    opp_idx = 1 - dir_idx
                    if outcome_prices[opp_idx] <= 0.05 and outcome_prices[dir_idx] > 0.5:
                        self.logger.info(
                            "MARTINGALE RESOLVED (early): opposite price=%.4f "
                            "≤ 0.05, %s price=%.4f — treating as WIN",
                            outcome_prices[opp_idx], direction,
                            outcome_prices[dir_idx],
                        )
                        return True, True
                self.logger.info(
                    "MARTINGALE RESOLUTION: no clear winner yet "
                    "(prices=%s, max=%.2f < 0.9)",
                    outcome_prices, max(outcome_prices),
                )
                return False, None

            winner_idx = outcome_prices.index(max(outcome_prices))
            won = winner_idx == dir_idx
            winner = outcomes[winner_idx] if winner_idx < len(outcomes) else "?"

            self.logger.info(
                "MARTINGALE RESOLVED: winner=%s (idx=%d), bet=%s (idx=%d), won=%s",
                winner, winner_idx, direction, dir_idx, won,
            )
            return True, won
        except Exception as exc:
            self.logger.info("MARTINGALE RESOLUTION: check error: %s", exc)
            return False, None

    def _check_resolution_orderbook(self, bet):
        """Fallback resolution via orderbook + last-trade prices.

        Uses multiple signals since the orderbook often empties after
        a 5-minute window closes:

        1. Orderbook best ask on our side (original logic)
        2. Last-trade-price on our side (persists after book empties)
        3. Last-trade-price on the opposite side
        4. Orderbook best ask on the opposite side
        """
        try:
            token_id = bet["token_id"]
            opp_token_id = bet.get("opposite_token_id")

            # --- 1. Our side orderbook ---
            ask_price, _ = self._get_best_ask(token_id)
            if ask_price is not None:
                if ask_price >= 0.95:
                    self.logger.info(
                        "MARTINGALE ORDERBOOK RESOLUTION: %s ask=$%.4f >= $0.95 — "
                        "treating as WIN",
                        bet["direction"], ask_price,
                    )
                    return True
                if ask_price <= 0.05:
                    self.logger.info(
                        "MARTINGALE ORDERBOOK RESOLUTION: %s ask=$%.4f <= $0.05 — "
                        "treating as LOSS",
                        bet["direction"], ask_price,
                    )
                    return False

            # --- 2. Our side last-trade-price ---
            our_ltp = self.clob_client.get_last_trade_price(token_id)
            if our_ltp is not None:
                if our_ltp >= 0.95:
                    self.logger.info(
                        "MARTINGALE LTP RESOLUTION: %s ltp=$%.4f >= $0.95 — "
                        "treating as WIN",
                        bet["direction"], our_ltp,
                    )
                    return True
                if our_ltp <= 0.05:
                    self.logger.info(
                        "MARTINGALE LTP RESOLUTION: %s ltp=$%.4f <= $0.05 — "
                        "treating as LOSS",
                        bet["direction"], our_ltp,
                    )
                    return False

            # --- 3 & 4. Opposite side (if token ID available) ---
            if opp_token_id:
                opp_ltp = self.clob_client.get_last_trade_price(opp_token_id)
                if opp_ltp is not None:
                    if opp_ltp >= 0.95:
                        self.logger.info(
                            "MARTINGALE OPP-LTP RESOLUTION: opposite ltp=$%.4f "
                            ">= $0.95 — treating as LOSS",
                            opp_ltp,
                        )
                        return False
                    if opp_ltp <= 0.05:
                        self.logger.info(
                            "MARTINGALE OPP-LTP RESOLUTION: opposite ltp=$%.4f "
                            "<= $0.05 — treating as WIN",
                            opp_ltp,
                        )
                        return True

                opp_ask, _ = self._get_best_ask(opp_token_id)
                if opp_ask is not None:
                    if opp_ask >= 0.95:
                        self.logger.info(
                            "MARTINGALE OPP-ASK RESOLUTION: opposite ask=$%.4f "
                            ">= $0.95 — treating as LOSS",
                            opp_ask,
                        )
                        return False
                    if opp_ask <= 0.05:
                        self.logger.info(
                            "MARTINGALE OPP-ASK RESOLUTION: opposite ask=$%.4f "
                            "<= $0.05 — treating as WIN",
                            opp_ask,
                        )
                        return True

            return None  # inconclusive
        except Exception:
            return None

    # -- pre-fetch for fast follow-up bet -----------------------------------

    def _prefetch_next_window(self):
        """Pre-fetch the next window's market so we're ready to bet instantly.

        Called in the final 30s of the current window.  Stores the slug,
        market data, and token ID so ``_try_place_bet`` can skip the
        expensive Gamma API call.  Also pre-runs balance check and USDC
        approval so they don't block the critical order path.
        """
        try:
            window = int(self._scfg("window", "martingale_window", 300))
            now = int(time.time())
            # Next window starts at the next aligned boundary
            next_window_ts = (now // window + 1) * window
            base = self._scfg("slug_base", "martingale_slug_base", "btc-updown-5m")
            slug = f"{base}-{next_window_ts}"

            market = self._fetch_market(slug)
            if not market:
                return  # market not available yet — will retry on next poll

            direction = self._scfg("direction", "martingale_direction", self.direction)
            token_id = (
                market["up_token"] if direction == "Up" else market["down_token"]
            )

            # Pre-run USDC approval so it's not on the critical path.
            # Use the next bet size (current_bet after potential doubling).
            neg_risk = market.get("neg_risk", False)
            raw_amount = int(self.current_bet * 2 * 1_000_000)  # approve 2x for headroom
            self._do_usdc_approval(CTF_EXCHANGE_ADDRESS, raw_amount, neg_risk=neg_risk)

            # Pre-check balance so we know early if we're short.
            balance_ok = True
            if self.executor:
                try:
                    usdc_bal = float(self.executor.get_usdc_balance(max_age_seconds=0))
                    if usdc_bal < self.current_bet:
                        self.logger.warning(
                            "MARTINGALE: prefetch balance warning — $%.2f < next bet $%.2f",
                            usdc_bal, self.current_bet,
                        )
                        balance_ok = False
                except Exception:
                    pass

            self._next_window_cache = {
                "window_ts": next_window_ts,
                "slug": slug,
                "market": market,
                "token_id": token_id,
                "approval_done": True,
                "balance_ok": balance_ok,
            }
            self.logger.info(
                "MARTINGALE: pre-cached next window market '%s' (token=%s..)",
                slug, token_id[:16] if token_id else "?",
            )
        except Exception as exc:
            self.logger.debug("MARTINGALE: prefetch failed: %s", exc)

    # -- check how a past window resolved (for recovery candles) ---------------

    def _check_window_resolution(self, window_ts):
        """Check how a specific past window resolved (Up or Down).

        Returns ``"Up"`` if Up won, ``"Down"`` if Down won, or ``None``
        if the window hasn't resolved yet.  Uses Gamma API market data.

        Falls back to pre-cached condition_ids when the Gamma ``/events``
        endpoint no longer returns data for resolved/past events.
        """
        base = self._scfg("slug_base", "martingale_slug_base", "btc-updown-5m")
        slug = f"{base}-{window_ts}"

        # Check resolution cache first (already resolved)
        cache = getattr(self, "_window_resolution_cache", {})
        if window_ts in cache:
            return cache[window_ts]

        if not hasattr(self, "_window_resolution_cache"):
            self._window_resolution_cache = {}

        # Try to get condition_id — first via slug lookup, then from pre-cache
        cid = None
        market = self._fetch_market(slug)
        if market:
            cid = market.get("condition_id")
            # Store in cid cache for future lookups
            if cid:
                if not hasattr(self, "_window_cid_cache"):
                    self._window_cid_cache = {}
                self._window_cid_cache[window_ts] = cid

        # Fallback: use pre-cached condition_id if slug lookup returned nothing
        # (Gamma API often stops returning event data after resolution)
        if not cid:
            cid_cache = getattr(self, "_window_cid_cache", {})
            cid = cid_cache.get(window_ts)
            if not cid:
                self.logger.debug(
                    "MARTINGALE RECOVERY: no market data for window %s (%s) "
                    "— slug lookup empty, no cached cid",
                    time.strftime("%H:%M:%S", time.localtime(window_ts)), slug,
                )
                return None

        # Check resolution using "Up" as direction — if resolved and won,
        # the window resolved Up; if resolved and lost, it resolved Down.
        resolved, up_won = self._check_resolution(cid, "Up")
        if not resolved:
            return None

        result = "Up" if up_won else "Down"
        # Cache the result (resolved windows never change)
        self._window_resolution_cache[window_ts] = result
        return result

    # -- streak recovery (market resolution based) -----------------------------

    def _check_streak_recovery(self):
        """Check windows that resolved AFTER the pause to see if market recovered.

        Called each cycle while ``_streak_paused`` is True.  Returns True
        when the recovery condition is met (N out of M windows resolved
        in our favor, counting only windows since we paused).

        Uses actual market resolution (Up/Down) — the definitive answer
        for whether BTC went up or down in each 5-minute window.
        """
        n_candles = int(self._scfg(
            "recovery_candles", "martingale_recovery_candles", 5))
        n_green = int(self._scfg(
            "recovery_green", "martingale_recovery_green", 3))
        window = int(self._scfg("window", "martingale_window", 300))

        direction = self._scfg("direction", "martingale_direction", self.direction)

        now = int(time.time())
        current_window_ts = now - (now % window)

        # Determine the first window to check: the one AFTER we paused.
        # _streak_paused_at is a datetime; convert to epoch and find next window.
        pause_epoch = getattr(self, "_recovery_pause_window_ts", 0)
        if not pause_epoch and self._streak_paused_at:
            pause_time = int(self._streak_paused_at.timestamp())
            # The window that was active when we paused (we lost this one)
            paused_window = pause_time - (pause_time % window)
            # First window to evaluate is the NEXT one after the pause
            pause_epoch = paused_window + window
            self._recovery_pause_window_ts = pause_epoch

        if not pause_epoch:
            return False

        # Pre-cache the CURRENT in-progress window's condition_id so we can
        # check its resolution in a future cycle (after it closes).  The Gamma
        # API only returns event data while the window is still active, so we
        # must grab it now before it disappears.
        base = self._scfg("slug_base", "martingale_slug_base", "btc-updown-5m")
        cid_cache = getattr(self, "_window_cid_cache", {})
        if not hasattr(self, "_window_cid_cache"):
            self._window_cid_cache = {}
            cid_cache = self._window_cid_cache
        if current_window_ts not in cid_cache:
            current_slug = f"{base}-{current_window_ts}"
            try:
                cur_market = self._fetch_market(current_slug)
                if cur_market and cur_market.get("condition_id"):
                    self._window_cid_cache[current_window_ts] = cur_market["condition_id"]
                    self.logger.debug(
                        "MARTINGALE RECOVERY: pre-cached cid for window %s (%s)",
                        time.strftime("%H:%M:%S", time.localtime(current_window_ts)),
                        current_slug,
                    )
            except Exception:
                pass
        # Also try to pre-cache the NEXT window (one ahead of current)
        next_window_ts = current_window_ts + window
        if next_window_ts not in self._window_cid_cache:
            next_slug = f"{base}-{next_window_ts}"
            try:
                nxt_market = self._fetch_market(next_slug)
                if nxt_market and nxt_market.get("condition_id"):
                    self._window_cid_cache[next_window_ts] = nxt_market["condition_id"]
            except Exception:
                pass

        # Scan forward from the first post-pause window up to (but not
        # including) the current in-progress window.
        resolved_results = []
        check_ts = pause_epoch
        while check_ts < current_window_ts:
            result = self._check_window_resolution(check_ts)
            if result is not None:
                resolved_results.append((check_ts, result))
            check_ts += window

        if not resolved_results:
            # Log periodic status so user can see we're waiting
            now_mono = time.monotonic()
            last_wait_log = getattr(self, "_recovery_wait_logged_at", 0)
            if now_mono - last_wait_log >= 60:  # log at most once per minute
                self._recovery_wait_logged_at = now_mono
                # Count how many windows we tried but couldn't resolve
                n_checked = max(0, (current_window_ts - pause_epoch) // window)
                n_cached = sum(
                    1 for ts in self._window_cid_cache
                    if pause_epoch <= ts < current_window_ts
                ) if hasattr(self, "_window_cid_cache") else 0
                next_check = pause_epoch
                wait_secs = max(0, next_check + window - now)
                self.logger.info(
                    "MARTINGALE [%s] RECOVERY: waiting for first post-pause "
                    "window to resolve — next window %s (~%ds away), "
                    "need %d/%d favorable | checked %d windows, %d have cached cid",
                    self.strategy_name,
                    time.strftime("%H:%M:%S", time.localtime(next_check)),
                    wait_secs, n_green, n_candles,
                    n_checked, n_cached,
                )
            return False

        # Take the last n_candles results (most recent)
        resolved_results = resolved_results[-n_candles:]

        favorable = sum(1 for _, r in resolved_results if r == direction)
        total = len(resolved_results)

        # Always update _recovery_candles so heartbeat has current data
        self._recovery_candles = [
            {"ts": ts, "green": r == "Up",
             "favorable": r == direction,
             "resolution": r}
            for ts, r in resolved_results
        ]

        # Only log detail when we have a new window to report (avoid spam)
        newest_ts = resolved_results[-1][0] if resolved_results else 0
        last_logged = getattr(self, "_recovery_last_logged_ts", 0)
        if newest_ts != last_logged:
            self._recovery_last_logged_ts = newest_ts

            # Build candle summary string (oldest → newest)
            candle_str = " ".join(
                "UP" if r == "Up" else "DN"
                for _, r in resolved_results
            )
            latest_result = resolved_results[-1][1]
            favor_label = "favorable" if latest_result == direction else "unfavorable"
            self.logger.info(
                "MARTINGALE [%s] RECOVERY: %s/%s (latest=%s) "
                "— %d/%d favorable (%d needed from %d) [%s]",
                self.strategy_name,
                latest_result, favor_label,
                time.strftime("%H:%M:%S", time.localtime(newest_ts)),
                favorable, total, n_green, n_candles, candle_str,
            )
            self._save_state()

        # Check recovery condition
        if total >= n_candles and favorable >= n_green:
            return True

        return False

    def _resume_from_streak_pause(self):
        """Resume trading after streak recovery is confirmed."""
        paused_dur = ""
        if self._streak_paused_at:
            delta = datetime.now() - self._streak_paused_at
            mins = int(delta.total_seconds() // 60)
            paused_dur = f" (paused for {mins}m)"

        favorable_count = sum(
            1 for c in self._recovery_candles
            if c.get("favorable", c.get("green"))
        )
        total = len(self._recovery_candles)

        self._streak_paused = False
        self._streak_paused_at = None
        self._recovery_candles = []
        self._recovery_candle_open = None
        self._recovery_candle_ts = 0
        self._recovery_pause_window_ts = 0
        self._recovery_last_logged_ts = 0

        streak_reset = self._scfg("streak_reset", "martingale_streak_reset", True)
        # Normalize string/bool — config may come as "true"/"false" string
        if isinstance(streak_reset, str):
            streak_reset = streak_reset.lower() not in ("false", "0", "no")

        if streak_reset:
            self.consecutive_losses = 0
            self.current_bet = self.start_bet
        else:
            # Keep the elevated bet and streak — just unpause
            pass

        # Enter post-recovery confirmation mode if configured
        post_confirm = self._scfg(
            "post_recovery_confirm", "martingale_post_recovery_confirm", True)
        if isinstance(post_confirm, str):
            post_confirm = post_confirm.lower() not in ("false", "0", "no")
        if post_confirm:
            self._post_recovery_mode = True
            self._post_recovery_losses = 0

        self._save_state()

        pause_minutes = 0
        if paused_dur:
            try:
                pause_minutes = int(paused_dur.strip(" ()").replace("paused for ", "").replace("m", ""))
            except (ValueError, AttributeError):
                pass

        confirm_note = ""
        if self._post_recovery_mode:
            n_total = int(self._scfg(
                "streak_confirm_total", "martingale_streak_confirm_total", 3))
            n_green_c = int(self._scfg(
                "streak_confirm_green", "martingale_streak_confirm_green", 2))
            confirm_note = (
                f" [post-recovery: requiring {n_green_c}/{n_total} "
                f"candle confirmation before next trade]"
            )
        msg = (
            f"MARTINGALE [{self.strategy_name}] RESUMED: recovery confirmed "
            f"({favorable_count}/{total} favorable candles){paused_dur} "
            f"— next bet=${self.current_bet:.2f}{confirm_note}"
        )
        self.logger.info(msg)
        self._log_event("streak_resume",
            favorable_candles=favorable_count,
            total_candles=total,
            pause_minutes=pause_minutes,
            post_recovery_mode=self._post_recovery_mode,
        )
        if self.notify_callback:
            try:
                self.notify_callback(msg)
            except Exception:
                pass

    # -- streak-level trend confirmation ------------------------------------

    def _check_streak_confirmation(self):
        """Check windows resolved AFTER confirmation started to confirm trend.

        Called each cycle while ``_streak_confirming`` is True.  Returns True
        when the confirmation condition is met (e.g. 2 out of 3 windows
        resolved in our favor, counting only windows since confirmation began).

        Uses actual market resolution data (not the Polymarket token ask)
        so that candle colors match the real chart.
        """
        n_total = int(self._scfg(
            "streak_confirm_total", "martingale_streak_confirm_total", 3))
        n_green = int(self._scfg(
            "streak_confirm_green", "martingale_streak_confirm_green", 2))
        window = int(self._scfg("window", "martingale_window", 300))

        direction = self._scfg("direction", "martingale_direction", self.direction)

        now = int(time.time())
        current_window_ts = now - (now % window)

        # Determine the first window to check: the one AFTER confirmation started.
        start_epoch = getattr(self, "_confirm_start_epoch", 0)
        if not start_epoch:
            return False
        start_window = start_epoch - (start_epoch % window)
        first_window = start_window + window

        # Scan forward from the first post-confirmation window
        resolved_results = []
        check_ts = first_window
        while check_ts < current_window_ts:
            result = self._check_window_resolution(check_ts)
            if result is not None:
                resolved_results.append((check_ts, result))
            check_ts += window

        if not resolved_results:
            return False

        # Take the last n_total results (most recent)
        resolved_results = resolved_results[-n_total:]

        favorable_count = sum(1 for _, r in resolved_results if r == direction)
        total = len(resolved_results)

        # Only log when we have a new window to report
        newest_ts = resolved_results[-1][0] if resolved_results else 0
        last_logged = getattr(self, "_confirm_last_logged_ts", 0)
        if newest_ts != last_logged:
            self._confirm_last_logged_ts = newest_ts

            candle_str = " ".join(
                "UP" if r == "Up" else "DN"
                for _, r in resolved_results
            )
            latest_result = resolved_results[-1][1]
            favor_label = "IN FAVOR" if latest_result == direction else "AGAINST"
            self.logger.info(
                "MARTINGALE [%s] streak confirmation window: %s %s "
                "(latest=%s, dir=%s) — %d/%d favorable (%d needed from %d) [%s]",
                self.strategy_name,
                latest_result, favor_label,
                time.strftime("%H:%M:%S", time.localtime(newest_ts)),
                direction, favorable_count, total, n_green, n_total,
                candle_str,
            )

            # Update state for compatibility
            self._streak_confirm_candles = [
                {"ts": ts, "green": r == "Up",
                 "favorable": r == direction,
                 "resolution": r}
                for ts, r in resolved_results
            ]
            self._save_state()

        if total >= n_total and favorable_count >= n_green:
            return True

        return False

    def _resume_from_streak_confirmation(self):
        """Clear confirmation state after trend is confirmed — proceed to bet."""
        favorable_count = sum(
            1 for c in self._streak_confirm_candles if c.get("favorable", c["green"]))
        total = len(self._streak_confirm_candles)
        direction = self._scfg("direction", "martingale_direction", self.direction)

        self._streak_confirming = False
        self._streak_confirmed = True  # gate bypass for _try_place_bet in same cycle
        self._streak_confirm_candles = []
        self._streak_confirm_candle_open = None
        self._streak_confirm_candle_ts = 0
        self._confirm_start_epoch = 0
        self._confirm_last_logged_ts = 0
        self._save_state()

        msg = (
            f"MARTINGALE [{self.strategy_name}] TREND CONFIRMED at streak "
            f"{self.consecutive_losses}: {favorable_count}/{total} candles "
            f"favorable (dir={direction}) "
            f"— proceeding with ${self.current_bet:.2f} bet"
        )
        self.logger.info(msg)
        self._log_event("streak_confirm_passed",
            streak=self.consecutive_losses,
            favorable_candles=favorable_count,
            total_candles=total,
            direction=direction,
        )
        if self.notify_callback:
            try:
                self.notify_callback(msg)
            except Exception:
                pass

    # -- bet placement & result handling ------------------------------------

    def _try_place_bet(self):
        """Attempt to place a bet on the current window.

        Returns ``True`` if a bet was placed, ``False`` otherwise.
        """
        # Safety: max streak — pause and wait for bullish recovery.
        # Skip if already in post-recovery mode (we already recovered once
        # and are now confirming each bet until hard_reset or win).
        max_streak = int(self._scfg("max_streak", "martingale_max_streak", 0))
        if (max_streak > 0
                and self.consecutive_losses >= max_streak
                and not self._streak_paused
                and not self._post_recovery_mode):
            self._streak_paused = True
            self._streak_paused_at = datetime.now()
            self._recovery_candles = []
            self._recovery_candle_open = None
            self._recovery_candle_ts = 0
            self._save_state()
            n_candles = int(self._scfg(
                "recovery_candles", "martingale_recovery_candles", 5))
            n_green = int(self._scfg(
                "recovery_green", "martingale_recovery_green", 3))
            msg = (
                f"MARTINGALE [{self.strategy_name}] PAUSED: max streak of "
                f"{max_streak} losses reached — waiting for {n_green}/{n_candles} "
                f"favorable candles before resuming"
            )
            self.logger.warning(msg)
            self._log_event("streak_pause",
                max_streak=max_streak,
                recovery_candles_needed=n_candles,
                recovery_green_needed=n_green,
            )
            if self.notify_callback:
                try:
                    self.notify_callback(msg)
                except Exception:
                    pass
            return False

        if self._streak_paused:
            return False  # recovery check happens in _cycle()

        # Trend confirmation gate — triggered either by:
        # 1. Streak reaching streak_confirm_at (e.g. streak 7), OR
        # 2. Post-recovery mode AFTER enough losses (default 2) since recovery
        confirm_at = int(self._scfg(
            "streak_confirm_at", "martingale_streak_confirm_at", 7))
        post_loss_threshold = int(self._scfg(
            "post_recovery_loss_threshold", "martingale_post_recovery_loss_threshold", 2))
        need_confirm = (
            (confirm_at > 0 and self.consecutive_losses >= confirm_at)
            or self._post_recovery_mode  # always require candle confirmation after recovery
        )
        if need_confirm and not self._streak_confirming and not getattr(self, '_streak_confirmed', False):
            self._streak_confirming = True
            self._streak_confirm_candles = []
            self._streak_confirm_candle_open = None
            self._streak_confirm_candle_ts = 0
            self._confirm_start_epoch = int(time.time())
            self._save_state()
            n_total = int(self._scfg(
                "streak_confirm_total", "martingale_streak_confirm_total", 3))
            n_green = int(self._scfg(
                "streak_confirm_green", "martingale_streak_confirm_green", 2))
            reason = "post-recovery" if self._post_recovery_mode else f"streak {self.consecutive_losses}"
            msg = (
                f"MARTINGALE [{self.strategy_name}] CONFIRMING ({reason}): "
                f"waiting for {n_green}/{n_total} "
                f"candles in our direction before placing ${self.current_bet:.2f} bet"
            )
            self.logger.warning(msg)
            self._log_event("streak_confirm_start",
                streak=self.consecutive_losses,
                confirm_green_needed=n_green,
                confirm_total=n_total,
                pending_bet=self.current_bet,
            )
            if self.notify_callback:
                try:
                    self.notify_callback(msg)
                except Exception:
                    pass
            return False

        if self._streak_confirming:
            return False  # confirmation check happens in _cycle()

        # NOTE: _streak_confirmed is cleared only after a bet is
        # successfully placed (see below).  If we clear it here and
        # the bet is skipped (e.g. low balance), the next cycle would
        # restart the full confirmation candle process from scratch.

        # Safety: max bet
        max_bet = float(self._scfg("max_bet", "martingale_max_bet", 0))
        if max_bet > 0 and self.current_bet > max_bet:
            self.logger.warning(
                "MARTINGALE [%s] STOPPED: bet $%.2f exceeds max $%.2f",
                self.strategy_name, self.current_bet, max_bet,
            )
            self.stop()
            return False

        # Avoid re-betting the same window
        window = int(self._scfg("window", "martingale_window", 300))
        current_window_ts = self._get_window_ts(window)
        if current_window_ts == self._last_window_ts:
            return False  # already bet or skipped this window

        # ── New window detected — check if previous window was missed ──
        # If _skip_reason is set, the previous window was attempted but
        # no bet was placed.  Record it as a missed window now.
        # Guard: only record once per window (retry paths don't update
        # _last_window_ts, so this block can fire on every poll).
        if (self._skip_reason
                and self._last_window_ts > 0
                and self._last_window_ts != self._miss_recorded_for_ts):
            missed_rec = {
                "ts": datetime.now().isoformat(),
                "window_ts": self._last_window_ts,
                "reason": self._skip_reason,
                "streak": self.consecutive_losses,
                "bet_would_be": self.current_bet,
                "ask_price": self._skip_price,
                "gap": self._skip_gap,
                "strategy": self.strategy_name,
            }
            self._missed_windows.append(missed_rec)
            _append_missed_window(missed_rec, logger=self.logger)
            self._miss_recorded_for_ts = self._last_window_ts
            self.logger.info(
                "MARTINGALE [%s]: recorded missed window %d — %s%s",
                self.strategy_name, self._last_window_ts, self._skip_reason,
                f" ({self._skip_gap})" if self._skip_gap else "",
            )
            self._log_event("window_missed",
                window_ts=self._last_window_ts,
                reason=self._skip_reason,
                ask_price=self._skip_price,
                gap=self._skip_gap,
            )
        # Reset for the new window
        self._skip_reason = None
        self._skip_price = None
        self._skip_gap = None

        now = int(time.time())
        seconds_into = now - current_window_ts

        # Skip if too far into the current window
        max_entry = int(self._scfg(
            "max_entry_seconds", "martingale_max_entry_seconds", 60,
        ))
        if max_entry > 0 and seconds_into > max_entry:
            if not self._skip_reason:
                self._skip_reason = f"too late ({seconds_into}s > {max_entry}s)"
            self.logger.info(
                "MARTINGALE [%s]: %ds into window > max_entry %ds — skipping",
                self.strategy_name, seconds_into, max_entry,
            )
            self._last_window_ts = current_window_ts
            self._save_state()
            return False

        # Allow runtime direction toggle via config (must happen before
        # both the cached and non-cached paths so direction is always set).
        direction = self._scfg("direction", "martingale_direction", self.direction)
        self.direction = direction

        # Use pre-fetched cache if it matches this window
        cache = self._next_window_cache
        prefetch_approval_done = False
        prefetch_balance_ok = None
        if cache and cache["window_ts"] == current_window_ts:
            slug = cache["slug"]
            market = cache["market"]
            token_id = cache["token_id"]
            prefetch_approval_done = cache.get("approval_done", False)
            prefetch_balance_ok = cache.get("balance_ok")
            self._next_window_cache = None  # consumed
            self.logger.info(
                "MARTINGALE: using pre-cached market for '%s'", slug,
            )
        else:
            # Fetch the market (no cache or stale cache)
            self._next_window_cache = None
            slug = self._generate_slug()
            market = self._fetch_market(slug)
            if not market:
                self.logger.info(
                    "MARTINGALE [%s]: market not available yet for '%s' — "
                    "will retry (%ds into window)",
                    self.strategy_name, slug, seconds_into,
                )
                self._skip_reason = f"market not found: {slug}"
                self._windows_no_market += 1
                return False  # don't mark as skipped — retry on next poll

            token_id = (
                market["up_token"] if direction == "Up"
                else market["down_token"]
            )
        ask_price, ask_size = self._get_best_ask(token_id)
        if not ask_price or ask_price <= 0:
            self.logger.info(
                "MARTINGALE [%s]: no asks for %s token yet — will retry "
                "(%ds into window)", self.strategy_name, direction, seconds_into,
            )
            self._skip_reason = f"no asks for {direction} token"
            return False

        # Price range check — only bet when price is within the target range
        price_min = float(self._scfg("price_min", "martingale_price_min", 0.40))
        price_max = float(self._scfg("price_max", "martingale_price_max", 0.55))

        # Dynamic price_max escalation based on loss streak.
        # At higher streaks, the bet is much larger so missing the window
        # entirely is worse than paying a few cents more per share.
        #
        # Config: price_max_streak = {4: 0.60, 5: 0.65, 6: 0.70}
        # — keys are streak thresholds, values are the new price_max.
        # The highest matching threshold wins.
        streak_overrides = self._scfg(
            "price_max_streak", "martingale_price_max_streak",
            {4: 0.60, 5: 0.65, 6: 0.70},
        )
        base_price_max = price_max
        if streak_overrides and self.consecutive_losses > 0:
            # Find the highest streak threshold that applies
            best_threshold = 0
            for threshold_str, cap in streak_overrides.items():
                threshold = int(threshold_str)
                if self.consecutive_losses >= threshold > best_threshold:
                    best_threshold = threshold
                    price_max = max(price_max, float(cap))
            if price_max > base_price_max:
                self.logger.info(
                    "MARTINGALE [%s]: streak %d — price cap raised $%.2f → $%.2f",
                    self.strategy_name, self.consecutive_losses,
                    base_price_max, price_max,
                )

        if ask_price < price_min or ask_price > price_max:
            if ask_price > price_max:
                gap_dir = "gap_up"
                gap_label = "GAP UP"
            else:
                gap_dir = "gap_down"
                gap_label = "GAP DOWN"
            self.logger.info(
                "MARTINGALE [%s]: %s — price $%.4f outside range "
                "[$%.2f–$%.2f] — will retry (streak=%d)",
                self.strategy_name, gap_label, ask_price, price_min,
                price_max, self.consecutive_losses,
            )
            self._skip_reason = f"price ${ask_price:.4f} outside [{price_min:.2f}–{price_max:.2f}]"
            self._skip_price = ask_price
            self._skip_gap = gap_dir
            return False  # don't mark skipped — price might come back

        # Check aggregate depth across all ask levels up to price_max.
        # FOK/market orders sweep multiple levels, so single-level
        # size is not the right measure.
        target_shares = self.current_bet / ask_price
        book_depth = self._get_book_depth(token_id, price_max)
        if book_depth < target_shares:
            self.logger.warning(
                "MARTINGALE [%s]: insufficient book depth "
                "(need %.1f shares, book has %.1f up to $%.2f)",
                self.strategy_name, target_shares, book_depth, price_max,
            )
            self._skip_reason = f"thin book ({book_depth:.0f}/{target_shares:.0f} shares)"
            self._skip_price = ask_price
            return False

        # Dry run guard
        if self.cfg.get("dry_run", False):
            self.logger.info(
                "MARTINGALE DRY RUN: would buy %s @ $%.4f, $%.2f",
                direction, ask_price, self.current_bet,
            )
            self._last_window_ts = current_window_ts
            return False

        # Pre-trade balance check — skip if prefetch already verified.
        if prefetch_balance_ok is False:
            self.logger.warning(
                "MARTINGALE [%s]: prefetch flagged low balance — skipping",
                self.strategy_name,
            )
            self._skip_reason = "low balance (prefetch)"
            return False
        if prefetch_balance_ok is None and self.executor:
            # No prefetch data — check now
            try:
                usdc_bal = float(self.executor.get_usdc_balance(max_age_seconds=0))
                if usdc_bal < self.current_bet:
                    self.logger.warning(
                        "MARTINGALE [%s]: USDC balance $%.2f < bet $%.2f — skipping",
                        self.strategy_name, usdc_bal, self.current_bet,
                    )
                    self._skip_reason = f"low balance (${usdc_bal:.2f} < ${self.current_bet:.2f})"
                    return False
            except Exception as exc:
                self.logger.debug("MARTINGALE: balance check failed (%s) — proceeding", exc)

        # Ensure USDC approval — skip if prefetch already handled it.
        neg_risk = market.get("neg_risk", False)
        if not prefetch_approval_done:
            raw_amount = int(self.current_bet * 1_000_000)
            if not self._do_usdc_approval(CTF_EXCHANGE_ADDRESS, raw_amount, neg_risk=neg_risk):
                # Approval definitively failed — diagnose the cause.
                diag = self._diagnose_allowance(CTF_EXCHANGE_ADDRESS, raw_amount)
                self.logger.warning(
                    "MARTINGALE [%s]: USDC approval NOT set — skipping order. %s "
                    "Configure rpc_url in config.json for on-chain approvals.",
                    self.strategy_name, diag,
                )
                self._skip_reason = "USDC approval failed"
                return False

        # Snapshot token balance before placing the order so that phantom-fill
        # detection can use the *delta* rather than the absolute balance
        # (which may include pre-existing shares from earlier positions).
        _pre_order_balance = self._check_phantom_fill(
            token_id, neg_risk=neg_risk,
        )

        result = self.clob_client.place_order(
            token_id=token_id,
            side="BUY",
            size_usdc=self.current_bet,
            price=ask_price,
            use_fok=True,
            max_retry_price=price_max,
            neg_risk=neg_risk,
        )

        # --- Phantom-fill protection ---
        # If the FOK got a network error, the API may have matched the
        # order but the HTTP response was lost.  Check on-chain balance
        # before retrying to avoid placing a duplicate bet.
        if isinstance(result, dict) and result.get("status") == "network_error":
            self._fok_network_errors += 1
            # Track per-window network error count
            if self._fok_net_err_window_ts != current_window_ts:
                self._fok_net_err_count = 0
                self._fok_net_err_window_ts = current_window_ts
            self._fok_net_err_count += 1
            max_net_retries = int(self._scfg(
                "max_net_error_retries", "martingale_max_net_error_retries", 1,
            ))
            self.logger.warning(
                "MARTINGALE: FOK network error for %s @ $%.4f — "
                "checking for phantom fill on-chain (pre-balance: %.1f raw) "
                "[window_net_err=%d/%d, session=%d]",
                direction, ask_price, _pre_order_balance,
                self._fok_net_err_count, max_net_retries,
                self._fok_network_errors,
            )
            self._log_event("fok_network_error",
                direction=direction,
                ask_price=ask_price,
                slug=slug,
                pre_balance_raw=_pre_order_balance,
                window_net_errors=self._fok_net_err_count,
                max_net_retries=max_net_retries,
                network_errors_session=self._fok_network_errors,
                reason=result.get("reason", ""),
            )
            # Retry on-chain balance checks with increasing delays.
            # A single 2 s wait was insufficient in practice — on-chain
            # settlement can take 5-10 s under load.
            _phantom_delays = [2, 3, 5]
            raw_delta = 0
            raw_balance = _pre_order_balance
            for _pd_i, _pd_wait in enumerate(_phantom_delays):
                time.sleep(_pd_wait)
                raw_balance = self._check_phantom_fill(
                    token_id, neg_risk=neg_risk,
                )
                raw_delta = raw_balance - _pre_order_balance
                if raw_delta > 0:
                    self.logger.info(
                        "MARTINGALE: phantom fill found on on-chain check %d/%d "
                        "(after %ds total wait)",
                        _pd_i + 1, len(_phantom_delays),
                        sum(_phantom_delays[:_pd_i + 1]),
                    )
                    break
                self.logger.debug(
                    "MARTINGALE: phantom fill check %d/%d — no delta yet "
                    "(post=%d, pre=%d)",
                    _pd_i + 1, len(_phantom_delays),
                    raw_balance, _pre_order_balance,
                )

            # Secondary signal: check CLOB trades API for a recent fill
            # on this token, in case on-chain balance is still lagging.
            if raw_delta <= 0 and self.executor:
                try:
                    _addr = self.executor.address
                    _recent = self.clob_client.get_trades_for_address(_addr, limit=5)
                    _now = time.time()
                    for _rt in (_recent or []):
                        _rt_asset = str(
                            _rt.get("asset_id") or _rt.get("token_id") or ""
                        )
                        if _rt_asset != token_id:
                            continue
                        # Check if this trade happened in the last 30 s
                        _rt_ts = _rt.get("match_time") or _rt.get("timestamp") or ""
                        try:
                            if isinstance(_rt_ts, (int, float)):
                                _rt_epoch = float(_rt_ts)
                            else:
                                from datetime import timezone
                                _rt_epoch = datetime.fromisoformat(
                                    str(_rt_ts).replace("Z", "+00:00")
                                ).timestamp()
                        except Exception:
                            _rt_epoch = 0
                        if _now - _rt_epoch < 30:
                            _rt_size = float(_rt.get("size") or 0)
                            _rt_price = float(_rt.get("price") or ask_price)
                            if _rt_size > 0:
                                raw_delta = 1  # sentinel — use trade data
                                raw_balance = _pre_order_balance + 1
                                # Override shares/cost from the actual trade
                                target_shares = _rt_size
                                ask_price = _rt_price
                                self.logger.warning(
                                    "MARTINGALE: PHANTOM FILL via CLOB trades API — "
                                    "%.1f shares @ $%.4f for token %s "
                                    "(on-chain balance lagged)",
                                    _rt_size, _rt_price, token_id[:16] + "...",
                                )
                                break
                except Exception as _trades_exc:
                    self.logger.debug(
                        "MARTINGALE: CLOB trades check failed: %s", _trades_exc,
                    )

            # Use the delta from the pre-order snapshot to exclude
            # pre-existing shares that were already in the wallet.
            _from_trades_api = (raw_delta == 1 and
                                raw_balance == _pre_order_balance + 1)
            if raw_delta > 0:
                if _from_trades_api:
                    # Shares/cost already set from the trades API data
                    actual_shares = target_shares
                    actual_cost = round(actual_shares * ask_price, 6)
                else:
                    actual_shares = float(
                        Decimal(raw_delta) / Decimal("1000000")
                    )
                    actual_cost = round(actual_shares * ask_price, 6)
                self._phantom_fills += 1
                self.logger.warning(
                    "MARTINGALE: PHANTOM FILL DETECTED — %.1f new shares "
                    "(delta) of token %s on-chain (expected ~%.1f, "
                    "pre-balance: %.1f raw). Treating as successful fill. "
                    "[phantom_fills=%d this session]",
                    actual_shares, token_id[:16] + "...", target_shares,
                    _pre_order_balance, self._phantom_fills,
                )
                self._log_event("phantom_fill_detected",
                    direction=direction,
                    ask_price=ask_price,
                    slug=slug,
                    actual_shares=round(actual_shares, 1),
                    actual_cost=round(actual_cost, 6),
                    pre_balance_raw=_pre_order_balance,
                    post_balance_raw=raw_balance,
                    phantom_fills_session=self._phantom_fills,
                )
                # Synthetic result so the success path below works
                result = {
                    "takingAmount": str(actual_shares),
                    "makingAmount": str(actual_cost),
                    "status": "matched",
                    "success": True,
                    "_phantom_fill": True,
                }
            else:
                # No phantom fill detected.  If we've already hit the
                # per-window network-error cap, give up on this window
                # to prevent duplicate bets (the orders may be settling
                # slower than the 2 s check).
                if self._fok_net_err_count >= max_net_retries:
                    self.logger.warning(
                        "MARTINGALE: no phantom fill (delta=0, post=%d, pre=%d) "
                        "AND hit network-error cap (%d/%d) — SKIPPING window %d "
                        "to avoid duplicate bets",
                        raw_balance, _pre_order_balance,
                        self._fok_net_err_count, max_net_retries,
                        current_window_ts,
                    )
                    self._log_event("fok_network_error_cap",
                        direction=direction,
                        ask_price=ask_price,
                        slug=slug,
                        window_net_errors=self._fok_net_err_count,
                        max_net_retries=max_net_retries,
                        window_ts=current_window_ts,
                    )
                    self._skip_reason = (
                        f"network error cap ({self._fok_net_err_count}x) "
                        f"@ ${ask_price:.4f}"
                    )
                    self._skip_price = ask_price
                    self._last_window_ts = current_window_ts
                    self._save_state()
                    return False
                self.logger.info(
                    "MARTINGALE: no phantom fill (delta=0, post=%d, pre=%d) — "
                    "will retry next poll (%d/%d net errors this window)",
                    raw_balance, _pre_order_balance,
                    self._fok_net_err_count, max_net_retries,
                )
                self._skip_reason = (
                    f"network error (no phantom fill) @ ${ask_price:.4f}"
                )
                self._skip_price = ask_price
                return False

        if isinstance(result, dict) and (
            result.get("status") == "fok_rejected" or result.get("error")
        ):
            self._fok_rejections += 1
            # Track per-window FOK retries
            if self._fok_retry_window_ts != current_window_ts:
                self._fok_retry_count = 0
                self._fok_retry_window_ts = current_window_ts
            self._fok_retry_count += 1
            # Keep retrying while still within the entry window
            now_retry = int(time.time())
            seconds_into_retry = now_retry - current_window_ts
            max_entry = int(self._scfg(
                "max_entry_seconds", "martingale_max_entry_seconds", 60,
            ))
            if max_entry > 0 and seconds_into_retry > max_entry:
                self.logger.warning(
                    "MARTINGALE: FOK rejected %d times for %s @ $%.4f "
                    "— out of time (%ds > %ds) on window %d "
                    "[rejections=%d this session]",
                    self._fok_retry_count,
                    direction, ask_price,
                    seconds_into_retry, max_entry, current_window_ts,
                    self._fok_rejections,
                )
                self._log_event("fok_rejected_timeout",
                    direction=direction,
                    ask_price=ask_price,
                    slug=slug,
                    retries=self._fok_retry_count,
                    seconds_into_window=seconds_into_retry,
                    max_entry_seconds=max_entry,
                    window_ts=current_window_ts,
                    rejections_session=self._fok_rejections,
                    reason=result.get("reason", ""),
                )
                self._skip_reason = (
                    f"FOK rejected {self._fok_retry_count}x @ ${ask_price:.4f}"
                )
                self._skip_price = ask_price
                self._last_window_ts = current_window_ts
                self._save_state()
                return False
            self.logger.warning(
                "MARTINGALE: FOK rejected for %s @ $%.4f — retry %d "
                "(%ds left in window) [rejections=%d this session]",
                direction, ask_price,
                self._fok_retry_count,
                max(0, max_entry - seconds_into_retry),
                self._fok_rejections,
            )
            self._log_event("fok_rejected",
                direction=direction,
                ask_price=ask_price,
                slug=slug,
                retry_num=self._fok_retry_count,
                seconds_left=max(0, max_entry - seconds_into_retry),
                rejections_session=self._fok_rejections,
                reason=result.get("reason", ""),
            )
            self._skip_reason = f"FOK rejected @ ${ask_price:.4f}"
            self._skip_price = ask_price
            return False

        if not result:
            # Reuse the same per-window retry counter as FOK rejections
            if self._fok_retry_window_ts != current_window_ts:
                self._fok_retry_count = 0
                self._fok_retry_window_ts = current_window_ts
            self._fok_retry_count += 1
            # Keep retrying while still within the entry window
            now_retry2 = int(time.time())
            seconds_into_retry2 = now_retry2 - current_window_ts
            max_entry2 = int(self._scfg(
                "max_entry_seconds", "martingale_max_entry_seconds", 60,
            ))
            if max_entry2 > 0 and seconds_into_retry2 > max_entry2:
                self.logger.warning(
                    "MARTINGALE: order failed %d times for %s "
                    "— out of time (%ds > %ds) on window %d",
                    self._fok_retry_count,
                    direction,
                    seconds_into_retry2, max_entry2, current_window_ts,
                )
                self._skip_reason = (
                    f"order failed {self._fok_retry_count}x for {direction}"
                )
                self._skip_price = ask_price
                self._last_window_ts = current_window_ts
                self._save_state()
                return False
            self.logger.warning(
                "MARTINGALE: order failed for %s — retry %d "
                "(%ds left in window)",
                direction, self._fok_retry_count,
                max(0, max_entry2 - seconds_into_retry2),
            )
            self._skip_reason = f"order failed for {direction}"
            self._skip_price = ask_price
            return False

        actual_shares = float(result.get("takingAmount", 0)) or target_shares
        actual_cost = float(result.get("makingAmount", 0)) or self.current_bet

        # -- Slippage: how far our fill deviated from $0.50 fair --
        # Positive = bought below fair (good), negative = bought above (bad)
        fill_price = round(actual_cost / actual_shares, 6) if actual_shares > 0 else ask_price
        slippage = round(0.50 - fill_price, 6)
        slippage_usdc = round(slippage * actual_shares, 6)

        # -- Post-fill sanity check --
        # If the fill price is wildly different from the ask we checked,
        # the book moved between our check and the execution.
        fill_deviation = abs(fill_price - ask_price)
        if fill_deviation > 0.10:
            self.logger.warning(
                "MARTINGALE [%s]: FILL PRICE DEVIATION — expected ~$%.4f "
                "(ask), got $%.4f (fill), deviation $%.4f. "
                "Book moved between check and execution.",
                self.strategy_name, ask_price, fill_price, fill_deviation,
            )

        opposite_token = (
            market["down_token"] if direction == "Up"
            else market["up_token"]
        )
        self._active_bet = {
            "slug": slug,
            "condition_id": market["condition_id"],
            "token_id": token_id,
            "opposite_token_id": opposite_token,
            "direction": direction,
            "bet_size": self.current_bet,
            "fill_price": fill_price,
            "shares": actual_shares,
            "cost": actual_cost,
            "slippage": slippage,
            "slippage_usdc": slippage_usdc,
            "question": market["question"],
            "window_end": self._get_window_end(),
            "ts": datetime.now().isoformat(),
        }
        # Tell the executor this token belongs to us so it won't
        # trigger auto-exit take-profit before the market resolves.
        if self.executor:
            self.executor._martingale_token_ids.add(token_id)
        self._last_window_ts = current_window_ts
        self._streak_confirmed = False  # clear one-shot confirmation bypass
        self._skip_reason = None  # bet placed successfully
        self._skip_price = None
        self._skip_gap = None
        self._windows_attempted += 1
        self._bet_placed_at = time.time()
        # Compute time from window open to bet placement
        entry_latency = round(self._bet_placed_at - current_window_ts, 1)
        self._save_state()

        slip_tag = ""
        if abs(slippage) >= 0.0001:
            slip_tag = f" | slip {slippage:+.4f} (${slippage_usdc:+.4f})"
        self.logger.info(
            "MARTINGALE BET [%s]: %s $%.2f @ $%.4f "
            "(%.1f shares) — streak: %d | entry %.1fs into window%s",
            self.strategy_name, direction, actual_cost, fill_price,
            actual_shares, self.consecutive_losses, entry_latency, slip_tag,
        )

        self._log_event("bet_placed",
            direction=direction,
            cost=round(actual_cost, 6),
            fill_price=fill_price,
            shares=round(actual_shares, 1),
            ask_price=ask_price,
            slippage=slippage,
            slippage_usdc=slippage_usdc,
            slug=slug,
            entry_latency_seconds=entry_latency,
            neg_risk=neg_risk,
        )
        if self.notify_callback:
            tg_slip = ""
            if abs(slippage) >= 0.0001:
                tg_slip = f" | slip {slippage:+.4f}"
            self.notify_callback(
                f"{self.strategy_name} BET ${actual_cost:.2f} "
                f"@ ${fill_price:.4f}{tg_slip} "
                f"| streak: {self.consecutive_losses}"
            )
        return True

    def _handle_win(self, bet):
        profit = bet["shares"] - bet["cost"]
        self.session_pnl += profit
        self._session_wins += 1

        # Timing: how long from bet placement to resolution
        resolution_seconds = 0.0
        if self._bet_placed_at:
            resolution_seconds = round(time.time() - self._bet_placed_at, 1)
            self._total_resolution_time += resolution_seconds
            self._resolution_count += 1

        slip = bet.get("slippage", 0)
        slip_info = ""
        if abs(slip) >= 0.0001:
            slip_info = f" | slip {slip:+.4f} (${bet.get('slippage_usdc', 0):+.4f})"

        self.logger.info(
            "MARTINGALE WIN: %s +$%.4f (shares=%.1f, cost=$%.2f) — "
            "resetting to $%.2f | session P&L: $%.4f | resolved in %.1fs%s",
            bet["direction"], profit, bet["shares"], bet["cost"],
            self.start_bet, self.session_pnl, resolution_seconds, slip_info,
        )

        self._log_event("win",
            direction=bet["direction"],
            profit=round(profit, 6),
            shares=round(bet["shares"], 1),
            cost=round(bet["cost"], 6),
            fill_price=bet.get("fill_price", 0),
            slug=bet.get("slug", ""),
            resolution_seconds=resolution_seconds,
            slippage=slip,
        )

        self._log_bet(bet, won=True, profit=profit)
        if self.notify_callback:
            tg_slip = ""
            if abs(slip) >= 0.0001:
                tg_slip = f" | fill ${bet.get('fill_price', 0):.4f} slip {slip:+.4f}"
            self.notify_callback(
                f"{self.strategy_name} WIN +${profit:.4f}{tg_slip} | "
                f"reset to ${self.start_bet:.2f}"
            )

        self.current_bet = self.start_bet
        self.consecutive_losses = 0
        # Clear any in-progress trend confirmation and post-recovery mode
        self._streak_confirming = False
        self._streak_confirm_candles = []
        self._streak_confirm_candle_open = None
        self._streak_confirm_candle_ts = 0
        self._post_recovery_mode = False
        self._post_recovery_losses = 0
        # Keep the token in _martingale_token_ids until the position is
        # actually redeemed/removed from _positions.  Removing it here
        # created a window where auto-exit could try to sell the resolved
        # position before the portfolio scan redeemed it on-chain.
        self._active_bet = None
        self._save_state()

    def _handle_loss(self, bet):
        loss = bet["cost"]
        self.session_pnl -= loss
        self._session_losses += 1

        # Check if the bet that just lost was in sync with the martingale
        # progression.  If the actual bet size doesn't match what the
        # progression predicted (start_bet × 2^streak), the bet was stale
        # or carried over from a different start_bet — reset to base
        # instead of advancing the streak.
        expected_bet_before_loss = round(
            self.start_bet * (2 ** self.consecutive_losses), 2
        )
        max_bet = float(self._scfg("max_bet", "martingale_max_bet", 0))
        if max_bet > 0:
            expected_bet_before_loss = min(expected_bet_before_loss, max_bet)
        actual_bet = bet.get("bet_size", bet["cost"])
        # Allow 10% tolerance for exchange rounding / minimum bumps
        if abs(actual_bet - expected_bet_before_loss) > max(0.50, expected_bet_before_loss * 0.10):
            self.logger.warning(
                "MARTINGALE [%s]: bet $%.2f was out of sync with progression "
                "(expected $%.2f at streak %d) — resetting to base $%.2f",
                self.strategy_name, actual_bet, expected_bet_before_loss,
                self.consecutive_losses, self.start_bet,
            )
            self.consecutive_losses = 0
            self.current_bet = self.start_bet
        else:
            self.consecutive_losses += 1
            # Use geometric doubling from start_bet to keep the martingale
            # progression clean and predictable: start_bet × 2^streak.
            # Previously this doubled the actual exchange cost, which caused
            # drift when place_order bumped the bet to meet Polymarket minimums
            # (e.g. $2.50 bumped to $2.60 → doubled to $5.20 instead of $5.00).
            self.current_bet = round(
                self.start_bet * (2 ** self.consecutive_losses), 2
            )

        # Log if the actual cost diverged from intended bet size
        actual_cost = bet.get("cost", 0)
        intended = bet.get("bet_size", 0)
        if actual_cost and intended and abs(actual_cost - intended) > 0.01:
            self.logger.info(
                "MARTINGALE [%s]: exchange cost $%.2f differed from "
                "intended $%.2f (bumped by %.0f%% to meet minimums)",
                self.strategy_name, actual_cost, intended,
                ((actual_cost - intended) / intended) * 100,
            )

        # Track losses since recovery for post-recovery confirmation gating
        if self._post_recovery_mode:
            self._post_recovery_losses += 1
            post_loss_threshold = int(self._scfg(
                "post_recovery_loss_threshold", "martingale_post_recovery_loss_threshold", 2))
            if self._post_recovery_losses >= post_loss_threshold:
                n_total = int(self._scfg(
                    "streak_confirm_total", "martingale_streak_confirm_total", 3))
                n_green = int(self._scfg(
                    "streak_confirm_green", "martingale_streak_confirm_green", 2))
                self.logger.info(
                    "MARTINGALE [%s] post-recovery: %d losses since recovery "
                    "— %d/%d confirmation now required before next bet",
                    self.strategy_name, self._post_recovery_losses,
                    n_green, n_total,
                )

        max_bet = float(self._scfg("max_bet", "martingale_max_bet", 0))
        if max_bet > 0 and self.current_bet > max_bet:
            self.current_bet = max_bet
            self.logger.warning(
                "MARTINGALE [%s]: bet capped at max $%.2f", self.strategy_name, max_bet,
            )

        # Hard reset streak — at streak Y, just reset to start_bet and keep
        # going without pausing.  This takes priority over max_streak pause.
        hard_reset = int(self._scfg(
            "hard_reset_streak", "martingale_hard_reset_streak", 0))
        if hard_reset > 0 and self.consecutive_losses >= hard_reset:
            old_streak = self.consecutive_losses
            self.consecutive_losses = 0
            self.current_bet = self.start_bet
            self._post_recovery_mode = False
            self._post_recovery_losses = 0
            msg = (
                f"MARTINGALE [{self.strategy_name}] HARD RESET: streak {old_streak} "
                f"hit limit {hard_reset} — resetting to ${self.start_bet:.2f} "
                f"and continuing (session P&L: ${self.session_pnl:.4f})"
            )
            self.logger.warning(msg)
            self._log_event("hard_reset",
                old_streak=old_streak,
                hard_reset_streak=hard_reset,
                session_pnl=self.session_pnl,
            )
            if self.notify_callback:
                try:
                    self.notify_callback(msg)
                except Exception:
                    pass

        slip = bet.get("slippage", 0)
        slip_info = ""
        if abs(slip) >= 0.0001:
            slip_info = f" | slip {slip:+.4f} (${bet.get('slippage_usdc', 0):+.4f})"

        # Timing: how long from bet placement to resolution
        resolution_seconds = 0.0
        if self._bet_placed_at:
            resolution_seconds = round(time.time() - self._bet_placed_at, 1)
            self._total_resolution_time += resolution_seconds
            self._resolution_count += 1

        self.logger.info(
            "MARTINGALE LOSS: %s -$%.2f (fill $%.4f) — next bet $%.2f "
            "(start $%.2f × 2^%d), streak: %d | session P&L: $%.4f "
            "| resolved in %.1fs%s",
            bet["direction"], loss, bet.get("fill_price", 0),
            self.current_bet, self.start_bet, self.consecutive_losses,
            self.consecutive_losses, self.session_pnl,
            resolution_seconds, slip_info,
        )

        self._log_event("loss",
            direction=bet["direction"],
            loss=round(loss, 6),
            fill_price=bet.get("fill_price", 0),
            next_bet=self.current_bet,
            slug=bet.get("slug", ""),
            resolution_seconds=resolution_seconds,
            slippage=slip,
        )

        self._log_bet(bet, won=False, profit=-loss)
        if self.notify_callback:
            tg_slip = ""
            if abs(slip) >= 0.0001:
                tg_slip = f" | fill ${bet.get('fill_price', 0):.4f} slip {slip:+.4f}"
            self.notify_callback(
                f"{self.strategy_name} LOSS -${loss:.2f}{tg_slip} | "
                f"next ${self.current_bet:.2f} (streak: {self.consecutive_losses})"
            )

        # Keep the token in _martingale_token_ids until the position is
        # actually redeemed/removed from _positions (same as _handle_win).
        self._active_bet = None
        self._save_state()

    def _check_phantom_fill(self, token_id, neg_risk=False):
        """Check on-chain token balance after a FOK network error.

        If the balance is non-zero for the given token, the FOK was
        matched on the exchange despite the network error — a phantom
        fill.  Returns the token balance in raw units (int), or 0.
        """
        if not self.executor:
            return 0
        try:
            balance = self.executor._get_token_balance(
                self.executor.address, token_id, neg_risk=neg_risk,
            )
            return balance
        except Exception as exc:
            self.logger.warning("Phantom-fill balance check failed: %s", exc)
            return 0

    def _log_bet(self, bet, won, profit):
        fill_price = bet.get("fill_price", 0)
        slippage = bet.get("slippage", 0)
        slippage_usdc = bet.get("slippage_usdc", 0)

        record = {
            "closed_at": datetime.now().isoformat(),
            "token_id": bet["token_id"],
            "market": f"{self.strategy_name} {bet['direction']}",
            "shares": bet["shares"],
            "entry_price": round(bet["cost"] / bet["shares"], 6) if bet["shares"] > 0 else 0,
            "exit_price": 1.0 if won else 0.0,
            "cost_basis_usdc": round(bet["cost"], 6),
            "proceeds_usdc": round(bet["shares"], 6) if won else 0.0,
            "pnl_usdc": round(profit, 6),
            "outcome": "won" if won else "lost",
            "reason": "martingale",
            "slippage": {
                "fill_price": fill_price,
                "vs_fair": slippage,
                "total_usdc": slippage_usdc,
            },
            "martingale_details": {
                "strategy": self.strategy_name,
                "direction": bet["direction"],
                "bet_size": bet["bet_size"],
                "streak": self.consecutive_losses,
                "session_pnl": round(self.session_pnl, 6),
            },
        }

        if self.log_trade_callback:
            try:
                self.log_trade_callback(record)
            except Exception as exc:
                self.logger.warning("Failed to log martingale trade to history: %s", exc)
        else:
            self.logger.warning(
                "MARTINGALE [%s]: log_trade_callback not set — trade record "
                "will not appear in trade_history.json (persisting to "
                "martingale_history.json only)",
                self.strategy_name,
            )

        # --- Persist to dedicated martingale history & summary ---
        self._persist_martingale_record(record)

    @staticmethod
    def _aggregate_slippage(records):
        """Compute slippage stats from a list of trade records.

        Slippage = $0.50 - fill_price.
        Positive = bought below fair (good), negative = bought above (bad).
        """
        slippages = []
        total_slip_usdc = 0.0
        fill_prices = []
        for r in records:
            s = r.get("slippage") or {}
            if s:
                vs_fair = s.get("vs_fair", 0)
                slippages.append(vs_fair)
                total_slip_usdc += s.get("total_usdc", 0)
                if s.get("fill_price"):
                    fill_prices.append(s["fill_price"])
        n = len(slippages) or 1
        return {
            "total_slippage_usdc": round(total_slip_usdc, 6),
            "avg_slippage": round(sum(slippages) / n, 6) if slippages else 0,
            "avg_fill_price": round(sum(fill_prices) / len(fill_prices), 6) if fill_prices else 0,
            "best_fill": round(max(slippages), 6) if slippages else 0,
            "worst_fill": round(min(slippages), 6) if slippages else 0,
            "bets_with_slippage_data": len(slippages),
        }

    def _persist_martingale_record(self, record):
        """Append to martingale_history.json and update martingale_summary.json."""
        try:
            with _TRADE_HISTORY_LOCK:
                # -- Append to martingale history --
                try:
                    with open(MARTINGALE_HISTORY_FILE, "r") as fh:
                        m_history = json.load(fh)
                except (FileNotFoundError, json.JSONDecodeError):
                    m_history = []
                m_history.append(record)
                tmp = MARTINGALE_HISTORY_FILE + ".tmp"
                with open(tmp, "w") as fh:
                    json.dump(m_history, fh, indent=2, default=str)
                os.replace(tmp, MARTINGALE_HISTORY_FILE)

                # -- Compute and persist martingale summary --
                total_cost = sum(r.get("cost_basis_usdc", 0) for r in m_history)
                total_proceeds = sum(r.get("proceeds_usdc", 0) for r in m_history)
                total_pnl = round(total_proceeds - total_cost, 6)
                wins = sum(1 for r in m_history if r.get("outcome") in ("won", "win"))
                losses = sum(1 for r in m_history if r.get("outcome") in ("lost", "loss"))
                decided = wins + losses
                win_rate = (wins / decided * 100) if decided > 0 else 0

                # Overall slippage aggregation
                overall_slippage = self._aggregate_slippage(m_history)

                # Per-strategy breakdown
                strat_buckets = {}
                for r in m_history:
                    details = r.get("martingale_details") or {}
                    name = details.get("strategy", "default")
                    strat_buckets.setdefault(name, []).append(r)

                strategies = {}
                for name, records in strat_buckets.items():
                    s_cost = sum(r.get("cost_basis_usdc", 0) for r in records)
                    s_proceeds = sum(r.get("proceeds_usdc", 0) for r in records)
                    s_wins = sum(1 for r in records if r.get("outcome") in ("won", "win"))
                    s_losses = sum(1 for r in records if r.get("outcome") in ("lost", "loss"))
                    s_decided = s_wins + s_losses
                    strategies[name] = {
                        "total_bets": len(records),
                        "wins": s_wins,
                        "losses": s_losses,
                        "win_rate_pct": round((s_wins / s_decided * 100) if s_decided > 0 else 0, 1),
                        "pnl_usdc": round(s_proceeds - s_cost, 6),
                        "capital_deployed_usdc": round(s_cost, 6),
                        "capital_returned_usdc": round(s_proceeds, 6),
                        "max_streak": max(
                            (r.get("martingale_details", {}).get("streak", 0) for r in records),
                            default=0,
                        ),
                        "slippage": self._aggregate_slippage(records),
                    }

                summary = {
                    "updated_at": datetime.now().isoformat(),
                    "total_bets": len(m_history),
                    "wins": wins,
                    "losses": losses,
                    "win_rate_pct": round(win_rate, 1),
                    "lifetime_pnl_usdc": total_pnl,
                    "capital_deployed_usdc": round(total_cost, 6),
                    "capital_returned_usdc": round(total_proceeds, 6),
                    "roi_pct": round((total_pnl / total_cost * 100) if total_cost > 0 else 0, 2),
                    "slippage": overall_slippage,
                    "strategies": strategies,
                }
                tmp_sf = MARTINGALE_SUMMARY_FILE + ".tmp"
                with open(tmp_sf, "w") as fh:
                    json.dump(summary, fh, indent=2)
                os.replace(tmp_sf, MARTINGALE_SUMMARY_FILE)
        except Exception as exc:
            self.logger.warning("Could not persist martingale record: %s", exc)

    # -- main loop ----------------------------------------------------------

    def _cycle(self):
        """One tick of the martingale state machine.

        Returns ``True`` when the bot is in a position and the window
        is still open (slow-poll mode), ``False`` for everything else
        (fast-poll mode: checking resolution OR hunting for next bet).
        """
        if self._active_bet:
            bet = self._active_bet
            now = int(time.time())

            # Pre-cache next window's market in the final 30s of the
            # current window so we're ready to bet immediately after
            # resolution.
            time_left = bet["window_end"] - now
            if 0 < time_left <= 30 and not self._next_window_cache:
                self._prefetch_next_window()

            # While the window is still open, slow-poll is fine
            if now < bet["window_end"]:
                return True  # window still open — slow poll

            # === WINDOW HAS ENDED — fast-poll from here on ===
            elapsed = now - bet["window_end"]

            # Small buffer (1s) before checking to let the orderbook
            # settle, but use fast poll so we retry quickly.
            if elapsed < 1:
                return False  # fast poll — check again soon

            # --- Orderbook is fastest — check it FIRST ---
            resolved = False
            won = None
            ob_result = self._check_resolution_orderbook(bet)
            if ob_result is not None:
                resolved = True
                won = ob_result

            # --- Gamma API as secondary confirmation ---
            if not resolved:
                resolved, won = self._check_resolution(
                    bet["condition_id"], bet["direction"],
                )

            if not resolved:
                # Timeout: if 10 min past window end, give up
                if now > bet["window_end"] + 600:
                    self.logger.warning(
                        "MARTINGALE: resolution timeout for %s, treating as loss",
                        bet["slug"],
                    )
                    self._log_event("resolution_timeout",
                        slug=bet["slug"],
                        direction=bet["direction"],
                        elapsed_seconds=now - bet["window_end"],
                    )
                    self._handle_loss(bet)
                return False  # FAST poll — keep checking every 2s

            if won:
                self._handle_win(bet)
            else:
                self._handle_loss(bet)
            return False  # resolved — fast poll for next bet
        else:
            # While paused for streak recovery, sample candles instead of betting
            if self._streak_paused:
                if self._check_streak_recovery():
                    self._resume_from_streak_pause()
                return False  # keep fast-polling to sample prices
            # While confirming trend at high streak, sample candles
            if self._streak_confirming:
                if self._check_streak_confirmation():
                    self._resume_from_streak_confirmation()
                    # Confirmation passed — fall through to _try_place_bet
                else:
                    return False  # keep fast-polling to sample prices
            placed = self._try_place_bet()
            return placed  # fast poll until placed, then slow poll

    def run(self):
        self.logger.info(
            "Martingale bot [%s] started — direction=%s, bet=$%.2f, streak=%d",
            self.strategy_name, self.direction, self.current_bet,
            self.consecutive_losses,
        )

        # One-time max USDC approval at startup so per-bet approvals are no-ops.
        max_bet = float(self._scfg("max_bet", "martingale_max_bet", 0)) or 10_000
        startup_raw = int(max_bet * 1_000_000)
        if self._do_usdc_approval(CTF_EXCHANGE_ADDRESS, startup_raw, neg_risk=True):
            self.logger.info(
                "MARTINGALE [%s]: USDC approval set at startup (up to $%.0f)",
                self.strategy_name, max_bet,
            )
        else:
            diag = self._diagnose_allowance(CTF_EXCHANGE_ADDRESS, startup_raw)
            self.logger.warning(
                "MARTINGALE [%s]: Could not set USDC approval at startup — "
                "orders WILL fail. %s  "
                "Configure rpc_url in config.json to enable on-chain approvals.",
                self.strategy_name, diag,
            )

        poll_slow = max(float(self._scfg("poll_seconds", "martingale_poll_seconds", 10)), 1)
        poll_fast = 1.0  # aggressive polling when looking for next bet

        self._log_event("bot_started",
            direction=self.direction,
            start_bet=self.start_bet,
            slug_base=self._scfg("slug_base", "martingale_slug_base", ""),
            window=int(self._scfg("window", "martingale_window", 300)),
        )

        while not self._stop_event.is_set():
            try:
                in_position = self._cycle()
                self._log_heartbeat()
            except Exception as exc:
                self.logger.error("Martingale error: %s", exc, exc_info=True)
                self._log_event("error", error=str(exc))
                in_position = False
            self._stop_event.wait(
                timeout=poll_slow if in_position else poll_fast,
            )

        self._log_event("bot_stopped")
        self.logger.info("Martingale bot stopped")

    def stop(self):
        self._stop_event.set()

    def get_status_summary(self):
        """One-line status string for the dashboard."""
        status = (
            f"[{self.strategy_name}] "
            f"Direction: {self.direction} | Bet: ${self.current_bet:.2f} | "
            f"Streak: {self.consecutive_losses} | P&L: ${self.session_pnl:+.4f}"
        )
        if self._active_bet:
            status += f" | Active: {self._active_bet['slug']}"
        elif self._skip_reason:
            status += f" | Skip: {self._skip_reason}"
        return status


# ---------------------------------------------------------------------------
# Martingale Manager – orchestrates multiple MartingaleBot instances
# ---------------------------------------------------------------------------

class MartingaleManager:
    """Spawn and manage one :class:`MartingaleBot` per configured strategy.

    If the user has the new ``martingale_strategies`` list configured, one
    bot is started for each entry.  Otherwise, falls back to the legacy
    flat ``martingale_*`` keys and starts a single bot.

    Example ``martingale_strategies`` config::

        [
            {"name": "BTC 5m",  "slug_base": "btc-updown-5m",  "window": 300,
             "start_bet": 1.0,  "max_bet": 64, "direction": "Up"},
            {"name": "BTC 15m", "slug_base": "btc-updown-15m", "window": 900,
             "start_bet": 5.0,  "max_bet": 320, "direction": "Up"},
        ]
    """

    def __init__(self, clob_client, cfg, logger=None, executor=None):
        self.clob_client = clob_client
        # Validate executor — same guard as MartingaleBot.
        if executor is not None and not hasattr(executor, "ensure_usdc_approval"):
            _log = logger or logging.getLogger("martingale")
            _log.error(
                "MARTINGALE MANAGER: executor is %s (expected TradeExecutor) "
                "— disabling on-chain ops.  Check that an RPC URL is "
                "configured.",
                type(executor).__name__,
            )
            executor = None
        self.executor = executor  # TradeExecutor for on-chain ops
        self.cfg = cfg
        self.logger = logger or logging.getLogger("martingale")
        self._bots = []  # list[MartingaleBot]

        strategies = cfg.get("martingale_strategies") or []
        if not isinstance(strategies, list) or not strategies:
            # Backward compat: build a single strategy from flat keys
            strategies = [
                {
                    "name": cfg.get("martingale_slug_base", "btc-updown-5m"),
                    "slug_base": cfg.get("martingale_slug_base", "btc-updown-5m"),
                    "window": cfg.get("martingale_window", 300),
                    "direction": cfg.get("martingale_direction", "Up"),
                    "start_bet": cfg.get("martingale_start_bet", 5.0),
                    "max_bet": cfg.get("martingale_max_bet", 0),
                    "max_streak": cfg.get("martingale_max_streak", 0),
                    "poll_seconds": cfg.get("martingale_poll_seconds", 10),
                    "price_min": cfg.get("martingale_price_min", 0.40),
                    "price_max": cfg.get("martingale_price_max", 0.55),
                    "max_entry_seconds": cfg.get("martingale_max_entry_seconds", 60),
                },
            ]

        for strat in strategies:
            bot = MartingaleBot(
                clob_client, cfg, logger=self.logger, strategy=strat,
                executor=executor,
            )
            self._bots.append(bot)

    # -- Callbacks (wired once by CopyTraderBot) ----------------------------

    def set_notify_callback(self, cb):
        for bot in self._bots:
            bot.notify_callback = cb

    def set_log_trade_callback(self, cb):
        for bot in self._bots:
            bot.log_trade_callback = cb

    # -- Lifecycle ----------------------------------------------------------

    def start(self):
        for bot in self._bots:
            bot.start()
            self.logger.info(
                "Martingale strategy [%s] started — direction=%s, "
                "start=$%.2f, max=$%.0f, slug=%s, window=%ds",
                bot.strategy_name,
                bot.direction,
                bot.start_bet,
                float(bot._scfg("max_bet", "martingale_max_bet", 0)),
                bot._scfg("slug_base", "martingale_slug_base", "?"),
                int(bot._scfg("window", "martingale_window", 300)),
            )

    def stop(self):
        for bot in self._bots:
            bot.stop()

    # -- Status / introspection ---------------------------------------------

    @property
    def bots(self):
        return list(self._bots)

    def get_status_summary(self):
        """Multi-line summary, one line per strategy."""
        return "\n".join(bot.get_status_summary() for bot in self._bots)

    def get_combined_pnl(self):
        """Total session P&L across all strategies."""
        return sum(bot.session_pnl for bot in self._bots)


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
        self.slippage_bps = cfg.get("slippage_tolerance_bps", 50)
        self._nonce_lock = threading.Lock()
        self._nonce = None

        # Balance cache – avoid hitting the RPC on every trade
        self._cached_balance = None
        self._balance_timestamp = 0

        # Open order tracking: order_id -> {placed_at, side, token_id, price, usdc}
        self._open_orders = {}
        self._order_ttl = cfg.get("order_ttl_seconds", 300)

        # Optional webhook callback (set by CopyTraderBot after init)
        self.notify_callback = None

        # Kill switch: stop the bot when session losses exceed threshold
        self.kill_switch_triggered = False
        self._session_pnl = 0.0

        # Cached proxy wallet address (discovered once, reused)
        self._proxy_address = None
        self._proxy_discovery_done = False
        self._proxy_is_safe = False  # True when proxy is a Gnosis Safe

        # Token IDs currently owned by the martingale bot — skip in
        # check_exit_conditions() and portfolio-scan seeding so the
        # martingale bot handles its own resolution logic.
        self._martingale_token_ids = set()

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
                ep = Decimal(str(entry.get("entry_price", 0)))
                positions[tid] = {
                    "tokens": Decimal(str(entry.get("tokens", 0))),
                    "entry_price": ep,
                    "neg_risk": entry.get("neg_risk", False),
                    "condition_id": entry.get("condition_id"),
                    "collateral_token": entry.get("collateral_token", USDC_ADDRESS),
                    "parent_collection_id": entry.get(
                        "parent_collection_id", "0x" + "00" * 32,
                    ),
                    "index_sets": entry.get("index_sets", [1, 2]),
                    "market_name": entry.get("market_name"),
                    "outcome_side": entry.get("outcome_side"),
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
                    "market_name": pos.get("market_name"),
                    "outcome_side": pos.get("outcome_side"),
                }
            pf = self._positions_file
            tmp = pf + ".tmp"
            with open(tmp, "w") as fh:
                json.dump(serialisable, fh, indent=2)
            os.replace(tmp, pf)
        except Exception as exc:
            self.logger.warning("Could not save positions: %s", exc)

    def backfill_market_names(self):
        """Enrich existing positions that lack a market_name or outcome_side.

        Called once at startup after the CLOB client is available.
        Looks up the market question via the API for any position where
        market_name or outcome_side is missing and persists the result.
        """
        if not self.clob_client:
            return
        updated = 0
        for tid, pos in self._positions.items():
            needs_name = not pos.get("market_name")
            needs_side = not pos.get("outcome_side")
            if not needs_name and not needs_side:
                continue
            try:
                market = self.clob_client.get_market_by_token(tid)
                if market:
                    if needs_name:
                        name = market.get("question") or market.get("slug")
                        if name:
                            pos["market_name"] = name
                            updated += 1
                    if needs_side:
                        for tok in (market.get("tokens") or []):
                            tok_id = tok.get("token_id") or tok.get("tokenId") or ""
                            if tok_id == tid:
                                pos["outcome_side"] = tok.get("outcome", "").capitalize()
                                updated += 1
                                break
            except Exception:
                pass  # non-critical; will retry next restart
        if updated:
            self._save_positions()
            self.logger.info(
                "Backfilled market data for %d position field(s)", updated,
            )

    # ------------------------------------------------------------------
    # Closed-trade history (persistent across sessions)
    # ------------------------------------------------------------------

    @property
    def _trade_history_file(self):
        return self.cfg.get("trade_history_file", TRADE_HISTORY_FILE)

    def _load_trade_history(self):
        """Load the persistent trade history from disk."""
        try:
            with open(self._trade_history_file, "r") as fh:
                return json.load(fh)
        except (FileNotFoundError, json.JSONDecodeError):
            return []

    def _log_closed_trade(self, token_id, entry_price, exit_price,
                          shares, reason, market=None):
        """Append a closed trade record to the persistent history file.

        Tracks *cost_basis* (capital deployed) and *proceeds* (capital
        returned) separately so lifetime P/L can be computed accurately
        as ``sum(proceeds) - sum(cost_basis)`` across all records.

        The *outcome* field categorises the result:
        - ``"won"``  — redeemed at $1.00 (full payout)
        - ``"lost"`` — redeemed at $0.00 (total loss)
        - ``"sold"`` — sold on market before resolution
        - ``"dust"`` — position too small to sell, written off
        """
        entry_p = float(entry_price) if entry_price else 0.0
        exit_p = float(exit_price) if exit_price else 0.0
        num_shares = float(shares) if shares else 0.0

        cost_basis = entry_p * num_shares
        proceeds = exit_p * num_shares
        pnl = round(proceeds - cost_basis, 6)

        # Classify the outcome for reporting
        if reason == "redeemed":
            outcome = "won" if exit_p >= 0.5 else "lost"
        elif reason in ("dust", "stale", "failed_redeem", "resolution_error",
                         "redeemed_external"):
            outcome = "lost"
        else:
            outcome = "sold"

        record = {
            "closed_at": datetime.now().isoformat(),
            "token_id": token_id,
            "market": market or token_id[:16] + "...",
            "shares": num_shares,
            "entry_price": entry_p,
            "exit_price": exit_p,
            "position_size_usdc": round(cost_basis, 6),
            "cost_basis_usdc": round(cost_basis, 6),
            "proceeds_usdc": round(proceeds, 6),
            "pnl_usdc": pnl,
            "outcome": outcome,
            "reason": reason,
        }

        with _TRADE_HISTORY_LOCK:
            history = self._load_trade_history()

            # Deduplicate: if a martingale bet already logged a record for
            # this token_id, skip the executor's "redeemed" duplicate.
            if reason == "redeemed":
                for existing in reversed(history):
                    if (existing.get("token_id") == token_id
                            and existing.get("reason") == "martingale"):
                        self.logger.debug(
                            "Skipping duplicate _log_closed_trade for %s "
                            "(already logged by martingale)",
                            token_id[:16] + "...",
                        )
                        return

            history.append(record)

            # Compute running totals
            total_cost = sum(r.get("cost_basis_usdc", 0) for r in history)
            total_proceeds = sum(r.get("proceeds_usdc", 0) for r in history)
            total_pnl = round(total_proceeds - total_cost, 6)
            total_position_size = sum(
                r.get("position_size_usdc", r.get("cost_basis_usdc", 0))
                for r in history
            )

            # Win/loss counts — accept both "won"/"lost" and "win"/"loss"
            wins = sum(1 for r in history if r.get("outcome") in ("won", "win"))
            losses = sum(1 for r in history if r.get("outcome") in ("lost", "loss"))
            total_trades = wins + losses
            win_rate = (wins / total_trades * 100) if total_trades > 0 else 0

            try:
                tmp = self._trade_history_file + ".tmp"
                with open(tmp, "w") as fh:
                    json.dump(history, fh, indent=2)
                os.replace(tmp, self._trade_history_file)
            except Exception as exc:
                self.logger.warning("Could not save trade history: %s", exc)

            # --- Persist strategy summary ---
            try:
                summary = {
                    "updated_at": datetime.now().isoformat(),
                    "total_trades": len(history),
                    "wins": wins,
                    "losses": losses,
                    "win_rate_pct": round(win_rate, 1),
                    "lifetime_pnl_usdc": total_pnl,
                    "total_position_size_usdc": round(total_position_size, 6),
                    "lifetime_cost_basis_usdc": round(total_cost, 6),
                    "capital_returned_usdc": round(total_proceeds, 6),
                }
                tmp_sf = STRATEGY_SUMMARY_FILE + ".tmp"
                with open(tmp_sf, "w") as fh:
                    json.dump(summary, fh, indent=2)
                os.replace(tmp_sf, STRATEGY_SUMMARY_FILE)
            except Exception as exc:
                self.logger.debug("Could not save strategy summary: %s", exc)

        # Kill switch: stop the bot if session losses exceed threshold
        self._session_pnl += pnl
        max_loss = float(self.cfg.get("max_loss_usdc", 0))

        self.logger.info(
            "CLOSED TRADE [%s]: %s | %.4f shares @ entry $%.4f -> exit $%.4f | "
            "size $%.2f | P&L $%+.4f | session $%+.4f | "
            "W/L %d/%d (%.0f%%) | lifetime $%+.4f",
            outcome.upper(), record["market"], num_shares, entry_p, exit_p,
            cost_basis, pnl, self._session_pnl,
            wins, losses, win_rate,
            total_pnl,
        )

        if max_loss > 0 and self._session_pnl <= -max_loss:
            self.kill_switch_triggered = True
            self.logger.critical(
                "KILL SWITCH: session P&L $%+.2f hit max loss limit "
                "of -$%.2f — stopping bot",
                self._session_pnl, max_loss,
            )
            if self.notify_callback:
                self.notify_callback(
                    "KILL SWITCH: session P&L $%+.2f hit -$%.2f limit — bot stopped"
                    % (self._session_pnl, max_loss)
                )

        return record

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
            for _attempt in range(4):
                try:
                    raw = self.usdc.functions.balanceOf(self.address).call()
                    break
                except (ConnectionError, OSError):
                    if _attempt < 3:
                        time.sleep(2 ** _attempt)
                    else:
                        raise
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

    def _rollback_position(self, order_id, info, reason):
        """Reverse the optimistic position update for an unfilled order.

        When an order is submitted, the bot immediately updates _positions
        with the expected tokens.  If the order is later cancelled, expired,
        or stale-cancelled, we must undo that to avoid phantom positions
        that have no on-chain backing.
        """
        token_id = info["token_id"]
        price = info["price"]
        side = info["side"]

        if not price or price <= 0:
            return

        tokens = Decimal(str(info["usdc"])) / Decimal(str(price))
        pos = self._positions.get(token_id)
        if pos is None:
            return

        if side == "BUY":
            old_tokens = pos["tokens"]
            pos["tokens"] = max(pos["tokens"] - tokens, Decimal("0"))
            if pos["tokens"] <= 0:
                self.logger.info(
                    "Rolled back phantom position %s (unfilled BUY %s, "
                    "%.2f tokens removed entirely)",
                    token_id[:16] + "...", reason, old_tokens,
                )
                del self._positions[token_id]
            else:
                self.logger.info(
                    "Rolled back %.2f tokens from %s (unfilled BUY %s, "
                    "%.2f tokens remain)",
                    tokens, token_id[:16] + "...", reason,
                    pos["tokens"],
                )
        elif side == "SELL":
            # Sell was cancelled — restore the tokens we pre-subtracted
            pos["tokens"] += tokens
            self.logger.info(
                "Restored %.2f tokens to %s (unfilled SELL %s, "
                "%.2f tokens now)",
                tokens, token_id[:16] + "...", reason,
                pos["tokens"],
            )

        self._save_positions()

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
                self._rollback_position(order_id, info, status.lower())
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
                self._rollback_position(order_id, info, "stale-cancelled")
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

        In **whale** exit_mode (default), the bot only exits when:
        - The whale sells (copied_sell, handled elsewhere)
        - The market resolves (redeemed)
        - Take-profit catches an extraordinary price run (safety catch)

        Stop-loss is DISABLED in whale mode to avoid selling into temporary
        dips that the whale holds through.  Polymarket is binary — a dip
        to $0.10 can still resolve at $1.00.

        In **auto** exit_mode, the legacy behaviour is preserved: both
        take-profit and stop-loss are active.

        Requires a CLOB client to fetch live prices and place sell orders.
        Returns a list of sell results (one per exited position), or an
        empty list when there is nothing to do.
        """
        if not self.clob_client:
            return []
        if not self._positions:
            return []

        # Martingale mode: never sell — all positions resolve on-chain.
        # The martingale bot handles its own win/loss logic via resolution
        # checks, and non-martingale positions should also just redeem.
        if self.cfg.get("martingale_enabled", False):
            return []

        exit_mode = self.cfg.get("exit_mode", "whale")

        # Read thresholds from config (fall back to module-level defaults)
        tp_price = Decimal(str(self.cfg.get("take_profit_price", TAKE_PROFIT_PRICE)))
        tp_pct = Decimal(str(self.cfg.get("take_profit_pct", 0)))
        sl_raw = Decimal(str(self.cfg.get("stop_loss_pct", STOP_LOSS_PCT * 100)))
        sl_pct = sl_raw / Decimal("100") if sl_raw > 0 else Decimal("0")

        results = []
        # Iterate over a snapshot so we can mutate _positions safely
        for token_id, pos in list(self._positions.items()):
            tokens = pos["tokens"]
            entry_price = pos["entry_price"]
            if tokens <= 0:
                continue

            # Martingale bot owns this token — let it handle resolution
            if token_id in self._martingale_token_ids:
                continue

            current_price = self.clob_client.get_last_trade_price(token_id)
            if current_price is None:
                self.logger.warning(
                    "Could not fetch price for %s — skipping exit check",
                    token_id[:16] + "...",
                )
                continue
            current_price_d = Decimal(str(current_price))

            reason = None
            # Percentage-based take-profit (e.g. 500 = sell at +500% gain)
            if tp_pct > 0 and entry_price > 0:
                gain_pct = ((current_price_d - entry_price) / entry_price) * Decimal("100")
                self.logger.debug(
                    "Exit check %s: entry=%.4f current=%.4f gain=%.1f%% (tp_pct=%.0f%%)",
                    token_id[:16] + "...", entry_price, current_price_d,
                    gain_pct, tp_pct,
                )
                if gain_pct >= tp_pct:
                    reason = "take-profit"
            # Absolute price take-profit (safety catch for extraordinary runs)
            if reason is None and current_price_d >= tp_price:
                reason = "take-profit"
            # Stop-loss: only active in "auto" mode — whale mode holds through dips
            elif (reason is None and exit_mode != "whale"
                  and sl_pct > 0 and entry_price > 0
                  and current_price_d <= entry_price * sl_pct):
                reason = "stop-loss"

            if reason is None:
                continue

            # --- Skip resolved markets — redemption at $1.00 beats selling ---
            # When take-profit fires (price near $1.00), the market may have
            # already resolved.  Selling at $0.99 minus slippage loses money
            # vs. redeeming at $1.00.  Do a quick on-chain check if we have
            # the condition_id cached.
            if reason == "take-profit":
                cond_id = pos.get("condition_id")
                # Try to look up condition_id if not cached in position
                if not cond_id and self.clob_client:
                    cond_id = getattr(
                        self.clob_client, "_token_to_condition", {},
                    ).get(token_id)
                if not cond_id:
                    try:
                        mkt = self.clob_client.get_market_by_token(token_id)
                        if mkt:
                            cond_id = mkt.get("condition_id")
                            if cond_id:
                                pos["condition_id"] = cond_id
                    except Exception:
                        pass
                if cond_id and hasattr(self, "_resolve_condition_id"):
                    try:
                        _, payout_denom = self._resolve_condition_id(
                            cond_id, neg_risk=pos.get("neg_risk", False),
                        )
                        if payout_denom > 0:
                            self.logger.info(
                                "Skipping auto-exit for %s — market already "
                                "resolved (payoutDenom=%d), redemption will "
                                "handle it at $1.00",
                                token_id[:16] + "...", payout_denom,
                            )
                            continue
                    except Exception as res_exc:
                        self.logger.debug(
                            "Resolution check failed for %s: %s",
                            token_id[:16] + "...", res_exc,
                        )

            # Final safety: never sell a martingale-owned token even if
            # we somehow got past the earlier guards (covers dry_run too).
            if (token_id in self._martingale_token_ids
                    or self.cfg.get("martingale_enabled", False)):
                self.logger.warning(
                    "AUTO-EXIT BLOCKED (martingale): refusing to sell %s "
                    "(in _martingale_token_ids=%s, martingale_enabled=%s)",
                    token_id[:16] + "...",
                    token_id in self._martingale_token_ids,
                    self.cfg.get("martingale_enabled", False),
                )
                continue

            sell_usdc = float(tokens * current_price_d)
            market_label = pos.get("market_name") or (token_id[:16] + "...")
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
            if self.notify_callback:
                self.notify_callback(
                    "AUTO-EXIT %s: SELL $%.2f — %s (entry %.4f -> %.4f)" % (
                        reason.upper(), sell_usdc, market_label,
                        entry_price, current_price_d,
                    )
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

            # Ensure the exchange can transfer our conditional tokens
            pos_neg_risk = pos.get("neg_risk", False)
            self.ensure_ct_approval(neg_risk=pos_neg_risk)

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
                # Log the closed trade before removing
                self._log_closed_trade(
                    token_id, entry_price, current_price_d,
                    tokens, reason,
                    market=pos.get("market_name"),

                )
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
                    self._log_closed_trade(
                        token_id, entry_price, current_price_d,
                        tokens, "dust",
                        market=pos.get("market_name"),
    
                    )
                    del self._positions[token_id]
                    self._save_positions()
                else:
                    self.logger.warning(
                        "Auto-exit %s order failed for token %s",
                        reason, token_id[:16] + "...",
                    )
                    # When a take-profit sell fails (e.g. market already
                    # resolved, balance/allowance issues), fall back to
                    # on-chain redemption which pays out at $1.00.
                    if reason == "take-profit":
                        redeemed = self._try_onchain_redeem(
                            token_id, pos, reason,
                        )
                        if redeemed:
                            results.append(redeemed)
                            continue

        return results

    # Maximum number of failed redemption/resolution attempts before we
    # give up on a position and remove it from tracking.
    MAX_REDEEM_RETRIES = 5

    def _try_onchain_redeem(self, token_id, pos, reason):
        """Attempt on-chain redemption for a position whose orderbook is dead.

        When the CLOB returns "orderbook does not exist", the market has
        resolved and the tokens can only be redeemed on-chain — not sold.

        Transient failures (no condition_id, RPC errors, failed txs) are
        retried up to MAX_REDEEM_RETRIES times before the position is
        removed.  This prevents a single hiccup from logging a false
        100% loss.

        Returns a result dict on success, or None.
        """
        retries = pos.get("_redeem_retries", 0)
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

        # Fallback: use cached condition_id from activity data
        if not condition_id and self.clob_client:
            condition_id = self.clob_client._token_to_condition.get(token_id)

        if not condition_id:
            retries += 1
            pos["_redeem_retries"] = retries
            if retries >= self.MAX_REDEEM_RETRIES:
                self.logger.warning(
                    "Cannot redeem token %s — no condition_id after %d "
                    "attempts. Removing stale position from tracking.",
                    token_id[:16] + "...", retries,
                )
                self._log_closed_trade(
                    token_id, pos.get("entry_price", 0), 0,
                    pos.get("tokens", 0), "stale",
                    market=pos.get("market_name"),

                )
                self._positions.pop(token_id, None)
                self._save_positions()
            else:
                self.logger.info(
                    "Cannot redeem token %s — no condition_id (attempt "
                    "%d/%d). Will retry next cycle.",
                    token_id[:16] + "...", retries,
                    self.MAX_REDEEM_RETRIES,
                )
                self._save_positions()
            return None

        # Check if actually resolved on-chain
        try:
            resolved_cid, payout_denom = self._resolve_condition_id(
                condition_id, neg_risk=neg_risk,
            )
        except Exception as exc:
            retries += 1
            pos["_redeem_retries"] = retries
            if retries >= self.MAX_REDEEM_RETRIES:
                self.logger.warning(
                    "On-chain resolution check failed for token %s "
                    "after %d attempts: %s — removing from tracking",
                    token_id[:16] + "...", retries, exc,
                )
                self._log_closed_trade(
                    token_id, pos.get("entry_price", 0), 0,
                    pos.get("tokens", 0), "resolution_error",
                    market=pos.get("market_name"),

                )
                self._positions.pop(token_id, None)
                self._save_positions()
            else:
                self.logger.info(
                    "On-chain resolution check failed for token %s: "
                    "%s (attempt %d/%d). Will retry.",
                    token_id[:16] + "...", exc, retries,
                    self.MAX_REDEEM_RETRIES,
                )
                self._save_positions()
            return None

        if payout_denom == 0:
            # Market not resolved yet — keep the position so the
            # stop-loss can still fire once the orderbook comes back,
            # or we can redeem when it does resolve.
            self.logger.info(
                "Market not resolved on-chain for token %s — "
                "orderbook dead but not settled yet. Keeping position "
                "for retry.",
                token_id[:16] + "...",
            )
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
            pre_bal = self.get_usdc_balance(max_age_seconds=0)
            tx = self._build_redeem_tx(resolved_cid, neg_risk=neg_risk)
            receipt = self._sign_and_send(tx)
            if receipt and receipt.status == 1:
                self.invalidate_balance_cache()
                post_bal = self.get_usdc_balance(max_age_seconds=0)
                exit_price = self._get_redemption_exit_price(
                    pre_bal, post_bal, pos.get("tokens", 0),
                    pos.get("entry_price", 0), receipt=receipt,
                )
                self.logger.info(
                    "Redemption OK (auto-exit %s): tx %s — exit $%.2f "
                    "(USDC %s%.4f)",
                    reason, receipt.transactionHash.hex(), exit_price,
                    "+" if post_bal >= pre_bal else "",
                    float(post_bal - pre_bal),
                )
                self._log_closed_trade(
                    token_id, pos.get("entry_price", 0), exit_price,
                    pos.get("tokens", 0), "redeemed",
                    market=pos.get("market_name"),

                )
                self._positions.pop(token_id, None)
                self._save_positions()
                return {
                    "status": "redeemed",
                    "reason": reason,
                    "token_id": token_id,
                    "tx_hash": receipt.transactionHash.hex(),
                    "exit_price": exit_price,
                }
            else:
                retries += 1
                pos["_redeem_retries"] = retries
                if retries >= self.MAX_REDEEM_RETRIES:
                    self.logger.warning(
                        "Redemption tx failed for token %s after %d "
                        "attempts — removing from tracking",
                        token_id[:16] + "...", retries,
                    )
                    self._log_closed_trade(
                        token_id, pos.get("entry_price", 0), 0,
                        pos.get("tokens", 0), "failed_redeem",
                        market=pos.get("market_name"),
    
                    )
                    self._positions.pop(token_id, None)
                else:
                    self.logger.info(
                        "Redemption tx failed for token %s (attempt "
                        "%d/%d). Will retry.",
                        token_id[:16] + "...", retries,
                        self.MAX_REDEEM_RETRIES,
                    )
                self._save_positions()
        except Exception as exc:
            retries += 1
            pos["_redeem_retries"] = retries
            if retries >= self.MAX_REDEEM_RETRIES:
                self.logger.warning(
                    "On-chain redeem failed for token %s after %d "
                    "attempts: %s — removing from tracking",
                    token_id[:16] + "...", retries, exc,
                )
                self._log_closed_trade(
                    token_id, pos.get("entry_price", 0), 0,
                    pos.get("tokens", 0), "failed_redeem",

                )
                self._positions.pop(token_id, None)
            else:
                self.logger.info(
                    "On-chain redeem failed for token %s: %s "
                    "(attempt %d/%d). Will retry.",
                    token_id[:16] + "...", exc, retries,
                    self.MAX_REDEEM_RETRIES,
                )
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
                    if market:
                        condition_id = market.get("condition_id")
                        if condition_id:
                            neg_risk = self._is_neg_risk_market(market)
                            # Cache neg_risk for future fallback lookups
                            self.clob_client._token_to_neg_risk[token_id] = neg_risk

                    # Fallback: use cached condition_id from activity data
                    # even when the Gamma/CLOB market lookup fails.
                    if not condition_id:
                        cached_cid = self.clob_client._token_to_condition.get(token_id)
                        if cached_cid:
                            condition_id = cached_cid
                            neg_risk = self.clob_client._token_to_neg_risk.get(
                                token_id, False
                            )
                            self.logger.debug(
                                "Using cached condition_id for token %s (neg_risk=%s)",
                                token_id[:16] + "...", neg_risk,
                            )
                        else:
                            no_market_count += 1
                            self.logger.info(
                                "No market info or cached condition_id for token %s "
                                "— cannot check resolution",
                                token_id[:16] + "...",
                            )
                            continue
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
                    # Skip tokens owned by the martingale bot
                    if token_id in self._martingale_token_ids:
                        active_seeded += 1
                        continue
                    if token_id not in self._positions:
                        tokens = Decimal(ct_balance) / Decimal("1000000")
                        # Use the user's actual VWAP buy price from
                        # activity history (cached during token discovery).
                        # Falls back to market last-trade-price only when
                        # the activity feed didn't contain buy records.
                        cached_entry = self.clob_client._token_to_avg_entry.get(token_id)
                        if cached_entry and cached_entry > 0:
                            entry_price = Decimal(str(round(cached_entry, 6)))
                            price_source = "activity VWAP"
                        else:
                            price = self.clob_client.get_last_trade_price(token_id)
                            entry_price = Decimal(str(price)) if price and price > 0 else Decimal("0")
                            price_source = "last trade"
                        seed_market_name = (
                            market.get("question") or market.get("slug")
                            if market else None
                        )
                        self._positions[token_id] = {
                            "tokens": tokens,
                            "entry_price": entry_price,
                            "opened_at": datetime.now().isoformat(),
                            "neg_risk": neg_risk,
                            "condition_id": condition_id,
                            "collateral_token": USDC_ADDRESS,
                            "parent_collection_id": "0x" + "00" * 32,
                            "index_sets": [1, 2],
                            "market_name": seed_market_name,
                        }
                        self._save_positions()
                        active_seeded += 1
                        if entry_price > 0:
                            self.logger.info(
                                "Discovered active position: %s (%.2f tokens @ $%.4f [%s], neg_risk=%s)",
                                token_id[:16] + "...", tokens, entry_price,
                                price_source, neg_risk,
                            )
                        else:
                            unresolved_no_price += 1
                            self.logger.info(
                                "Discovered active position (no price available): %s "
                                "(%.2f tokens, balance=%d, neg_risk=%s, question=%s)",
                                token_id[:16] + "...", tokens, ct_balance, neg_risk,
                                (market.get("question", "?") if market else "?")[:50],
                            )
                    continue

                # 5. Market is resolved on-chain — redeem!
                #    Use resolved_cid (may differ from the API's condition_id
                #    if the API returned a questionId rather than the derived
                #    CTF conditionId).
                question = (market.get("question", "unknown") if market
                            else existing_pos.get("market_name") if existing_pos
                            else "unknown")
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

                pre_bal = self.get_usdc_balance(max_age_seconds=0)
                tx = self._build_redeem_tx(resolved_cid, neg_risk=neg_risk)

                receipt = self._sign_and_send(tx)
                if receipt and receipt.status == 1:
                    self.invalidate_balance_cache()
                    post_bal = self.get_usdc_balance(max_age_seconds=0)
                    # Determine actual payout from balance change
                    pos = self._positions.get(token_id)
                    tokens_held = (
                        pos.get("tokens", 0) if pos
                        else Decimal(ct_balance) / Decimal("1000000")
                    )
                    exit_price = self._get_redemption_exit_price(
                        pre_bal, post_bal, tokens_held,
                        pos.get("entry_price", 0) if pos else 0,
                        receipt=receipt,
                    )
                    receipt_payout = self._usdc_payout_from_receipt(receipt)
                    payout_usdc = float(receipt_payout) if receipt_payout else float(post_bal - pre_bal)
                    self.logger.info(
                        "Redemption OK: tx %s — exit $%.2f "
                        "(USDC %s%.4f)",
                        receipt.transactionHash.hex(), exit_price,
                        "+" if payout_usdc >= 0 else "",
                        payout_usdc,
                    )
                    if pos:
                        self._log_closed_trade(
                            token_id, pos.get("entry_price", 0), exit_price,
                            pos.get("tokens", 0), "redeemed",
                            market=pos.get("market_name"),
        
                        )
                    else:
                        # Position wasn't tracked yet (discovered already
                        # resolved).  Use VWAP entry from activity history
                        # so P/L is still recorded accurately.
                        disc_tokens = Decimal(ct_balance) / Decimal("1000000")
                        cached_entry = self.clob_client._token_to_avg_entry.get(
                            token_id, 0
                        )
                        disc_market = (
                            market.get("question") or market.get("slug")
                            if market else None
                        )
                        self._log_closed_trade(
                            token_id, cached_entry, exit_price,
                            disc_tokens, "redeemed",
                            market=disc_market,
                        )
                    self._positions.pop(token_id, None)
                    self._save_positions()
                    results.append({
                        "status": "redeemed",
                        "token_id": token_id,
                        "balance": ct_balance,
                        "tx_hash": receipt.transactionHash.hex(),
                        "exit_price": exit_price,
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

        # Prune stale entries from _martingale_token_ids: remove token IDs
        # that are no longer tracked in _positions (already redeemed/removed).
        if self._martingale_token_ids:
            stale = self._martingale_token_ids - set(self._positions.keys())
            if stale:
                self._martingale_token_ids -= stale

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

    def _determine_exit_price(self, condition_id_hex, token_id, neg_risk=False):
        """Determine whether a redeemed token was a winner ($1) or loser ($0).

        Queries ``payoutNumerators`` on the ConditionalTokens contract for
        both outcome indices.  Polymarket binary markets have exactly two
        outcomes; only one has a non-zero numerator.

        For the winning token, ``redeemPositions`` returns 1 USDC per token.
        For the losing token, ``redeemPositions`` returns 0 USDC.

        Returns 1.0 for winners, 0.0 for losers, or None if undetermined.
        """
        try:
            cond_bytes = bytes.fromhex(condition_id_hex.replace("0x", ""))
            num_0 = self.conditional_tokens.functions.payoutNumerators(
                cond_bytes, 0,
            ).call()
            num_1 = self.conditional_tokens.functions.payoutNumerators(
                cond_bytes, 1,
            ).call()

            if num_0 == 0 and num_1 == 0:
                # Not resolved yet
                return None

            # Determine which outcome index this token_id corresponds to.
            # Polymarket token IDs encode the position: the token for
            # outcome index 0 vs 1.  We check which index has a non-zero
            # payout numerator, then verify by checking if redeeming this
            # token actually returns USDC.
            #
            # The most reliable method: check USDC balance before/after.
            # But since we call this before redemption, we use a heuristic:
            # compare the token_id against both outcome token IDs from
            # the market data (if available), or fall back to balance-based
            # detection after redemption.
            #
            # Simpler fallback: we check the balance CHANGE approach from
            # the caller.  For now, return both numerators so the caller
            # can use balance-diff to determine exit price.
            self.logger.debug(
                "Payout numerators for condition %s: [%d, %d]",
                condition_id_hex[:16] + "...", num_0, num_1,
            )
            return {"num_0": num_0, "num_1": num_1}
        except Exception as exc:
            self.logger.debug(
                "Could not query payoutNumerators: %s", exc,
            )
            return None

    # ERC-20 Transfer(address,address,uint256) event topic
    _TRANSFER_TOPIC = bytes.fromhex(
        "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    )

    def _usdc_payout_from_receipt(self, receipt):
        """Extract the total USDC transferred *to* our wallet from a tx receipt.

        Parses ERC-20 Transfer events emitted by the USDC contract where
        the ``to`` address matches our wallet.  This is immune to concurrent
        balance changes (e.g. a martingale bet spending USDC at the same time)
        because it reads the specific transaction's logs, not a balance diff.

        Returns the payout as a :class:`Decimal` in USDC (6-decimal scaled),
        or ``None`` if no matching transfer was found.
        """
        usdc_addr = Web3.to_checksum_address(USDC_ADDRESS).lower()
        wallet = self.address.lower()
        total = 0
        for log in receipt.get("logs", []):
            if log["address"].lower() != usdc_addr:
                continue
            topics = log.get("topics", [])
            if len(topics) < 3:
                continue
            if topics[0] != self._TRANSFER_TOPIC:
                continue
            # topics[2] is the `to` address (zero-padded to 32 bytes)
            to_addr = "0x" + topics[2].hex()[-40:]
            if to_addr.lower() == wallet:
                total += int(log["data"].hex(), 16)
        if total > 0:
            return Decimal(total) / Decimal("1000000")
        return None

    def _get_redemption_exit_price(self, pre_balance, post_balance,
                                   tokens, entry_price, receipt=None):
        """Compute the actual exit price from a redemption.

        Prefers parsing the USDC Transfer event from *receipt* (immune to
        concurrent balance changes).  Falls back to the pre/post balance diff
        if the receipt is not provided or contains no matching transfers.
        """
        tokens_f = float(tokens) if isinstance(tokens, Decimal) else tokens

        if tokens_f <= 0:
            return 0.0

        # Primary: use receipt logs (race-free)
        if receipt is not None:
            payout = self._usdc_payout_from_receipt(receipt)
            if payout is not None:
                payout_per_token = float(payout) / tokens_f
                return 1.0 if payout_per_token >= 0.5 else 0.0

        # Fallback: balance diff (may be inaccurate under concurrency)
        balance_diff = float(post_balance - pre_balance)

        # Compute per-token payout
        payout_per_token = balance_diff / tokens_f if tokens_f > 0 else 0

        # Winner: ~$1 per token.  Loser: ~$0 per token.
        # Use 0.5 as the threshold since payouts are binary.
        if payout_per_token >= 0.5:
            return 1.0
        else:
            return 0.0

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
                        self.logger.warning(
                            "REDEEM BLOCKED: no market info for token %s "
                            "(%s) — cannot determine condition_id",
                            token_id[:16] + "...",
                            pos.get("market_name", "unknown"),
                        )
                        continue
                    condition_id = market.get("condition_id")
                    if not condition_id:
                        self.logger.warning(
                            "REDEEM BLOCKED: market for token %s has no "
                            "condition_id — %s",
                            token_id[:16] + "...",
                            pos.get("market_name", "unknown"),
                        )
                        continue
                    neg_risk = self._is_neg_risk_market(market)
                    # Cache neg_risk for future fallback lookups
                    if self.clob_client:
                        self.clob_client._token_to_neg_risk[token_id] = neg_risk
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
                        "already redeemed or order never filled. "
                        "Removing from tracking (no P/L entry).",
                        token_id[:16] + "...",
                    )
                    # No on-chain tokens.  Either:
                    # 1. Already redeemed in a prior cycle (P/L already logged)
                    # 2. The buy order was never filled (phantom position)
                    # In both cases, do NOT log a closed trade — it would
                    # either double-count the redemption or create a fake
                    # P/L=$0 entry that inflates trade counts.
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
                pre_bal = self.get_usdc_balance(max_age_seconds=0)
                tx = self._build_redeem_tx(resolved_cid, neg_risk=neg_risk)

                receipt = self._sign_and_send(tx)
                if receipt and receipt.status == 1:
                    self.invalidate_balance_cache()
                    post_bal = self.get_usdc_balance(max_age_seconds=0)
                    exit_price = self._get_redemption_exit_price(
                        pre_bal, post_bal, pos.get("tokens", 0),
                        pos.get("entry_price", 0), receipt=receipt,
                    )
                    receipt_payout = self._usdc_payout_from_receipt(receipt)
                    payout_usdc = float(receipt_payout) if receipt_payout else float(post_bal - pre_bal)
                    self.logger.info(
                        "Redemption confirmed: tx %s — exit $%.2f "
                        "(USDC %s%.4f)",
                        receipt.transactionHash.hex(), exit_price,
                        "+" if payout_usdc >= 0 else "",
                        payout_usdc,
                    )
                    self._log_closed_trade(
                        token_id, pos.get("entry_price", 0), exit_price,
                        pos.get("tokens", 0), "redeemed",
                        market=pos.get("market_name"),
    
                    )
                    del self._positions[token_id]
                    self._save_positions()
                    results.append({
                        "status": "redeemed",
                        "condition_id": condition_id,
                        "token_id": token_id,
                        "balance": ct_balance,
                        "tx_hash": receipt.transactionHash.hex(),
                        "exit_price": exit_price,
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
                    "Error checking redemption for token %s (%s): %s",
                    token_id[:16] + "...",
                    pos.get("market_name", "unknown"),
                    exc,
                    exc_info=True,
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

        # 1b. Cached "no proxy" from a previous run — skip the 25s scan
        if cfg_addr == "none":
            self.logger.info(
                "Proxy discovery cached as 'none' from previous run — "
                "skipping auto-discovery. Clear proxy_address in config.json "
                "or set PROXY_ADDRESS env var to re-scan."
            )
            self._proxy_address = None
            self._proxy_discovery_done = True
            return None

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
            self._persist_proxy_result(api_result)
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
                    self._persist_proxy_result(addr)
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
            self._persist_proxy_result(result)
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
            self._persist_proxy_result(result)
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
            self._persist_proxy_result(scan_result)
            return scan_result

        self.logger.warning(
            "Could not discover proxy wallet for EOA %s. "
            "Set PROXY_ADDRESS env var or proxy_address in the GUI "
            "to provide it manually.",
            self.address,
        )
        self._proxy_discovery_done = True  # don't retry every 5 min
        # Cache the "no proxy" result so next restart skips the ~25s scan.
        self._persist_proxy_result("none")
        return None

    def _persist_proxy_result(self, address):
        """Write discovered proxy address into config.json for next startup.

        Pass ``"none"`` to record that no proxy exists (skips future scans).
        Pass a ``0x...`` address to cache the discovered proxy.
        """
        try:
            saved = load_user_config()
            saved["proxy_address"] = address
            save_user_config(saved)
            self.logger.info(
                "Proxy discovery result cached to %s (value: %s)",
                USER_CONFIG_FILE, address[:20] if address else "none",
            )
        except Exception as exc:
            self.logger.debug("Could not cache proxy result: %s", exc)

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
        # web3.py v7+ renamed signHash → unsafe_sign_hash
        _sign_hash = getattr(
            self.w3.eth.account, "unsafe_sign_hash",
            getattr(self.w3.eth.account, "signHash", None),
        )
        signed = _sign_hash(safe_tx_hash, self.private_key)
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

        # ── Phase 1: resolve condition_id + neg_risk from cache/API ──
        # This is cheap (mostly cache hits), so run sequentially.
        token_meta = {}  # token_id -> (condition_id, neg_risk, market)
        for token_id in token_ids:
            try:
                existing_pos = self._positions.get(token_id)
                condition_id = existing_pos.get("condition_id") if existing_pos else None
                neg_risk = existing_pos.get("neg_risk", False) if existing_pos else False
                market = None

                if not condition_id:
                    market = self.clob_client.get_market_by_token(token_id)
                    if market:
                        condition_id = market.get("condition_id")
                        if condition_id:
                            neg_risk = self._is_neg_risk_market(market)
                            self.clob_client._token_to_neg_risk[token_id] = neg_risk

                    if not condition_id:
                        cached_cid = self.clob_client._token_to_condition.get(token_id)
                        if cached_cid:
                            condition_id = cached_cid
                            neg_risk = self.clob_client._token_to_neg_risk.get(
                                token_id, False
                            )
                        else:
                            continue

                token_meta[token_id] = (condition_id, neg_risk, market)
            except Exception as exc:
                self.logger.info(
                    "Error resolving proxy token %s: %s", token_id[:16] + "...", exc,
                )

        # ── Phase 2: parallel balance check (the expensive part) ──
        # Most tokens will have 0 balance; parallelizing cuts ~12s to ~1-2s.
        held_tokens = {}  # token_id -> balance (only non-zero)

        def _check_balance(tid):
            cid, nr, _mkt = token_meta[tid]
            bal = self._get_token_balance(proxy_address, tid, neg_risk=nr)
            return tid, bal

        scan_workers = min(10, len(token_meta))
        if scan_workers > 0:
            with ThreadPoolExecutor(max_workers=scan_workers) as pool:
                futures = {pool.submit(_check_balance, tid): tid for tid in token_meta}
                for fut in futures:
                    try:
                        tid, bal = fut.result(timeout=10)
                        if bal > 0:
                            held_tokens[tid] = bal
                    except Exception as exc:
                        tid = futures[fut]
                        self.logger.info(
                            "Balance check failed for %s: %s", tid[:16] + "...", exc,
                        )

        self.logger.info(
            "Balance scan done: %d/%d tokens have holdings",
            len(held_tokens), len(token_meta),
        )

        # ── Phase 3: resolve + redeem only the tokens with balance ──
        results = []
        for token_id, ct_balance in held_tokens.items():
            condition_id, neg_risk, market = token_meta[token_id]
            try:
                resolved_cid, payout_denom = self._resolve_condition_id(
                    condition_id, neg_risk=neg_risk,
                )
                if payout_denom == 0:
                    self.logger.info(
                        "Proxy holds active position: token %s, balance %d, neg_risk=%s",
                        token_id[:16] + "...", ct_balance, neg_risk,
                    )
                    continue

                question = (market or {}).get("question", "unknown")
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
        """Approve the spender for at least *amount_raw* USDC if needed.

        Returns ``True`` if already approved, or the tx receipt on new
        approval.  Raises on failure.
        """
        spender = Web3.to_checksum_address(spender)
        current = self.usdc.functions.allowance(self.address, spender).call()
        if current >= amount_raw:
            return True  # Already approved

        self.logger.info("Approving USDC spend for %s ...", spender)
        tx = self.usdc.functions.approve(
            spender, 2**256 - 1  # max approval (common pattern)
        ).build_transaction(self._base_tx_params())
        return self._sign_and_send(tx)

    def ensure_ct_approval(self, neg_risk=False):
        """Approve the CTF Exchange to transfer conditional tokens for SELL orders.

        For standard markets the exchange needs setApprovalForAll on the
        ConditionalTokens contract.  For Neg Risk markets it also needs
        approval on the NegRiskAdapter (wrapped tokens).
        """
        exchange = Web3.to_checksum_address(CTF_EXCHANGE_ADDRESS)
        neg_exchange = Web3.to_checksum_address(NEG_RISK_CTF_EXCHANGE_ADDRESS)

        # Standard CT approval for the CTF Exchange
        try:
            approved = self.conditional_tokens.functions.isApprovedForAll(
                self.address, exchange,
            ).call()
            if not approved:
                self.logger.info(
                    "Approving CTF Exchange to transfer conditional tokens..."
                )
                tx = self.conditional_tokens.functions.setApprovalForAll(
                    exchange, True,
                ).build_transaction(self._base_tx_params())
                self._sign_and_send(tx)
        except Exception as exc:
            self.logger.warning("CT approval check/set failed: %s", exc)

        if neg_risk:
            # Neg Risk adapter approval for the Neg Risk CTF Exchange
            try:
                approved = self.neg_risk_adapter.functions.isApprovedForAll(
                    self.address, neg_exchange,
                ).call()
                if not approved:
                    self.logger.info(
                        "Approving NegRisk Exchange to transfer wrapped tokens..."
                    )
                    tx = self.neg_risk_adapter.functions.setApprovalForAll(
                        neg_exchange, True,
                    ).build_transaction(self._base_tx_params())
                    self._sign_and_send(tx)
            except Exception as exc:
                self.logger.warning("NegRisk CT approval check/set failed: %s", exc)

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
                # Refresh nonce on retries so we don't resend a stale value
                if attempt > 0:
                    tx["nonce"] = self._get_nonce()
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
                        # Cache neg_risk for future fallback lookups
                        self.clob_client._token_to_neg_risk[token_id] = neg_risk
                except Exception as exc:
                    self.logger.debug(
                        "Market lookup failed for token %s: %s",
                        token_id[:16] + "...", exc,
                    )
                # Fallback 1: use cached condition_id from activity data
                if not condition_id:
                    condition_id = self.clob_client._token_to_condition.get(token_id)
                    neg_risk = self.clob_client._token_to_neg_risk.get(
                        token_id, neg_risk
                    )
                # Fallback 2: extract condition_id from the trade_info itself
                if not condition_id:
                    condition_id = (
                        trade_info.get("conditionId")
                        or trade_info.get("condition_id")
                    )
                    if condition_id:
                        # Cache it for future trades on the same token
                        self.clob_client._token_to_condition[token_id] = str(condition_id)
            if not condition_id:
                self.logger.warning(
                    "No condition_id found for token %s — positions will "
                    "lack redemption params until resolved",
                    token_id[:16] + "...",
                )

            # --- Enforce Polymarket order minimums early ---
            # The CLOB API requires both ≥5 tokens AND ≥$1 USDC notional.
            # Compute the minimum viable USDC now so the balance cap and
            # allowance approval use the real order size (not the pre-bump
            # amount that place_order would silently inflate).
            #
            # Use the whale's price directly — no adverse slippage applied.
            price_f = float(price) if not isinstance(price, float) else price
            if price_f > 0:
                effective_price = round(price_f, 2)
                effective_price = max(min(effective_price, 0.99), 0.01)
                min_viable_usdc = max(
                    MIN_ORDER_SIZE_TOKENS * effective_price,
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

            # --- SELL guard: never sell martingale-owned tokens ---
            if side == "SELL" and (
                token_id in self._martingale_token_ids
                or self.cfg.get("martingale_enabled", False)
            ):
                self.logger.warning(
                    "COPY SELL BLOCKED (martingale): refusing to sell %s "
                    "(in _martingale_token_ids=%s, martingale_enabled=%s)",
                    token_id[:16] + "...",
                    token_id in self._martingale_token_ids,
                    self.cfg.get("martingale_enabled", False),
                )
                return None

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

            # Log latency info if the trade was annotated by get_new_trades()
            detection_latency = trade_info.get("_detection_latency_s")
            latency_str = " [latency=%.1fs]" % detection_latency if detection_latency else ""
            self.logger.info(
                "COPY TRADE: %s %.2f USDC of token %s (original: %.2f USDC, price: %s)%s",
                side, copy_amount, token_id[:16] + "..." if len(token_id) > 16 else token_id,
                original_usdc, price, latency_str,
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

            # Ensure approvals on the correct exchange.
            if side == "BUY":
                raw_amount = int(copy_amount * Decimal("1000000"))
                self.ensure_usdc_approval(CTF_EXCHANGE_ADDRESS, raw_amount)
                if neg_risk:
                    self.ensure_usdc_approval(
                        NEG_RISK_CTF_EXCHANGE_ADDRESS, raw_amount,
                    )
            else:
                # SELL: exchange needs ERC1155 approval to transfer our tokens
                self.ensure_ct_approval(neg_risk=neg_risk)

            # Place order via the CLOB API (requires py-clob-client + API creds)
            if self.clob_client and self.clob_client.clob_sdk:
                # --- Smart price selection ---
                # The whale's trade price is STALE by the time we detect it
                # (5-30s later).  Using whale_price + slippage guarantees we
                # overpay on buys and undersell on sells.
                #
                # Instead, fetch the CURRENT orderbook and use the live
                # best_ask (for buys) or best_bid (for sells) as our limit.
                # This ensures we fill at the actual market price, not a
                # worse price derived from stale data.
                #
                # We still apply max_price_deviation_pct as a safety cap to
                # avoid filling when the market has moved too far from the
                # whale's entry (suggesting the opportunity is gone).
                slippage_mult = Decimal(str(self.slippage_bps)) / Decimal("10000")
                whale_price = float(price)

                # --- FOK at whale price (buys) / whale price - slippage (sells) ---
                # For BUYS: use the whale's exact price as our FOK limit.
                # If the market has moved above whale price, our FOK won't fill
                # and we skip the trade.  Better to miss than overpay — paying
                # more than the whale guarantees we underperform them.
                #
                # For SELLS (copied_sell): apply slippage tolerance since we
                # need to exit and a small concession is acceptable.
                if side == "BUY":
                    adjusted_price = float(price)
                    adjusted_price = min(adjusted_price, 0.99)
                else:
                    adjusted_price = float(
                        Decimal(str(price)) * (Decimal("1") - slippage_mult)
                    )
                    adjusted_price = max(adjusted_price, 0.01)

                # --- Fetch live orderbook for stale-price safety check ---
                max_dev_pct = self.cfg.get("max_price_deviation_pct", 5)
                best_bid = 0
                best_ask = 0
                try:
                    book = self.clob_client.get_order_book(token_id)
                    if book:
                        bids = book.get("bids") or []
                        asks = book.get("asks") or []
                        best_bid = float(bids[0].get("price", 0)) if bids else 0
                        best_ask = float(asks[0].get("price", 0)) if asks else 0
                except Exception as book_exc:
                    self.logger.debug(
                        "Orderbook fetch failed (using whale price): %s",
                        book_exc,
                    )

                if best_bid > 0 and best_ask > 0:
                    mid_price = (best_bid + best_ask) / 2.0

                    # --- Stale price protection ---
                    if whale_price > 0 and max_dev_pct > 0:
                        deviation = abs(mid_price - whale_price) / whale_price * 100
                        if deviation > max_dev_pct:
                            self.logger.warning(
                                "STALE PRICE: whale traded at %.4f but "
                                "current mid=%.4f (%.1f%% deviation > %d%% "
                                "max) — skipping %s",
                                whale_price, mid_price, deviation,
                                max_dev_pct, token_id[:16] + "...",
                            )
                            return {
                                "status": "skipped_stale_price",
                                "side": side,
                                "amount_usdc": float(copy_amount),
                                "token_id": token_id,
                                "price": float(price),
                                "mid_price": mid_price,
                                "deviation_pct": round(deviation, 2),
                            }

                    # For SELLS only: use best_bid for better fill if available
                    if side == "SELL":
                        market_price = float(
                            Decimal(str(best_bid)) * (Decimal("1") - slippage_mult)
                        )
                        market_price = max(market_price, 0.01)
                        adjusted_price = max(market_price, adjusted_price)

                self.logger.info(
                    "WHALE PRICE FOK: whale=%.4f, bid=%.4f, ask=%.4f, "
                    "limit=%.4f (%s)",
                    whale_price, best_bid, best_ask, adjusted_price, side,
                )

                result = self.clob_client.place_order(
                    token_id=token_id,
                    side=side,
                    size_usdc=float(copy_amount),
                    price=adjusted_price,
                    use_fok=True,
                )
                if result:
                    self.logger.info("Order submitted to CLOB: %s", result)

                    # If the order was rejected, do NOT update positions
                    order_status = result.get("status", "") if isinstance(result, dict) else ""
                    if order_status in ("fok_rejected", "error"):
                        return result

                    self.invalidate_balance_cache()

                    # Update position tracker — use actual fill amounts from
                    # CLOB response when available (FOK fills may execute at a
                    # better price, giving more tokens than limit_price implies).
                    actual_tokens = None
                    actual_price = None
                    if isinstance(result, dict):
                        taking = result.get("takingAmount")
                        making = result.get("makingAmount")
                        if side == "BUY" and taking:
                            try:
                                actual_tokens = Decimal(str(taking))
                                if making:
                                    actual_price = Decimal(str(making)) / actual_tokens
                            except Exception:
                                pass
                        elif side == "SELL" and making:
                            try:
                                actual_tokens = Decimal(str(making))
                                if taking:
                                    actual_price = Decimal(str(taking)) / actual_tokens
                            except Exception:
                                pass
                    tokens = actual_tokens if actual_tokens else (
                        copy_amount / Decimal(str(adjusted_price))
                        if adjusted_price > 0
                        else Decimal("0")
                    )
                    fill_price = float(actual_price) if actual_price else adjusted_price
                    market_name = (
                        market.get("question") or market.get("slug")
                        if market else None
                    )
                    # Determine outcome side (Yes/No) from market tokens
                    outcome_side = None
                    if market:
                        for tok in (market.get("tokens") or []):
                            tid = tok.get("token_id") or tok.get("tokenId") or ""
                            if tid == token_id:
                                outcome_side = tok.get("outcome", "").capitalize()
                                break
                    redeem_params = {
                        "neg_risk": neg_risk,
                        "condition_id": condition_id,
                        "collateral_token": USDC_ADDRESS,
                        "parent_collection_id": "0x" + "00" * 32,
                        "index_sets": [1, 2],
                        "market_name": market_name,
                        "outcome_side": outcome_side,
                    }
                    if side == "BUY":
                        pos = self._positions.get(token_id)
                        if pos:
                            # Weighted-average entry price (both bot and whale)
                            old_tokens = pos["tokens"]
                            old_cost = old_tokens * pos["entry_price"]
                            new_cost = tokens * Decimal(str(fill_price))
                            total_tokens = old_tokens + tokens
                            avg_price = (
                                (old_cost + new_cost) / total_tokens
                                if total_tokens > 0
                                else Decimal(str(fill_price))
                            )
                            pos["tokens"] = total_tokens
                            pos["entry_price"] = avg_price
                            pos.update(redeem_params)
                        else:
                            self._positions[token_id] = {
                                "tokens": tokens,
                                "entry_price": Decimal(str(fill_price)),
                                "opened_at": datetime.now().isoformat(),
                                **redeem_params,
                            }
                    elif side == "SELL":
                        pos = self._positions.get(token_id)
                        if pos:
                            sold_tokens = min(tokens, pos["tokens"])
                            pos["tokens"] = max(pos["tokens"] - tokens, Decimal("0"))
                            # Remove position entirely if fully closed
                            if pos["tokens"] <= 0:
                                self._log_closed_trade(
                                    token_id, pos.get("entry_price", 0),
                                    fill_price, sold_tokens,
                                    "copied_sell",
                                    market=pos.get("market_name"),
                
                                )
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
                            fill_price, float(copy_amount),
                        )

                    return {
                        "status": "submitted",
                        "side": side,
                        "amount_usdc": float(copy_amount),
                        "token_id": token_id,
                        "price": fill_price,
                        "fill_tokens": float(tokens),
                        "whale_price": float(price),
                        "original_usdc": float(original_usdc),
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
        self._ws_monitor = None
        self._ws_wake = threading.Event()
        self._arb_monitor = None
        self._martingale_mgr = None
        self._telegram_bot = None
        self.executor = None
        self.clob_client = None

    def _notify(self, message):
        """Send a webhook and/or Telegram notification if configured."""
        url = self.cfg.get("webhook_url", "")
        send_webhook(url, message, logger=self.logger)
        tg_token = self.cfg.get("telegram_bot_token", "")
        tg_chat = self.cfg.get("telegram_chat_id", "")
        if tg_token and tg_chat:
            send_telegram(tg_token, tg_chat, message, logger=self.logger)

    def _append_trade_history(self, record):
        """Append a trade record to the persistent trade_history.json.

        Used by ArbitrageMonitor and MartingaleBot to log trades into the
        same history file that the TradeExecutor uses for copy trades, so
        all trades appear in the History tab and dashboard stats.

        Thread-safe: uses the module-level ``_TRADE_HISTORY_LOCK`` to
        prevent concurrent read-modify-write from multiple threads.

        Also updates the per-strategy summary file (strategy_summary.json)
        with combined stats, and writes the martingale-only summary to
        martingale_summary.json when the record is a martingale trade.
        """
        try:
            with _TRADE_HISTORY_LOCK:
                history_file = self.cfg.get("trade_history_file", TRADE_HISTORY_FILE)
                try:
                    with open(history_file, "r") as fh:
                        history = json.load(fh)
                except (FileNotFoundError, json.JSONDecodeError):
                    history = []
                history.append(record)
                tmp = history_file + ".tmp"
                with open(tmp, "w") as fh:
                    json.dump(history, fh, indent=2, default=str)
                os.replace(tmp, history_file)

                # --- Update combined strategy summary ---
                total_cost = sum(r.get("cost_basis_usdc", 0) for r in history)
                total_proceeds = sum(r.get("proceeds_usdc", 0) for r in history)
                total_pnl = round(total_proceeds - total_cost, 6)
                wins = sum(1 for r in history if r.get("outcome") in ("won", "win"))
                losses = sum(1 for r in history if r.get("outcome") in ("lost", "loss"))
                decided = wins + losses
                win_rate = (wins / decided * 100) if decided > 0 else 0
                total_position_size = sum(
                    r.get("position_size_usdc", r.get("cost_basis_usdc", 0))
                    for r in history
                )
                try:
                    summary = {
                        "updated_at": datetime.now().isoformat(),
                        "total_trades": len(history),
                        "wins": wins,
                        "losses": losses,
                        "win_rate_pct": round(win_rate, 1),
                        "lifetime_pnl_usdc": total_pnl,
                        "total_position_size_usdc": round(total_position_size, 6),
                        "lifetime_cost_basis_usdc": round(total_cost, 6),
                        "capital_returned_usdc": round(total_proceeds, 6),
                    }
                    tmp_sf = STRATEGY_SUMMARY_FILE + ".tmp"
                    with open(tmp_sf, "w") as fh:
                        json.dump(summary, fh, indent=2)
                    os.replace(tmp_sf, STRATEGY_SUMMARY_FILE)
                except Exception:
                    pass

        except Exception as exc:
            self.logger.warning("Could not append trade history: %s", exc)

    def _save_session_trades(self):
        """Persist session trades to disk immediately (crash-safe).

        Writes after every trade so data survives crashes, kills, or
        unexpected shutdowns.  Uses atomic write (tmp + rename).
        """
        try:
            tmp = SESSION_TRADES_FILE + ".tmp"
            with open(tmp, "w") as fh:
                json.dump(self._trade_history, fh, indent=2, default=str)
            os.replace(tmp, SESSION_TRADES_FILE)
        except Exception as exc:
            self.logger.debug("Could not save session trades: %s", exc)

    def _init_web3(self):
        """Initialize Web3 provider from config."""
        if Web3 is None:
            self.logger.error("web3 library not installed – on-chain features disabled")
            return False

        rpc_url = self.cfg.get("rpc_url", "")
        ws_url = self.cfg.get("ws_rpc_url", "")

        if ws_url:
            if _SyncWebSocketProvider is None:
                self.logger.warning(
                    "ws_rpc_url configured but websocket-client or web3 base "
                    "provider not available — will try HTTP RPC instead"
                )
            else:
                try:
                    provider = _SyncWebSocketProvider(ws_url)
                    self.w3 = Web3(provider)
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
            self.executor.notify_callback = self._notify
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

        # --- Display active configuration at startup ---
        watched = self.cfg.get("watched_addresses", [])
        self.logger.info("=" * 60)
        self.logger.info("COPY TRADER CONFIGURATION")
        self.logger.info("=" * 60)
        self.logger.info("  Watched addresses:      %d", len(watched))
        for i, addr in enumerate(watched):
            self.logger.info("    [%d] %s", i + 1, addr)
        self.logger.info("  Copy percentage:        %s%%",
                         self.cfg.get("copy_percentage", 50))
        self.logger.info("  Max trade size:         $%s",
                         self.cfg.get("max_trade_usdc", 100))
        self.logger.info("  Slippage tolerance:     %s bps",
                         self.cfg.get("slippage_tolerance_bps", 0))
        self.logger.info("  Poll interval:          %ss",
                         self.cfg.get("poll_interval_seconds", 2))
        self.logger.info("  Order TTL:              %ss",
                         self.cfg.get("order_ttl_seconds", 10))
        self.logger.info("  Max price deviation:    %s%%",
                         self.cfg.get("max_price_deviation_pct", 2))
        self.logger.info("  Trade max age:          %ss",
                         self.cfg.get("trade_max_age_seconds", 30))
        self.logger.info("  Resume threshold:       $%s",
                         self.cfg.get("resume_threshold_usdc", 5))
        self.logger.info("  Dry run:                %s",
                         self.cfg.get("dry_run", False))
        self.logger.info("  Auto redeem settled:    %s",
                         self.cfg.get("auto_redeem_settled", True))
        ws_status = "disabled"
        if self.cfg.get("ws_rpc_url"):
            ws_status = "enabled" if HAS_WS_CLIENT else "no websocket-client"
        self.logger.info("  WebSocket detection:    %s", ws_status)
        arb_status = "disabled"
        if self.cfg.get("arb_enabled"):
            dynamic_slug = self.cfg.get("arb_dynamic_slug", "").strip()
            if dynamic_slug:
                arb_status = (
                    f"enabled (dynamic '{dynamic_slug}' every "
                    f"{self.cfg.get('arb_dynamic_window', 300)}s, "
                    f"min edge {self.cfg.get('arb_min_edge_pct', 1.0)}%)"
                )
            else:
                n_arb = len(self.cfg.get("arb_condition_ids", []))
                arb_status = f"enabled ({n_arb} market(s), min edge {self.cfg.get('arb_min_edge_pct', 1.0)}%)"
        self.logger.info("  Arbitrage mode:         %s", arb_status)
        self.logger.info("=" * 60)

        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self.logger.info("Bot started")
        self._notify("Bot started — monitoring %d address(es)" % len(watched))

    def stop(self):
        self.running = False
        # Stop WebSocket monitor if running
        if self._ws_monitor:
            self._ws_monitor.stop()
        # Stop Arbitrage monitor if running
        if self._arb_monitor:
            self._arb_monitor.stop()
        # Stop Martingale bots if running
        if self._martingale_mgr:
            self._martingale_mgr.stop()
        # Stop Telegram command bot if running
        if self._telegram_bot:
            self._telegram_bot.stop()
        # Wake the main loop so it exits the Event.wait() immediately
        if hasattr(self, '_ws_wake'):
            self._ws_wake.set()
        self.logger.info("Bot stop requested")
        self._notify("Bot stopped")
        # Report is generated at the end of _run_loop after the while
        # loop exits, so positions and balance are still accessible.

    def _run_loop(self):
        """Main monitoring loop."""
        # Initialise connections – CLOB first so executor can use it
        self._init_clob()

        # Event used by WebSocketMonitor to wake the main loop instantly
        # when a whale trade is detected on-chain.
        self._ws_wake = threading.Event()
        self._ws_monitor = None

        web3_ok = self._init_web3()
        if web3_ok:
            self._init_executor()
            watched = self.cfg.get("watched_addresses", [])
            self.on_chain_monitor = OnChainMonitor(self.w3, watched, self.logger)

            # Start WebSocket monitor for real-time detection if ws_rpc_url
            # is configured and websocket-client is installed.
            ws_url = self.cfg.get("ws_rpc_url", "")
            if ws_url and HAS_WS_CLIENT and watched:
                self._ws_monitor = WebSocketMonitor(
                    ws_url, watched, self._ws_wake, self.logger,
                )
                self._ws_monitor.start()
                self.logger.info(
                    "Real-time WebSocket detection enabled — "
                    "poll interval serves as fallback only"
                )
            elif ws_url and not HAS_WS_CLIENT:
                self.logger.warning(
                    "ws_rpc_url configured but websocket-client not installed. "
                    "Install with: pip install websocket-client"
                )

        # ---- Start Arbitrage Monitor (independent of copy trading) ----
        if self.cfg.get("arb_enabled") and self.clob_client:
            arb_cids = self.cfg.get("arb_condition_ids", [])
            dynamic_slugs = self.cfg.get("arb_dynamic_slugs", [])
            dynamic_slug = self.cfg.get("arb_dynamic_slug", "").strip()
            has_dynamic = bool(dynamic_slugs or dynamic_slug)
            if arb_cids or has_dynamic:
                self._arb_monitor = ArbitrageMonitor(
                    self.clob_client, self.cfg, self.logger,
                )
                self._arb_monitor.notify_callback = self._notify
                self._arb_monitor.log_trade_callback = self._append_trade_history
                self._arb_monitor.start()
                self.logger.info(
                    "Copy trading from watched wallets DISABLED while "
                    "arb mode is active (wallet polling skipped)"
                )
                if has_dynamic:
                    # Use the monitor's helper to get the effective slug list
                    eff_slugs = self._arb_monitor._get_effective_slugs()
                    self.logger.info(
                        "Arbitrage monitor enabled — %d dynamic slug(s), "
                        "min edge %.1f%%, $%.0f/side, poll every %ds",
                        len(eff_slugs),
                        self.cfg.get("arb_min_edge_pct", 1.0),
                        self.cfg.get("arb_size_usdc", 10.0),
                        self.cfg.get("arb_poll_seconds", 2),
                    )
                    for s in eff_slugs:
                        self.logger.info(
                            "  slug: '%s' (window %ds, format %s)",
                            s["slug"], s["window"], s.get("format", "timestamp"),
                        )
                else:
                    self.logger.info(
                        "Arbitrage monitor enabled — scanning %d market(s), "
                        "min edge %.1f%%, $%.0f/side, poll every %ds",
                        len(arb_cids),
                        self.cfg.get("arb_min_edge_pct", 1.0),
                        self.cfg.get("arb_size_usdc", 10.0),
                        self.cfg.get("arb_poll_seconds", 2),
                    )
            else:
                self.logger.warning(
                    "arb_enabled=True but no arb_condition_ids or "
                    "arb_dynamic_slugs configured"
                )

        # ---- Start Martingale Bot(s) (independent of copy trading) ----
        if self.cfg.get("martingale_enabled") and self.clob_client:
            if not self.executor:
                self.logger.warning(
                    "MARTINGALE: no TradeExecutor available — on-chain USDC "
                    "approval cannot be set automatically.  Orders may fail "
                    "with 'not enough balance / allowance'.  Configure an "
                    "RPC URL (rpc_url or ws_rpc_url) to enable on-chain ops."
                )
            self._martingale_mgr = MartingaleManager(
                self.clob_client, self.cfg, self.logger,
                executor=self.executor,
            )
            self._martingale_mgr.set_notify_callback(self._notify)
            self._martingale_mgr.set_log_trade_callback(self._append_trade_history)
            self._martingale_mgr.start()

        # ---- Start Telegram command bot (independent of trading mode) ----
        tg_token = self.cfg.get("telegram_bot_token", "").strip()
        tg_chat = self.cfg.get("telegram_chat_id", "").strip()
        if tg_token and tg_chat and requests:
            self._telegram_bot = TelegramCommandBot(
                tg_token, tg_chat, self, self.logger,
            )
            self._telegram_bot.start()
            self._notify("Bot started")

        if web3_ok:
            # ---- Backfill market names for legacy positions ----
            if self.executor:
                try:
                    self.executor.backfill_market_names()
                except Exception as bf_exc:
                    self.logger.debug("Market name backfill failed (non-fatal): %s", bf_exc)

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

        poll_interval = self.cfg.get("poll_interval_seconds", 5)
        self.logger.info(
            "Monitoring %d address(es), poll interval %ds",
            len(self.cfg.get("watched_addresses", [])),
            poll_interval,
        )

        resume_threshold = Decimal(
            str(self.cfg.get("resume_threshold_usdc", 5))
        )
        _last_pause_log = 0  # timestamp of last "still paused" INFO log
        # Initialise to now so the main loop doesn't immediately re-run
        # the same scans that the startup sequence just completed.
        _now = time.time()
        _last_redeem_check = _now
        _last_proxy_check = _now
        _last_portfolio_scan = _now
        _last_exit_check = 0  # exit checks should start immediately
        exit_check_interval = self.cfg.get("exit_check_seconds", 5)

        # Runtime-togglable config keys — re-read from config.json
        _RUNTIME_TOGGLE_KEYS = {"arb_enabled", "martingale_enabled"}
        _last_config_reload = 0

        while self.running:
            try:
                # --- Hot-reload togglable config from config.json ---
                _now_reload = time.time()
                if _now_reload - _last_config_reload >= 30:
                    _last_config_reload = _now_reload
                    try:
                        disk_cfg = load_user_config()
                        for k in _RUNTIME_TOGGLE_KEYS:
                            if k in disk_cfg and disk_cfg[k] != self.cfg.get(k):
                                old = self.cfg.get(k)
                                self.cfg[k] = disk_cfg[k]
                                self.logger.info(
                                    "Config hot-reload: %s changed %s → %s",
                                    k, old, disk_cfg[k],
                                )
                    except Exception:
                        pass  # config.json may not exist or be corrupt

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
                                self._notify(
                                    "Balance recovered to $%.2f — resuming "
                                    "trading" % balance
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
                            self._notify(
                                "LOW BALANCE $%.2f — pausing trades, waiting "
                                "for settlements" % balance
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

                # --- Auto-redeem settled positions back to USDC ---
                # Runs BEFORE auto-exit so resolved markets get redeemed
                # at $1.00 instead of being sold at $0.99 minus slippage.
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
                        self.logger.warning(
                            "Redemption check error: %s", redeem_exc,
                            exc_info=True,
                        )

                # --- Auto-exit positions at take-profit / stop-loss ---
                # Runs even while paused so we protect existing positions.
                # Uses its own faster timer (default 5s) independent of
                # the poll interval to catch price moves quickly.
                # Martingale mode: skip entirely — positions resolve on-chain.
                now_exit = time.time()
                if (
                    self.executor
                    and not self.cfg.get("martingale_enabled", False)
                    and now_exit - _last_exit_check >= exit_check_interval
                ):
                    _last_exit_check = now_exit
                    try:
                        self.executor.check_exit_conditions()
                    except Exception as exit_exc:
                        self.logger.warning(
                            "Exit condition check error: %s", exit_exc,
                            exc_info=True,
                        )

                # --- Full portfolio scan (periodic) ---
                # Re-scans the wallet's complete trade history to catch
                # positions that resolved since the last check.  More
                # expensive than check_and_redeem_settled (which only
                # checks _positions dict) but catches positions that
                # were missed or not tracked.
                # When paused, uses the same accelerated cadence as the
                # proxy scan so we can redeem quickly and resume trading.
                portfolio_scan_interval = (
                    PAUSED_REDEEM_INTERVAL_SECONDS
                    if self._paused_low_balance
                    else PORTFOLIO_SCAN_INTERVAL_SECONDS
                )
                if (
                    self.executor
                    and self.cfg.get("auto_redeem_settled", True)
                    and now - _last_portfolio_scan >= portfolio_scan_interval
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
                        self.logger.warning(
                            "Portfolio scan error: %s", scan_exc,
                            exc_info=True,
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
                        self.logger.warning(
                            "Proxy redemption check error: %s", proxy_exc,
                            exc_info=True,
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

                # --- Skip copy trading when arb mode is active ---
                # Arb and copy trading share the same CLOB client and
                # balance; running both simultaneously slows the arb bot
                # down with unnecessary wallet polling & API calls.
                _arb_mon = getattr(self, "_arb_monitor", None)
                _arb_active = (
                    self.cfg.get("arb_enabled")
                    and _arb_mon is not None
                    and _arb_mon.is_alive()
                )
                # Clean up dead monitor (e.g. after runtime toggle-off)
                if _arb_mon is not None and not _arb_mon.is_alive():
                    self._arb_monitor = None

                # --- Skip trade detection & execution while paused ---
                if self._paused_low_balance:
                    pass  # just wait for balance to recover
                elif _arb_active:
                    pass  # arb mode owns the trading loop
                else:
                    new_trades = []

                    # --- CLOB API polling (primary) ---
                    if self.clob_client:
                        for addr in self.cfg.get("watched_addresses", []):
                            api_trades = self.clob_client.get_new_trades(addr)
                            if api_trades:
                                latencies = [
                                    t.get("_detection_latency_s")
                                    for t in api_trades
                                    if t.get("_detection_latency_s") is not None
                                ]
                                latency_info = ""
                                if latencies:
                                    latency_info = (
                                        " (avg latency=%.1fs, max=%.1fs)"
                                        % (sum(latencies) / len(latencies),
                                           max(latencies))
                                    )
                                self.logger.info(
                                    "CLOB API: %d new trade(s) from %s%s",
                                    len(api_trades), addr[:10] + "...",
                                    latency_info,
                                )
                                new_trades.extend(api_trades)

                    # --- On-chain polling (fallback) ---
                    if self.on_chain_monitor and web3_ok:
                        current_watched = self.cfg.get("watched_addresses", [])
                        self.on_chain_monitor.update_watched(current_watched)
                        if self._ws_monitor:
                            self._ws_monitor.update_watched(current_watched)
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
                                # Crash-safe: persist session trades immediately
                                self._save_session_trades()
                                if result.get("status") == "submitted":
                                    self._notify(
                                        "TRADE %s $%.2f @ $%.4f — %s" % (
                                            result.get("side", "?"),
                                            result.get("amount_usdc", 0),
                                            result.get("price", 0),
                                            result.get("token_id", "")[:16] + "...",
                                        )
                                    )
                        else:
                            self.logger.warning(
                                "Trade detected but executor not ready: %s",
                                json.dumps(trade, default=str)[:200],
                            )

            except Exception as exc:
                self.logger.error("Error in monitoring loop: %s", exc, exc_info=True)
                self._notify("ERROR in monitoring loop: %s" % exc)

            # Kill switch: stop if cumulative losses exceeded threshold
            if self.executor and self.executor.kill_switch_triggered:
                self.logger.critical("Kill switch activated — stopping bot")
                self.running = False
                break

            # Wait for next cycle.  If the WebSocket monitor detects a whale
            # trade it sets _ws_wake, waking us instantly instead of waiting
            # the full poll_interval.  Without WebSocket this behaves like a
            # normal sleep with early-exit support.
            woke_by_ws = self._ws_wake.wait(timeout=float(poll_interval))
            self._ws_wake.clear()
            if woke_by_ws:
                self.logger.debug("Main loop woken by WebSocket event")

        try:
            self._generate_stop_report()
        except Exception as exc:
            self.logger.warning("Stop report failed (non-fatal): %s", exc)
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

        # ---- Realized P/L from trade history file ----
        closed_trades = []
        if self.executor:
            closed_trades = self.executor._load_trade_history()
        total_cost_basis = sum(r.get("cost_basis_usdc", 0) for r in closed_trades)
        total_proceeds = sum(r.get("proceeds_usdc", 0) for r in closed_trades)
        realized_pnl = round(total_proceeds - total_cost_basis, 6)

        wins = sum(1 for r in closed_trades if r.get("outcome") == "won")
        losses = sum(1 for r in closed_trades if r.get("outcome") == "lost")
        total_resolved = wins + losses
        win_rate = (wins / total_resolved * 100) if total_resolved > 0 else 0

        # Unrealized value of open positions (at entry price as conservative estimate)
        open_cost = sum(
            p.get("cost_basis_usdc", 0) for p in positions.values()
        )

        # ---- Martingale breakdown from dedicated history ----
        martingale_section = {}
        try:
            with open(MARTINGALE_SUMMARY_FILE, "r") as fh:
                martingale_section = json.load(fh)
        except (FileNotFoundError, json.JSONDecodeError):
            pass

        # Also split closed_trades into copy vs martingale for the report
        copy_trades = [r for r in closed_trades if r.get("reason") != "martingale"]
        mart_trades = [r for r in closed_trades if r.get("reason") == "martingale"]
        copy_cost = sum(r.get("cost_basis_usdc", 0) for r in copy_trades)
        copy_proceeds = sum(r.get("proceeds_usdc", 0) for r in copy_trades)
        copy_pnl = round(copy_proceeds - copy_cost, 6)
        copy_wins = sum(1 for r in copy_trades if r.get("outcome") in ("won", "win"))
        copy_losses = sum(1 for r in copy_trades if r.get("outcome") in ("lost", "loss"))
        copy_decided = copy_wins + copy_losses
        copy_wr = (copy_wins / copy_decided * 100) if copy_decided > 0 else 0

        mart_cost = sum(r.get("cost_basis_usdc", 0) for r in mart_trades)
        mart_proceeds = sum(r.get("proceeds_usdc", 0) for r in mart_trades)
        mart_pnl = round(mart_proceeds - mart_cost, 6)
        mart_wins = sum(1 for r in mart_trades if r.get("outcome") in ("won", "win"))
        mart_losses = sum(1 for r in mart_trades if r.get("outcome") in ("lost", "loss"))
        mart_decided = mart_wins + mart_losses
        mart_wr = (mart_wins / mart_decided * 100) if mart_decided > 0 else 0

        report = {
            "session_start": (
                self._session_start.isoformat() if self._session_start else None
            ),
            "session_end": session_end.isoformat(),
            "usdc_balance_at_stop": current_balance,
            "total_session_trades": len(self._trade_history),
            "total_bought_usdc": round(buy_total, 6),
            "total_sold_usdc": round(sell_total, 6),
            "net_spent_usdc": round(buy_total - sell_total, 6),
            "lifetime_capital_deployed": round(total_cost_basis, 6),
            "lifetime_capital_returned": round(total_proceeds, 6),
            "lifetime_realized_pnl": realized_pnl,
            "wins": wins,
            "losses": losses,
            "win_rate_pct": round(win_rate, 1),
            "open_positions_cost_basis": round(open_cost, 6),
            "closed_trade_count": len(closed_trades),
            "trade_history": self._trade_history,
            "open_positions": positions,
            "copy_trading": {
                "trades": len(copy_trades),
                "wins": copy_wins,
                "losses": copy_losses,
                "win_rate_pct": round(copy_wr, 1),
                "pnl_usdc": copy_pnl,
                "capital_deployed_usdc": round(copy_cost, 6),
                "capital_returned_usdc": round(copy_proceeds, 6),
            },
            "martingale": {
                "trades": len(mart_trades),
                "wins": mart_wins,
                "losses": mart_losses,
                "win_rate_pct": round(mart_wr, 1),
                "pnl_usdc": mart_pnl,
                "capital_deployed_usdc": round(mart_cost, 6),
                "capital_returned_usdc": round(mart_proceeds, 6),
                "lifetime_summary": martingale_section,
            },
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
            "Session summary: %d session trades | bought $%.2f | sold $%.2f | "
            "net spent $%.2f | %d open position(s) | "
            "lifetime realized P&L: $%+.2f (deployed $%.2f, returned $%.2f)",
            len(self._trade_history), buy_total, sell_total,
            buy_total - sell_total, len(positions),
            realized_pnl, total_cost_basis, total_proceeds,
        )
        if total_resolved > 0:
            self.logger.info(
                "Strategy stats: W/L %d/%d (%.0f%%) | "
                "lifetime P&L $%+.2f",
                wins, losses, win_rate, realized_pnl,
            )
        if mart_trades:
            self.logger.info(
                "Martingale stats: %d bets | W/L %d/%d (%.0f%%) | "
                "P&L $%+.2f (deployed $%.2f, returned $%.2f)",
                len(mart_trades), mart_wins, mart_losses, mart_wr,
                mart_pnl, mart_cost, mart_proceeds,
            )
        if copy_trades:
            self.logger.info(
                "Copy trading stats: %d trades | W/L %d/%d (%.0f%%) | "
                "P&L $%+.2f",
                len(copy_trades), copy_wins, copy_losses, copy_wr, copy_pnl,
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
    """Logging handler that writes to a Tkinter ScrolledText widget.

    Safe to call from any thread — schedules the actual write via
    ``after()``.  Also safe after the widget or root window has been
    destroyed (silently drops the message).
    """

    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget
        self._closed = False

    def close(self):
        self._closed = True
        super().close()

    def emit(self, record):
        if self._closed:
            return
        msg = self.format(record) + "\n"
        try:
            self.text_widget.after(0, self._append, msg)
        except Exception:
            # Widget or root window already destroyed — silently ignore.
            pass

    def _append(self, msg):
        try:
            # Only auto-scroll if the user is already at the bottom.
            # yview() returns (top_fraction, bottom_fraction); if bottom
            # is >= 0.98 the user hasn't scrolled up to read old logs.
            _, bottom = self.text_widget.yview()
            at_bottom = bottom >= 0.98

            self.text_widget.configure(state="normal")
            self.text_widget.insert(tk.END, msg)
            if at_bottom:
                self.text_widget.see(tk.END)
            self.text_widget.configure(state="disabled")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Tkinter GUI Application
# ---------------------------------------------------------------------------

class CopyTraderGUI:
    """Main application window."""

    def __init__(self):
        _import_tkinter()

        # Start from built-in defaults, then overlay any previously-saved
        # settings from config.json (RPC URLs, proxy address, etc.).
        self.cfg = dict(DEFAULT_CONFIG)
        self.cfg.update(load_user_config())

        self.bot = None
        self.logger = None

        self.root = tk.Tk()
        self.root.title(f"Polymarket Copy Trader Bot  v{VERSION}")
        self.root.geometry("900x720")
        self.root.minsize(700, 550)

        self._build_ui()

        # Set up logging with GUI handler
        self._gui_handler = TextHandler(self.log_area)
        self.logger = setup_logging(self._gui_handler)
        self.logger.info("Polymarket Copy Trader v%s started", VERSION)
        self._load_fields_from_config()

    # ---- UI construction ----

    def _build_ui(self):
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)

        # Tab 1: Dashboard (live positions & balance)
        dash_frame = ttk.Frame(notebook, padding=10)
        notebook.add(dash_frame, text="Dashboard")
        self._build_dashboard_tab(dash_frame)

        # Tab 2: Configuration (scrollable – many fields)
        config_outer = ttk.Frame(notebook)
        notebook.add(config_outer, text="Configuration")
        config_canvas = tk.Canvas(config_outer, highlightthickness=0)
        config_scrollbar = ttk.Scrollbar(config_outer, orient=tk.VERTICAL, command=config_canvas.yview)
        config_frame = ttk.Frame(config_canvas, padding=10)
        config_frame.bind(
            "<Configure>",
            lambda e: config_canvas.configure(scrollregion=config_canvas.bbox("all")),
        )
        config_canvas.create_window((0, 0), window=config_frame, anchor="nw")
        config_canvas.configure(yscrollcommand=config_scrollbar.set)
        config_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        config_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        # Mousewheel scrolling
        def _on_config_mousewheel(event):
            config_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        config_canvas.bind_all("<MouseWheel>", _on_config_mousewheel)
        self._config_canvas = config_canvas
        self._config_mousewheel_handler = _on_config_mousewheel
        self._build_config_tab(config_frame)

        # Tab 3: Watched Addresses
        addr_frame = ttk.Frame(notebook, padding=10)
        notebook.add(addr_frame, text="Watched Addresses")
        self._build_address_tab(addr_frame)

        # Tab 4: Arbitrage
        arb_frame = ttk.Frame(notebook, padding=10)
        notebook.add(arb_frame, text="Arbitrage")
        self._build_arb_tab(arb_frame)

        # Tab 5: Martingale
        mart_frame = ttk.Frame(notebook, padding=10)
        notebook.add(mart_frame, text="Martingale")
        self._build_martingale_tab(mart_frame)

        # Tab 6: Trade History
        history_frame = ttk.Frame(notebook, padding=10)
        notebook.add(history_frame, text="Trade History")
        self._build_history_tab(history_frame)

        # Tab 7: Equity Chart
        equity_frame = ttk.Frame(notebook, padding=10)
        notebook.add(equity_frame, text="Equity")
        self._build_equity_tab(equity_frame)

        # Tab 8: Log / Status
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
        self.status_var = tk.StringVar(value="Status: Idle")
        ttk.Label(ctrl, textvariable=self.status_var).pack(side=tk.RIGHT, padx=10)

    # ---- Dashboard tab ----

    def _build_dashboard_tab(self, parent):
        # Top info bar: balances & summary
        info_frame = ttk.LabelFrame(parent, text="Account", padding=8)
        info_frame.pack(fill=tk.X, pady=(0, 6))

        self.dash_usdc_var = tk.StringVar(value="USDC Balance: --")
        self.dash_matic_var = tk.StringVar(value="MATIC Balance: --")
        self.dash_total_pnl_var = tk.StringVar(value="Floating P/L: --")
        self.dash_positions_var = tk.StringVar(value="Open Positions: 0")

        row_f = ttk.Frame(info_frame)
        row_f.pack(fill=tk.X)
        ttk.Label(row_f, textvariable=self.dash_usdc_var, font=("Courier", 11, "bold")).pack(side=tk.LEFT, padx=(0, 20))
        ttk.Label(row_f, textvariable=self.dash_matic_var, font=("Courier", 11)).pack(side=tk.LEFT, padx=(0, 20))
        ttk.Label(row_f, textvariable=self.dash_positions_var, font=("Courier", 11)).pack(side=tk.LEFT, padx=(0, 20))
        ttk.Label(row_f, textvariable=self.dash_total_pnl_var, font=("Courier", 11, "bold")).pack(side=tk.LEFT)

        # Strategy P/L bar
        strategy_frame = ttk.LabelFrame(parent, text="Martingale Strategy P/L", padding=8)
        strategy_frame.pack(fill=tk.X, pady=(0, 6))

        self.dash_session_pnl_var = tk.StringVar(value="Session P/L: --")
        self.dash_lifetime_pnl_var = tk.StringVar(value="Lifetime P/L: --")
        self.dash_winrate_var = tk.StringVar(value="W/L: --")

        strategy_row = ttk.Frame(strategy_frame)
        strategy_row.pack(fill=tk.X)
        ttk.Label(strategy_row, textvariable=self.dash_session_pnl_var, font=("Courier", 10, "bold")).pack(side=tk.LEFT, padx=(0, 20))
        ttk.Label(strategy_row, textvariable=self.dash_lifetime_pnl_var, font=("Courier", 10, "bold")).pack(side=tk.LEFT, padx=(0, 20))
        ttk.Label(strategy_row, textvariable=self.dash_winrate_var, font=("Courier", 10)).pack(side=tk.LEFT)

        # Positions treeview
        columns = ("direction", "market", "shares", "avg_price", "cur_price",
                    "cost_basis", "cur_value", "float_pnl", "pnl_pct", "time_held")
        col_headings = {
            "direction": "",
            "market": "Market",
            "shares": "Shares",
            "avg_price": "Avg Price",
            "cur_price": "Cur Price",
            "cost_basis": "Cost Basis",
            "cur_value": "Cur Value",
            "float_pnl": "Float P/L",
            "pnl_pct": "P/L %",
            "time_held": "Time Held",
        }
        col_widths = {
            "direction": 30, "market": 210, "shares": 75, "avg_price": 75,
            "cur_price": 75, "cost_basis": 85, "cur_value": 85,
            "float_pnl": 85, "pnl_pct": 70, "time_held": 90,
        }

        tree_frame = ttk.Frame(parent)
        tree_frame.pack(fill=tk.BOTH, expand=True)

        scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL)
        self.dash_tree = ttk.Treeview(
            tree_frame, columns=columns, show="headings",
            yscrollcommand=scrollbar.set, height=18,
        )
        scrollbar.config(command=self.dash_tree.yview)

        for col in columns:
            self.dash_tree.heading(col, text=col_headings[col])
            anchor = tk.CENTER if col == "direction" else (tk.W if col == "market" else tk.E)
            self.dash_tree.column(col, width=col_widths.get(col, 80), anchor=anchor)

        self.dash_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # Summary row
        self.dash_summary_var = tk.StringVar(value="")
        ttk.Label(parent, textvariable=self.dash_summary_var, font=("Courier", 10)).pack(
            anchor=tk.W, pady=(5, 0),
        )

        # Buttons
        btn_frame = ttk.Frame(parent)
        btn_frame.pack(fill=tk.X, pady=3)
        ttk.Button(btn_frame, text="Refresh Now", command=self._refresh_dashboard).pack(side=tk.LEFT)

        self._dash_refresh_id = None  # tkinter .after() handle
        self._dash_price_cache = {}   # token_id -> (price, timestamp)

    def _start_dashboard_refresh(self):
        """Begin periodic dashboard updates (every 10 seconds)."""
        self._refresh_dashboard()

    def _stop_dashboard_refresh(self):
        """Cancel the periodic dashboard refresh."""
        if self._dash_refresh_id is not None:
            self.root.after_cancel(self._dash_refresh_id)
            self._dash_refresh_id = None

    def _refresh_dashboard(self):
        """Pull live data from the bot and update the dashboard table."""
        # Schedule next refresh (10 seconds)
        if self.bot and self.bot.running:
            self._dash_refresh_id = self.root.after(10_000, self._refresh_dashboard)
        else:
            self._dash_refresh_id = None

        # Clear existing rows
        for row in self.dash_tree.get_children():
            self.dash_tree.delete(row)

        # No bot running — show idle state
        if not self.bot or not self.bot.running or not getattr(self.bot, "executor", None):
            self.dash_usdc_var.set("USDC Balance: --")
            self.dash_matic_var.set("MATIC Balance: --")
            self.dash_total_pnl_var.set("Floating P/L: --")
            self.dash_positions_var.set("Open Positions: 0")
            self.dash_summary_var.set("Bot not running")
            self.dash_session_pnl_var.set("Session P/L: --")
            self.dash_lifetime_pnl_var.set("Lifetime P/L: --")
            self.dash_winrate_var.set("W/L: --")
            return

        executor = self.bot.executor

        # Fetch balances (cached, thread-safe)
        try:
            usdc_bal = executor.get_usdc_balance(max_age_seconds=15)
            self.dash_usdc_var.set(f"USDC Balance: ${usdc_bal:,.2f}")
        except Exception:
            self.dash_usdc_var.set("USDC Balance: error")

        try:
            matic_bal = executor.get_matic_balance()
            self.dash_matic_var.set(f"MATIC Balance: {matic_bal:,.4f}")
        except Exception:
            self.dash_matic_var.set("MATIC Balance: error")

        # Build positions table
        positions = dict(executor._positions)  # snapshot
        self.dash_positions_var.set(f"Open Positions: {len(positions)}")

        total_cost = Decimal("0")
        total_value = Decimal("0")
        now = datetime.now()

        for token_id, pos in positions.items():
            tokens = pos.get("tokens", Decimal("0"))
            entry_price = pos.get("entry_price", Decimal("0"))
            market_name = pos.get("market_name") or (token_id[:20] + "...")

            if tokens <= 0:
                continue

            cost_basis = tokens * entry_price
            total_cost += cost_basis

            # Fetch current price (use CLOB client if available)
            cur_price = self._dash_get_price(token_id)
            if cur_price is not None:
                cur_price_d = Decimal(str(cur_price))
                cur_value = tokens * cur_price_d
                total_value += cur_value
                float_pnl = cur_value - cost_basis
                pnl_pct = ((cur_price_d - entry_price) / entry_price * 100) if entry_price > 0 else Decimal("0")
                cur_price_str = f"${cur_price_d:.4f}"
                cur_value_str = f"${cur_value:.2f}"
                float_pnl_str = f"{'+'if float_pnl >= 0 else ''}${float_pnl:.2f}"
                pnl_pct_str = f"{'+'if pnl_pct >= 0 else ''}{pnl_pct:.1f}%"
            else:
                cur_value = cost_basis  # fallback estimate
                total_value += cur_value
                cur_price_str = "--"
                cur_value_str = "--"
                float_pnl_str = "--"
                pnl_pct_str = "--"

            # Time held
            opened_at = pos.get("opened_at")
            if opened_at:
                try:
                    opened_dt = datetime.fromisoformat(opened_at)
                    delta = now - opened_dt
                    hours, remainder = divmod(int(delta.total_seconds()), 3600)
                    minutes = remainder // 60
                    if hours >= 24:
                        days = hours // 24
                        time_held = f"{days}d {hours % 24}h"
                    else:
                        time_held = f"{hours}h {minutes}m"
                except Exception:
                    time_held = "--"
            else:
                time_held = "--"

            # Direction indicator: UP = bought Yes, DOWN = bought No
            outcome_side = pos.get("outcome_side", "")
            if outcome_side == "Yes":
                direction = "UP"
            elif outcome_side == "No":
                direction = "DOWN"
            else:
                direction = "--"

            self.dash_tree.insert("", tk.END, values=(
                direction,
                market_name[:45],
                f"{tokens:.2f}",
                f"${entry_price:.4f}",
                cur_price_str,
                f"${cost_basis:.2f}",
                cur_value_str,
                float_pnl_str,
                pnl_pct_str,
                time_held,
            ))

        # --- Arb positions ---
        arb_mon = getattr(self.bot, "_arb_monitor", None)
        arb_total_cost = Decimal("0")
        arb_total_profit = Decimal("0")
        n_arb_positions = 0
        if arb_mon:
            arb_positions = dict(arb_mon._active_positions)
            n_arb_positions = len(arb_positions)
            for _cid, apos in arb_positions.items():
                a_cost = Decimal(str(apos.get("total_cost", 0)))
                a_profit = Decimal(str(apos.get("locked_profit", 0)))
                a_yes = apos.get("yes_shares", 0)
                a_no = apos.get("no_shares", 0)
                a_matched = min(a_yes, a_no)
                a_question = apos.get("question", "Unknown")
                is_partial = apos.get("partial", False)
                arb_total_cost += a_cost
                arb_total_profit += a_profit

                # Time held
                arb_time = "--"
                arb_ts = apos.get("ts")
                if arb_ts:
                    try:
                        arb_dt = datetime.fromisoformat(arb_ts)
                        delta = now - arb_dt
                        mins = int(delta.total_seconds()) // 60
                        arb_time = f"{mins}m" if mins < 60 else f"{mins // 60}h {mins % 60}m"
                    except Exception:
                        pass

                direction = "ARB" if not is_partial else "ARB!"
                pnl_str = f"+${a_profit:.4f}" if a_profit > 0 else f"${a_profit:.4f}"
                edge = apos.get("edge_pct", 0)

                self.dash_tree.insert("", tk.END, values=(
                    direction,
                    a_question[:45],
                    f"{a_matched:.2f}",
                    f"${a_cost / Decimal(str(a_matched)) if a_matched > 0 else 0:.4f}",
                    "$1.0000",  # arb exits at $1 per matched pair
                    f"${a_cost:.2f}",
                    f"${a_matched:.2f}" if not is_partial else "--",
                    pnl_str,
                    f"+{edge:.1f}%" if edge > 0 else "--",
                    arb_time,
                ))

        # --- Martingale active bets ---
        mart_mgr = getattr(self.bot, "_martingale_mgr", None)
        mart_pnl = Decimal("0")
        mart_cost = Decimal("0")
        n_mart_active = 0
        if mart_mgr:
            for mart_bot in mart_mgr.bots:
                if mart_bot._active_bet:
                    mb = mart_bot._active_bet
                    m_cost = Decimal(str(mb.get("cost", 0)))
                    m_shares = Decimal(str(mb.get("shares", 0)))
                    mart_cost += m_cost
                    n_mart_active += 1
                    self.dash_tree.insert("", tk.END, values=(
                        f"MART-{mb.get('direction', '?')}",
                        mb.get("question", "?")[:45],
                        f"{m_shares:.2f}",
                        f"${m_cost / m_shares if m_shares > 0 else 0:.4f}",
                        "pending",
                        f"${m_cost:.2f}",
                        "pending",
                        "--",
                        f"[{mart_bot.strategy_name}] bet #{mart_bot.consecutive_losses + 1}",
                        "--",
                    ))
            mart_pnl = Decimal(str(mart_mgr.get_combined_pnl()))

        # Totals (include arb + martingale positions)
        total_float_pnl = total_value - total_cost + arb_total_profit + mart_pnl
        total_positions = len(positions) + n_arb_positions + n_mart_active
        self.dash_positions_var.set(f"Open Positions: {total_positions}")
        self.dash_total_pnl_var.set(
            f"Floating P/L: {'+'if total_float_pnl >= 0 else ''}${total_float_pnl:.2f}"
        )
        arb_summary = ""
        if n_arb_positions > 0:
            arb_summary = (
                f"  |  Arb: {n_arb_positions} pos, "
                f"cost ${arb_total_cost:.2f}, "
                f"locked +${arb_total_profit:.4f}"
            )
        mart_summary = ""
        if mart_mgr:
            parts = []
            for mb in mart_mgr.bots:
                parts.append(
                    f"[{mb.strategy_name}] ${mb.current_bet:.2f} "
                    f"s{mb.consecutive_losses}"
                )
            mart_summary = (
                f"  |  Martingale: {', '.join(parts)}, "
                f"P&L ${mart_mgr.get_combined_pnl():+.4f}"
            )
        self.dash_summary_var.set(
            f"Total Cost Basis: ${total_cost + arb_total_cost + mart_cost:.2f}  |  "
            f"Total Current Value: ${total_value + Decimal(str(arb_total_profit)):.2f}  |  "
            f"Net: {'+'if total_float_pnl >= 0 else ''}${total_float_pnl:.2f}"
            f"{arb_summary}{mart_summary}"
        )

        # Update arb status label if monitor is running
        if arb_mon and hasattr(self, 'arb_status_var'):
            try:
                self.arb_status_var.set("Arbitrage: " + arb_mon.get_status_summary())
            except Exception:
                pass

        # Update martingale status label if bots are running
        if mart_mgr and hasattr(self, 'mart_status_var'):
            try:
                self.mart_status_var.set(
                    "Martingale: " + mart_mgr.get_status_summary()
                )
            except Exception:
                pass

        # Strategy P/L panel (read from strategy_summary.json)
        self._refresh_strategy_summary()

    def _refresh_strategy_summary(self):
        """Load strategy_summary.json and update the P/L labels.

        Also loads martingale_summary.json and appends a separate
        martingale stats line so the user can see martingale performance
        independently from copy-trading.
        """
        try:
            with open(STRATEGY_SUMMARY_FILE, "r") as fh:
                ss = json.load(fh)
        except (FileNotFoundError, json.JSONDecodeError):
            self.dash_session_pnl_var.set("Session P/L: $0.00 (no closed trades)")
            self.dash_lifetime_pnl_var.set("Lifetime P/L: $0.00")
            self.dash_winrate_var.set("W/L: 0/0 (0%)")
            return

        lifetime_pnl = ss.get("lifetime_pnl_usdc", 0)
        wins = ss.get("wins", 0)
        losses = ss.get("losses", 0)
        win_rate = ss.get("win_rate_pct", 0)
        total_trades = ss.get("total_trades", 0)

        # Session P/L from the running bot instance
        session_pnl = 0.0
        if self.bot:
            session_pnl = getattr(self.bot, "_session_pnl", 0.0)

        session_dir = "UP" if session_pnl > 0 else ("DOWN" if session_pnl < 0 else "--")
        self.dash_session_pnl_var.set(
            f"Session P/L: {session_dir} ${session_pnl:+,.2f} ({total_trades} trades)"
        )

        lifetime_dir = "UP" if lifetime_pnl > 0 else ("DOWN" if lifetime_pnl < 0 else "--")
        self.dash_lifetime_pnl_var.set(
            f"Lifetime P/L: {lifetime_dir} ${lifetime_pnl:+,.2f}"
        )

        # -- Martingale-specific stats with slippage --
        mart_label = ""
        try:
            with open(MARTINGALE_SUMMARY_FILE, "r") as fh:
                ms = json.load(fh)
            m_bets = ms.get("total_bets", 0)
            if m_bets > 0:
                m_wins = ms.get("wins", 0)
                m_losses = ms.get("losses", 0)
                m_pnl = ms.get("lifetime_pnl_usdc", 0)
                m_wr = ms.get("win_rate_pct", 0)
                slip = ms.get("slippage") or {}
                avg_fill = slip.get("avg_fill_price", 0)
                total_slip = slip.get("total_slippage_usdc", 0)
                edge_tag = ""
                if avg_fill > 0:
                    edge_tag = (
                        f" avg ${avg_fill:.3f}"
                        f" slip ${total_slip:+,.2f}"
                    )
                mart_label = (
                    f"  |  Mart: W/L {m_wins}/{m_losses} "
                    f"({m_wr:.0f}%) P&L ${m_pnl:+,.2f}{edge_tag}"
                )
        except (FileNotFoundError, json.JSONDecodeError):
            pass

        self.dash_winrate_var.set(
            f"W/L: {wins}/{losses} ({win_rate:.0f}%){mart_label}"
        )

    def _dash_get_price(self, token_id):
        """Get current price for a token, with 15-second caching."""
        now = datetime.now()
        cached = self._dash_price_cache.get(token_id)
        if cached:
            price, ts = cached
            if (now - ts).total_seconds() < 15:
                return price

        if not self.bot or not self.bot.clob_client:
            return None
        try:
            price = self.bot.clob_client.get_last_trade_price(token_id)
            if price is not None:
                self._dash_price_cache[token_id] = (price, now)
            return price
        except Exception:
            return cached[0] if cached else None

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

        # Take-profit price
        row += 1
        ttk.Label(parent, text="Take-Profit Price:").grid(row=row, column=0, sticky=tk.W, pady=3)
        tp_frame = ttk.Frame(parent)
        tp_frame.grid(row=row, column=1, sticky=tk.W, pady=3)
        self.take_profit_entry = ttk.Entry(tp_frame, width=10)
        self.take_profit_entry.pack(side=tk.LEFT)
        ttk.Label(tp_frame, text="(sell when price >= this, e.g. 0.90)").pack(side=tk.LEFT, padx=5)

        # Take-profit percentage gain
        row += 1
        ttk.Label(parent, text="Take-Profit Gain (%):").grid(row=row, column=0, sticky=tk.W, pady=3)
        tp_pct_frame = ttk.Frame(parent)
        tp_pct_frame.grid(row=row, column=1, sticky=tk.W, pady=3)
        self.take_profit_pct_entry = ttk.Entry(tp_pct_frame, width=10)
        self.take_profit_pct_entry.pack(side=tk.LEFT)
        ttk.Label(tp_pct_frame, text="(sell at +N% gain from entry, 0=off)").pack(side=tk.LEFT, padx=5)

        # Stop-loss percentage
        row += 1
        ttk.Label(parent, text="Stop-Loss (%):").grid(row=row, column=0, sticky=tk.W, pady=3)
        sl_frame = ttk.Frame(parent)
        sl_frame.grid(row=row, column=1, sticky=tk.W, pady=3)
        self.stop_loss_entry = ttk.Entry(sl_frame, width=10)
        self.stop_loss_entry.pack(side=tk.LEFT)
        ttk.Label(sl_frame, text="(sell when price drops to this % of entry)").pack(side=tk.LEFT, padx=5)

        # Kill switch — max loss
        row += 1
        ttk.Label(parent, text="Max Loss Kill Switch (USDC):").grid(row=row, column=0, sticky=tk.W, pady=3)
        kill_frame = ttk.Frame(parent)
        kill_frame.grid(row=row, column=1, sticky=tk.W, pady=3)
        self.max_loss_entry = ttk.Entry(kill_frame, width=10)
        self.max_loss_entry.pack(side=tk.LEFT)
        ttk.Label(kill_frame, text="(stop bot after losing this much, 0=off)").pack(side=tk.LEFT, padx=5)

        # Poll interval
        row += 1
        ttk.Label(parent, text="Poll Interval (seconds):").grid(row=row, column=0, sticky=tk.W, pady=3)
        self.poll_entry = ttk.Entry(parent, width=20)
        self.poll_entry.grid(row=row, column=1, sticky=tk.W, pady=3)

        # Exit check interval
        row += 1
        ttk.Label(parent, text="Exit Check Interval (seconds):").grid(row=row, column=0, sticky=tk.W, pady=3)
        exit_frame = ttk.Frame(parent)
        exit_frame.grid(row=row, column=1, sticky=tk.W, pady=3)
        self.exit_check_entry = ttk.Entry(exit_frame, width=10)
        self.exit_check_entry.pack(side=tk.LEFT)
        ttk.Label(exit_frame, text="(how often TP/SL prices are checked)").pack(side=tk.LEFT, padx=5)

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

        # --- Telegram Notifications ---
        row += 1
        ttk.Separator(parent, orient=tk.HORIZONTAL).grid(
            row=row, column=0, columnspan=3, sticky=tk.EW, pady=8
        )

        row += 1
        ttk.Label(
            parent, text="Telegram Notifications (mobile alerts & commands):",
            font=("TkDefaultFont", 9, "bold"),
        ).grid(row=row, column=0, columnspan=3, sticky=tk.W, pady=3)

        row += 1
        ttk.Label(parent, text="Bot Token:").grid(row=row, column=0, sticky=tk.W, pady=2)
        self.tg_token_entry = ttk.Entry(parent, width=70, show="*")
        self.tg_token_entry.grid(row=row, column=1, columnspan=2, sticky=tk.EW, pady=2)

        row += 1
        ttk.Label(parent, text="Chat ID:").grid(row=row, column=0, sticky=tk.W, pady=2)
        tg_chat_frame = ttk.Frame(parent)
        tg_chat_frame.grid(row=row, column=1, columnspan=2, sticky=tk.EW, pady=2)
        self.tg_chat_entry = ttk.Entry(tg_chat_frame, width=20)
        self.tg_chat_entry.pack(side=tk.LEFT)
        ttk.Button(
            tg_chat_frame, text="Test",
            command=self._test_telegram,
        ).pack(side=tk.LEFT, padx=10)

        row += 1
        ttk.Label(
            parent,
            text="Create a bot via @BotFather on Telegram to get the token. "
                 "Send /start to your bot, then use @userinfobot to find your Chat ID.",
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

    # ---- Arbitrage tab ----

    def _build_arb_tab(self, parent):
        # Enable checkbox
        self.arb_enabled_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            parent, text="Enable Arbitrage Mode", variable=self.arb_enabled_var,
        ).pack(anchor=tk.W, pady=(0, 5))

        ttk.Label(
            parent,
            text="Buy both sides of a binary market when the combined ask "
                 "price drops below $1, locking in guaranteed profit.",
            wraplength=600,
        ).pack(anchor=tk.W, pady=(0, 8))

        # Dynamic slugs section (multiple rotating markets)
        dyn_frame = ttk.LabelFrame(
            parent, text="Dynamic Markets (Rotating Slugs)", padding=8,
        )
        dyn_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 8))
        ttk.Label(
            dyn_frame,
            text="Add rotating market slugs below. Each slug auto-discovers "
                 "the current market every window.\n"
                 "Timestamp format (e.g. btc-updown-15m, window 900) for "
                 "markets like btc-updown-15m-1771293600.\n"
                 "Hourly format (e.g. ethereum-up-or-down, window 3600) for "
                 "markets like ethereum-up-or-down-february-16-9pm-et.",
            wraplength=600,
        ).pack(anchor=tk.W, pady=(0, 5))

        slug_list_frame = ttk.Frame(dyn_frame)
        slug_list_frame.pack(fill=tk.BOTH, expand=True, pady=3)
        self.arb_slugs_listbox = tk.Listbox(
            slug_list_frame, height=5, font=("Courier", 9),
        )
        slug_scroll = ttk.Scrollbar(
            slug_list_frame, orient=tk.VERTICAL,
            command=self.arb_slugs_listbox.yview,
        )
        self.arb_slugs_listbox.configure(yscrollcommand=slug_scroll.set)
        self.arb_slugs_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        slug_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # Entry row for adding new slug
        slug_entry_frame = ttk.Frame(dyn_frame)
        slug_entry_frame.pack(fill=tk.X, pady=3)
        ttk.Label(slug_entry_frame, text="Slug:").pack(side=tk.LEFT)
        self.arb_new_slug_entry = ttk.Entry(slug_entry_frame, width=25)
        self.arb_new_slug_entry.pack(side=tk.LEFT, padx=3)
        ttk.Label(slug_entry_frame, text="Window:").pack(side=tk.LEFT)
        self.arb_new_window_entry = ttk.Entry(slug_entry_frame, width=6)
        self.arb_new_window_entry.insert(0, "900")
        self.arb_new_window_entry.pack(side=tk.LEFT, padx=3)
        ttk.Label(slug_entry_frame, text="Format:").pack(side=tk.LEFT)
        self.arb_new_format_var = tk.StringVar(value="timestamp")
        fmt_combo = ttk.Combobox(
            slug_entry_frame, textvariable=self.arb_new_format_var,
            values=["timestamp", "hourly"], width=10, state="readonly",
        )
        fmt_combo.pack(side=tk.LEFT, padx=3)

        slug_btn_frame = ttk.Frame(dyn_frame)
        slug_btn_frame.pack(fill=tk.X)
        ttk.Button(
            slug_btn_frame, text="Add Slug", command=self._arb_add_slug,
        ).pack(side=tk.LEFT, padx=3)
        ttk.Button(
            slug_btn_frame, text="Remove Selected",
            command=self._arb_remove_slug,
        ).pack(side=tk.LEFT, padx=3)
        ttk.Button(
            slug_btn_frame, text="Clear All", command=self._arb_clear_slugs,
        ).pack(side=tk.LEFT, padx=3)

        # Settings row
        settings_frame = ttk.LabelFrame(parent, text="Arbitrage Settings", padding=8)
        settings_frame.pack(fill=tk.X, pady=(0, 8))

        row = 0
        ttk.Label(settings_frame, text="Min Edge (%):").grid(
            row=row, column=0, sticky=tk.W, pady=3,
        )
        edge_frame = ttk.Frame(settings_frame)
        edge_frame.grid(row=row, column=1, sticky=tk.W, pady=3)
        self.arb_min_edge_entry = ttk.Entry(edge_frame, width=8)
        self.arb_min_edge_entry.pack(side=tk.LEFT)
        ttk.Label(
            edge_frame,
            text="(e.g. 1.0 = only trade when spread >= 1%)",
        ).pack(side=tk.LEFT, padx=5)

        row += 1
        ttk.Label(settings_frame, text="Size per Side (USDC):").grid(
            row=row, column=0, sticky=tk.W, pady=3,
        )
        size_frame = ttk.Frame(settings_frame)
        size_frame.grid(row=row, column=1, sticky=tk.W, pady=3)
        self.arb_size_entry = ttk.Entry(size_frame, width=8)
        self.arb_size_entry.pack(side=tk.LEFT)
        ttk.Label(
            size_frame, text="(USDC to spend on each side)",
        ).pack(side=tk.LEFT, padx=5)

        row += 1
        ttk.Label(settings_frame, text="Max Positions:").grid(
            row=row, column=0, sticky=tk.W, pady=3,
        )
        self.arb_max_pos_entry = ttk.Entry(settings_frame, width=8)
        self.arb_max_pos_entry.grid(row=row, column=1, sticky=tk.W, pady=3)

        row += 1
        ttk.Label(settings_frame, text="Poll Interval (seconds):").grid(
            row=row, column=0, sticky=tk.W, pady=3,
        )
        self.arb_poll_entry = ttk.Entry(settings_frame, width=8)
        self.arb_poll_entry.grid(row=row, column=1, sticky=tk.W, pady=3)

        # Market condition IDs list
        markets_frame = ttk.LabelFrame(parent, text="Markets to Monitor (Condition IDs)", padding=8)
        markets_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 5))

        ttk.Label(
            markets_frame,
            text="Add the condition_id of each binary market to scan. "
                 "Find this on the Polymarket market page URL or API.",
            wraplength=600,
        ).pack(anchor=tk.W, pady=(0, 5))

        list_frame = ttk.Frame(markets_frame)
        list_frame.pack(fill=tk.BOTH, expand=True, pady=3)

        self.arb_market_listbox = tk.Listbox(
            list_frame, height=6, font=("Courier", 9),
        )
        arb_scroll = ttk.Scrollbar(
            list_frame, orient=tk.VERTICAL, command=self.arb_market_listbox.yview,
        )
        self.arb_market_listbox.configure(yscrollcommand=arb_scroll.set)
        self.arb_market_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        arb_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        entry_frame = ttk.Frame(markets_frame)
        entry_frame.pack(fill=tk.X, pady=3)
        ttk.Label(entry_frame, text="Condition ID:").pack(side=tk.LEFT)
        self.arb_new_cid_entry = ttk.Entry(
            entry_frame, width=50, font=("Courier", 9),
        )
        self.arb_new_cid_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)

        btn_frame = ttk.Frame(markets_frame)
        btn_frame.pack(fill=tk.X)
        ttk.Button(
            btn_frame, text="Add", command=self._arb_add_market,
        ).pack(side=tk.LEFT, padx=3)
        ttk.Button(
            btn_frame, text="Remove Selected", command=self._arb_remove_market,
        ).pack(side=tk.LEFT, padx=3)
        ttk.Button(
            btn_frame, text="Clear All", command=self._arb_clear_markets,
        ).pack(side=tk.LEFT, padx=3)

        # Status label (updated while running)
        self.arb_status_var = tk.StringVar(value="Arbitrage: idle")
        ttk.Label(
            parent, textvariable=self.arb_status_var,
            font=("Courier", 10, "bold"),
        ).pack(anchor=tk.W, pady=(5, 0))

    def _arb_add_market(self):
        cid = self.arb_new_cid_entry.get().strip()
        if cid and cid not in list(self.arb_market_listbox.get(0, tk.END)):
            self.arb_market_listbox.insert(tk.END, cid)
            self.arb_new_cid_entry.delete(0, tk.END)

    def _arb_remove_market(self):
        sel = self.arb_market_listbox.curselection()
        if sel:
            self.arb_market_listbox.delete(sel[0])

    def _arb_clear_markets(self):
        self.arb_market_listbox.delete(0, tk.END)

    def _arb_add_slug(self):
        slug = self.arb_new_slug_entry.get().strip()
        if not slug:
            return
        try:
            window = int(self.arb_new_window_entry.get().strip())
        except ValueError:
            window = 900
        fmt = self.arb_new_format_var.get() or "timestamp"
        display = f"{slug}  |  window={window}s  |  {fmt}"
        # Avoid duplicates by slug name
        existing = list(self.arb_slugs_listbox.get(0, tk.END))
        for e in existing:
            if e.split("|")[0].strip() == slug:
                return
        self.arb_slugs_listbox.insert(tk.END, display)
        self.arb_new_slug_entry.delete(0, tk.END)

    def _arb_remove_slug(self):
        sel = self.arb_slugs_listbox.curselection()
        if sel:
            self.arb_slugs_listbox.delete(sel[0])

    def _arb_clear_slugs(self):
        self.arb_slugs_listbox.delete(0, tk.END)

    @staticmethod
    def _parse_slug_display(display_str):
        """Parse a listbox display string back into a slug dict."""
        parts = [p.strip() for p in display_str.split("|")]
        slug = parts[0].strip() if parts else ""
        window = 900
        fmt = "timestamp"
        for p in parts[1:]:
            if p.startswith("window="):
                try:
                    window = int(p.replace("window=", "").replace("s", ""))
                except ValueError:
                    pass
            elif p in ("timestamp", "hourly"):
                fmt = p
        return {"slug": slug, "window": window, "format": fmt}

    @staticmethod
    def _format_slug_display(entry):
        """Format a slug dict for display in the listbox."""
        return (
            f"{entry['slug']}  |  window={entry.get('window', 900)}s  "
            f"|  {entry.get('format', 'timestamp')}"
        )

    # ---- Martingale tab ----

    def _build_martingale_tab(self, parent):
        # Enable checkbox
        self.mart_enabled_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            parent, text="Enable Martingale Mode",
            variable=self.mart_enabled_var,
        ).pack(anchor=tk.W, pady=(0, 5))

        ttk.Label(
            parent,
            text=(
                "Double-on-loss strategy: configure one or more independent "
                "strategies with different bet sizes and market windows."
            ),
            wraplength=500, justify=tk.LEFT,
        ).pack(anchor=tk.W, pady=(0, 10))

        # In-memory list of strategy dicts (the source of truth for the UI)
        self._mart_strategies = []
        self._mart_selected_idx = None

        # --- Top: strategy list + buttons on the left, edit form on right ---
        top_frame = ttk.Frame(parent)
        top_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 5))

        # -- Left: strategy list --
        list_frame = ttk.LabelFrame(top_frame, text="Strategies", padding=5)
        list_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=False, padx=(0, 5))

        self.mart_listbox = tk.Listbox(list_frame, width=30, height=10)
        self.mart_listbox.pack(fill=tk.BOTH, expand=True)
        self.mart_listbox.bind("<<ListboxSelect>>", self._mart_on_select)

        btn_row = ttk.Frame(list_frame)
        btn_row.pack(fill=tk.X, pady=(5, 0))
        ttk.Button(btn_row, text="Add", width=8,
                   command=self._mart_add_strategy).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_row, text="Remove", width=8,
                   command=self._mart_remove_strategy).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_row, text="Duplicate", width=8,
                   command=self._mart_duplicate_strategy).pack(side=tk.LEFT, padx=2)

        # -- Right: edit form for selected strategy --
        edit_frame = ttk.LabelFrame(top_frame, text="Strategy Settings", padding=5)
        edit_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Build the fields in a grid
        fields = [
            ("Name:", "mart_e_name", 20),
            ("Slug Base:", "mart_e_slug", 25),
            ("Window (seconds):", "mart_e_window", 10),
            ("Direction (Up/Down):", "mart_e_direction", 10),
            ("Starting Bet (USDC):", "mart_e_start_bet", 10),
            ("Max Bet (0=no limit):", "mart_e_max_bet", 10),
            ("Max Streak (0=no limit):", "mart_e_max_streak", 10),
            ("Poll Interval (seconds):", "mart_e_poll", 10),
            ("Buy Price Min:", "mart_e_price_min", 10),
            ("Buy Price Max:", "mart_e_price_max", 10),
            ("Max Entry (sec into window):", "mart_e_max_entry", 10),
            ("Recovery Candles (total):", "mart_e_recovery_candles", 10),
            ("Recovery Green (needed):", "mart_e_recovery_green", 10),
            ("Recovery Interval (sec):", "mart_e_recovery_interval", 10),
        ]
        self._mart_entries = {}
        for row, (label, attr, width) in enumerate(fields):
            ttk.Label(edit_frame, text=label).grid(
                row=row, column=0, sticky=tk.W, padx=2, pady=2,
            )
            entry = ttk.Entry(edit_frame, width=width)
            entry.grid(row=row, column=1, sticky=tk.W, padx=2, pady=2)
            self._mart_entries[attr] = entry

        # Checkbox: reset bet to initial when streak recovery completes
        next_row = len(fields)
        self.mart_e_streak_reset_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            edit_frame,
            text="Reset bet to initial on streak recovery",
            variable=self.mart_e_streak_reset_var,
        ).grid(row=next_row, column=0, columnspan=2, sticky=tk.W, padx=2, pady=4)

        ttk.Button(
            edit_frame, text="Apply to Selected",
            command=self._mart_apply_edit,
        ).grid(row=next_row + 1, column=0, columnspan=2, pady=(10, 0))

        # --- Bottom: status + reset ---
        status_frame = ttk.Frame(parent)
        status_frame.pack(fill=tk.X, pady=(10, 0))

        self.mart_status_var = tk.StringVar(value="Martingale: idle")
        ttk.Label(
            status_frame, textvariable=self.mart_status_var,
            font=("Courier", 10, "bold"),
        ).pack(side=tk.LEFT)

        ttk.Button(
            status_frame, text="Reset All State",
            command=self._reset_martingale_state,
        ).pack(side=tk.RIGHT, padx=5)

    # -- Martingale strategy list helpers ------------------------------------

    def _mart_refresh_listbox(self):
        """Repopulate the listbox from self._mart_strategies."""
        self.mart_listbox.delete(0, tk.END)
        for s in self._mart_strategies:
            name = s.get("name", "unnamed")
            bet = s.get("start_bet", 5.0)
            slug = s.get("slug_base", "?")
            self.mart_listbox.insert(
                tk.END, f"{name}  (${bet}, {slug})",
            )

    def _mart_on_select(self, event=None):
        """Load selected strategy into the edit fields."""
        sel = self.mart_listbox.curselection()
        if not sel:
            return
        idx = sel[0]
        self._mart_selected_idx = idx
        s = self._mart_strategies[idx]
        mapping = {
            "mart_e_name": ("name", ""),
            "mart_e_slug": ("slug_base", "btc-updown-5m"),
            "mart_e_window": ("window", 300),
            "mart_e_direction": ("direction", "Up"),
            "mart_e_start_bet": ("start_bet", 5.0),
            "mart_e_max_bet": ("max_bet", 0),
            "mart_e_max_streak": ("max_streak", 0),
            "mart_e_poll": ("poll_seconds", 10),
            "mart_e_price_min": ("price_min", 0.40),
            "mart_e_price_max": ("price_max", 0.55),
            "mart_e_max_entry": ("max_entry_seconds", 60),
            "mart_e_recovery_candles": ("recovery_candles", 5),
            "mart_e_recovery_green": ("recovery_green", 3),
            "mart_e_recovery_interval": ("recovery_interval", 300),
        }
        for attr, (key, default) in mapping.items():
            entry = self._mart_entries[attr]
            entry.delete(0, tk.END)
            entry.insert(0, str(s.get(key, default)))
        # Load streak_reset checkbox
        self.mart_e_streak_reset_var.set(s.get("streak_reset", True))

    def _mart_apply_edit(self):
        """Write the edit fields back into the selected strategy dict."""
        idx = self._mart_selected_idx
        if idx is None or idx >= len(self._mart_strategies):
            return
        s = self._mart_strategies[idx]
        s["name"] = self._mart_entries["mart_e_name"].get().strip() or "unnamed"
        s["slug_base"] = self._mart_entries["mart_e_slug"].get().strip() or "btc-updown-5m"
        s["direction"] = self._mart_entries["mart_e_direction"].get().strip() or "Up"
        for attr, key, conv in [
            ("mart_e_window", "window", int),
            ("mart_e_start_bet", "start_bet", float),
            ("mart_e_max_bet", "max_bet", float),
            ("mart_e_max_streak", "max_streak", int),
            ("mart_e_poll", "poll_seconds", int),
            ("mart_e_price_min", "price_min", float),
            ("mart_e_price_max", "price_max", float),
            ("mart_e_max_entry", "max_entry_seconds", int),
            ("mart_e_recovery_candles", "recovery_candles", int),
            ("mart_e_recovery_green", "recovery_green", int),
            ("mart_e_recovery_interval", "recovery_interval", int),
        ]:
            try:
                s[key] = conv(self._mart_entries[attr].get().strip())
            except (ValueError, TypeError):
                pass
        # Save streak_reset checkbox
        s["streak_reset"] = self.mart_e_streak_reset_var.get()
        self._mart_refresh_listbox()
        # Re-select the same index
        if idx < self.mart_listbox.size():
            self.mart_listbox.selection_set(idx)

    def _mart_add_strategy(self):
        """Add a new strategy with defaults."""
        n = len(self._mart_strategies) + 1
        self._mart_strategies.append({
            "name": f"Strategy {n}",
            "slug_base": "btc-updown-5m",
            "window": 300,
            "direction": "Up",
            "start_bet": 5.0,
            "max_bet": 0,
            "max_streak": 0,
            "streak_reset": True,
            "poll_seconds": 10,
            "price_min": 0.40,
            "price_max": 0.55,
            "max_entry_seconds": 60,
            "recovery_candles": 5,
            "recovery_green": 3,
            "recovery_interval": 300,
        })
        self._mart_refresh_listbox()
        # Select the new entry
        idx = len(self._mart_strategies) - 1
        self.mart_listbox.selection_set(idx)
        self._mart_selected_idx = idx
        self._mart_on_select()

    def _mart_remove_strategy(self):
        """Remove the selected strategy."""
        sel = self.mart_listbox.curselection()
        if not sel:
            return
        idx = sel[0]
        del self._mart_strategies[idx]
        self._mart_selected_idx = None
        self._mart_refresh_listbox()
        # Clear edit fields
        for entry in self._mart_entries.values():
            entry.delete(0, tk.END)

    def _mart_duplicate_strategy(self):
        """Duplicate the selected strategy."""
        sel = self.mart_listbox.curselection()
        if not sel:
            return
        import copy as _copy
        original = self._mart_strategies[sel[0]]
        dup = _copy.deepcopy(original)
        dup["name"] = original.get("name", "unnamed") + " (copy)"
        self._mart_strategies.append(dup)
        self._mart_refresh_listbox()
        idx = len(self._mart_strategies) - 1
        self.mart_listbox.selection_set(idx)
        self._mart_selected_idx = idx
        self._mart_on_select()

    # ------------------------------------------------------------------
    # Equity Chart tab
    # ------------------------------------------------------------------

    def _build_equity_tab(self, parent):
        """Build an hourly cumulative P&L chart using tkinter Canvas."""
        # Controls bar
        ctrl = ttk.Frame(parent)
        ctrl.pack(fill=tk.X, pady=(0, 4))
        ttk.Button(ctrl, text="Refresh", command=self._refresh_equity_chart).pack(
            side=tk.LEFT,
        )
        self.equity_session_only_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            ctrl, text="Current Session Only",
            variable=self.equity_session_only_var,
            command=self._refresh_equity_chart,
        ).pack(side=tk.LEFT, padx=10)

        # Date range filter controls
        ttk.Label(ctrl, text="From:", font=("Courier", 9)).pack(side=tk.LEFT, padx=(10, 2))
        self.equity_from_var = tk.StringVar(value="")
        from_entry = ttk.Entry(ctrl, textvariable=self.equity_from_var, width=8, font=("Courier", 9))
        from_entry.pack(side=tk.LEFT)
        from_entry.insert(0, "")
        ttk.Label(ctrl, text="To:", font=("Courier", 9)).pack(side=tk.LEFT, padx=(6, 2))
        self.equity_to_var = tk.StringVar(value="")
        to_entry = ttk.Entry(ctrl, textvariable=self.equity_to_var, width=8, font=("Courier", 9))
        to_entry.pack(side=tk.LEFT)
        ttk.Button(ctrl, text="Filter", command=self._refresh_equity_chart).pack(
            side=tk.LEFT, padx=(6, 0),
        )
        ttk.Button(ctrl, text="Clear", command=self._equity_clear_date_filter).pack(
            side=tk.LEFT, padx=(4, 0),
        )

        self.equity_info_var = tk.StringVar(value="")
        ttk.Label(ctrl, textvariable=self.equity_info_var, font=("Courier", 10)).pack(
            side=tk.RIGHT,
        )

        # Canvas
        self.equity_canvas = tk.Canvas(
            parent, bg="#1e1e1e", highlightthickness=0,
        )
        self.equity_canvas.pack(fill=tk.BOTH, expand=True)
        self.equity_canvas.bind("<Configure>", lambda e: self._refresh_equity_chart())

        # Hover tooltip state
        self._equity_points = []       # [(px, py, ts_str, pnl), ...]
        self._equity_tooltip_id = None  # canvas item id for tooltip
        self._equity_highlight_id = None
        self.equity_canvas.bind("<Motion>", self._equity_on_hover)
        self.equity_canvas.bind("<Leave>", self._equity_on_leave)

    def _equity_clear_date_filter(self):
        """Clear the date range entries and refresh the chart."""
        self.equity_from_var.set("")
        self.equity_to_var.set("")
        self._refresh_equity_chart()

    def _refresh_equity_chart(self):
        """Redraw the equity chart from trade_history.json data."""
        canvas = self.equity_canvas
        canvas.delete("all")
        self._equity_points = []
        w = canvas.winfo_width()
        h = canvas.winfo_height()
        if w < 80 or h < 80:
            return

        # ── Load & filter trade records ──
        try:
            with open(TRADE_HISTORY_FILE, "r") as fh:
                history = json.load(fh)
        except (FileNotFoundError, json.JSONDecodeError):
            history = []

        if not history:
            canvas.create_text(
                w // 2, h // 2, text="No trade data yet",
                fill="#888888", font=("Courier", 14),
            )
            return

        history.sort(key=lambda r: r.get("closed_at", ""))

        # Session filter
        if self.equity_session_only_var.get():
            session_start = None
            if hasattr(self, "bot") and self.bot:
                ss = getattr(self.bot, "_session_start", None)
                if ss:
                    session_start = ss.isoformat() if hasattr(ss, "isoformat") else str(ss)
            if session_start:
                history = [
                    r for r in history
                    if r.get("closed_at", "") >= session_start
                ]

        # Date range filter (From / To entries, format M/D or MM/DD)
        from_text = self.equity_from_var.get().strip()
        to_text = self.equity_to_var.get().strip()
        if from_text or to_text:
            import re
            now = datetime.now()
            def _parse_md(s):
                m = re.match(r"^(\d{1,2})/(\d{1,2})$", s.strip())
                if m:
                    try:
                        return datetime(now.year, int(m.group(1)), int(m.group(2))).date()
                    except ValueError:
                        pass
                return None
            start_d = _parse_md(from_text) if from_text else None
            end_d = _parse_md(to_text) if to_text else None
            if start_d or end_d:
                start_iso = datetime.combine(start_d, datetime.min.time()).isoformat() if start_d else ""
                end_iso = datetime.combine(end_d, datetime.max.time()).isoformat() if end_d else "9999"
                history = [
                    r for r in history
                    if start_iso <= r.get("closed_at", "") <= end_iso
                ]

        if not history:
            canvas.create_text(
                w // 2, h // 2, text="No trades in selected range",
                fill="#888888", font=("Courier", 14),
            )
            return

        # ── Compute cumulative P&L at each trade ──
        cum_pnl = []
        running = 0.0
        for rec in history:
            running += rec.get("pnl_usdc", 0)
            ts_str = rec.get("closed_at", "")
            cum_pnl.append((ts_str, round(running, 2)))

        # ── Group into hourly buckets for the X-axis ──
        # Keep the last P&L value for each hour bucket.
        hourly = {}
        for ts_str, pnl in cum_pnl:
            hour_key = ts_str[:13]  # "2026-02-17 14" or "2026-02-17T14"
            hourly[hour_key] = pnl
        # Also keep individual trade points for plotting
        trade_points = cum_pnl

        # ── Chart geometry ──
        margin_l = 70   # left margin for Y-axis labels
        margin_r = 20
        margin_t = 25
        margin_b = 50   # bottom margin for X-axis labels
        chart_w = w - margin_l - margin_r
        chart_h = h - margin_t - margin_b
        if chart_w < 40 or chart_h < 40:
            return

        # ── Data range ──
        pnl_values = [p for _, p in trade_points]
        pnl_min = min(min(pnl_values), 0)
        pnl_max = max(max(pnl_values), 0)
        pnl_range = pnl_max - pnl_min
        if pnl_range == 0:
            pnl_range = 10  # avoid division by zero
        # Add 10% padding
        pnl_min -= pnl_range * 0.1
        pnl_max += pnl_range * 0.1
        pnl_range = pnl_max - pnl_min

        def y_px(val):
            return margin_t + chart_h * (1 - (val - pnl_min) / pnl_range)

        def x_px(idx):
            n = len(trade_points)
            if n <= 1:
                return margin_l + chart_w // 2
            return margin_l + chart_w * idx / (n - 1)

        # ── Draw grid lines & Y-axis labels ──
        # Choose nice Y-axis tick spacing
        raw_step = pnl_range / 6
        magnitude = 10 ** int(f"{raw_step:.0e}".split("e")[1]) if raw_step > 0 else 1
        nice_step = max(magnitude, 1)
        for mult in [1, 2, 5, 10, 20, 50, 100]:
            if magnitude * mult >= raw_step:
                nice_step = magnitude * mult
                break

        tick = nice_step * (int(pnl_min / nice_step))
        while tick <= pnl_max:
            if pnl_min <= tick <= pnl_max:
                yp = y_px(tick)
                color = "#333333"
                if tick == 0:
                    color = "#555555"
                canvas.create_line(
                    margin_l, yp, w - margin_r, yp, fill=color, dash=(2, 4),
                )
                canvas.create_text(
                    margin_l - 5, yp, text=f"${tick:+,.0f}",
                    fill="#aaaaaa", font=("Courier", 8), anchor=tk.E,
                )
            tick += nice_step

        # ── Zero line (prominent) ──
        zero_y = y_px(0)
        canvas.create_line(
            margin_l, zero_y, w - margin_r, zero_y,
            fill="#666666", width=1,
        )

        # ── Draw filled area + line ──
        if len(trade_points) >= 2:
            # Build polygon points for filled area (from zero line)
            fill_above = []  # green segments (P&L > 0)
            fill_below = []  # red segments (P&L < 0)

            # Simple approach: draw the line and a filled polygon
            line_coords = []
            for i, (_, pnl) in enumerate(trade_points):
                px = x_px(i)
                py = y_px(pnl)
                line_coords.extend([px, py])

            # Filled polygon from line to zero
            poly_coords = [x_px(0), zero_y]
            for i, (_, pnl) in enumerate(trade_points):
                poly_coords.extend([x_px(i), y_px(pnl)])
            poly_coords.extend([x_px(len(trade_points) - 1), zero_y])

            # Determine dominant color
            final_pnl = trade_points[-1][1]
            fill_color = "#0a3d0a" if final_pnl >= 0 else "#3d0a0a"
            line_color = "#00cc44" if final_pnl >= 0 else "#cc4444"

            canvas.create_polygon(
                poly_coords, fill=fill_color, outline="",
            )
            canvas.create_line(
                line_coords, fill=line_color, width=2, smooth=True,
            )

            # ── Dot markers at each trade point ──
            self._equity_points = []
            for i, (ts_str, pnl) in enumerate(trade_points):
                px = x_px(i)
                py = y_px(pnl)
                dot_color = "#00ff55" if pnl >= 0 else "#ff4444"
                r = 2 if len(trade_points) > 30 else 3
                canvas.create_oval(
                    px - r, py - r, px + r, py + r,
                    fill=dot_color, outline="",
                )
                self._equity_points.append((px, py, ts_str, pnl))
        elif len(trade_points) == 1:
            self._equity_points = []
            px = x_px(0)
            py = y_px(trade_points[0][1])
            dot_color = "#00ff55" if trade_points[0][1] >= 0 else "#ff4444"
            canvas.create_oval(px - 5, py - 5, px + 5, py + 5, fill=dot_color, outline="")
            self._equity_points.append((px, py, trade_points[0][0], trade_points[0][1]))

        # ── X-axis time labels ──
        # Show a subset of labels to avoid overlap
        n = len(trade_points)
        max_labels = max(chart_w // 90, 2)
        step = max(n // max_labels, 1)
        for i in range(0, n, step):
            ts_str = trade_points[i][0]
            # Extract "HH:MM" or "MM/DD HH:MM"
            label = ts_str[11:16] if len(ts_str) >= 16 else ts_str[:10]
            # Add date prefix if data spans multiple days
            if i == 0 or (i > 0 and trade_points[i][0][:10] != trade_points[i - 1][0][:10]):
                label = ts_str[5:10] + "\n" + ts_str[11:16] if len(ts_str) >= 16 else ts_str[:10]
            px = x_px(i)
            canvas.create_text(
                px, h - margin_b + 15, text=label,
                fill="#aaaaaa", font=("Courier", 7), anchor=tk.N,
            )

        # ── Axis borders ──
        canvas.create_line(
            margin_l, margin_t, margin_l, h - margin_b, fill="#666666",
        )
        canvas.create_line(
            margin_l, h - margin_b, w - margin_r, h - margin_b, fill="#666666",
        )

        # ── Title + info label ──
        final_pnl = trade_points[-1][1] if trade_points else 0
        title_color = "#00cc44" if final_pnl >= 0 else "#cc4444"
        canvas.create_text(
            margin_l + 10, margin_t - 8,
            text=f"Cumulative P&L: ${final_pnl:+,.2f}  ({len(trade_points)} trades)",
            fill=title_color, font=("Courier", 10, "bold"), anchor=tk.W,
        )

        first_ts = trade_points[0][0][:16].replace("T", " ") if trade_points else ""
        last_ts = trade_points[-1][0][:16].replace("T", " ") if trade_points else ""
        self.equity_info_var.set(f"{first_ts}  →  {last_ts}")

    def _equity_on_hover(self, event):
        """Show tooltip when hovering near a data point on the equity chart."""
        canvas = self.equity_canvas
        # Clear previous tooltip
        self._equity_clear_tooltip()

        if not self._equity_points:
            return

        mx, my = event.x, event.y
        # Find the nearest point within a reasonable radius
        best_dist = float("inf")
        best = None
        for px, py, ts_str, pnl in self._equity_points:
            dist = ((mx - px) ** 2 + (my - py) ** 2) ** 0.5
            if dist < best_dist:
                best_dist = dist
                best = (px, py, ts_str, pnl)

        # Snap threshold — must be within 20px of a data point
        if best is None or best_dist > 20:
            return

        px, py, ts_str, pnl = best
        ts_label = ts_str[:19].replace("T", " ") if len(ts_str) >= 16 else ts_str
        pnl_label = f"${pnl:+,.2f}"

        # Highlight the point
        r = 5
        self._equity_highlight_id = canvas.create_oval(
            px - r, py - r, px + r, py + r,
            outline="#ffffff", width=2, fill="",
        )

        # Tooltip background + text
        text = f"{ts_label}\nP&L: {pnl_label}"
        # Position tooltip above and to the right; flip if near edges
        tx = px + 12
        ty = py - 12
        anchor = tk.SW
        cw = canvas.winfo_width()
        if tx + 120 > cw:
            tx = px - 12
            anchor = tk.SE
        if ty < 30:
            ty = py + 12
            anchor = tk.NW if anchor == tk.SW else tk.NE

        bg_color = "#2a2a2a"
        text_color = "#00ff88" if pnl >= 0 else "#ff6666"

        # Draw tooltip text (use a tag so we can delete it)
        self._equity_tooltip_id = canvas.create_text(
            tx, ty, text=text, fill=text_color,
            font=("Courier", 9, "bold"), anchor=anchor,
            tags=("tooltip",),
        )
        # Draw background rectangle behind text
        bbox = canvas.bbox(self._equity_tooltip_id)
        if bbox:
            pad = 4
            bg = canvas.create_rectangle(
                bbox[0] - pad, bbox[1] - pad,
                bbox[2] + pad, bbox[3] + pad,
                fill=bg_color, outline="#555555",
                tags=("tooltip_bg",),
            )
            canvas.tag_raise(self._equity_tooltip_id, bg)

    def _equity_on_leave(self, event):
        """Clear tooltip when mouse leaves the canvas."""
        self._equity_clear_tooltip()

    def _equity_clear_tooltip(self):
        canvas = self.equity_canvas
        canvas.delete("tooltip")
        canvas.delete("tooltip_bg")
        if self._equity_highlight_id:
            canvas.delete(self._equity_highlight_id)
            self._equity_highlight_id = None
        self._equity_tooltip_id = None

    def _build_log_tab(self, parent):
        self.log_area = scrolledtext.ScrolledText(
            parent, state="disabled", wrap=tk.WORD, font=("Courier", 9), height=25
        )
        self.log_area.pack(fill=tk.BOTH, expand=True)
        ttk.Button(parent, text="Clear Log", command=self._clear_log).pack(anchor=tk.E, pady=3)

    def _build_history_tab(self, parent):
        columns = (
            "closed_at", "market", "shares", "entry_price",
            "exit_price", "pnl_usdc", "slip_usdc", "outcome", "reason",
        )
        col_headings = {
            "closed_at": "Closed At",
            "market": "Market / Token",
            "shares": "Shares",
            "entry_price": "Entry",
            "exit_price": "Exit",
            "pnl_usdc": "P&L ($)",
            "slip_usdc": "Slip ($)",
            "outcome": "Result",
            "reason": "Reason",
        }
        col_widths = {
            "closed_at": 145, "market": 150, "shares": 70,
            "entry_price": 70, "exit_price": 70, "pnl_usdc": 85,
            "slip_usdc": 75, "outcome": 55, "reason": 85,
        }

        tree_frame = ttk.Frame(parent)
        tree_frame.pack(fill=tk.BOTH, expand=True)

        scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL)
        self.history_tree = ttk.Treeview(
            tree_frame, columns=columns, show="headings",
            yscrollcommand=scrollbar.set, height=20,
        )
        scrollbar.config(command=self.history_tree.yview)

        for col in columns:
            self.history_tree.heading(col, text=col_headings[col])
            anchor = tk.E if col in ("shares", "entry_price", "exit_price", "pnl_usdc", "slip_usdc") else tk.W
            self.history_tree.column(col, width=col_widths.get(col, 80), anchor=anchor)

        self.history_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # Summary labels
        self.history_summary_var = tk.StringVar(value="")
        ttk.Label(parent, textvariable=self.history_summary_var, font=("Courier", 10)).pack(
            anchor=tk.W, pady=(5, 0),
        )
        self.history_fill_var = tk.StringVar(value="")
        ttk.Label(parent, textvariable=self.history_fill_var, font=("Courier", 10)).pack(
            anchor=tk.W, pady=(1, 0),
        )

        btn_frame = ttk.Frame(parent)
        btn_frame.pack(fill=tk.X, pady=3)
        ttk.Button(btn_frame, text="Refresh", command=self._refresh_history).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="Export CSV", command=self._export_history_csv).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Clear History", command=self._clear_trade_history).pack(side=tk.LEFT, padx=5)

        self.history_session_only_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            btn_frame, text="Current Session Only",
            variable=self.history_session_only_var,
            command=self._refresh_history,
        ).pack(side=tk.LEFT, padx=10)

        # Load existing history on startup
        self._refresh_history()

    def _refresh_history(self):
        """Reload trade_history.json into the treeview."""
        for row in self.history_tree.get_children():
            self.history_tree.delete(row)

        try:
            with open(TRADE_HISTORY_FILE, "r") as fh:
                history = json.load(fh)
        except (FileNotFoundError, json.JSONDecodeError):
            history = []

        # Sort by timestamp so display is always chronological, even if
        # records were written out of order by concurrent threads.
        history.sort(key=lambda r: r.get("closed_at", ""))

        # Filter to current session if the toggle is checked
        session_filter = getattr(self, "history_session_only_var", None)
        if session_filter and session_filter.get():
            session_start = None
            if hasattr(self, "bot") and self.bot:
                ss = getattr(self.bot, "_session_start", None)
                if ss:
                    session_start = ss.isoformat() if hasattr(ss, "isoformat") else str(ss)
            if session_start:
                history = [
                    r for r in history
                    if r.get("closed_at", "") >= session_start
                ]

        total_cost = 0.0
        total_proceeds = 0.0
        total_slip_usdc = 0.0
        total_slip_shares = 0.0       # shares-weighted for avg fill calc
        total_slip_cost = 0.0         # cost of trades with slippage data
        slip_count = 0
        wins = losses = 0
        for rec in reversed(history):  # newest first
            pnl = rec.get("pnl_usdc", 0)
            total_cost += rec.get("cost_basis_usdc", 0)
            total_proceeds += rec.get("proceeds_usdc", 0)
            outcome = rec.get("outcome", "")
            if outcome in ("won", "win"):
                wins += 1
            elif outcome in ("lost", "loss"):
                losses += 1

            # Per-trade slippage (martingale records only)
            slip = rec.get("slippage") or {}
            slip_usdc = slip.get("total_usdc", 0)
            fill_price = slip.get("fill_price", 0)
            shares = rec.get("shares", 0)
            if slip and fill_price:
                total_slip_usdc += slip_usdc
                total_slip_shares += shares
                total_slip_cost += rec.get("cost_basis_usdc", 0)
                slip_count += 1
            slip_display = f"{slip_usdc:+.2f}" if slip_usdc else ""

            closed_at = rec.get("closed_at", "")
            # Shorten the ISO timestamp for display
            if "T" in closed_at:
                closed_at = closed_at.replace("T", " ")[:19]
            # For martingale records, prefer the short strategy name
            # over the long Gamma API question text.
            market_label = rec.get("market", "")
            details = rec.get("martingale_details")
            if details and details.get("strategy"):
                market_label = (
                    f"{details['strategy']} {details.get('direction', '')}"
                ).strip()

            self.history_tree.insert("", tk.END, values=(
                closed_at,
                market_label,
                f"{rec.get('shares', 0):.4f}",
                f"{rec.get('entry_price', 0):.4f}",
                f"{rec.get('exit_price', 0):.4f}",
                f"{pnl:+.4f}",
                slip_display,
                outcome.upper() if outcome else rec.get("reason", ""),
                rec.get("reason", ""),
            ))

        n = len(history)
        total_pnl = total_proceeds - total_cost
        win_rate = f"{wins/(wins+losses)*100:.0f}%" if (wins + losses) > 0 else "N/A"
        self.history_summary_var.set(
            f"Trades: {n}  |  Deployed: ${total_cost:,.2f}  |  "
            f"Returned: ${total_proceeds:,.2f}  |  "
            f"P&L: ${total_pnl:+,.2f}  |  "
            f"W/L: {wins}/{losses} ({win_rate})"
        )

        # Fill-cost analysis line (only shown when slippage data exists)
        if slip_count > 0:
            avg_fill = total_slip_cost / total_slip_shares if total_slip_shares else 0
            # total_slip_usdc: positive = net saved (bought below .50),
            #                  negative = net overpaid (bought above .50)
            if total_slip_usdc >= 0:
                net_label = f"saved ${total_slip_usdc:,.2f}"
            else:
                net_label = f"overpaid ${abs(total_slip_usdc):,.2f}"
            self.history_fill_var.set(
                f"Fill Analysis ({slip_count} trades):  "
                f"Avg Fill: ${avg_fill:.4f}  |  "
                f"Net vs $0.50 Fair: {net_label}  |  "
                f"on ${total_slip_cost:,.2f} deployed"
            )
        else:
            self.history_fill_var.set("")

    def _export_history_csv(self):
        """Export trade history to a CSV file."""
        try:
            with open(TRADE_HISTORY_FILE, "r") as fh:
                history = json.load(fh)
        except (FileNotFoundError, json.JSONDecodeError):
            history = []

        if not history:
            return

        import csv
        csv_path = os.path.join(
            os.path.dirname(os.path.abspath(TRADE_HISTORY_FILE)),
            "trade_history.csv",
        )
        fieldnames = [
            "closed_at", "token_id", "market", "shares",
            "entry_price", "exit_price", "cost_basis_usdc",
            "proceeds_usdc", "pnl_usdc", "reason",
        ]
        with open(csv_path, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(history)

        self.history_summary_var.set(
            self.history_summary_var.get() + f"  |  Exported to {csv_path}"
        )

    def _clear_trade_history(self):
        """Reset all trade history, P&L tracking, and position data.

        Prompts for confirmation, then clears trade_history.json,
        whale_comparison.json, session_trades.json, and positions.json
        so the user can start fresh on a new session.
        """
        import tkinter.messagebox as mbox
        ok = mbox.askyesno(
            "Clear Trade History",
            "This will permanently delete:\n\n"
            "  - All trade history and P&L records\n"
            "  - Strategy summary data\n"
            "  - Session trades\n"
            "  - Open position tracking\n\n"
            "Are you sure you want to reset everything?",
        )
        if not ok:
            return

        files_to_clear = [
            TRADE_HISTORY_FILE,
            STRATEGY_SUMMARY_FILE,
            SESSION_TRADES_FILE,
            POSITIONS_FILE,
        ]
        for fpath in files_to_clear:
            try:
                if os.path.exists(fpath):
                    os.remove(fpath)
            except Exception as exc:
                self.logger.warning("Could not remove %s: %s", fpath, exc)

        # Reset in-memory state if bot executor is running
        if hasattr(self, '_trade_history'):
            self._trade_history.clear()

        self._refresh_history()
        self.history_summary_var.set(
            "History cleared — all P&L and position data reset."
        )

    # ---- Field load/save ----

    def _load_fields_from_config(self):
        self.rpc_entry.insert(0, self.cfg.get("rpc_url", ""))
        self.ws_rpc_entry.insert(0, self.cfg.get("ws_rpc_url", ""))
        self.copy_pct_var.set(self.cfg.get("copy_percentage", 50))
        self.max_trade_entry.insert(0, str(self.cfg.get("max_trade_usdc", 100)))
        self.slippage_entry.insert(0, str(self.cfg.get("slippage_tolerance_bps", 50)))
        self.resume_threshold_entry.insert(0, str(self.cfg.get("resume_threshold_usdc", 5)))
        self.take_profit_entry.insert(0, str(self.cfg.get("take_profit_price", 0.99)))
        self.take_profit_pct_entry.insert(0, str(self.cfg.get("take_profit_pct", 0)))
        self.stop_loss_entry.insert(0, str(self.cfg.get("stop_loss_pct", 50)))
        self.max_loss_entry.insert(0, str(self.cfg.get("max_loss_usdc", 0)))
        self.poll_entry.insert(0, str(self.cfg.get("poll_interval_seconds", 5)))
        self.exit_check_entry.insert(0, str(self.cfg.get("exit_check_seconds", 5)))
        self.use_clob_var.set(self.cfg.get("use_clob_api", True))
        self.dry_run_var.set(self.cfg.get("dry_run", False))
        self.auto_redeem_var.set(self.cfg.get("auto_redeem_settled", True))
        # API credentials
        self.api_key_entry.insert(0, self.cfg.get("clob_api_key", ""))
        self.api_secret_entry.insert(0, self.cfg.get("clob_api_secret", ""))
        self.api_passphrase_entry.insert(0, self.cfg.get("clob_api_passphrase", ""))
        # Telegram fields
        self.tg_token_entry.insert(0, self.cfg.get("telegram_bot_token", ""))
        self.tg_chat_entry.insert(0, self.cfg.get("telegram_chat_id", ""))
        pk = load_private_key(self.cfg)
        if pk:
            self.pk_entry.insert(0, pk)
        for addr in self.cfg.get("watched_addresses", []):
            self.addr_listbox.insert(tk.END, addr)
        # Arbitrage fields
        self.arb_enabled_var.set(self.cfg.get("arb_enabled", False))
        self.arb_min_edge_entry.insert(0, str(self.cfg.get("arb_min_edge_pct", 1.0)))
        self.arb_size_entry.insert(0, str(self.cfg.get("arb_size_usdc", 10.0)))
        self.arb_max_pos_entry.insert(0, str(self.cfg.get("arb_max_positions", 5)))
        self.arb_poll_entry.insert(0, str(self.cfg.get("arb_poll_seconds", 2)))
        for cid in self.cfg.get("arb_condition_ids", []):
            self.arb_market_listbox.insert(tk.END, cid)
        # Load dynamic slugs list (new format) or migrate from legacy single slug
        slugs = self.cfg.get("arb_dynamic_slugs", [])
        if not slugs:
            legacy = self.cfg.get("arb_dynamic_slug", "").strip()
            if legacy:
                slugs = [{
                    "slug": legacy,
                    "window": int(self.cfg.get("arb_dynamic_window", 300)),
                    "format": "timestamp",
                }]
        for entry in slugs:
            if isinstance(entry, dict) and entry.get("slug"):
                self.arb_slugs_listbox.insert(
                    tk.END, self._format_slug_display(entry),
                )
        # Martingale fields
        self.mart_enabled_var.set(self.cfg.get("martingale_enabled", False))
        strategies = self.cfg.get("martingale_strategies") or []
        if not isinstance(strategies, list) or not strategies:
            # Backward compat: build one strategy from flat keys
            strategies = [{
                "name": self.cfg.get("martingale_slug_base", "btc-updown-5m"),
                "slug_base": self.cfg.get("martingale_slug_base", "btc-updown-5m"),
                "window": self.cfg.get("martingale_window", 300),
                "direction": self.cfg.get("martingale_direction", "Up"),
                "start_bet": self.cfg.get("martingale_start_bet", 5.0),
                "max_bet": self.cfg.get("martingale_max_bet", 0),
                "max_streak": self.cfg.get("martingale_max_streak", 0),
                "poll_seconds": self.cfg.get("martingale_poll_seconds", 10),
                "price_min": self.cfg.get("martingale_price_min", 0.40),
                "price_max": self.cfg.get("martingale_price_max", 0.55),
                "max_entry_seconds": self.cfg.get("martingale_max_entry_seconds", 60),
            }]
        import copy as _copy
        self._mart_strategies = [_copy.deepcopy(s) for s in strategies]
        self._mart_refresh_listbox()
        if self._mart_strategies:
            self.mart_listbox.selection_set(0)
            self._mart_selected_idx = 0
            self._mart_on_select()

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
        # Telegram
        self.cfg["telegram_bot_token"] = self.tg_token_entry.get().strip()
        self.cfg["telegram_chat_id"] = self.tg_chat_entry.get().strip()
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
            val = float(self.take_profit_entry.get().strip())
            if 0 < val <= 1:
                self.cfg["take_profit_price"] = val
        except ValueError:
            pass
        try:
            val = float(self.take_profit_pct_entry.get().strip())
            if val >= 0:
                self.cfg["take_profit_pct"] = val
        except ValueError:
            pass
        try:
            val = float(self.stop_loss_entry.get().strip())
            if 0 < val <= 100:
                self.cfg["stop_loss_pct"] = val
        except ValueError:
            pass
        try:
            val = float(self.max_loss_entry.get().strip())
            if val >= 0:
                self.cfg["max_loss_usdc"] = val
        except ValueError:
            pass
        try:
            self.cfg["poll_interval_seconds"] = int(self.poll_entry.get().strip())
        except ValueError:
            pass
        try:
            val = int(self.exit_check_entry.get().strip())
            if val >= 1:
                self.cfg["exit_check_seconds"] = val
        except ValueError:
            pass
        self.cfg["watched_addresses"] = list(self.addr_listbox.get(0, tk.END))
        # Arbitrage fields
        self.cfg["arb_enabled"] = self.arb_enabled_var.get()
        self.cfg["arb_condition_ids"] = list(self.arb_market_listbox.get(0, tk.END))
        # Read dynamic slugs from listbox
        slug_displays = list(self.arb_slugs_listbox.get(0, tk.END))
        self.cfg["arb_dynamic_slugs"] = [
            self._parse_slug_display(d) for d in slug_displays
        ]
        # Clear legacy single-slug field when using the new list
        self.cfg["arb_dynamic_slug"] = ""
        self.cfg["arb_dynamic_window"] = 300
        try:
            val = float(self.arb_min_edge_entry.get().strip())
            if val > 0:
                self.cfg["arb_min_edge_pct"] = val
        except ValueError:
            pass
        try:
            val = float(self.arb_size_entry.get().strip())
            if val > 0:
                self.cfg["arb_size_usdc"] = val
        except ValueError:
            pass
        try:
            val = int(self.arb_max_pos_entry.get().strip())
            if val >= 1:
                self.cfg["arb_max_positions"] = val
        except ValueError:
            pass
        try:
            val = int(self.arb_poll_entry.get().strip())
            if val >= 1:
                self.cfg["arb_poll_seconds"] = val
        except ValueError:
            pass
        # Martingale fields — save the strategies list
        self.cfg["martingale_enabled"] = self.mart_enabled_var.get()
        import copy as _copy
        self.cfg["martingale_strategies"] = _copy.deepcopy(self._mart_strategies)
        # Also keep flat keys in sync with the first strategy for backward compat
        if self._mart_strategies:
            s0 = self._mart_strategies[0]
            self.cfg["martingale_direction"] = s0.get("direction", "Up")
            self.cfg["martingale_start_bet"] = s0.get("start_bet", 5.0)
            self.cfg["martingale_max_bet"] = s0.get("max_bet", 0)
            self.cfg["martingale_max_streak"] = s0.get("max_streak", 0)
            self.cfg["martingale_slug_base"] = s0.get("slug_base", "btc-updown-5m")
            self.cfg["martingale_window"] = s0.get("window", 300)
            self.cfg["martingale_poll_seconds"] = s0.get("poll_seconds", 10)
            self.cfg["martingale_price_min"] = s0.get("price_min", 0.40)
            self.cfg["martingale_price_max"] = s0.get("price_max", 0.55)
            self.cfg["martingale_max_entry_seconds"] = s0.get("max_entry_seconds", 60)

    # ---- Button handlers ----

    def _reset_martingale_state(self):
        """Delete all martingale state files and reset in-memory state."""
        # Remove all martingale_state*.json files
        import glob as _glob
        for f in _glob.glob("martingale_state*.json"):
            try:
                os.remove(f)
            except OSError:
                pass
        # Reset running bots if any
        mgr = getattr(self.bot, "_martingale_mgr", None) if self.bot else None
        if mgr:
            for mb in mgr.bots:
                mb.reset_state()
        self.mart_status_var.set("Martingale: all state reset")
        self._append_log("[Martingale] All strategy states reset — next run starts fresh\n")

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

    def _test_telegram(self):
        """Send a test message to the configured Telegram chat."""
        token = self.tg_token_entry.get().strip()
        chat_id = self.tg_chat_entry.get().strip()
        if not token or not chat_id:
            messagebox.showwarning(
                "Missing Config",
                "Enter both Bot Token and Chat ID first.",
            )
            return
        if not requests:
            messagebox.showerror(
                "Missing Library",
                "The requests library is not installed.",
            )
            return
        try:
            url = TELEGRAM_API.format(token=token) + "/sendMessage"
            resp = requests.post(
                url,
                json={"chat_id": chat_id, "text": "Polymarket Bot: test message received!"},
                timeout=10,
            )
            if resp.status_code == 200:
                messagebox.showinfo("Success", "Test message sent! Check your Telegram.")
            else:
                detail = resp.json().get("description", resp.text[:200])
                messagebox.showerror("Failed", f"Telegram API error:\n{detail}")
        except Exception as exc:
            messagebox.showerror("Error", f"Could not send message:\n{exc}")

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

    def _start_bot(self):
        # Pull the latest values from the GUI fields into self.cfg.
        self._read_fields_to_config()

        # Persist settings to config.json so RPC URLs, proxy address, etc.
        # survive restarts without re-entering them.
        save_user_config(self.cfg)

        # Persist the private key to its own file (needed by the bot at
        # runtime).
        pk = self.pk_entry.get().strip()
        if pk:
            save_private_key(pk, self.cfg)

        arb_mode = self.cfg.get("arb_enabled") and (
            self.cfg.get("arb_condition_ids")
            or self.cfg.get("arb_dynamic_slug")
            or self.cfg.get("arb_dynamic_slugs")
        )
        martingale_mode = self.cfg.get("martingale_enabled")
        if (not self.cfg.get("watched_addresses")
                and not arb_mode and not martingale_mode):
            messagebox.showwarning(
                "No Mode Selected",
                "Enable at least one trading mode:\n"
                "• Add a watched address for copy trading\n"
                "• Enable arbitrage mode with market IDs or slugs\n"
                "• Enable martingale mode",
            )
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
        self._start_dashboard_refresh()

    def _stop_bot(self):
        self._stop_dashboard_refresh()
        if self.bot:
            self.bot.stop()
        self.stop_btn.configure(state=tk.DISABLED)
        self.status_var.set("Status: Stopping...")
        # Poll until the bot thread actually finishes (non-blocking so
        # the Tk event loop stays responsive).
        self._poll_bot_thread_done()

    def _poll_bot_thread_done(self):
        """Non-blocking poll: check every 250ms if the bot thread exited."""
        thread = self.bot._thread if self.bot else None
        if thread and thread.is_alive():
            self.root.after(250, self._poll_bot_thread_done)
            return
        # Thread is done — re-enable start button
        self.start_btn.configure(state=tk.NORMAL)
        self.status_var.set("Status: Stopped")

    def run(self):
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.mainloop()

    def _on_close(self):
        self._stop_dashboard_refresh()
        # Disable the GUI handler so background threads don't try to
        # write to the destroyed widget.
        if hasattr(self, "_gui_handler"):
            self._gui_handler.close()
        if self.bot and self.bot.running:
            self.bot.stop()
        # Give the bot thread a moment to finish — but don't block
        # forever (daemon threads will die with the process anyway).
        thread = self.bot._thread if self.bot else None
        if thread and thread.is_alive():
            thread.join(timeout=3)
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
    """Run the bot in headless mode using defaults + env vars + config.json.

    Priority: built-in defaults < config.json < environment variables.
    """
    logger = setup_logging()

    # Start from built-in defaults, then overlay saved config.json.
    cfg = dict(DEFAULT_CONFIG)
    cfg.update(load_user_config())

    # Allow env-var overrides for headless operation
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
    if os.environ.get("TAKE_PROFIT_PRICE"):
        cfg["take_profit_price"] = float(os.environ["TAKE_PROFIT_PRICE"])
    if os.environ.get("TAKE_PROFIT_PCT"):
        cfg["take_profit_pct"] = float(os.environ["TAKE_PROFIT_PCT"])
    if os.environ.get("STOP_LOSS_PCT"):
        cfg["stop_loss_pct"] = float(os.environ["STOP_LOSS_PCT"])
    if os.environ.get("MAX_LOSS_USDC"):
        cfg["max_loss_usdc"] = float(os.environ["MAX_LOSS_USDC"])
    if os.environ.get("EXIT_CHECK_SECONDS"):
        cfg["exit_check_seconds"] = int(os.environ["EXIT_CHECK_SECONDS"])
    if os.environ.get("AUTO_REDEEM_SETTLED"):
        cfg["auto_redeem_settled"] = os.environ["AUTO_REDEEM_SETTLED"].lower() in ("1", "true", "yes")
    if os.environ.get("PROXY_REDEEM"):
        cfg["proxy_redeem"] = os.environ["PROXY_REDEEM"].lower() in ("1", "true", "yes")
    if os.environ.get("PROXY_WITHDRAW"):
        cfg["proxy_withdraw"] = os.environ["PROXY_WITHDRAW"].lower() in ("1", "true", "yes")
    if os.environ.get("PROXY_ADDRESS"):
        cfg["proxy_address"] = os.environ["PROXY_ADDRESS"].strip()
    # Arbitrage env vars
    if os.environ.get("ARB_ENABLED"):
        cfg["arb_enabled"] = os.environ["ARB_ENABLED"].lower() in ("1", "true", "yes")
    if os.environ.get("ARB_CONDITION_IDS"):
        cfg["arb_condition_ids"] = [
            c.strip() for c in os.environ["ARB_CONDITION_IDS"].split(",") if c.strip()
        ]
    if os.environ.get("ARB_DYNAMIC_SLUGS"):
        # JSON list: [{"slug":"btc-updown-15m","window":900},...]
        try:
            cfg["arb_dynamic_slugs"] = json.loads(os.environ["ARB_DYNAMIC_SLUGS"])
        except (json.JSONDecodeError, TypeError):
            logger.warning("ARB_DYNAMIC_SLUGS env var is not valid JSON — ignoring")
    if os.environ.get("ARB_DYNAMIC_SLUG"):
        # Legacy single slug — only used if arb_dynamic_slugs is empty
        cfg["arb_dynamic_slug"] = os.environ["ARB_DYNAMIC_SLUG"].strip()
    if os.environ.get("ARB_DYNAMIC_WINDOW"):
        cfg["arb_dynamic_window"] = int(os.environ["ARB_DYNAMIC_WINDOW"])
    if os.environ.get("ARB_MIN_EDGE_PCT"):
        cfg["arb_min_edge_pct"] = float(os.environ["ARB_MIN_EDGE_PCT"])
    if os.environ.get("ARB_SIZE_USDC"):
        cfg["arb_size_usdc"] = float(os.environ["ARB_SIZE_USDC"])
    if os.environ.get("ARB_MAX_POSITIONS"):
        cfg["arb_max_positions"] = int(os.environ["ARB_MAX_POSITIONS"])
    if os.environ.get("ARB_POLL_SECONDS"):
        cfg["arb_poll_seconds"] = int(os.environ["ARB_POLL_SECONDS"])
    # Martingale env vars
    if os.environ.get("MARTINGALE_ENABLED"):
        cfg["martingale_enabled"] = os.environ["MARTINGALE_ENABLED"].lower() in ("1", "true", "yes")
    if os.environ.get("MARTINGALE_DIRECTION"):
        cfg["martingale_direction"] = os.environ["MARTINGALE_DIRECTION"].strip()
    if os.environ.get("MARTINGALE_START_BET"):
        cfg["martingale_start_bet"] = float(os.environ["MARTINGALE_START_BET"])
    if os.environ.get("MARTINGALE_MAX_BET"):
        cfg["martingale_max_bet"] = float(os.environ["MARTINGALE_MAX_BET"])
    if os.environ.get("MARTINGALE_MAX_STREAK"):
        cfg["martingale_max_streak"] = int(os.environ["MARTINGALE_MAX_STREAK"])
    if os.environ.get("MARTINGALE_SLUG_BASE"):
        cfg["martingale_slug_base"] = os.environ["MARTINGALE_SLUG_BASE"].strip()
    if os.environ.get("MARTINGALE_WINDOW"):
        cfg["martingale_window"] = int(os.environ["MARTINGALE_WINDOW"])
    if os.environ.get("MARTINGALE_POLL_SECONDS"):
        cfg["martingale_poll_seconds"] = int(os.environ["MARTINGALE_POLL_SECONDS"])
    if os.environ.get("MARTINGALE_PRICE_MIN"):
        cfg["martingale_price_min"] = float(os.environ["MARTINGALE_PRICE_MIN"])
    if os.environ.get("MARTINGALE_PRICE_MAX"):
        cfg["martingale_price_max"] = float(os.environ["MARTINGALE_PRICE_MAX"])
    if os.environ.get("MARTINGALE_MAX_ENTRY_SECONDS"):
        cfg["martingale_max_entry_seconds"] = int(os.environ["MARTINGALE_MAX_ENTRY_SECONDS"])
    # Multi-strategy JSON override: MARTINGALE_STRATEGIES='[{"name":"BTC 5m",...}]'
    if os.environ.get("MARTINGALE_STRATEGIES"):
        try:
            parsed = json.loads(os.environ["MARTINGALE_STRATEGIES"])
            if isinstance(parsed, list):
                cfg["martingale_strategies"] = parsed
                logger.info("MARTINGALE_STRATEGIES env var: loaded %d strategy(ies)", len(parsed))
            else:
                logger.warning("MARTINGALE_STRATEGIES env var is not a JSON list — ignoring")
        except (json.JSONDecodeError, TypeError):
            logger.warning("MARTINGALE_STRATEGIES env var is not valid JSON — ignoring")
    else:
        # When flat MARTINGALE_* env vars are set (defining a single strategy)
        # but MARTINGALE_STRATEGIES is NOT set, clear any multi-strategy list
        # that may have been saved to config.json by the GUI.  This ensures
        # env-var users get exactly ONE strategy from the flat keys.
        _flat_mart_envs = (
            "MARTINGALE_SLUG_BASE", "MARTINGALE_DIRECTION",
            "MARTINGALE_START_BET", "MARTINGALE_WINDOW",
        )
        if any(os.environ.get(k) for k in _flat_mart_envs):
            if cfg.get("martingale_strategies"):
                logger.info(
                    "Flat MARTINGALE_* env vars detected — clearing saved "
                    "martingale_strategies list (%d entries) to use single "
                    "strategy from env vars",
                    len(cfg["martingale_strategies"]),
                )
                cfg["martingale_strategies"] = []

    # Telegram env vars
    if os.environ.get("TELEGRAM_BOT_TOKEN"):
        cfg["telegram_bot_token"] = os.environ["TELEGRAM_BOT_TOKEN"].strip()
    if os.environ.get("TELEGRAM_CHAT_ID"):
        cfg["telegram_chat_id"] = os.environ["TELEGRAM_CHAT_ID"].strip()

    # Delete stale state file for a clean start
    if os.environ.get("MARTINGALE_RESET", "").strip().lower() in ("1", "true", "yes"):
        try:
            os.remove(MartingaleBot.STATE_FILE)
            logger.info("MARTINGALE_RESET: deleted %s for fresh start", MartingaleBot.STATE_FILE)
        except OSError:
            pass

    arb_mode = cfg.get("arb_enabled") and (
        cfg.get("arb_condition_ids")
        or cfg.get("arb_dynamic_slugs")
        or cfg.get("arb_dynamic_slug")
    )
    martingale_mode = cfg.get("martingale_enabled")
    if not cfg.get("watched_addresses") and not arb_mode and not martingale_mode:
        logger.error(
            "No watched addresses, arb markets, or martingale configured. "
            "Set WATCHED_ADDRESSES, ARB_ENABLED=1, or MARTINGALE_ENABLED=1."
        )
        sys.exit(1)

    logger.info("=== Polymarket Copy Trader — Headless Mode ===")
    logger.info("Watched addresses: %s", cfg["watched_addresses"])
    logger.info("Copy %%: %s | Max trade: %s USDC | Resume threshold: $%s | Dry run: %s | Auto-redeem: %s",
                cfg.get("copy_percentage"), cfg.get("max_trade_usdc"),
                cfg.get("resume_threshold_usdc", 5), cfg.get("dry_run", False),
                cfg.get("auto_redeem_settled", True))
    logger.info("Take-profit price: %s | Take-profit gain: %s%% | Stop-loss: %s%%",
                cfg.get("take_profit_price", 0.99), cfg.get("take_profit_pct", 0),
                cfg.get("stop_loss_pct", 50))
    logger.info("Proxy redeem: %s | Proxy withdraw: %s",
                cfg.get("proxy_redeem", True), cfg.get("proxy_withdraw", True))
    if cfg.get("arb_enabled"):
        dyn_slugs = cfg.get("arb_dynamic_slugs", [])
        dyn_legacy = cfg.get("arb_dynamic_slug", "").strip()
        if dyn_slugs:
            logger.info("Arbitrage: ENABLED | %d dynamic slug(s) | "
                        "Min edge: %s%% | Size: $%s/side",
                        len(dyn_slugs),
                        cfg.get("arb_min_edge_pct", 1.0),
                        cfg.get("arb_size_usdc", 10.0))
            for s in dyn_slugs:
                logger.info("  slug: '%s' (window %ds, format %s)",
                            s.get("slug", "?"),
                            s.get("window", 300),
                            s.get("format", "timestamp"))
        elif dyn_legacy:
            logger.info("Arbitrage: ENABLED | Dynamic slug: '%s' (window %ds) | "
                        "Min edge: %s%% | Size: $%s/side",
                        dyn_legacy, cfg.get("arb_dynamic_window", 300),
                        cfg.get("arb_min_edge_pct", 1.0),
                        cfg.get("arb_size_usdc", 10.0))
        else:
            logger.info("Arbitrage: ENABLED | Markets: %d | Min edge: %s%% | Size: $%s/side",
                        len(cfg.get("arb_condition_ids", [])),
                        cfg.get("arb_min_edge_pct", 1.0),
                        cfg.get("arb_size_usdc", 10.0))
    if cfg.get("martingale_enabled"):
        _ms = cfg.get("martingale_strategies") or []
        if _ms:
            logger.info(
                "Martingale: ENABLED | %d strategy(ies) from martingale_strategies",
                len(_ms),
            )
            for _s in _ms:
                logger.info(
                    "  [%s] slug=%s, window=%ds, direction=%s, start=$%s",
                    _s.get("name", "?"), _s.get("slug_base", "?"),
                    _s.get("window", 300), _s.get("direction", "?"),
                    _s.get("start_bet", "?"),
                )
        else:
            logger.info(
                "Martingale: ENABLED | 1 strategy (flat keys) | "
                "Direction: %s | Start bet: $%s | "
                "Max bet: $%s | Slug: %s (window %ds)",
                cfg.get("martingale_direction", "Up"),
                cfg.get("martingale_start_bet", 5.0),
                cfg.get("martingale_max_bet", 0),
                cfg.get("martingale_slug_base", "btc-updown-5m"),
                cfg.get("martingale_window", 300),
            )

    # Persist merged config so next restart picks up everything.
    save_user_config(cfg)

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
   All settings are entered through the GUI. In headless mode, use
   environment variables (see section 5).

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
     passphrase will be generated and stored in memory.

   Option B – Automatic on first start:
     If no credentials are saved but a private key is present, the bot
     will derive them automatically when started.

   Option C – Manual:
     Use the py-clob-client SDK directly:
       from py_clob_client.client import ClobClient
       client = ClobClient("https://clob.polymarket.com", 137, key="0x...")
       creds = client.create_or_derive_api_creds()
       print(creds)
     Then paste apiKey, secret, passphrase into the GUI.

   IMPORTANT: Each wallet can only have ONE active API key at a time.
   Calling create_or_derive_api_creds() is safe to repeat — it returns
   the same key deterministically without invalidating it.

4. PRIVATE KEY SETUP
   -----------------
   Enter your private key in the GUI. It will be saved to .private_key.

   The file is created with 0600 permissions (owner-only read/write).

   ⚠  SECURITY WARNINGS:
   • NEVER share your private key with anyone.
   • NEVER commit .private_key to version control.
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
