#!/usr/bin/env python3
"""
Polymarket Martingale Bot
=========================
Automated martingale betting strategy on Polymarket binary markets.

Features:
- Tkinter GUI for configuration and monitoring
- Polymarket CLOB API integration for order placement
- Multi-strategy martingale with independent parameters
- Streak-pause and recovery logic
- Persistent JSON configuration
- Comprehensive logging (file, console, GUI)
- Telegram command bot for remote control
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
    "gas_multiplier": 1.2,
    "use_clob_api": True,
    "clob_api_key": "",
    "clob_api_secret": "",
    "clob_api_passphrase": "",
    "dry_run": False,
    "resume_threshold_usdc": 5.0,
    "auto_redeem_settled": True,
    "proxy_redeem": True,
    "proxy_withdraw": True,
    "proxy_address": "",
    "webhook_url": "",
    "telegram_bot_token": "",
    "telegram_chat_id": "",
    "max_loss_usdc": 0,
    # --- Martingale mode (double-on-loss binary market betting) ---
    "martingale_enabled": False,
    "martingale_direction": "Up",      # "Up" or "Down"
    "martingale_start_bet": 5.0,       # starting bet size in USDC
    "martingale_max_bet": 0,           # max bet cap in USDC (0 = no limit)
    "martingale_max_streak": 0,        # stop after N consecutive losses (0 = no limit)
    "martingale_recovery_candles": 10,  # number of candles to evaluate for recovery
    "martingale_recovery_green": 5,     # how many of those candles must be green to resume
    "martingale_recovery_interval": 300, # candle interval in seconds for recovery sampling
    "martingale_slug_base": "btc-updown-5m",  # slug prefix for the market
    "martingale_window": 300,          # window size in seconds (300 = 5 min)
    "martingale_poll_seconds": 10,     # how often to check for resolution
    "martingale_price_min": 0.40,      # min ask price to accept (lower bound of buy range)
    "martingale_price_max": 0.55,      # max ask price to accept (upper bound of buy range)
    "martingale_max_entry_seconds": 60, # only bet in the first N seconds of a window
    "martingale_normalize_slippage": True,  # overbet when ask > fair to get expected shares
    "martingale_fair_price": 0.50,          # fair price per share for normalization
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
    "resume_threshold_usdc", "max_loss_usdc",
    "dry_run", "auto_redeem_settled", "proxy_redeem", "proxy_withdraw",
    "webhook_url",
    "telegram_bot_token", "telegram_chat_id",
    "martingale_enabled", "martingale_direction", "martingale_start_bet",
    "martingale_max_bet", "martingale_max_streak",
    "martingale_slug_base", "martingale_window", "martingale_poll_seconds",
    "martingale_price_min", "martingale_price_max", "martingale_max_entry_seconds",
    "martingale_normalize_slippage", "martingale_fair_price",
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
    logger = logging.getLogger("MartingaleBot")
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
        self.bot = bot_ref          # MartingaleEngine instance
        self.logger = logger
        self._stop_event = threading.Event()
        self._thread = None
        self._last_update_id = 0

    def send(self, message, parse_mode=None):
        """Send a message to the configured Telegram chat."""
        try:
            send_telegram(self.token, self.chat_id, message,
                          logger=self.logger, parse_mode=parse_mode)
        except Exception as exc:
            self.logger.warning("Telegram send failed: %s", exc)

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

    def stop(self):
        self._stop_event.set()

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
        """Register bot commands with Telegram for the command menu."""
        try:
            url = TELEGRAM_API.format(token=self.token) + "/setMyCommands"
            commands = [
                {"command": "balance", "description": "USDC & MATIC balances"},
                {"command": "positions", "description": "Open positions"},
                {"command": "trades", "description": "Recent trade history"},
                {"command": "stats", "description": "W/L record, P&L, ROI"},
                {"command": "strategies", "description": "Per-strategy diagnostic"},
                {"command": "status", "description": "Bot running state"},
                {"command": "missed", "description": "Missed windows"},
                {"command": "chart", "description": "Equity curve"},
                {"command": "help", "description": "Available commands"},
                {"command": "stop", "description": "Graceful shutdown"},
                {"command": "toggle_martingale", "description": "Toggle martingale on/off"},
                {"command": "set_max_bet", "description": "Set max bet"},
                {"command": "reset_martingale", "description": "Reset streak & bet"},
                {"command": "redeem", "description": "Scan & redeem settled"},
            ]
            resp = requests.post(url, json={"commands": commands}, timeout=10)
            if resp.status_code == 200:
                self.logger.debug("Telegram commands registered")
        except Exception as exc:
            self.logger.debug("Failed to register commands: %s", exc)

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
            "/toggle_martingale": self._cmd_toggle_martingale,
            "/dry_run": self._cmd_dry_run,
            # -- Parameter tuning --
            "/set_max_bet": self._cmd_set_max_bet,
            "/set_max_loss": self._cmd_set_max_loss,
            # -- Martingale management --
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
            #  1) Trade result (in-memory): has side, amount_usdc, price
            #  2) Closed/resolved position (_log_closed_trade): has market,
            #     entry_price, cost_basis_usdc, outcome
            #  3) Martingale bet: like #2 but reason="martingale"
            has_entry = "entry_price" in t or "cost_basis_usdc" in t
            if has_entry:
                # Types 2 & 3: resolved positions and martingale
                outcome = t.get("outcome", "")
                label = t.get("market", outcome or "closed")
                amount = t.get("cost_basis_usdc", 0)
                price = t.get("entry_price", 0)
                ts = t.get("closed_at", "")
            else:
                # Type 1: in-memory trade result
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

        if pos_trades:
            c = self._bucket_stats(pos_trades)
            lines += [
                "",
                "OTHER TRADES",
                f"  {c['n']} trades  |  W/L {c['wins']}/{c['losses']}"
                f" ({c['win_pct']:.0f}%)  |  P&L ${c['pnl']:+,.2f}",
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
            mgr = getattr(self.bot, "_martingale_mgr", None)
            if mgr:
                for mg in mgr.bots:
                    status_extra = ""
                    if mg._streak_paused:
                        green = sum(1 for c in mg._recovery_candles if c.get("green"))
                        total = len(mg._recovery_candles)
                        n_needed = int(mg._scfg(
                            "recovery_green", "martingale_recovery_green", 5))
                        n_candles = int(mg._scfg(
                            "recovery_candles", "martingale_recovery_candles", 10))
                        status_extra = (
                            f" [PAUSED — recovery {green}/{total} "
                            f"green, need {n_needed}/{n_candles}]"
                        )
                    elif mg._confirm_waiting:
                        fav = sum(1 for c in mg._confirm_candles if c.get("favorable"))
                        total = len(mg._confirm_candles)
                        status_extra = (
                            f" [CONFIRMING — {fav}/{total} "
                            f"favorable, need 2/3]"
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

        # Session filter: /chart session
        session_only = args and args[0].lower() == "session"
        if session_only and self.bot:
            ss = getattr(self.bot, "_session_start", None)
            if ss:
                session_start = ss.isoformat() if hasattr(ss, "isoformat") else str(ss)
                history = [r for r in history if r.get("closed_at", "") >= session_start]

        if not history:
            self.send("No trades in current session.")
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

        caption = f"Equity Chart | {timestamps[0].strftime('%m/%d %H:%M')} → {timestamps[-1].strftime('%m/%d %H:%M')} | P&L ${final_pnl:+,.2f}"
        send_telegram_photo(self.token, self.chat_id, png_bytes, caption=caption, logger=self.logger)

    def _cmd_help(self, args=None):
        """Show available commands."""
        lines = [
            "Available commands:",
            "",
            "📊 *Info*",
            "/balance — USDC & MATIC balances",
            "/positions — open positions with P&L",
            r"/trades \[N] — recent trade history",
            r"/stats \[1H|1D|ALL] — W/L, P&L, ROI",
            "/strategies — per-strategy diagnostic",
            "/status — bot running state",
            r"/missed \[24H|7D] — missed windows",
            "/chart — equity curve (image)",
            "",
            "⚙️ *Control*",
            "/stop — graceful shutdown",
            "/pause — pause trading",
            "/resume — resume trading",
            "/kill — emergency stop",
            r"/toggle\_martingale on|off",
            r"/dry\_run on|off",
            "",
            "🔧 *Tuning*",
            r"/set\_max\_bet <usdc>",
            r"/set\_max\_loss <usdc>",
            "",
            "🎲 *Martingale*",
            r"/reset\_martingale \[strategy]",
            "/redeem — scan & redeem settled",
        ]
        self.send("\n".join(lines), parse_mode="Markdown")

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

    # -------------------------------------------------------------------
    # Position management commands
    # -------------------------------------------------------------------

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
        self.base_url = CLOB_API_BASE.rstrip("/")
        self.gamma_url = GAMMA_API_BASE.rstrip("/")
        self.data_url = DATA_API_BASE.rstrip("/")
        self.session = requests.Session() if requests else None
        self.logger = logger or logging.getLogger("MartingaleBot")
        self._last_trade_ids = {}  # address -> set of seen trade IDs
        self._token_to_condition = {}  # token_id -> condition_id from activity
        self._token_to_neg_risk = {}  # token_id -> neg_risk flag from activity/API
        self._token_to_slug = {}  # token_id -> market slug from activity
        self._token_to_avg_entry = {}  # token_id -> VWAP entry price from activity

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
                    use_fok=False, max_retry_price=None,
                    target_shares=None):
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
            target_shares: If set (from slippage normalization), the FOK
                retry recalculates USDC spend as target_shares × retry_price
                so that the retry still targets the correct share count
                instead of reusing the original dollar amount at a worse price.

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
                # Snapshot balance before FOK so we can detect phantom fills
                _fok_pre_bal = 0
                try:
                    _fok_pre_bal = self._get_token_balance(
                        self.address, token_id, neg_risk=neg_risk,
                    )
                except Exception:
                    pass
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
                    # FOK failed — the market has moved away from our price.
                    # Retry ONCE with a refreshed orderbook price before
                    # giving up.  This catches cases where our smart price
                    # was slightly stale by the time the order hit the book.
                    self.logger.warning(
                        "FOK rejected (%s) — retrying with refreshed price",
                        fok_exc,
                    )

                    # ── Phantom fill guard ──
                    # Network errors (status_code=None) mean the order may
                    # have actually filled on-chain despite the exception.
                    # Check if token balance increased since the pre-FOK
                    # snapshot to avoid placing a duplicate retry order.
                    try:
                        time.sleep(1.5)  # brief pause for on-chain state
                        _fok_post_bal = self._get_token_balance(
                            self.address, token_id, neg_risk=neg_risk,
                        )
                        _fok_delta = _fok_post_bal - _fok_pre_bal
                        if _fok_delta > 0:
                            delta_shares = float(
                                Decimal(_fok_delta) / Decimal("1000000")
                            )
                            self.logger.warning(
                                "FOK phantom fill detected — balance "
                                "increased by %d (pre=%d, post=%d, "
                                "~%.1f shares). Skipping retry.",
                                _fok_delta, _fok_pre_bal, _fok_post_bal,
                                delta_shares,
                            )
                            return {
                                "takingAmount": str(delta_shares),
                                "makingAmount": str(actual_usdc),
                                "status": "matched",
                                "_phantom_fill": True,
                            }
                    except Exception as pf_exc:
                        self.logger.debug(
                            "FOK phantom fill check failed: %s", pf_exc,
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

                        # When target_shares is set (slippage normalization),
                        # recalculate USDC so the retry still targets the
                        # correct share count at the new price.  Cap at 110%
                        # of the original order to prevent runaway spend.
                        if target_shares and side.upper() == "BUY" and retry_price > 0:
                            retry_usdc = round(
                                min(target_shares * retry_price,
                                    actual_usdc * 1.10),
                                2,
                            )
                        else:
                            retry_usdc = round(actual_usdc, 2)
                        retry_tokens = round(retry_usdc / retry_price, 2) if retry_price > 0 else 0
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
                            side, retry_usdc, retry_price, price,
                            token_id[:16] + "...", resp2,
                        )
                        return resp2
                    except Exception as retry_exc:
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
        self.executor = executor  # TradeExecutor for on-chain operations
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
        self._windows_attempted = 0     # windows where we tried to bet
        self._windows_no_market = 0     # market slug not found on Gamma API
        self._missed_windows = []       # windows skipped due to price/book/balance
        self._phantom_fills = 0         # phantom fills detected this session

        # Streak-pause recovery state
        self._streak_paused = False     # True when max_streak hit, waiting for recovery
        self._streak_paused_at = None   # datetime when pause started
        self._recovery_candles = []     # list of {"open": px, "close": px, "green": bool}
        self._recovery_candle_open = None   # price at start of current candle
        self._recovery_candle_ts = 0    # epoch when current candle opened

        # Pre-bet candle confirmation for recovery (any streak > 0)
        self._confirm_candles = []      # recent candles for direction confirmation
        self._confirm_candle_open = None
        self._confirm_candle_ts = 0
        self._confirm_waiting = False   # True while waiting for candle confirmation

        # Callbacks (wired by MartingaleEngine)
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
            "confirm_candles": self._confirm_candles[-10:],
            "confirm_waiting": self._confirm_waiting,
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
            self.session_pnl = float(state.get("session_pnl", 0.0))
            self.direction = state.get("direction", self.direction)
            self._active_bet = state.get("active_bet")
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
            self._confirm_candles = state.get("confirm_candles", [])
            self._confirm_waiting = bool(state.get("confirm_waiting", False))

            # On restart while paused, prune recovery candles that are
            # outside the lookback window so the bot evaluates recent
            # market conditions.  Candles within the last N*interval
            # seconds are kept — the bot only needs to fill the gap
            # rather than re-collecting all 10 from scratch.
            if self._streak_paused and self._recovery_candles:
                interval = int(self._scfg(
                    "recovery_interval", "martingale_recovery_interval", 300))
                n_candles = int(self._scfg(
                    "recovery_candles", "martingale_recovery_candles", 10))
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
                green = sum(1 for c in self._recovery_candles if c["green"])
                self.logger.info(
                    "MARTINGALE [%s]: recovery status on restart: "
                    "%d/%d candles (%d green, need %d)",
                    self.strategy_name, len(self._recovery_candles),
                    n_candles, green,
                    int(self._scfg(
                        "recovery_green", "martingale_recovery_green", 5)),
                )

            self.logger.info(
                "Loaded martingale state: bet=$%.2f, streak=%d, pnl=$%.4f, "
                "dir=%s, last_window_ts=%d, missed=%d%s",
                self.current_bet, self.consecutive_losses,
                self.session_pnl, self.direction, self._last_window_ts,
                len(self._missed_windows),
                " [PAUSED — waiting for recovery]" if self._streak_paused else "",
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
        self._streak_paused = False
        self._streak_paused_at = None
        self._recovery_candles = []
        self._recovery_candle_open = None
        self._recovery_candle_ts = 0
        self._confirm_candles = []
        self._confirm_candle_open = None
        self._confirm_candle_ts = 0
        self._confirm_waiting = False
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
            if self.executor:
                try:
                    raw_amount = int(self.current_bet * 2 * 1_000_000)  # approve 2x for headroom
                    self.executor.ensure_usdc_approval(CTF_EXCHANGE_ADDRESS, raw_amount)
                    if neg_risk:
                        self.executor.ensure_usdc_approval(
                            NEG_RISK_CTF_EXCHANGE_ADDRESS, raw_amount,
                        )
                except Exception as exc:
                    self.logger.debug("MARTINGALE: prefetch approval failed (%s)", exc)

            # Pre-check balance so we know early if we're short.
            balance_ok = True
            try:
                usdc_bal = float(self.clob_client.get_usdc_balance(max_age_seconds=0))
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

    # -- pre-bet candle confirmation (recovery) --------------------------------

    def _check_confirm_candles(self):
        """Sample price and build candles to confirm direction before a
        recovery bet.  Returns True when 2 out of 3 candles are favorable
        (green for Up, red for Down).  Uses the bet window interval.

        Called each cycle while ``_confirm_waiting`` is True.
        """
        window = int(self._scfg("window", "martingale_window", 300))
        n_needed = 2
        n_total = 3

        # Get current price from the orderbook
        slug = self._generate_slug()
        market = self._fetch_market(slug)
        if not market:
            return False

        direction = self._scfg("direction", "martingale_direction", self.direction)
        token_id = (
            market["up_token"] if direction == "Up"
            else market["down_token"]
        )
        price, _ = self._get_best_ask(token_id)
        if not price or price <= 0:
            return False

        now = int(time.time())

        # Start a new candle or close the current one
        if self._confirm_candle_ts == 0 or (now - self._confirm_candle_ts) >= window:
            # Close the previous candle if one was open
            if self._confirm_candle_open is not None and self._confirm_candle_ts > 0:
                is_green = price >= self._confirm_candle_open
                # For Up bets: favorable = green candle
                # For Down bets: favorable = red candle
                favorable = is_green if direction == "Up" else not is_green
                candle = {
                    "open": self._confirm_candle_open,
                    "close": price,
                    "green": is_green,
                    "favorable": favorable,
                    "ts": self._confirm_candle_ts,
                }
                self._confirm_candles.append(candle)
                self._confirm_candles = self._confirm_candles[-n_total:]
                self._save_state()

                color = "GREEN" if is_green else "RED"
                fav_count = sum(1 for c in self._confirm_candles if c.get("favorable"))
                total = len(self._confirm_candles)
                self.logger.info(
                    "MARTINGALE [%s] confirm candle: %s (%.4f → %.4f) "
                    "— %d/%d favorable (%d needed from %d)",
                    self.strategy_name, color, candle["open"], candle["close"],
                    fav_count, total, n_needed, n_total,
                )

                # Check if confirmation is met
                if total >= n_total and fav_count >= n_needed:
                    self.logger.info(
                        "MARTINGALE [%s]: candle confirmation met — "
                        "%d/%d favorable, proceeding with recovery bet $%.2f",
                        self.strategy_name, fav_count, total,
                        self.current_bet,
                    )
                    self._confirm_waiting = False
                    self._confirm_candles = []
                    self._confirm_candle_open = None
                    self._confirm_candle_ts = 0
                    self._save_state()
                    return True

            # Open a new candle
            self._confirm_candle_open = price
            self._confirm_candle_ts = now

        return False

    # -- streak recovery (candle sampling) ------------------------------------

    def _check_streak_recovery(self):
        """Sample price to build candles and check if market has recovered.

        Called each cycle while ``_streak_paused`` is True.  Returns True
        when the recovery condition is met (N out of M candles are green),
        meaning the bot should resume trading.

        A "candle" is simply the price at the start and end of each
        *recovery_interval* period.  Green = close >= open.
        """
        interval = int(self._scfg(
            "recovery_interval", "martingale_recovery_interval", 300))
        n_candles = int(self._scfg(
            "recovery_candles", "martingale_recovery_candles", 10))
        n_green = int(self._scfg(
            "recovery_green", "martingale_recovery_green", 5))

        # Get a token_id to sample — use the current window's market
        slug = self._generate_slug()
        market = self._fetch_market(slug)
        if not market:
            return False  # can't sample yet

        direction = self._scfg("direction", "martingale_direction", self.direction)
        token_id = (
            market["up_token"] if direction == "Up"
            else market["down_token"]
        )
        price, _ = self._get_best_ask(token_id)
        if not price or price <= 0:
            return False  # no asks — can't sample

        now = int(time.time())

        # Start a new candle if none open or interval elapsed
        if self._recovery_candle_ts == 0 or (now - self._recovery_candle_ts) >= interval:
            # Close the previous candle if one was open
            if self._recovery_candle_open is not None and self._recovery_candle_ts > 0:
                candle = {
                    "open": self._recovery_candle_open,
                    "close": price,
                    "green": price >= self._recovery_candle_open,
                    "ts": self._recovery_candle_ts,
                }
                self._recovery_candles.append(candle)
                # Keep only the last N candles
                self._recovery_candles = self._recovery_candles[-n_candles:]
                self._save_state()

                green_count = sum(1 for c in self._recovery_candles if c["green"])
                total = len(self._recovery_candles)
                color = "GREEN" if candle["green"] else "RED"
                self.logger.info(
                    "MARTINGALE [%s] recovery candle: %s (%.4f → %.4f) "
                    "— %d/%d green (%d needed from %d candles)",
                    self.strategy_name, color, candle["open"], candle["close"],
                    green_count, total, n_green, n_candles,
                )

                # Check recovery condition
                if total >= n_candles and green_count >= n_green:
                    return True

            # Open a new candle
            self._recovery_candle_open = price
            self._recovery_candle_ts = now

        return False

    def _resume_from_streak_pause(self):
        """Resume trading after streak recovery is confirmed."""
        paused_dur = ""
        if self._streak_paused_at:
            delta = datetime.now() - self._streak_paused_at
            mins = int(delta.total_seconds() // 60)
            paused_dur = f" (paused for {mins}m)"

        green_count = sum(1 for c in self._recovery_candles if c["green"])
        total = len(self._recovery_candles)

        self._streak_paused = False
        self._streak_paused_at = None
        self._recovery_candles = []
        self._recovery_candle_open = None
        self._recovery_candle_ts = 0
        self.consecutive_losses = 0
        self._save_state()

        msg = (
            f"MARTINGALE [{self.strategy_name}] RESUMED: recovery confirmed "
            f"({green_count}/{total} green candles){paused_dur} "
            f"— next bet=${self.current_bet:.2f}"
        )
        self.logger.info(msg)
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
        # Safety: max streak — pause and wait for bullish recovery
        max_streak = int(self._scfg("max_streak", "martingale_max_streak", 0))
        if max_streak > 0 and self.consecutive_losses >= max_streak and not self._streak_paused:
            self._streak_paused = True
            self._streak_paused_at = datetime.now()
            self._recovery_candles = []
            self._recovery_candle_open = None
            self._recovery_candle_ts = 0
            self._save_state()
            n_candles = int(self._scfg(
                "recovery_candles", "martingale_recovery_candles", 10))
            n_green = int(self._scfg(
                "recovery_green", "martingale_recovery_green", 5))
            msg = (
                f"MARTINGALE [{self.strategy_name}] PAUSED: max streak of "
                f"{max_streak} losses reached — waiting for {n_green}/{n_candles} "
                f"green candles before resuming"
            )
            self.logger.warning(msg)
            if self.notify_callback:
                try:
                    self.notify_callback(msg)
                except Exception:
                    pass
            return False

        if self._streak_paused:
            return False  # recovery check happens in _cycle()

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

        # Normalize bet for slippage — overbet when ask > fair so we
        # still receive the expected number of shares.  Never reduces
        # the bet when ask < fair (caller keeps the bonus shares).
        order_bet, expected_shares = self._normalize_bet_for_slippage(
            self.current_bet, ask_price,
        )

        # Check aggregate depth across all ask levels up to price_max.
        # FOK/market orders sweep multiple levels, so single-level
        # size is not the right measure.
        target_shares = order_bet / ask_price
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
                "MARTINGALE DRY RUN: would buy %s @ $%.4f, $%.2f (order $%.2f)",
                direction, ask_price, self.current_bet, order_bet,
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
        if prefetch_balance_ok is None:
            # No prefetch data — check now
            try:
                usdc_bal = float(self.clob_client.get_usdc_balance(max_age_seconds=0))
                if usdc_bal < order_bet:
                    self.logger.warning(
                        "MARTINGALE [%s]: USDC balance $%.2f < bet $%.2f — skipping",
                        self.strategy_name, usdc_bal, order_bet,
                    )
                    self._skip_reason = f"low balance (${usdc_bal:.2f} < ${order_bet:.2f})"
                    return False
            except Exception as exc:
                self.logger.debug("MARTINGALE: balance check failed (%s) — proceeding", exc)

        # Ensure USDC approval — skip if prefetch already handled it.
        neg_risk = market.get("neg_risk", False)
        if not prefetch_approval_done and self.executor:
            try:
                raw_amount = int(order_bet * 1_000_000)
                self.executor.ensure_usdc_approval(CTF_EXCHANGE_ADDRESS, raw_amount)
                if neg_risk:
                    self.executor.ensure_usdc_approval(
                        NEG_RISK_CTF_EXCHANGE_ADDRESS, raw_amount,
                    )
            except Exception as exc:
                self.logger.warning("MARTINGALE: approval failed (%s) — proceeding", exc)

        # Snapshot token balance before the order so phantom-fill
        # detection can use the delta (excludes pre-existing shares).
        _pre_order_balance = self._check_phantom_fill(
            token_id, neg_risk=neg_risk,
        )

        # Only propagate target share count to the FOK retry when slippage
        # normalization actually adjusted the bet upward.  When normalization
        # is inactive (ask <= fair), the retry should keep the same USDC
        # amount — NOT chase an inflated share count derived from a low ask.
        norm_target = expected_shares if order_bet > self.current_bet else None

        result = self.clob_client.place_order(
            token_id=token_id,
            side="BUY",
            size_usdc=order_bet,
            price=ask_price,
            use_fok=True,
            max_retry_price=price_max,
            neg_risk=neg_risk,
            target_shares=norm_target,
        )

        if isinstance(result, dict) and (
            result.get("status") == "fok_rejected" or result.get("error")
        ):
            # FOK was rejected — but the order may have actually filled
            # on-chain with the response lost.  Check quickly.
            phantom = self._detect_phantom_fill(
                token_id, _pre_order_balance, ask_price,
                target_shares, neg_risk=neg_risk,
            )
            if phantom:
                result = phantom
            else:
                # Check if this was a network error (order may have filled
                # silently).  If so, lock out the window to prevent
                # duplicate bets — accept a missed window over a triple fill.
                reason = result.get("reason", "") if isinstance(result, dict) else ""
                is_network_error = (
                    "Request exception" in reason
                    or "status_code=None" in reason
                    or "ConnectionError" in reason
                    or "Timeout" in reason
                )
                if is_network_error:
                    self.logger.warning(
                        "MARTINGALE: FOK network error for %s @ $%.4f — "
                        "locking window to prevent duplicate bet",
                        direction, ask_price,
                    )
                    self._last_window_ts = current_window_ts
                else:
                    self.logger.warning(
                        "MARTINGALE: FOK rejected for %s @ $%.4f — will retry",
                        direction, ask_price,
                    )
                self._skip_reason = f"FOK rejected @ ${ask_price:.4f}"
                self._skip_price = ask_price
                return False

        if not result:
            # Order returned None — network error likely.  Check for
            # phantom fill before giving up on this window.
            phantom = self._detect_phantom_fill(
                token_id, _pre_order_balance, ask_price,
                target_shares, neg_risk=neg_risk,
            )
            if phantom:
                result = phantom
            else:
                # Network error → lock window to prevent duplicate bets
                self.logger.warning(
                    "MARTINGALE: order failed (network) for %s — "
                    "locking window to prevent duplicate",
                    direction,
                )
                self._last_window_ts = current_window_ts
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
            "order_bet": order_bet,
            "expected_shares": round(expected_shares, 2),
            "fill_price": fill_price,
            "shares": actual_shares,
            "cost": actual_cost,
            "slippage": slippage,
            "slippage_usdc": slippage_usdc,
            "question": market["question"],
            "window_end": self._get_window_end(),
            "ts": datetime.now().isoformat(),
        }
        self._last_window_ts = current_window_ts
        self._skip_reason = None  # bet placed successfully
        self._skip_price = None
        self._skip_gap = None
        self._windows_attempted += 1
        self._save_state()

        slip_tag = ""
        if abs(slippage) >= 0.0001:
            slip_tag = f" | slip {slippage:+.4f} (${slippage_usdc:+.4f})"
        self.logger.info(
            "MARTINGALE BET [%s]: %s $%.2f @ $%.4f "
            "(%.1f shares) — streak: %d%s",
            self.strategy_name, direction, actual_cost, fill_price,
            actual_shares, self.consecutive_losses, slip_tag,
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

        slip = bet.get("slippage", 0)
        slip_info = ""
        if abs(slip) >= 0.0001:
            slip_info = f" | slip {slip:+.4f} (${bet.get('slippage_usdc', 0):+.4f})"

        self.logger.info(
            "MARTINGALE WIN: %s +$%.4f (shares=%.1f, cost=$%.2f) — "
            "resetting to $%.2f | session P&L: $%.4f%s",
            bet["direction"], profit, bet["shares"], bet["cost"],
            self.start_bet, self.session_pnl, slip_info,
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
        self._active_bet = None
        # Clear candle confirmation — no confirmation needed at streak 0
        self._confirm_candles = []
        self._confirm_candle_open = None
        self._confirm_candle_ts = 0
        self._confirm_waiting = False
        self._save_state()

    def _handle_loss(self, bet):
        loss = bet["cost"]
        self.session_pnl -= loss
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

        max_bet = float(self._scfg("max_bet", "martingale_max_bet", 0))
        if max_bet > 0 and self.current_bet > max_bet:
            self.current_bet = max_bet
            self.logger.warning(
                "MARTINGALE [%s]: bet capped at max $%.2f", self.strategy_name, max_bet,
            )

        slip = bet.get("slippage", 0)
        slip_info = ""
        if abs(slip) >= 0.0001:
            slip_info = f" | slip {slip:+.4f} (${bet.get('slippage_usdc', 0):+.4f})"

        self.logger.info(
            "MARTINGALE LOSS: %s -$%.2f (fill $%.4f) — next bet $%.2f "
            "(start $%.2f × 2^%d), streak: %d | session P&L: $%.4f%s",
            bet["direction"], loss, bet.get("fill_price", 0),
            self.current_bet, self.start_bet, self.consecutive_losses,
            self.consecutive_losses, self.session_pnl, slip_info,
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

        self._active_bet = None
        # Start candle confirmation for the recovery bet
        self._confirm_candles = []
        self._confirm_candle_open = None
        self._confirm_candle_ts = 0
        self._confirm_waiting = True
        self.logger.info(
            "MARTINGALE [%s]: waiting for 2/3 candle confirmation "
            "before recovery bet (streak %d, next $%.2f)",
            self.strategy_name, self.consecutive_losses, self.current_bet,
        )
        self._save_state()

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

    def _check_phantom_fill(self, token_id, neg_risk=False):
        """Return the raw on-chain token balance for *token_id*.

        Used before and after placing an order to detect phantom fills
        (orders that matched on-chain but whose HTTP response was lost).
        """
        try:
            addr = self.clob_client.address
            return self.clob_client._get_token_balance(addr, token_id, neg_risk=neg_risk)
        except Exception:
            return 0

    def _detect_phantom_fill(self, token_id, pre_balance, ask_price,
                             target_shares, neg_risk=False):
        """Fast phantom fill detection after a network error or ambiguous failure.

        Checks on-chain balance with quick retries (1s, 1s, 2s) and falls
        back to the CLOB trades API.  Returns a synthetic result dict if a
        fill is detected, or ``None`` if no fill found.
        """
        delays = [1, 1, 2]
        raw_delta = 0
        raw_balance = pre_balance
        for i, wait in enumerate(delays):
            time.sleep(wait)
            raw_balance = self._check_phantom_fill(token_id, neg_risk=neg_risk)
            raw_delta = raw_balance - pre_balance
            if raw_delta > 0:
                self.logger.info(
                    "MARTINGALE: phantom fill found on check %d/%d "
                    "(after %ds total wait)",
                    i + 1, len(delays), sum(delays[:i + 1]),
                )
                break

        # Secondary: check CLOB trades API if on-chain balance hasn't moved
        from_trades = False
        if raw_delta <= 0:
            try:
                addr = self.clob_client.address
                recent = self.clob_client.get_trades_for_address(addr, limit=5)
                now = time.time()
                for rt in (recent or []):
                    rt_asset = str(rt.get("asset_id") or rt.get("token_id") or "")
                    if rt_asset != token_id:
                        continue
                    rt_ts = rt.get("match_time") or rt.get("timestamp") or ""
                    try:
                        if isinstance(rt_ts, (int, float)):
                            rt_epoch = float(rt_ts)
                        else:
                            from datetime import timezone
                            rt_epoch = datetime.fromisoformat(
                                str(rt_ts).replace("Z", "+00:00")
                            ).timestamp()
                    except Exception:
                        rt_epoch = 0
                    if now - rt_epoch < 30:
                        rt_size = float(rt.get("size") or 0)
                        rt_price = float(rt.get("price") or ask_price)
                        if rt_size > 0:
                            from_trades = True
                            target_shares = rt_size
                            ask_price = rt_price
                            self.logger.warning(
                                "MARTINGALE: PHANTOM FILL via CLOB trades API — "
                                "%.1f shares @ $%.4f",
                                rt_size, rt_price,
                            )
                            break
            except Exception as exc:
                self.logger.debug("MARTINGALE: CLOB trades check failed: %s", exc)

        if raw_delta > 0 or from_trades:
            self._phantom_fills += 1
            if from_trades:
                actual_shares = target_shares
                actual_cost = round(actual_shares * ask_price, 6)
            else:
                actual_shares = float(
                    Decimal(raw_delta) / Decimal("1000000")
                )
                actual_cost = round(actual_shares * ask_price, 6)
            self.logger.warning(
                "MARTINGALE: PHANTOM FILL DETECTED — %.1f shares "
                "(pre=%d, post=%d). Treating as successful fill. "
                "[phantom_fills=%d this session]",
                actual_shares, pre_balance, raw_balance,
                self._phantom_fills,
            )
            return {
                "takingAmount": str(actual_shares),
                "makingAmount": str(actual_cost),
                "status": "matched",
                "_phantom_fill": True,
            }
        return None

    def _normalize_bet_for_slippage(self, base_bet, ask_price):
        """Adjust bet size upward when ask exceeds fair price so the WIN
        PROFIT stays constant regardless of slippage.

        Profit = shares × (1 - ask_price).  To preserve profit = base_bet:
            shares = base_bet / (1 - ask_price)
            cost   = shares × ask_price

        Never adjusts *downward* — if the ask is at or below fair, the
        original bet is returned unchanged (the caller keeps the bonus
        profit from favorable pricing).

        Returns ``(adjusted_bet, expected_shares)`` where *expected_shares*
        is the share count needed to preserve the target profit.
        """
        normalize = self._scfg(
            "normalize_slippage", "martingale_normalize_slippage", True,
        )
        fair = float(self._scfg(
            "fair_price", "martingale_fair_price", 0.50,
        ))
        expected_shares = base_bet / fair
        if not normalize or ask_price <= fair:
            return base_bet, expected_shares
        # Profit-preserving formula: shares = base_bet / (1 - ask_price)
        # so that shares × (1 - ask_price) = base_bet regardless of ask.
        expected_shares = base_bet / (1 - ask_price)
        adjusted = round(expected_shares * ask_price, 2)
        if adjusted > base_bet:
            self.logger.info(
                "MARTINGALE [%s]: slippage normalization — ask $%.4f > fair "
                "$%.4f, overbetting $%.2f -> $%.2f to target %.1f shares "
                "(profit-preserving: $%.2f win profit)",
                self.strategy_name, ask_price, fair,
                base_bet, adjusted, expected_shares, base_bet,
            )
        return adjusted, expected_shares

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
            # While waiting for candle confirmation before recovery bet
            if self._confirm_waiting:
                self._check_confirm_candles()
                return False  # keep fast-polling to sample prices
            placed = self._try_place_bet()
            return placed  # fast poll until placed, then slow poll

    def run(self):
        self.logger.info(
            "Martingale bot [%s] started — direction=%s, bet=$%.2f, streak=%d",
            self.strategy_name, self.direction, self.current_bet,
            self.consecutive_losses,
        )

        poll_slow = max(float(self._scfg("poll_seconds", "martingale_poll_seconds", 10)), 1)
        poll_fast = 1.0  # aggressive polling when looking for next bet

        while not self._stop_event.is_set():
            try:
                in_position = self._cycle()
            except Exception as exc:
                self.logger.error("Martingale error: %s", exc, exc_info=True)
                in_position = False
            self._stop_event.wait(
                timeout=poll_slow if in_position else poll_fast,
            )

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
        self.cfg = cfg
        self.logger = logger or logging.getLogger("martingale")
        self.executor = executor
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

    # -- Callbacks (wired once by MartingaleEngine) ----------------------------

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
    """Handles trade execution via the Polymarket CLOB API and on-chain.

    Manages balance checking, position tracking, redemption of settled
    positions, and USDC approval management.
    """

    def __init__(self, w3, private_key, cfg, clob_client=None, logger=None):
        self.w3 = w3
        self.private_key = private_key
        self.account = w3.eth.account.from_key(private_key)
        self.address = self.account.address
        self.cfg = cfg
        self.clob_client = clob_client  # PolymarketCLOBClient for order placement
        self.logger = logger or logging.getLogger("MartingaleBot")
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

        # Optional webhook callback (set by MartingaleEngine after init)
        self.notify_callback = None

        # Kill switch: stop the bot when session losses exceed threshold
        self.kill_switch_triggered = False
        self._session_pnl = 0.0

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

# ---------------------------------------------------------------------------
# Core bot engine (runs in a background thread)
# ---------------------------------------------------------------------------

class MartingaleEngine:
    """Orchestrates the monitoring loop and trade execution."""

    def __init__(self, cfg, logger):
        self.cfg = cfg
        self.logger = logger
        self.running = False
        self._stop_event = threading.Event()
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

        Used by MartingaleBot to log trades into the
        same history file that the TradeExecutor uses, so
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


    def start(self):
        if self.running:
            self.logger.warning("Bot is already running")
            return
        self.running = True
        self._session_start = datetime.now()
        self._trade_history = []

        # --- Display active configuration at startup ---
        self.logger.info("=" * 60)
        self.logger.info("MARTINGALE BOT CONFIGURATION")
        self.logger.info("=" * 60)
        self.logger.info("  Martingale enabled:     %s",
                         self.cfg.get("martingale_enabled", False))
        self.logger.info("  Resume threshold:       $%s",
                         self.cfg.get("resume_threshold_usdc", 5))
        self.logger.info("  Dry run:                %s",
                         self.cfg.get("dry_run", False))
        self.logger.info("  Auto redeem settled:    %s",
                         self.cfg.get("auto_redeem_settled", True))
        self.logger.info("=" * 60)

        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self.logger.info("Bot started")
        self._notify("Martingale bot started")

    def stop(self):
        self.running = False
        self._stop_event.set()
        # Stop Martingale bots if running
        if self._martingale_mgr:
            self._martingale_mgr.stop()
        # Stop Telegram command bot if running
        if self._telegram_bot:
            self._telegram_bot.stop()
        self.logger.info("Bot stop requested")
        self._notify("Bot stopped")
        # Report is generated at the end of _run_loop after the while
        # loop exits, so positions and balance are still accessible.

    def _run_loop(self):
        """Main monitoring loop."""
        # Initialise connections – CLOB first so executor can use it
        self._init_clob()

        web3_ok = self._init_web3()
        if web3_ok:
            self._init_executor()

        # ---- Start Martingale Bot(s) ----
        if self.cfg.get("martingale_enabled") and self.clob_client:
            self._martingale_mgr = MartingaleManager(
                self.clob_client, self.cfg, self.logger,
                executor=self.executor,
            )
            self._martingale_mgr.set_notify_callback(self._notify)
            self._martingale_mgr.set_log_trade_callback(self._append_trade_history)
            self._martingale_mgr.start()

        # ---- Start Telegram command bot ----
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

            # ---- Startup redemption ----
            self.logger.info("Startup: redeeming settled positions...")

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

            # Report balance after redemptions
            if self.executor:
                try:
                    balance = self.executor.get_usdc_balance(max_age_seconds=0)
                    self.logger.info(
                        "Startup redemption complete — USDC balance: $%.2f",
                        balance,
                    )
                except Exception:
                    pass

        poll_interval = self.cfg.get("poll_interval_seconds", 10)

        resume_threshold = Decimal(
            str(self.cfg.get("resume_threshold_usdc", 5))
        )
        _last_pause_log = 0
        _now = time.time()
        _last_redeem_check = _now
        _last_proxy_check = _now
        _last_portfolio_scan = _now

        # Runtime-togglable config keys — re-read from config.json
        _RUNTIME_TOGGLE_KEYS = {"martingale_enabled"}
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
                        pass

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

                # --- Auto-redeem settled positions back to USDC ---
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

                # --- Full portfolio scan (periodic) ---
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

                # If we just redeemed while paused, immediately re-check balance
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
                        pass

            except Exception as exc:
                self.logger.error("Error in monitoring loop: %s", exc, exc_info=True)
                self._notify("ERROR in monitoring loop: %s" % exc)

            # Kill switch: stop if cumulative losses exceeded threshold
            if self.executor and self.executor.kill_switch_triggered:
                self.logger.critical("Kill switch activated — stopping bot")
                self.running = False
                break

            # Wait for next cycle — use event so stop() interrupts immediately
            self._stop_event.wait(timeout=float(poll_interval))

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

        mart_trades = [r for r in closed_trades if r.get("reason") == "martingale"]
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
# Tooltip Helper
# ---------------------------------------------------------------------------

class ToolTip:
    """Hover tooltip for any tkinter widget."""

    _DELAY_MS = 400  # ms before tooltip appears
    _WRAP_PX = 320   # text wrap width in pixels

    def __init__(self, widget, text):
        self.widget = widget
        self.text = text
        self._tip_window = None
        self._after_id = None
        widget.bind("<Enter>", self._schedule, add="+")
        widget.bind("<Leave>", self._cancel, add="+")
        widget.bind("<ButtonPress>", self._cancel, add="+")

    def _schedule(self, _event=None):
        self._cancel()
        self._after_id = self.widget.after(self._DELAY_MS, self._show)

    def _cancel(self, _event=None):
        if self._after_id:
            self.widget.after_cancel(self._after_id)
            self._after_id = None
        self._hide()

    def _show(self):
        if self._tip_window:
            return
        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 4
        tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(
            tw, text=self.text, justify=tk.LEFT,
            background="#ffffe0", foreground="#333",
            relief=tk.SOLID, borderwidth=1,
            wraplength=self._WRAP_PX,
            font=("TkDefaultFont", 9),
            padx=6, pady=4,
        )
        label.pack()
        self._tip_window = tw

    def _hide(self):
        if self._tip_window:
            self._tip_window.destroy()
            self._tip_window = None


# ---------------------------------------------------------------------------
# Tkinter GUI Application
# ---------------------------------------------------------------------------

class MartingaleGUI:
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
        self.root.title(f"Polymarket Martingale Bot  v{VERSION}")
        self.root.geometry("900x720")
        self.root.minsize(700, 550)

        self._build_ui()

        # Set up logging with GUI handler
        self._gui_handler = TextHandler(self.log_area)
        self.logger = setup_logging(self._gui_handler)
        self.logger.info("Polymarket Martingale Bot v%s started", VERSION)
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

        # Tab 3: Martingale
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

        # Tab 9: Help / User Guide
        help_frame = ttk.Frame(notebook, padding=10)
        notebook.add(help_frame, text="Help")
        self._build_help_tab(help_frame)

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
        # Helper to add a small gray description below a field
        def _help(text, row, col=0, colspan=3):
            ttk.Label(
                parent, text=text, foreground="#666",
                wraplength=580, font=("TkDefaultFont", 8),
            ).grid(row=row, column=col, columnspan=colspan, sticky=tk.W, padx=(20, 0), pady=(0, 4))

        # RPC URL
        row = 0
        _lbl = ttk.Label(parent, text="HTTP RPC URL:")
        _lbl.grid(row=row, column=0, sticky=tk.W, pady=3)
        self.rpc_entry = ttk.Entry(parent, width=70)
        self.rpc_entry.grid(row=row, column=1, columnspan=2, sticky=tk.EW, pady=3)
        _tip = ("Polygon network HTTP RPC endpoint. You can get a free one from "
                "Alchemy, Infura, or QuickNode. Example: https://polygon-rpc.com")
        ToolTip(_lbl, _tip)
        ToolTip(self.rpc_entry, _tip)
        row += 1
        _help("Polygon HTTP endpoint from Alchemy, Infura, or QuickNode (e.g. https://polygon-rpc.com)", row)

        row += 1
        _lbl = ttk.Label(parent, text="WebSocket RPC URL:")
        _lbl.grid(row=row, column=0, sticky=tk.W, pady=3)
        self.ws_rpc_entry = ttk.Entry(parent, width=70)
        self.ws_rpc_entry.grid(row=row, column=1, columnspan=2, sticky=tk.EW, pady=3)
        _tip = ("Polygon WebSocket RPC endpoint for real-time event streaming. "
                "Usually starts with wss://. Optional \u2014 the bot falls back to HTTP polling.")
        ToolTip(_lbl, _tip)
        ToolTip(self.ws_rpc_entry, _tip)
        row += 1
        _help("Optional. WebSocket endpoint (wss://...) for faster updates. Falls back to HTTP if blank.", row)

        # Private key
        row += 1
        _lbl = ttk.Label(parent, text="Private Key:")
        _lbl.grid(row=row, column=0, sticky=tk.W, pady=3)
        self.pk_entry = ttk.Entry(parent, width=70, show="*")
        self.pk_entry.grid(row=row, column=1, sticky=tk.EW, pady=3)
        _pk_btn = ttk.Button(parent, text="Show/Hide", command=self._toggle_pk)
        _pk_btn.grid(row=row, column=2, padx=5)
        _tip = ("Your Polygon wallet private key (hex string starting with 0x). "
                "This is used to sign transactions. Never share it with anyone.")
        ToolTip(_lbl, _tip)
        ToolTip(self.pk_entry, _tip)
        ToolTip(_pk_btn, "Toggle visibility of the private key field.")
        row += 1
        _help("Your Polygon wallet private key (starts with 0x). Export from MetaMask: Account Details \u2192 Export Private Key.", row)

        row += 1
        ttk.Label(
            parent,
            text="\u26a0 WARNING: Your private key controls your funds. "
                 "Never share it. It is stored locally in a restricted file.",
            foreground="red",
            wraplength=600,
        ).grid(row=row, column=0, columnspan=3, sticky=tk.W, pady=3)

        # Resume threshold
        row += 1
        _lbl = ttk.Label(parent, text="Resume Threshold (USDC):")
        _lbl.grid(row=row, column=0, sticky=tk.W, pady=3)
        resume_frame = ttk.Frame(parent)
        resume_frame.grid(row=row, column=1, sticky=tk.W, pady=3)
        self.resume_threshold_entry = ttk.Entry(resume_frame, width=10)
        self.resume_threshold_entry.pack(side=tk.LEFT)
        _tip = ("Minimum USDC wallet balance required before the bot will resume "
                "betting after an automatic pause. Prevents betting with too little capital.")
        ToolTip(_lbl, _tip)
        ToolTip(self.resume_threshold_entry, _tip)
        row += 1
        _help("Minimum USDC balance needed to resume betting after a pause. Prevents trading with insufficient funds.", row)

        # Kill switch — max loss
        row += 1
        _lbl = ttk.Label(parent, text="Max Loss Kill Switch (USDC):")
        _lbl.grid(row=row, column=0, sticky=tk.W, pady=3)
        kill_frame = ttk.Frame(parent)
        kill_frame.grid(row=row, column=1, sticky=tk.W, pady=3)
        self.max_loss_entry = ttk.Entry(kill_frame, width=10)
        self.max_loss_entry.pack(side=tk.LEFT)
        _tip = ("Emergency stop: the bot shuts down entirely after cumulative losses "
                "reach this USDC amount. Set to 0 to disable this safety limit.")
        ToolTip(_lbl, _tip)
        ToolTip(self.max_loss_entry, _tip)
        row += 1
        _help("Emergency stop \u2014 bot shuts down after losing this much total. Set 0 to disable.", row)

        # Use CLOB API checkbox
        row += 1
        self.use_clob_var = tk.BooleanVar(value=True)
        _cb = ttk.Checkbutton(
            parent, text="Use Polymarket CLOB API (recommended)", variable=self.use_clob_var
        )
        _cb.grid(row=row, column=0, columnspan=2, sticky=tk.W, pady=3)
        ToolTip(_cb, "Use the Polymarket Central Limit Order Book API for placing trades. "
                "This is faster and more reliable than on-chain transactions. Recommended.")
        row += 1
        _help("Routes orders through Polymarket's order book for faster, cheaper execution. Leave checked.", row)

        # Dry-run mode checkbox
        row += 1
        self.dry_run_var = tk.BooleanVar(value=False)
        _cb = ttk.Checkbutton(
            parent, text="Dry Run Mode (detect trades but do NOT execute)",
            variable=self.dry_run_var
        )
        _cb.grid(row=row, column=0, columnspan=2, sticky=tk.W, pady=3)
        ToolTip(_cb, "Simulate everything without spending real money. The bot will log "
                "what it would do but will not place any actual orders. Great for testing.")
        row += 1
        _help("Test mode \u2014 the bot logs what it would do but does NOT spend real money. Great for first-time setup.", row)

        # Auto-redeem settled positions checkbox
        row += 1
        self.auto_redeem_var = tk.BooleanVar(value=True)
        _cb = ttk.Checkbutton(
            parent,
            text="Auto-redeem settled positions (convert winning tokens back to USDC)",
            variable=self.auto_redeem_var,
        )
        _cb.grid(row=row, column=0, columnspan=2, sticky=tk.W, pady=3)
        ToolTip(_cb, "Automatically convert winning outcome tokens back to USDC after "
                "a market settles. Keeps your balance liquid for the next bet.")
        row += 1
        _help("Automatically cashes out winning tokens to USDC after a market settles. Keeps your balance ready.", row)

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
        _help("Most users can leave these blank \u2014 click 'Derive' or start the bot and they are generated automatically.", row)

        row += 1
        _lbl = ttk.Label(parent, text="API Key:")
        _lbl.grid(row=row, column=0, sticky=tk.W, pady=2)
        self.api_key_entry = ttk.Entry(parent, width=70)
        self.api_key_entry.grid(row=row, column=1, columnspan=2, sticky=tk.EW, pady=2)
        _tip = "Your Polymarket CLOB API key. Leave blank to auto-derive from your private key."
        ToolTip(_lbl, _tip)
        ToolTip(self.api_key_entry, _tip)

        row += 1
        _lbl = ttk.Label(parent, text="API Secret:")
        _lbl.grid(row=row, column=0, sticky=tk.W, pady=2)
        self.api_secret_entry = ttk.Entry(parent, width=70, show="*")
        self.api_secret_entry.grid(row=row, column=1, columnspan=2, sticky=tk.EW, pady=2)
        _tip = "Your CLOB API secret. Leave blank to auto-derive from your private key."
        ToolTip(_lbl, _tip)
        ToolTip(self.api_secret_entry, _tip)

        row += 1
        _lbl = ttk.Label(parent, text="API Passphrase:")
        _lbl.grid(row=row, column=0, sticky=tk.W, pady=2)
        self.api_passphrase_entry = ttk.Entry(parent, width=70, show="*")
        self.api_passphrase_entry.grid(row=row, column=1, columnspan=2, sticky=tk.EW, pady=2)
        _tip = "Your CLOB API passphrase. Leave blank to auto-derive from your private key."
        ToolTip(_lbl, _tip)
        ToolTip(self.api_passphrase_entry, _tip)

        row += 1
        self.derive_btn = ttk.Button(
            parent, text="Derive Credentials from Private Key",
            command=self._derive_api_creds,
        )
        self.derive_btn.grid(row=row, column=1, sticky=tk.W, pady=3)
        ToolTip(self.derive_btn, "Generate API Key, Secret, and Passphrase automatically "
                "from your private key. This is the easiest way to set up credentials.")
        row += 1
        _help("Click to auto-generate all three fields from your private key. Or leave blank \u2014 they are created on first Start.", row)

        # --- Telegram Notifications ---
        row += 1
        ttk.Separator(parent, orient=tk.HORIZONTAL).grid(
            row=row, column=0, columnspan=3, sticky=tk.EW, pady=8
        )

        row += 1
        ttk.Label(
            parent, text="Telegram Notifications (optional \u2014 get mobile alerts):",
            font=("TkDefaultFont", 9, "bold"),
        ).grid(row=row, column=0, columnspan=3, sticky=tk.W, pady=3)
        row += 1
        _help("Optional. Receive push notifications on your phone for every bet, win, loss, and status change.", row)

        row += 1
        _lbl = ttk.Label(parent, text="Bot Token:")
        _lbl.grid(row=row, column=0, sticky=tk.W, pady=2)
        self.tg_token_entry = ttk.Entry(parent, width=70, show="*")
        self.tg_token_entry.grid(row=row, column=1, columnspan=2, sticky=tk.EW, pady=2)
        _tip = ("Telegram bot token from @BotFather. Enables mobile notifications "
                "for wins, losses, and bot status updates.")
        ToolTip(_lbl, _tip)
        ToolTip(self.tg_token_entry, _tip)
        row += 1
        _help("Get this from @BotFather on Telegram: send /newbot and follow the prompts.", row)

        row += 1
        _lbl = ttk.Label(parent, text="Chat ID:")
        _lbl.grid(row=row, column=0, sticky=tk.W, pady=2)
        tg_chat_frame = ttk.Frame(parent)
        tg_chat_frame.grid(row=row, column=1, columnspan=2, sticky=tk.EW, pady=2)
        self.tg_chat_entry = ttk.Entry(tg_chat_frame, width=20)
        self.tg_chat_entry.pack(side=tk.LEFT)
        _test_btn = ttk.Button(
            tg_chat_frame, text="Test",
            command=self._test_telegram,
        )
        _test_btn.pack(side=tk.LEFT, padx=10)
        _tip = ("Your personal Telegram Chat ID. Send /start to your bot, "
                "then message @userinfobot to find this number.")
        ToolTip(_lbl, _tip)
        ToolTip(self.tg_chat_entry, _tip)
        ToolTip(_test_btn, "Send a test message to verify your Telegram setup works.")
        row += 1
        _help("Send /start to your bot, then message @userinfobot to get your numeric Chat ID. Click Test to verify.", row)

        parent.columnconfigure(1, weight=1)

    def _build_martingale_tab(self, parent):
        # Enable checkbox
        self.mart_enabled_var = tk.BooleanVar(value=False)
        _cb = ttk.Checkbutton(
            parent, text="Enable Martingale Mode",
            variable=self.mart_enabled_var,
        )
        _cb.pack(anchor=tk.W, pady=(0, 5))
        ToolTip(_cb, "Turn Martingale betting on or off globally. "
                "When disabled, no martingale bets will be placed.")

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

        # Build the fields in a grid — (label, key, width, tooltip, inline_help)
        fields = [
            ("Name:", "mart_e_name", 20,
             "A friendly label for this strategy (e.g. 'BTC 5m Up'). "
             "Shown in the strategy list and logs.",
             "Label shown in list & logs"),
            ("Slug Base:", "mart_e_slug", 25,
             "The Polymarket market slug prefix (e.g. 'btc-updown-5m'). "
             "The bot appends the current window timestamp to form the full market slug.",
             "e.g. btc-updown-5m"),
            ("Window (seconds):", "mart_e_window", 10,
             "Duration of each betting window in seconds. For a 5-minute market use 300. "
             "The bot aligns bets to these windows automatically.",
             "300 = 5 min, 60 = 1 min"),
            ("Direction (Up/Down):", "mart_e_direction", 10,
             "Which side to bet on \u2014 'Up' or 'Down'. "
             "The bot always buys this outcome each window.",
             "Up or Down"),
            ("Starting Bet (USDC):", "mart_e_start_bet", 10,
             "Your initial wager in USDC. After a loss the bet doubles; "
             "after a win it resets back to this amount.",
             "First bet; doubles on loss"),
            ("Max Bet (0=no limit):", "mart_e_max_bet", 10,
             "Cap on the maximum single bet in USDC. If doubling would exceed "
             "this amount, the bet is clamped here. Set 0 for no limit.",
             "Safety cap on bet size"),
            ("Max Streak (0=no limit):", "mart_e_max_streak", 10,
             "Maximum number of consecutive losses before the strategy pauses. "
             "Useful as a safety valve. Set 0 for unlimited.",
             "Pause after N losses in a row"),
            ("Poll Interval (seconds):", "mart_e_poll", 10,
             "How often (in seconds) the bot checks market prices and places bets. "
             "Lower = more responsive but more API calls.",
             "10 recommended"),
            ("Buy Price Min:", "mart_e_price_min", 10,
             "Only buy when the token price is at or above this value (0.00\u20131.00). "
             "Filters out unfavorable odds.",
             "0.00\u20131.00; skip cheap odds"),
            ("Buy Price Max:", "mart_e_price_max", 10,
             "Only buy when the token price is at or below this value (0.00\u20131.00). "
             "Prevents buying at extremely high prices.",
             "0.00\u20131.00; skip expensive odds"),
            ("Max Entry (sec into window):", "mart_e_max_entry", 10,
             "Latest point (in seconds after window start) at which a bet can be placed. "
             "Prevents entering too late when the outcome is nearly decided.",
             "Don\u2019t bet after this many sec"),
            ("Recovery Candles:", "mart_e_recovery_candles", 10,
             "Number of candles to evaluate when deciding whether to resume "
             "after a max-streak pause. More candles = more confirmation.",
             "Candles to sample after pause"),
            ("Recovery Green:", "mart_e_recovery_green", 10,
             "How many of the recovery candles must be green (close >= open) "
             "before the bot resumes betting.",
             "Green candles needed to resume"),
            ("Recovery Interval (sec):", "mart_e_recovery_interval", 10,
             "Duration of each recovery candle in seconds. "
             "Controls how long the bot waits between price samples.",
             "Seconds per recovery candle"),
        ]
        self._mart_entries = {}
        for row, (label, attr, width, tip, inline) in enumerate(fields):
            lbl = ttk.Label(edit_frame, text=label)
            lbl.grid(row=row, column=0, sticky=tk.W, padx=2, pady=2)
            entry = ttk.Entry(edit_frame, width=width)
            entry.grid(row=row, column=1, sticky=tk.W, padx=2, pady=2)
            ttk.Label(edit_frame, text=inline, foreground="#666",
                      font=("TkDefaultFont", 8)).grid(
                row=row, column=2, sticky=tk.W, padx=(6, 2), pady=2)
            self._mart_entries[attr] = entry
            ToolTip(lbl, tip)
            ToolTip(entry, tip)

        _apply_btn = ttk.Button(
            edit_frame, text="Apply to Selected",
            command=self._mart_apply_edit,
        )
        _apply_btn.grid(row=len(fields), column=0, columnspan=2, pady=(10, 0))
        ToolTip(_apply_btn, "Save the current field values to the selected strategy in the list.")

        # --- Bottom: status + reset ---
        status_frame = ttk.Frame(parent)
        status_frame.pack(fill=tk.X, pady=(10, 0))

        self.mart_status_var = tk.StringVar(value="Martingale: idle")
        _status_lbl = ttk.Label(
            status_frame, textvariable=self.mart_status_var,
            font=("Courier", 10, "bold"),
        )
        _status_lbl.pack(side=tk.LEFT)
        ToolTip(_status_lbl, "Current martingale state: shows streak count, "
                "current bet size, and last outcome.")

        _reset_btn = ttk.Button(
            status_frame, text="Reset All State",
            command=self._reset_martingale_state,
        )
        _reset_btn.pack(side=tk.RIGHT, padx=5)
        ToolTip(_reset_btn, "Reset all strategies back to their starting bet "
                "and clear the loss streak counter. Does not delete strategies.")

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
            "mart_e_recovery_candles": ("recovery_candles", 10),
            "mart_e_recovery_green": ("recovery_green", 5),
            "mart_e_recovery_interval": ("recovery_interval", 300),
        }
        for attr, (key, default) in mapping.items():
            entry = self._mart_entries[attr]
            entry.delete(0, tk.END)
            entry.insert(0, str(s.get(key, default)))

    def _mart_apply_edit(self):
        """Write the edit fields back into the selected strategy dict.

        If the bot is currently running, automatically restarts it so the
        new configuration takes effect immediately.
        """
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
        self._mart_refresh_listbox()
        # Re-select the same index
        if idx < self.mart_listbox.size():
            self.mart_listbox.selection_set(idx)

        # Auto-restart: if the bot is running, stop and restart with new config
        if self.bot and self.bot.running:
            self.logger.info("Config changed — restarting bot with new settings")
            self._stop_bot()
            self._pending_restart = True
            self._poll_restart()

    def _poll_restart(self):
        """Poll until the bot thread exits, then restart."""
        thread = self.bot._thread if self.bot else None
        if thread and thread.is_alive():
            self.root.after(250, self._poll_restart)
            return
        if getattr(self, "_pending_restart", False):
            self._pending_restart = False
            self._start_bot()

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
            "poll_seconds": 10,
            "price_min": 0.40,
            "price_max": 0.55,
            "max_entry_seconds": 60,
            "recovery_candles": 10,
            "recovery_green": 5,
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

        if not history:
            canvas.create_text(
                w // 2, h // 2, text="No trades in current session",
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

    def _build_help_tab(self, parent):
        """Built-in user guide displayed as a scrollable text widget."""
        help_text = scrolledtext.ScrolledText(
            parent, wrap=tk.WORD, font=("TkDefaultFont", 10), padx=10, pady=10,
        )
        help_text.pack(fill=tk.BOTH, expand=True)

        # --- Insert formatted guide content ---
        help_text.tag_configure("h1", font=("TkDefaultFont", 16, "bold"), spacing3=8)
        help_text.tag_configure("h2", font=("TkDefaultFont", 13, "bold"), spacing1=14, spacing3=4)
        help_text.tag_configure("h3", font=("TkDefaultFont", 11, "bold"), spacing1=10, spacing3=2)
        help_text.tag_configure("body", font=("TkDefaultFont", 10), spacing1=2, lmargin1=10, lmargin2=10)
        help_text.tag_configure("bullet", font=("TkDefaultFont", 10), lmargin1=24, lmargin2=36, spacing1=1)
        help_text.tag_configure("field", font=("TkDefaultFont", 10, "bold"))
        help_text.tag_configure("code", font=("Courier", 10), background="#f0f0f0")
        help_text.tag_configure("warn", foreground="red", font=("TkDefaultFont", 10, "bold"))

        def h1(t):
            help_text.insert(tk.END, t + "\n", "h1")

        def h2(t):
            help_text.insert(tk.END, t + "\n", "h2")

        def h3(t):
            help_text.insert(tk.END, t + "\n", "h3")

        def p(t):
            help_text.insert(tk.END, t + "\n\n", "body")

        def bullet(t):
            help_text.insert(tk.END, "\u2022 " + t + "\n", "bullet")

        def field_desc(name, desc):
            help_text.insert(tk.END, name, "field")
            help_text.insert(tk.END, " \u2014 " + desc + "\n", "bullet")

        h1("Polymarket Martingale Bot \u2014 User Guide")
        p(f"Version {VERSION}")

        # ----- Quick Start -----
        h2("Quick Start")
        bullet("Install Python 3.8+ from python.org (check 'Add to PATH' on Windows)")
        bullet("Clone or extract the bot folder to your Desktop")
        bullet("Double-click install_and_run.bat (Windows) or run ./install_and_run.sh (Mac/Linux)")
        bullet("Go to the Configuration tab and enter your Private Key and HTTP RPC URL")
        bullet("Go to the Martingale tab, click Add, configure a strategy, enable Martingale Mode")
        bullet("Click Start Bot at the bottom of the window")
        help_text.insert(tk.END, "\n")

        # ----- What Is Martingale? -----
        h2("What Is Martingale Betting?")
        p("The martingale strategy doubles your bet after every loss. When you win, "
          "you recover all previous losses plus a profit equal to your starting bet.")
        p("Example with a $5 starting bet:\n"
          "  Round 1: Bet $5  \u2192 Loss  \u2192 Running P/L: -$5\n"
          "  Round 2: Bet $10 \u2192 Loss  \u2192 Running P/L: -$15\n"
          "  Round 3: Bet $20 \u2192 Win   \u2192 Running P/L: +$5")
        p("After the win, the bet resets to $5 and the cycle starts again. "
          "Use Max Bet and Max Streak limits to protect against long losing streaks.")

        # ----- Dashboard Tab -----
        h2("Dashboard Tab")
        p("Your live overview of account status and open positions.")
        field_desc("USDC Balance", "Current USDC balance on Polygon \u2014 your betting capital")
        field_desc("MATIC Balance", "MATIC (POL) for gas fees \u2014 keep at least 1\u20132 MATIC")
        field_desc("Open Positions", "Number of markets where you hold tokens")
        field_desc("Floating P/L", "Unrealized profit/loss across all open positions")
        field_desc("Session P/L", "Profit/loss since you last started the bot")
        field_desc("Lifetime P/L", "Total accumulated profit/loss across all sessions")
        field_desc("W/L", "Win/loss record (e.g. 12/5 = 12 wins, 5 losses)")
        help_text.insert(tk.END, "\n")
        p("The positions table shows Direction, Market, Shares, Avg Price, Current Price, "
          "Cost Basis, Current Value, Floating P/L, P/L %, and Time Held for each position.")

        # ----- Configuration Tab -----
        h2("Configuration Tab")

        h3("Connection")
        field_desc("HTTP RPC URL",
                   "Polygon HTTP endpoint \u2014 get a free one from Alchemy, Infura, or QuickNode")
        field_desc("WebSocket RPC URL",
                   "Optional WebSocket endpoint (wss://...) for faster updates")
        help_text.insert(tk.END, "\n")

        h3("Wallet")
        field_desc("Private Key",
                   "Your Polygon wallet private key (starts with 0x). "
                   "Export from MetaMask: Account Details \u2192 Export Private Key")
        help_text.insert(tk.END, "\n")

        h3("Safety Limits")
        field_desc("Resume Threshold (USDC)",
                   "Minimum balance to resume betting after a pause")
        field_desc("Max Loss Kill Switch (USDC)",
                   "Bot shuts down after losing this much total. Set 0 to disable")
        help_text.insert(tk.END, "\n")

        h3("Mode Switches")
        field_desc("Use Polymarket CLOB API",
                   "Routes orders through Polymarket\u2019s order book. Faster & cheaper. Leave checked")
        field_desc("Dry Run Mode",
                   "Simulates everything without spending real money. Great for first-time testing")
        field_desc("Auto-redeem settled positions",
                   "Cashes out winning tokens to USDC automatically when markets settle")
        help_text.insert(tk.END, "\n")

        h3("CLOB API Credentials")
        p("Leave blank \u2014 they are auto-generated from your private key on first start. "
          "Or click 'Derive Credentials from Private Key' to generate them manually.")
        field_desc("API Key / Secret / Passphrase",
                   "Auto-derived from your private key. Only fill manually if you have custom credentials")
        help_text.insert(tk.END, "\n")

        h3("Telegram Notifications (Optional)")
        field_desc("Bot Token",
                   "From @BotFather on Telegram \u2014 send /newbot to create a bot")
        field_desc("Chat ID",
                   "Your numeric ID \u2014 message @userinfobot on Telegram to find it")
        field_desc("Test button",
                   "Sends a test message to verify your setup works")
        help_text.insert(tk.END, "\n")

        # ----- Martingale Tab -----
        h2("Martingale Tab")
        p("Configure one or more independent martingale betting strategies.")
        field_desc("Enable Martingale Mode",
                   "Master switch \u2014 must be checked for any bets to be placed")
        help_text.insert(tk.END, "\n")

        h3("Strategy List (Left Panel)")
        p("Shows all your strategies. Use Add / Remove / Duplicate to manage them.")

        h3("Strategy Settings (Right Panel)")
        field_desc("Name", "Friendly label (e.g. 'BTC 5m Up')")
        field_desc("Slug Base", "Market slug prefix (e.g. btc-updown-5m)")
        field_desc("Window (seconds)", "Betting window duration \u2014 300 for 5-minute markets")
        field_desc("Direction", "Up or Down \u2014 which outcome to buy each window")
        field_desc("Starting Bet (USDC)", "Initial bet amount; doubles after each loss")
        field_desc("Max Bet", "Maximum single bet cap in USDC (0 = no limit)")
        field_desc("Max Streak", "Pause strategy after N consecutive losses (0 = unlimited)")
        field_desc("Poll Interval", "Seconds between price checks (10 recommended)")
        field_desc("Buy Price Min", "Only buy above this price (0.00\u20131.00)")
        field_desc("Buy Price Max", "Only buy below this price (0.00\u20131.00)")
        field_desc("Max Entry (sec)", "Don\u2019t enter a bet after this many seconds into the window")
        help_text.insert(tk.END, "\n")
        field_desc("Apply to Selected", "Saves your edits to the selected strategy")
        field_desc("Reset All State", "Resets streaks and bet sizes to starting values (doesn\u2019t delete strategies)")
        help_text.insert(tk.END, "\n")

        # ----- Trade History Tab -----
        h2("Trade History Tab")
        p("Review all completed trades with columns: Closed At, Market, Shares, Entry, "
          "Exit, P&L ($), Slippage ($), Result (WON/LOST), and Reason.")
        field_desc("Export CSV", "Save full trade history to a spreadsheet file")
        field_desc("Clear History", "Delete all saved trade history (cannot be undone)")
        field_desc("Current Session Only", "Filter to show only this session\u2019s trades")
        help_text.insert(tk.END, "\n")

        # ----- Equity Tab -----
        h2("Equity Tab")
        p("Visual chart of your cumulative profit/loss over time. Green = profit, red = loss. "
          "Hover over data points to see exact values. The zero line marks break-even.")
        help_text.insert(tk.END, "\n")

        # ----- Log Tab -----
        h2("Log Tab")
        p("Real-time scrolling log of all bot activity: market checks, bets, wins, losses, "
          "errors, and status changes. Click Clear Log to erase the display.")

        # ----- FAQ -----
        h2("FAQ & Troubleshooting")

        h3("\"Insufficient USDC balance\"")
        p("Make sure your wallet has USDC on the Polygon network (not Ethereum mainnet).")

        h3("\"API key derivation failed\"")
        p("Check that your private key is correct and starts with 0x. "
          "Click 'Derive Credentials from Private Key' to regenerate.")

        h3("Bot is running but not placing bets")
        bullet("Is Martingale Mode enabled?")
        bullet("Do you have at least one strategy configured?")
        bullet("Is the current price within your Buy Price Min/Max range?")
        bullet("Is Dry Run Mode turned off?")
        help_text.insert(tk.END, "\n")

        h3("Running the bot 24/7")
        p("Deploy to a cloud VPS and run: python polymarket_martingale.py --headless")

        h3("Internet drops")
        p("The bot retries connections automatically. Open positions settle on-chain "
          "regardless of whether the bot is running.")

        h3("Multiple strategies")
        p("Yes \u2014 each strategy runs independently with its own bet size, streak counter, "
          "and market slug. Add as many as you want.")

        # Make read-only
        help_text.configure(state="disabled")

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
        self.resume_threshold_entry.insert(0, str(self.cfg.get("resume_threshold_usdc", 5)))
        self.max_loss_entry.insert(0, str(self.cfg.get("max_loss_usdc", 0)))
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
            self.cfg["resume_threshold_usdc"] = float(self.resume_threshold_entry.get().strip())
        except ValueError:
            pass
        try:
            val = float(self.max_loss_entry.get().strip())
            if val >= 0:
                self.cfg["max_loss_usdc"] = val
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

        if not self.cfg.get("martingale_enabled"):
            messagebox.showwarning(
                "Martingale Not Enabled",
                "Enable martingale mode in the Martingale tab before starting.",
            )
            return
        if not self.cfg.get("rpc_url") and not self.cfg.get("ws_rpc_url"):
            if not self.cfg.get("use_clob_api"):
                messagebox.showwarning(
                    "No RPC", "Provide an RPC URL or enable the CLOB API."
                )
                return

        self.bot = MartingaleEngine(self.cfg, self.logger)
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
    if os.environ.get("RESUME_THRESHOLD_USDC"):
        cfg["resume_threshold_usdc"] = float(os.environ["RESUME_THRESHOLD_USDC"])
    if os.environ.get("DRY_RUN"):
        cfg["dry_run"] = os.environ["DRY_RUN"].lower() in ("1", "true", "yes")
    if os.environ.get("MAX_LOSS_USDC"):
        cfg["max_loss_usdc"] = float(os.environ["MAX_LOSS_USDC"])
    if os.environ.get("AUTO_REDEEM_SETTLED"):
        cfg["auto_redeem_settled"] = os.environ["AUTO_REDEEM_SETTLED"].lower() in ("1", "true", "yes")
    if os.environ.get("PROXY_REDEEM"):
        cfg["proxy_redeem"] = os.environ["PROXY_REDEEM"].lower() in ("1", "true", "yes")
    if os.environ.get("PROXY_WITHDRAW"):
        cfg["proxy_withdraw"] = os.environ["PROXY_WITHDRAW"].lower() in ("1", "true", "yes")
    if os.environ.get("PROXY_ADDRESS"):
        cfg["proxy_address"] = os.environ["PROXY_ADDRESS"].strip()
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

    if not cfg.get("martingale_enabled"):
        logger.error(
            "Martingale not enabled. Set MARTINGALE_ENABLED=1."
        )
        sys.exit(1)

    logger.info("=== Polymarket Martingale Bot — Headless Mode ===")
    logger.info("Dry run: %s | Auto-redeem: %s | Resume threshold: $%s",
                cfg.get("dry_run", False),
                cfg.get("auto_redeem_settled", True),
                cfg.get("resume_threshold_usdc", 5))
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

    bot = MartingaleEngine(cfg, logger)

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
        app = MartingaleGUI()
        app.run()


if __name__ == "__main__":
    main()


# ---------------------------------------------------------------------------
# USAGE INSTRUCTIONS
# ---------------------------------------------------------------------------
"""
=============================================================================
POLYMARKET MARTINGALE BOT — USAGE INSTRUCTIONS
=============================================================================

1. REQUIRED INSTALLATIONS
   ----------------------
   pip install web3 requests py-clob-client

   Python 3.8+ is required. Tkinter is included with standard Python
   installations on most platforms.

2. CONFIGURATION
   -------------
   All settings are entered through the GUI. In headless mode, use
   environment variables.

   Recommended Polygon RPC providers:
   - Alchemy:  https://alchemy.com  (free tier available)
   - Infura:   https://infura.io
   - QuickNode: https://quicknode.com

3. RUNNING THE BOT
   ----------------
   python polymarket_martingale.py

   The Tkinter GUI will open. From there you can:
   - Configure RPC endpoints and your private key
   - Derive or enter CLOB API credentials
   - Configure martingale strategies
   - Start/Stop the bot
   - View real-time logs in the Log tab

4. HEADLESS MODE
   --------------
   python polymarket_martingale.py --headless

   Key environment variables:
   - RPC_URL, WS_RPC_URL, PRIVATE_KEY
   - MARTINGALE_ENABLED=1
   - MARTINGALE_DIRECTION, MARTINGALE_START_BET
   - MARTINGALE_SLUG_BASE, MARTINGALE_WINDOW
   - MARTINGALE_STRATEGIES (JSON list for multi-strategy)

5. DISCLAIMER
   ----------
   This software is provided for EDUCATIONAL PURPOSES ONLY. Trading
   on prediction markets involves substantial risk of loss. Use at
   your own risk.
=============================================================================
"""
