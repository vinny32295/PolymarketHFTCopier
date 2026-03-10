"""
Unit tests for the Polymarket Martingale Bot.

Tests core logic using mocks — no real blockchain or API calls needed.
Run with: python -m pytest tests/ -v
"""

import json
import logging
import os
import tempfile
import time
import unittest
from decimal import Decimal
from unittest.mock import MagicMock, patch, PropertyMock

# Ensure imports work even without web3/requests/tkinter installed
import sys

# Mock tkinter if not available (e.g., headless CI environment)
if "tkinter" not in sys.modules:
    tk_mock = MagicMock()
    sys.modules["tkinter"] = tk_mock
    sys.modules["tkinter.ttk"] = MagicMock()
    sys.modules["tkinter.scrolledtext"] = MagicMock()
    sys.modules["tkinter.messagebox"] = MagicMock()

sys.modules.setdefault("requests", MagicMock())

# We need to mock web3 and py_clob_client before importing the bot module
mock_web3_module = MagicMock()
mock_web3_module.Web3 = MagicMock()
mock_web3_module.Web3.to_checksum_address = lambda x: x
mock_web3_module.Web3.HTTPProvider = MagicMock()
mock_web3_module.Web3.WebSocketProvider = MagicMock()
sys.modules.setdefault("web3", mock_web3_module)
sys.modules.setdefault("web3.middleware", MagicMock())

mock_clob = MagicMock()
sys.modules.setdefault("py_clob_client", mock_clob)
sys.modules.setdefault("py_clob_client.client", mock_clob)
sys.modules.setdefault("py_clob_client.clob_types", mock_clob)

import polymarket_martingale as bot


class TestConfigHelpers(unittest.TestCase):
    """Test private key management."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.pk_path = os.path.join(self.tmpdir, "test_pk")

    def tearDown(self):
        if os.path.exists(self.pk_path):
            os.remove(self.pk_path)
        os.rmdir(self.tmpdir)

    def test_save_and_load_private_key(self):
        cfg = {"private_key_file": self.pk_path}
        bot.save_private_key("0xdeadbeef1234", cfg)
        loaded = bot.load_private_key(cfg)
        self.assertEqual(loaded, "0xdeadbeef1234")

    def test_load_missing_private_key(self):
        cfg = {"private_key_file": "/nonexistent/pk"}
        self.assertEqual(bot.load_private_key(cfg), "")


class TestCLOBClient(unittest.TestCase):
    """Test PolymarketCLOBClient trade tracking logic."""

    def _make_client(self):
        cfg = dict(bot.DEFAULT_CONFIG)
        logger = logging.getLogger("test")
        logger.handlers = []
        logger.addHandler(logging.NullHandler())
        client = bot.PolymarketCLOBClient.__new__(bot.PolymarketCLOBClient)
        client.cfg = cfg
        client.base_url = bot.CLOB_API_BASE
        client.gamma_url = bot.GAMMA_API_BASE
        client.session = MagicMock()
        client.logger = logger
        client._last_trade_ids = {}
        client._token_to_condition = {}
        client._token_to_slug = {}
        client.clob_sdk = None
        return client

    def test_seed_seen_trades(self):
        client = self._make_client()
        client.get_trades_for_address = MagicMock(return_value=[
            {"id": "t1", "side": "BUY", "size": "100"},
            {"id": "t2", "side": "SELL", "size": "200"},
        ])
        client.seed_seen_trades("0xabc")
        self.assertEqual(client._last_trade_ids["0xabc"], {"t1", "t2"})

    def test_get_new_trades_filters_seen(self):
        client = self._make_client()
        client._last_trade_ids["0xabc"] = {"t1"}
        client.get_trades_for_address = MagicMock(return_value=[
            {"id": "t1", "side": "BUY", "size": "100"},
            {"id": "t2", "side": "SELL", "size": "200"},
            {"id": "t3", "side": "BUY", "size": "50"},
        ])
        new = client.get_new_trades("0xABC")  # also tests case-insensitive
        self.assertEqual(len(new), 2)
        ids = {t["id"] for t in new}
        self.assertEqual(ids, {"t2", "t3"})

    def test_get_new_trades_empty(self):
        client = self._make_client()
        client.get_trades_for_address = MagicMock(return_value=[])
        new = client.get_new_trades("0xabc")
        self.assertEqual(new, [])

    def test_get_new_trades_uses_transactionHash(self):
        """Trades with transactionHash instead of id are tracked."""
        client = self._make_client()
        client.get_trades_for_address = MagicMock(return_value=[
            {"transactionHash": "0xhash1", "side": "BUY"},
        ])
        new = client.get_new_trades("0xabc")
        self.assertEqual(len(new), 1)
        # Second call with same trade returns nothing new
        new2 = client.get_new_trades("0xabc")
        self.assertEqual(len(new2), 0)


class TestPlaceOrderMinSize(unittest.TestCase):
    """Test minimum order size enforcement and GTC order placement."""

    def _make_client(self):
        cfg = dict(bot.DEFAULT_CONFIG)
        logger = logging.getLogger("test")
        logger.handlers = []
        logger.addHandler(logging.NullHandler())
        client = bot.PolymarketCLOBClient.__new__(bot.PolymarketCLOBClient)
        client.cfg = cfg
        client.base_url = bot.CLOB_API_BASE
        client.gamma_url = bot.GAMMA_API_BASE
        client.session = MagicMock()
        client.logger = logger
        client._last_trade_ids = {}
        client._token_to_condition = {}
        client._token_to_slug = {}
        client.clob_sdk = MagicMock()
        client.clob_sdk.create_order.return_value = {"signed": True}
        client.clob_sdk.post_order.return_value = {"orderID": "test123"}
        return client

    def _get_order_size(self, client):
        """Extract the size (tokens) kwarg passed to OrderArgs."""
        call_kwargs = bot.OrderArgs.call_args[1]
        return call_kwargs["size"]

    def _get_order_price(self, client):
        """Extract the price kwarg passed to OrderArgs."""
        call_kwargs = bot.OrderArgs.call_args[1]
        return call_kwargs["price"]

    def _get_order_usdc(self, client):
        """Compute USDC from OrderArgs size * price."""
        call_kwargs = bot.OrderArgs.call_args[1]
        return round(call_kwargs["size"] * call_kwargs["price"], 2)

    def test_normal_size_not_bumped(self):
        """When USDC >= $1 and tokens >= 5, amount is not changed."""
        client = self._make_client()
        # $5 USDC at price 0.50 -> 10 tokens, no bump
        client.place_order("token1", "BUY", 5.0, 0.50)
        self.assertEqual(self._get_order_size(client), 10.0)

    def test_small_tokens_bumped(self):
        """When tokens < 5, USDC amount bumped to cover 5 tokens."""
        client = self._make_client()
        # $2 at price 0.90 -> effective = max(2, 4.5, 1.05) = $4.50 -> 5 tokens
        client.place_order("token1", "BUY", 2.0, 0.90)
        self.assertEqual(self._get_order_size(client), 5.0)

    def test_small_notional_bumped(self):
        """When USDC < $1.05 notional minimum, bumped to $1.05."""
        client = self._make_client()
        # $0.30 at price 0.05 -> effective = max(0.30, 0.25, 1.05) = $1.05 -> 21 tokens
        client.place_order("token1", "BUY", 0.30, 0.05)
        self.assertEqual(self._get_order_size(client), 21.0)

    def test_both_minimums_token_wins(self):
        """When both minimums trigger, the larger USDC requirement wins."""
        client = self._make_client()
        # $0.50 at price 0.80 -> effective = max(0.50, 4.0, 1.05) = $4.0 -> 5 tokens
        client.place_order("token1", "BUY", 0.50, 0.80)
        self.assertEqual(self._get_order_size(client), 5.0)

    def test_both_minimums_notional_wins(self):
        """When notional minimum requires more USDC than token minimum."""
        client = self._make_client()
        # $0.50 at price 0.10 -> effective = max(0.50, 0.50, 1.05) = $1.05 -> 10.5 tokens
        client.place_order("token1", "BUY", 0.50, 0.10)
        self.assertEqual(self._get_order_size(client), 10.5)

    def test_zero_price_returns_none(self):
        """Price of 0 should return None, not divide by zero."""
        client = self._make_client()
        result = client.place_order("token1", "BUY", 5.0, 0.0)
        self.assertIsNone(result)
        client.clob_sdk.create_order.assert_not_called()

    def test_exact_minimum_not_bumped(self):
        """Exactly 5 tokens and >= $1 USDC should not be bumped."""
        client = self._make_client()
        # $5 USDC at price 1.0 -> 5 tokens, no bump
        client.place_order("token1", "BUY", 5.0, 1.0)
        self.assertEqual(self._get_order_size(client), 5.0)

    def test_order_uses_gtc_type(self):
        """Orders should use GTC (Good-Till-Cancelled) order type."""
        client = self._make_client()
        client.place_order("token1", "BUY", 5.0, 0.50)
        # post_order should be called with GTC
        post_kwargs = client.clob_sdk.post_order.call_args[1]
        self.assertEqual(post_kwargs["orderType"], bot.OrderType.GTC)


class TestBalanceCaching(unittest.TestCase):
    """Test USDC balance caching in TradeExecutor."""

    def _make_executor(self):
        w3 = MagicMock()
        mock_account = MagicMock()
        mock_account.address = "0x" + "1" * 40
        w3.eth.account.from_key.return_value = mock_account

        mock_contract = MagicMock()
        mock_contract.functions.balanceOf.return_value.call.return_value = 500_000_000  # 500 USDC
        w3.eth.contract.return_value = mock_contract

        cfg = dict(bot.DEFAULT_CONFIG)
        executor = bot.TradeExecutor(
            w3=w3,
            private_key="0x" + "a" * 64,
            cfg=cfg,
            logger=logging.getLogger("test"),
        )
        return executor, mock_contract

    def test_balance_is_cached(self):
        executor, mock_contract = self._make_executor()
        b1 = executor.get_usdc_balance()
        b2 = executor.get_usdc_balance()
        self.assertEqual(b1, b2)
        # balanceOf should only be called once (cached on second call)
        self.assertEqual(
            mock_contract.functions.balanceOf.return_value.call.call_count, 1
        )

    def test_cache_bypass_with_zero_max_age(self):
        executor, mock_contract = self._make_executor()
        executor.get_usdc_balance()
        executor.get_usdc_balance(max_age_seconds=0)
        self.assertEqual(
            mock_contract.functions.balanceOf.return_value.call.call_count, 2
        )

    def test_invalidate_balance_cache(self):
        executor, mock_contract = self._make_executor()
        executor.get_usdc_balance()
        executor.invalidate_balance_cache()
        executor.get_usdc_balance()
        self.assertEqual(
            mock_contract.functions.balanceOf.return_value.call.call_count, 2
        )

    def test_cache_expiry(self):
        executor, mock_contract = self._make_executor()
        executor.get_usdc_balance(max_age_seconds=1)
        # Manually expire the cache
        executor._balance_timestamp -= 2
        executor.get_usdc_balance(max_age_seconds=1)
        self.assertEqual(
            mock_contract.functions.balanceOf.return_value.call.call_count, 2
        )

    def test_rpc_failure_returns_stale_cache(self):
        executor, mock_contract = self._make_executor()
        # First call succeeds and populates cache
        balance = executor.get_usdc_balance()
        self.assertEqual(balance, Decimal("500"))
        # Expire cache, then make RPC fail
        executor._balance_timestamp -= 600
        mock_contract.functions.balanceOf.return_value.call.side_effect = Exception("RPC down")
        # Should return stale cached value instead of raising
        result = executor.get_usdc_balance()
        self.assertEqual(result, Decimal("500"))

    def test_rpc_failure_no_cache_raises(self):
        executor, mock_contract = self._make_executor()
        # Make RPC fail on the very first call (no cache yet)
        mock_contract.functions.balanceOf.return_value.call.side_effect = Exception("RPC down")
        with self.assertRaises(Exception):
            executor.get_usdc_balance()


class TestConstants(unittest.TestCase):
    """Verify contract addresses and ABIs are well-formed."""

    def test_addresses_are_checksummed_length(self):
        for addr in [
            bot.CTF_EXCHANGE_ADDRESS,
            bot.NEG_RISK_CTF_EXCHANGE_ADDRESS,
            bot.NEG_RISK_ADAPTER_ADDRESS,
            bot.USDC_ADDRESS,
            bot.CONDITIONAL_TOKENS_ADDRESS,
        ]:
            self.assertTrue(addr.startswith("0x"), f"{addr} missing 0x prefix")
            self.assertEqual(len(addr), 42, f"{addr} wrong length")

    def test_erc20_abi_is_list(self):
        self.assertIsInstance(bot.ERC20_ABI, list)
        self.assertGreater(len(bot.ERC20_ABI), 0)
        names = {item.get("name") for item in bot.ERC20_ABI}
        self.assertIn("balanceOf", names)
        self.assertIn("approve", names)
        self.assertIn("allowance", names)

    def test_ctf_exchange_abi_has_events(self):
        self.assertIsInstance(bot.CTF_EXCHANGE_ABI, list)
        event_names = {
            item["name"]
            for item in bot.CTF_EXCHANGE_ABI
            if item.get("type") == "event"
        }
        self.assertIn("OrderFilled", event_names)
        self.assertIn("OrdersMatched", event_names)


class TestOrderMonitoring(unittest.TestCase):
    """Test open order tracking, fill detection, and stale cancellation."""

    def _make_executor(self, order_ttl=300):
        """Create a TradeExecutor with mocked Web3 and CLOB client."""
        w3 = MagicMock()
        mock_account = MagicMock()
        mock_account.address = "0x" + "1" * 40
        w3.eth.account.from_key.return_value = mock_account
        w3.eth.contract.return_value = MagicMock()

        cfg = dict(bot.DEFAULT_CONFIG)
        cfg["order_ttl_seconds"] = order_ttl

        executor = bot.TradeExecutor(
            w3=w3,
            private_key="0x" + "a" * 64,
            cfg=cfg,
            logger=logging.getLogger("test"),
        )
        executor.get_usdc_balance = MagicMock(return_value=Decimal("1000"))

        # Mock a CLOB client
        executor.clob_client = MagicMock()
        executor.clob_client.clob_sdk = MagicMock()
        return executor

    def test_track_order_stores_info(self):
        executor = self._make_executor()
        executor.track_order("order1", "BUY", "token1", 0.50, 5.0)
        self.assertIn("order1", executor._open_orders)
        self.assertEqual(executor._open_orders["order1"]["side"], "BUY")
        self.assertEqual(executor._open_orders["order1"]["usdc"], 5.0)

    def test_monitor_detects_filled_order(self):
        executor = self._make_executor()
        executor.track_order("order1", "BUY", "token1", 0.50, 5.0)
        # Simulate the API returning MATCHED status
        executor.clob_client.get_order.return_value = {"status": "MATCHED"}
        result = executor.monitor_open_orders()
        self.assertEqual(result["filled"], 1)
        self.assertEqual(result["cancelled"], 0)
        self.assertNotIn("order1", executor._open_orders)

    def test_monitor_detects_cancelled_order(self):
        executor = self._make_executor()
        executor.track_order("order1", "BUY", "token1", 0.50, 5.0)
        executor.clob_client.get_order.return_value = {"status": "CANCELLED"}
        result = executor.monitor_open_orders()
        self.assertEqual(result["filled"], 0)
        self.assertNotIn("order1", executor._open_orders)

    def test_monitor_cancels_stale_order(self):
        executor = self._make_executor(order_ttl=60)
        executor.track_order("order1", "BUY", "token1", 0.50, 5.0)
        # Backdate the placement time so it's stale
        executor._open_orders["order1"]["placed_at"] -= 120
        executor.clob_client.get_order.return_value = {"status": "LIVE"}
        result = executor.monitor_open_orders()
        self.assertEqual(result["cancelled"], 1)
        executor.clob_client.cancel_order.assert_called_once_with("order1")
        self.assertNotIn("order1", executor._open_orders)

    def test_monitor_keeps_fresh_live_order(self):
        executor = self._make_executor(order_ttl=300)
        executor.track_order("order1", "BUY", "token1", 0.50, 5.0)
        executor.clob_client.get_order.return_value = {"status": "LIVE"}
        result = executor.monitor_open_orders()
        self.assertEqual(result["still_open"], 1)
        self.assertEqual(result["filled"], 0)
        self.assertEqual(result["cancelled"], 0)
        self.assertIn("order1", executor._open_orders)

    def test_monitor_noop_when_no_tracked_orders(self):
        executor = self._make_executor()
        result = executor.monitor_open_orders()
        self.assertIsNone(result)

    def test_monitor_handles_api_failure_gracefully(self):
        executor = self._make_executor()
        executor.track_order("order1", "BUY", "token1", 0.50, 5.0)
        # Simulate API failure
        executor.clob_client.get_order.return_value = None
        result = executor.monitor_open_orders()
        self.assertEqual(result["still_open"], 1)
        # Order should still be tracked
        self.assertIn("order1", executor._open_orders)


class TestCLOBClientOrderMethods(unittest.TestCase):
    """Test PolymarketCLOBClient order management methods."""

    def _make_client(self):
        cfg = dict(bot.DEFAULT_CONFIG)
        logger = logging.getLogger("test")
        logger.handlers = [logging.NullHandler()]
        client = bot.PolymarketCLOBClient.__new__(bot.PolymarketCLOBClient)
        client.cfg = cfg
        client.base_url = bot.CLOB_API_BASE
        client.gamma_url = bot.GAMMA_API_BASE
        client.data_url = bot.DATA_API_BASE
        client.session = MagicMock()
        client.logger = logger
        client._last_trade_ids = {}
        client._token_to_condition = {}
        client._token_to_slug = {}
        client.clob_sdk = MagicMock()
        return client

    def test_get_open_orders_returns_list(self):
        client = self._make_client()
        client.clob_sdk.get_orders.return_value = [
            {"id": "order1", "status": "LIVE"},
            {"id": "order2", "status": "LIVE"},
        ]
        orders = client.get_open_orders()
        self.assertEqual(len(orders), 2)

    def test_get_open_orders_no_sdk(self):
        client = self._make_client()
        client.clob_sdk = None
        self.assertEqual(client.get_open_orders(), [])

    def test_get_order_returns_dict(self):
        client = self._make_client()
        client.clob_sdk.get_order.return_value = {"id": "order1", "status": "MATCHED"}
        order = client.get_order("order1")
        self.assertEqual(order["status"], "MATCHED")

    def test_cancel_order_calls_sdk(self):
        client = self._make_client()
        client.clob_sdk.cancel.return_value = {"cancelled": True}
        resp = client.cancel_order("order1")
        client.clob_sdk.cancel.assert_called_once_with("order1")
        self.assertIsNotNone(resp)

    def test_cancel_all_orders_calls_sdk(self):
        client = self._make_client()
        client.clob_sdk.cancel_all.return_value = {"cancelled": 3}
        resp = client.cancel_all_orders()
        client.clob_sdk.cancel_all.assert_called_once()
        self.assertIsNotNone(resp)

    def test_cancel_order_no_sdk(self):
        client = self._make_client()
        client.clob_sdk = None
        self.assertIsNone(client.cancel_order("order1"))


class TestAutoRedeemSettled(unittest.TestCase):
    """Test automatic redemption of settled (resolved) positions back to USDC."""

    # Use realistic numeric token IDs (like real Polymarket ERC-1155 IDs)
    TOK1 = "12345678901234567890"
    TOK2 = "98765432109876543210"
    TOK3 = "55555555555555555555"

    def _make_executor(self, dry_run=False, auto_redeem=True):
        w3 = MagicMock()
        mock_account = MagicMock()
        mock_account.address = "0x" + "1" * 40
        w3.eth.account.from_key.return_value = mock_account

        # Create separate mocks for each contract so we can configure them
        # independently.
        mock_usdc = MagicMock()
        mock_ctf_exchange = MagicMock()
        mock_conditional_tokens = MagicMock()
        mock_neg_risk_adapter = MagicMock()
        # Default: neg risk adapter reports 0 balance (no wrapped tokens)
        mock_neg_risk_adapter.functions.balanceOf.return_value.call.return_value = 0

        def contract_factory(address, abi):
            addr = address.lower() if hasattr(address, "lower") else address
            if addr == bot.USDC_ADDRESS.lower():
                return mock_usdc
            if addr == bot.CTF_EXCHANGE_ADDRESS.lower():
                return mock_ctf_exchange
            if addr == bot.CONDITIONAL_TOKENS_ADDRESS.lower():
                return mock_conditional_tokens
            if addr == bot.NEG_RISK_ADAPTER_ADDRESS.lower():
                return mock_neg_risk_adapter
            return MagicMock()

        w3.eth.contract.side_effect = contract_factory

        cfg = dict(bot.DEFAULT_CONFIG)
        cfg["dry_run"] = dry_run
        cfg["auto_redeem_settled"] = auto_redeem

        executor = bot.TradeExecutor(
            w3=w3,
            private_key="0x" + "a" * 64,
            cfg=cfg,
            logger=logging.getLogger("test_redeem"),
        )
        executor.get_usdc_balance = MagicMock(return_value=Decimal("500"))

        # Re-assign contract references so tests can configure them
        executor.conditional_tokens = mock_conditional_tokens
        executor.usdc = mock_usdc
        executor.ctf_exchange = mock_ctf_exchange
        executor.neg_risk_adapter = mock_neg_risk_adapter

        # Mock CLOB client with get_market_by_token
        executor.clob_client = MagicMock()
        executor.clob_client.clob_sdk = MagicMock()

        # Mock _sign_and_send to avoid real blockchain interaction
        executor._sign_and_send = MagicMock()
        executor._base_tx_params = MagicMock(return_value={
            "from": executor.address, "nonce": 0,
            "maxFeePerGas": 100, "maxPriorityFeePerGas": 30,
            "chainId": 137,
        })

        return executor

    def test_no_positions_returns_empty(self):
        executor = self._make_executor()
        results = executor.check_and_redeem_settled()
        self.assertEqual(results, [])

    def test_no_clob_client_returns_empty(self):
        executor = self._make_executor()
        executor._positions[self.TOK1] = {"tokens": Decimal("10"), "entry_price": Decimal("0.50")}
        executor.clob_client = None
        results = executor.check_and_redeem_settled()
        self.assertEqual(results, [])

    def test_disabled_via_config_returns_empty(self):
        """When auto_redeem_settled is False, redemption is skipped."""
        executor = self._make_executor(auto_redeem=False)
        executor._positions[self.TOK1] = {"tokens": Decimal("10"), "entry_price": Decimal("0.50")}
        results = executor.check_and_redeem_settled()
        self.assertEqual(results, [])

    def test_skips_unresolved_market(self):
        """Positions not resolved on-chain (payoutDenominator == 0) are not redeemed."""
        executor = self._make_executor()
        executor._positions[self.TOK1] = {"tokens": Decimal("10"), "entry_price": Decimal("0.50")}
        executor.clob_client.get_market_by_token.return_value = {
            "condition_id": "0x" + "a" * 64,
            "closed": False,
            "active": True,
        }
        # On-chain: oracle has NOT reported yet
        executor.conditional_tokens.functions.payoutDenominator.return_value.call.return_value = 0
        results = executor.check_and_redeem_settled()
        self.assertEqual(results, [])
        self.assertIn(self.TOK1, executor._positions)

    def test_skips_when_not_resolved_on_chain(self):
        """Market closed in API but payoutDenominator == 0 on-chain."""
        executor = self._make_executor()
        executor._positions[self.TOK1] = {"tokens": Decimal("10"), "entry_price": Decimal("0.50")}
        executor.clob_client.get_market_by_token.return_value = {
            "condition_id": "0x" + "a" * 64,
            "closed": True,
            "active": False,
        }
        executor.conditional_tokens.functions.payoutDenominator.return_value.call.return_value = 0
        results = executor.check_and_redeem_settled()
        self.assertEqual(results, [])

    def test_clears_stale_position_when_no_on_chain_balance(self):
        """If market resolved but token balance is 0, clear the stale position."""
        executor = self._make_executor()
        executor._positions[self.TOK1] = {"tokens": Decimal("10"), "entry_price": Decimal("0.50")}
        executor.clob_client.get_market_by_token.return_value = {
            "condition_id": "0x" + "a" * 64,
            "closed": True,
            "active": False,
        }
        executor.conditional_tokens.functions.payoutDenominator.return_value.call.return_value = 1
        executor.conditional_tokens.functions.balanceOf.return_value.call.return_value = 0
        results = executor.check_and_redeem_settled()
        self.assertEqual(results, [])
        # Position should be cleared
        self.assertNotIn(self.TOK1, executor._positions)

    def test_successful_redemption(self):
        """Full redemption flow: resolved market with tokens redeemed to USDC."""
        executor = self._make_executor()
        executor._positions[self.TOK1] = {"tokens": Decimal("10"), "entry_price": Decimal("0.50")}

        executor.clob_client.get_market_by_token.return_value = {
            "condition_id": "0x" + "a" * 64,
            "closed": True,
            "active": False,
        }
        executor.conditional_tokens.functions.payoutDenominator.return_value.call.return_value = 1000000
        executor.conditional_tokens.functions.balanceOf.return_value.call.return_value = 10_000000
        executor.conditional_tokens.functions.redeemPositions.return_value.build_transaction.return_value = {}

        mock_receipt = MagicMock()
        mock_receipt.status = 1
        mock_receipt.transactionHash.hex.return_value = "0xabc123"
        executor._sign_and_send.return_value = mock_receipt

        results = executor.check_and_redeem_settled()

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "redeemed")
        self.assertEqual(results[0]["tx_hash"], "0xabc123")
        # Position should be cleared
        self.assertNotIn(self.TOK1, executor._positions)

    def test_redemption_tx_failure_keeps_position(self):
        """If the redemption tx reverts, position is kept for retry."""
        executor = self._make_executor()
        executor._positions[self.TOK1] = {"tokens": Decimal("10"), "entry_price": Decimal("0.50")}

        executor.clob_client.get_market_by_token.return_value = {
            "condition_id": "0x" + "a" * 64,
            "closed": True,
            "active": False,
        }
        executor.conditional_tokens.functions.payoutDenominator.return_value.call.return_value = 1000000
        executor.conditional_tokens.functions.balanceOf.return_value.call.return_value = 10_000000
        executor.conditional_tokens.functions.redeemPositions.return_value.build_transaction.return_value = {}

        mock_receipt = MagicMock()
        mock_receipt.status = 0  # reverted
        executor._sign_and_send.return_value = mock_receipt

        results = executor.check_and_redeem_settled()

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "failed")
        # Position should still exist for retry
        self.assertIn(self.TOK1, executor._positions)

    def test_dry_run_skips_transaction(self):
        """In dry-run mode, redemption is logged but no tx is sent."""
        executor = self._make_executor(dry_run=True)
        executor._positions[self.TOK1] = {"tokens": Decimal("10"), "entry_price": Decimal("0.50")}

        executor.clob_client.get_market_by_token.return_value = {
            "condition_id": "0x" + "a" * 64,
            "closed": True,
            "active": False,
        }
        executor.conditional_tokens.functions.payoutDenominator.return_value.call.return_value = 1000000
        executor.conditional_tokens.functions.balanceOf.return_value.call.return_value = 10_000000

        results = executor.check_and_redeem_settled()

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "dry_run")
        # No transaction should have been sent
        executor._sign_and_send.assert_not_called()
        # Position should remain (dry run)
        self.assertIn(self.TOK1, executor._positions)

    def test_multiple_positions_redeemed(self):
        """Multiple resolved positions are redeemed in one pass."""
        executor = self._make_executor()
        executor._positions[self.TOK1] = {"tokens": Decimal("10"), "entry_price": Decimal("0.50")}
        executor._positions[self.TOK2] = {"tokens": Decimal("5"), "entry_price": Decimal("0.70")}
        executor._positions[self.TOK3] = {"tokens": Decimal("8"), "entry_price": Decimal("0.40")}

        cond_resolved = "0x" + "b" * 64
        cond_active = "0x" + "c" * 64

        def mock_market(token_id):
            if token_id == self.TOK3:
                return {"condition_id": cond_active, "closed": False, "active": True}
            return {
                "condition_id": cond_resolved,
                "closed": True,
                "active": False,
            }

        executor.clob_client.get_market_by_token.side_effect = mock_market

        # On-chain: only the resolved conditionId returns payoutDenominator > 0;
        # everything else (including derived conditionIds) returns 0.
        resolved_cond_bytes = bytes.fromhex(cond_resolved.replace("0x", ""))
        def payout_side_effect(cond_bytes):
            result = MagicMock()
            if cond_bytes == resolved_cond_bytes:
                result.call.return_value = 1000000  # resolved
            else:
                result.call.return_value = 0  # not resolved
            return result
        executor.conditional_tokens.functions.payoutDenominator.side_effect = payout_side_effect

        executor.conditional_tokens.functions.balanceOf.return_value.call.return_value = 10_000000
        executor.conditional_tokens.functions.redeemPositions.return_value.build_transaction.return_value = {}

        mock_receipt = MagicMock()
        mock_receipt.status = 1
        mock_receipt.transactionHash.hex.return_value = "0xhash"
        executor._sign_and_send.return_value = mock_receipt

        results = executor.check_and_redeem_settled()

        # TOK1 and TOK2 redeemed, TOK3 still active (not resolved on-chain)
        self.assertEqual(len(results), 2)
        self.assertTrue(all(r["status"] == "redeemed" for r in results))
        self.assertNotIn(self.TOK1, executor._positions)
        self.assertNotIn(self.TOK2, executor._positions)
        self.assertIn(self.TOK3, executor._positions)

    def test_gamma_api_failure_skips_gracefully(self):
        """If the Gamma API returns None, the position is skipped."""
        executor = self._make_executor()
        executor._positions[self.TOK1] = {"tokens": Decimal("10"), "entry_price": Decimal("0.50")}
        executor.clob_client.get_market_by_token.return_value = None

        results = executor.check_and_redeem_settled()
        self.assertEqual(results, [])
        self.assertIn(self.TOK1, executor._positions)

    def test_missing_condition_id_skips(self):
        """If market data has no condition_id, skip gracefully."""
        executor = self._make_executor()
        executor._positions[self.TOK1] = {"tokens": Decimal("10"), "entry_price": Decimal("0.50")}
        executor.clob_client.get_market_by_token.return_value = {
            "closed": True,
            "active": False,
            # no condition_id
        }
        results = executor.check_and_redeem_settled()
        self.assertEqual(results, [])

    def test_zero_token_position_skipped(self):
        """Position with 0 tokens should be skipped."""
        executor = self._make_executor()
        executor._positions[self.TOK1] = {"tokens": Decimal("0"), "entry_price": Decimal("0.50")}
        results = executor.check_and_redeem_settled()
        self.assertEqual(results, [])


class TestConditionIdDerivation(unittest.TestCase):
    """Test _derive_condition_id and _resolve_condition_id."""

    def _make_executor(self):
        """Create a minimal TradeExecutor with mocked web3."""
        cfg = dict(bot.DEFAULT_CONFIG)
        w3 = MagicMock()
        w3.eth.account.from_key.return_value.address = "0x" + "1" * 40
        w3.eth.contract.return_value = MagicMock()
        w3.eth.get_transaction_count.return_value = 0
        executor = bot.TradeExecutor(
            w3, "0x" + "a" * 64, cfg, clob_client=MagicMock(),
            logger=logging.getLogger("test_cid"),
        )
        return executor

    def test_resolve_uses_api_value_when_it_works(self):
        """If API conditionId has payoutDenom > 0, use it directly."""
        executor = self._make_executor()
        api_cid = "0x" + "ab" * 32
        api_cond_bytes = bytes.fromhex(api_cid[2:])

        def payout_side_effect(cond_bytes):
            result = MagicMock()
            if cond_bytes == api_cond_bytes:
                result.call.return_value = 500  # resolved with API value
            else:
                result.call.return_value = 0
            return result

        executor.conditional_tokens.functions.payoutDenominator.side_effect = payout_side_effect

        resolved_cid, pd = executor._resolve_condition_id(api_cid, neg_risk=True)
        self.assertEqual(pd, 500)
        self.assertEqual(resolved_cid, api_cid)

    def test_resolve_falls_back_to_derived(self):
        """When API conditionId has payoutDenom=0, derived conditionId is tried."""
        executor = self._make_executor()
        api_cid = "0x" + "ab" * 32

        # Simulate: getConditionId returns a derived conditionId
        derived_bytes = b"\xdd" * 32
        executor.conditional_tokens.functions.getConditionId.return_value.call.return_value = derived_bytes

        def payout_side_effect(cond_bytes):
            result = MagicMock()
            if cond_bytes == derived_bytes:
                result.call.return_value = 999  # resolved via derivation
            else:
                result.call.return_value = 0  # not resolved
            return result

        executor.conditional_tokens.functions.payoutDenominator.side_effect = payout_side_effect

        resolved_cid, pd = executor._resolve_condition_id(api_cid, neg_risk=True)
        self.assertEqual(pd, 999)
        self.assertEqual(resolved_cid, "0x" + "dd" * 32)

    def test_resolve_returns_zero_when_nothing_works(self):
        """When no conditionId resolves, returns 0."""
        executor = self._make_executor()
        api_cid = "0x" + "ab" * 32
        executor.conditional_tokens.functions.payoutDenominator.return_value.call.return_value = 0
        executor.conditional_tokens.functions.getConditionId.return_value.call.return_value = b"\xee" * 32

        resolved_cid, pd = executor._resolve_condition_id(api_cid, neg_risk=False)
        self.assertEqual(pd, 0)
        self.assertEqual(resolved_cid, api_cid)


class TestAutoRedeemConfig(unittest.TestCase):
    """Test auto_redeem_settled config and constants."""

    def test_default_config_has_auto_redeem(self):
        self.assertIn("auto_redeem_settled", bot.DEFAULT_CONFIG)
        self.assertTrue(bot.DEFAULT_CONFIG["auto_redeem_settled"])

    def test_redeem_check_interval_constant(self):
        self.assertEqual(bot.REDEEM_CHECK_INTERVAL_SECONDS, 300)

    def test_conditional_tokens_abi_is_valid(self):
        self.assertIsInstance(bot.CONDITIONAL_TOKENS_ABI, list)
        names = {item.get("name") for item in bot.CONDITIONAL_TOKENS_ABI}
        self.assertIn("balanceOf", names)
        self.assertIn("payoutDenominator", names)
        self.assertIn("redeemPositions", names)

    def test_env_var_override_auto_redeem(self):
        """AUTO_REDEEM_SETTLED env var should override the config."""
        cfg = dict(bot.DEFAULT_CONFIG)
        with patch.dict(os.environ, {"AUTO_REDEEM_SETTLED": "false"}):
            if os.environ.get("AUTO_REDEEM_SETTLED"):
                cfg["auto_redeem_settled"] = os.environ["AUTO_REDEEM_SETTLED"].lower() in ("1", "true", "yes")
        self.assertFalse(cfg["auto_redeem_settled"])

        with patch.dict(os.environ, {"AUTO_REDEEM_SETTLED": "true"}):
            if os.environ.get("AUTO_REDEEM_SETTLED"):
                cfg["auto_redeem_settled"] = os.environ["AUTO_REDEEM_SETTLED"].lower() in ("1", "true", "yes")
        self.assertTrue(cfg["auto_redeem_settled"])


class TestGetMarketByToken(unittest.TestCase):
    """Test PolymarketCLOBClient.get_market_by_token."""

    def _make_client(self):
        cfg = dict(bot.DEFAULT_CONFIG)
        logger = logging.getLogger("test")
        logger.handlers = [logging.NullHandler()]
        client = bot.PolymarketCLOBClient.__new__(bot.PolymarketCLOBClient)
        client.cfg = cfg
        client.base_url = bot.CLOB_API_BASE
        client.gamma_url = bot.GAMMA_API_BASE
        client.data_url = bot.DATA_API_BASE
        client.session = MagicMock()
        client.logger = logger
        client._last_trade_ids = {}
        client._token_to_condition = {}
        client._token_to_slug = {}
        client.clob_sdk = None
        return client

    def test_returns_first_market(self):
        client = self._make_client()
        client._get_public = MagicMock(return_value=[
            {"condition_id": "0xabc", "closed": True, "question": "Test market?"},
        ])
        result = client.get_market_by_token("token123")
        self.assertEqual(result["condition_id"], "0xabc")
        client._get_public.assert_called_once_with(
            f"{bot.GAMMA_API_BASE}/markets",
            params={"clob_token_ids": "token123"},
        )

    def test_returns_none_on_empty_response(self):
        client = self._make_client()
        client._get_public = MagicMock(return_value=[])
        result = client.get_market_by_token("token123")
        self.assertIsNone(result)

    def test_returns_none_on_api_failure(self):
        client = self._make_client()
        client._get_public = MagicMock(return_value=None)
        result = client.get_market_by_token("token123")
        self.assertIsNone(result)


class TestGetWalletTokenIds(unittest.TestCase):
    """Test PolymarketCLOBClient.get_wallet_token_ids."""

    def _make_client(self):
        cfg = dict(bot.DEFAULT_CONFIG)
        logger = logging.getLogger("test")
        logger.handlers = [logging.NullHandler()]
        client = bot.PolymarketCLOBClient.__new__(bot.PolymarketCLOBClient)
        client.cfg = cfg
        client.base_url = bot.CLOB_API_BASE
        client.gamma_url = bot.GAMMA_API_BASE
        client.data_url = bot.DATA_API_BASE
        client.session = MagicMock()
        client.logger = logger
        client._last_trade_ids = {}
        client._token_to_condition = {}
        client._token_to_slug = {}
        client._token_to_avg_entry = {}
        client.clob_sdk = None
        return client

    def test_extracts_token_ids_from_list(self):
        client = self._make_client()
        client._get_public = MagicMock(return_value=[
            {"asset_id": "111", "side": "BUY"},
            {"asset_id": "222", "side": "SELL"},
            {"asset_id": "111", "side": "BUY"},  # duplicate
        ])
        ids = client.get_wallet_token_ids("0xabc")
        self.assertEqual(ids, {"111", "222"})

    def test_extracts_from_nested_data(self):
        client = self._make_client()
        client._get_public = MagicMock(return_value={
            "data": [
                {"asset": "333"},
                {"token_id": "444"},
            ]
        })
        ids = client.get_wallet_token_ids("0xabc")
        self.assertEqual(ids, {"333", "444"})

    def test_returns_empty_on_failure(self):
        client = self._make_client()
        client._get_public = MagicMock(return_value=None)
        ids = client.get_wallet_token_ids("0xabc")
        self.assertEqual(ids, set())

    def test_returns_empty_on_empty_list(self):
        client = self._make_client()
        client._get_public = MagicMock(return_value=[])
        ids = client.get_wallet_token_ids("0xabc")
        self.assertEqual(ids, set())


class TestScanAndRedeemPortfolio(unittest.TestCase):
    """Test the startup portfolio scan that discovers and redeems all positions."""

    TOK_RESOLVED = "11111111111111111111"
    TOK_ACTIVE = "22222222222222222222"
    TOK_EMPTY = "33333333333333333333"

    def _make_executor(self, dry_run=False, auto_redeem=True):
        w3 = MagicMock()
        mock_account = MagicMock()
        mock_account.address = "0x" + "1" * 40
        w3.eth.account.from_key.return_value = mock_account

        mock_conditional_tokens = MagicMock()
        mock_neg_risk_adapter = MagicMock()
        # Default: neg risk adapter reports 0 balance (no wrapped tokens)
        mock_neg_risk_adapter.functions.balanceOf.return_value.call.return_value = 0
        w3.eth.contract.return_value = MagicMock()

        cfg = dict(bot.DEFAULT_CONFIG)
        cfg["dry_run"] = dry_run
        cfg["auto_redeem_settled"] = auto_redeem

        executor = bot.TradeExecutor(
            w3=w3,
            private_key="0x" + "a" * 64,
            cfg=cfg,
            logger=logging.getLogger("test_scan"),
        )
        executor.get_usdc_balance = MagicMock(return_value=Decimal("0.20"))
        executor.conditional_tokens = mock_conditional_tokens
        executor.neg_risk_adapter = mock_neg_risk_adapter

        executor.clob_client = MagicMock()
        executor.clob_client.clob_sdk = MagicMock()

        executor._sign_and_send = MagicMock()
        executor._base_tx_params = MagicMock(return_value={
            "from": executor.address, "nonce": 0,
            "maxFeePerGas": 100, "maxPriorityFeePerGas": 30,
            "chainId": 137,
        })

        return executor

    def test_no_clob_client_skips(self):
        executor = self._make_executor()
        executor.clob_client = None
        results = executor.scan_and_redeem_portfolio()
        self.assertEqual(results, [])

    def test_disabled_via_config(self):
        executor = self._make_executor(auto_redeem=False)
        results = executor.scan_and_redeem_portfolio()
        self.assertEqual(results, [])

    def test_no_trade_history(self):
        executor = self._make_executor()
        executor.clob_client.get_wallet_token_ids.return_value = set()
        results = executor.scan_and_redeem_portfolio()
        self.assertEqual(results, [])

    @unittest.skip("Pre-existing: seeding logic changed; needs rework")
    def test_redeems_resolved_and_seeds_active(self):
        """Resolved positions are redeemed; active ones are seeded into _positions."""
        executor = self._make_executor()
        executor.clob_client.get_wallet_token_ids.return_value = {
            self.TOK_RESOLVED, self.TOK_ACTIVE, self.TOK_EMPTY,
        }

        cond_resolved = "0x" + "a" * 64
        cond_active = "0x" + "b" * 64

        # Token balance: resolved has 50M, active has 10M, empty has 0
        def mock_balance(addr, token_id):
            bal_mock = MagicMock()
            if token_id == int(self.TOK_RESOLVED):
                bal_mock.call.return_value = 50_000000
            elif token_id == int(self.TOK_ACTIVE):
                bal_mock.call.return_value = 10_000000
            else:
                bal_mock.call.return_value = 0
            return bal_mock

        executor.conditional_tokens.functions.balanceOf.side_effect = mock_balance

        # Market info
        def mock_market(token_id):
            if token_id == self.TOK_RESOLVED:
                return {
                    "condition_id": cond_resolved,
                    "closed": True,
                    "active": False,
                    "question": "Resolved market?",
                }
            if token_id == self.TOK_ACTIVE:
                return {
                    "condition_id": cond_active,
                    "closed": False,
                    "active": True,
                    "question": "Active market?",
                }
            return None

        executor.clob_client.get_market_by_token.side_effect = mock_market
        executor.clob_client.get_last_trade_price.return_value = 0.65

        # On-chain: only the resolved conditionId returns payoutDenominator > 0;
        # everything else (including derived conditionIds) returns 0.
        resolved_cond_bytes = bytes.fromhex(cond_resolved.replace("0x", ""))
        def payout_side_effect(cond_bytes):
            result = MagicMock()
            if cond_bytes == resolved_cond_bytes:
                result.call.return_value = 1000000  # resolved
            else:
                result.call.return_value = 0  # not resolved
            return result
        executor.conditional_tokens.functions.payoutDenominator.side_effect = payout_side_effect

        executor.conditional_tokens.functions.redeemPositions.return_value.build_transaction.return_value = {}

        mock_receipt = MagicMock()
        mock_receipt.status = 1
        mock_receipt.transactionHash.hex.return_value = "0xredeemed"
        executor._sign_and_send.return_value = mock_receipt

        results = executor.scan_and_redeem_portfolio()

        # Resolved position should be redeemed
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "redeemed")

        # Active position should be seeded into _positions
        self.assertIn(self.TOK_ACTIVE, executor._positions)
        self.assertEqual(
            executor._positions[self.TOK_ACTIVE]["entry_price"],
            Decimal("0.65"),
        )

        # Empty-balance token should be skipped entirely
        self.assertNotIn(self.TOK_EMPTY, executor._positions)

    def test_dry_run_does_not_redeem(self):
        executor = self._make_executor(dry_run=True)
        executor.clob_client.get_wallet_token_ids.return_value = {self.TOK_RESOLVED}
        executor.conditional_tokens.functions.balanceOf.return_value.call.return_value = 50_000000
        executor.clob_client.get_market_by_token.return_value = {
            "condition_id": "0x" + "a" * 64,
            "closed": True,
            "active": False,
            "question": "Test?",
        }
        executor.conditional_tokens.functions.payoutDenominator.return_value.call.return_value = 1000000

        results = executor.scan_and_redeem_portfolio()

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "dry_run")
        executor._sign_and_send.assert_not_called()

    def test_api_failure_skips_gracefully(self):
        executor = self._make_executor()
        executor.clob_client.get_wallet_token_ids.return_value = {self.TOK_RESOLVED}
        executor.conditional_tokens.functions.balanceOf.return_value.call.return_value = 50_000000
        executor.clob_client.get_market_by_token.return_value = None

        results = executor.scan_and_redeem_portfolio()
        self.assertEqual(results, [])


class TestProxyWalletDiscovery(unittest.TestCase):
    """Test proxy wallet discovery with multi-method fallback."""

    PROXY_ADDR = "0x" + "Aa" * 20  # valid hex for checksum

    def _make_executor(self, proxy_address=""):
        w3 = MagicMock()
        mock_account = MagicMock()
        mock_account.address = "0x" + "1" * 40
        w3.eth.account.from_key.return_value = mock_account

        mock_usdc = MagicMock()
        mock_ctf_exchange = MagicMock()
        mock_conditional_tokens = MagicMock()

        def contract_factory(address, abi):
            addr = address.lower() if hasattr(address, "lower") else address
            if addr == bot.USDC_ADDRESS.lower():
                return mock_usdc
            if addr == bot.CTF_EXCHANGE_ADDRESS.lower():
                return mock_ctf_exchange
            if addr == bot.CONDITIONAL_TOKENS_ADDRESS.lower():
                return mock_conditional_tokens
            return MagicMock()

        w3.eth.contract.side_effect = contract_factory

        cfg = dict(bot.DEFAULT_CONFIG)
        cfg["proxy_address"] = proxy_address
        executor = bot.TradeExecutor(
            w3=w3,
            private_key="0x" + "a" * 64,
            cfg=cfg,
            logger=logging.getLogger("test_proxy"),
        )
        executor.conditional_tokens = mock_conditional_tokens
        executor.usdc = mock_usdc
        executor.clob_client = MagicMock()
        executor.clob_client.clob_sdk = MagicMock()
        executor._sign_and_send = MagicMock()
        executor._base_tx_params = MagicMock(return_value={
            "from": executor.address, "nonce": 0,
            "maxFeePerGas": 100, "maxPriorityFeePerGas": 30,
            "chainId": 137,
        })
        return executor

    def test_config_override_used_first(self):
        """Manual proxy_address config skips all on-chain discovery."""
        addr = "0x" + "ab" * 20
        executor = self._make_executor(proxy_address=addr)
        result = executor.discover_proxy_wallet()
        self.assertEqual(result, addr)
        # Should be cached
        self.assertTrue(executor._proxy_discovery_done)

    def test_discover_via_getProxy(self):
        executor = self._make_executor()
        mock_factory = MagicMock()
        mock_factory.functions.getProxy.return_value.call.return_value = self.PROXY_ADDR
        executor.w3.eth.contract.side_effect = None
        executor.w3.eth.contract.return_value = mock_factory

        result = executor.discover_proxy_wallet()
        self.assertEqual(result, self.PROXY_ADDR)
        self.assertTrue(executor._proxy_discovery_done)

    def test_falls_through_to_proxies_fn(self):
        """When getProxy fails, tries the 'proxies' function."""
        executor = self._make_executor()
        mock_factory = MagicMock()
        # getProxy fails
        mock_factory.functions.getProxy.return_value.call.side_effect = Exception("no such fn")
        # proxies succeeds
        mock_factory.functions.proxies.return_value.call.return_value = self.PROXY_ADDR
        executor.w3.eth.contract.side_effect = None
        executor.w3.eth.contract.return_value = mock_factory

        result = executor.discover_proxy_wallet()
        self.assertEqual(result, self.PROXY_ADDR)

    def test_caches_result(self):
        """Second call returns cached result without querying again."""
        executor = self._make_executor()
        executor._proxy_address = self.PROXY_ADDR
        executor._proxy_discovery_done = True

        # Even if factory would fail, cached result is returned
        executor.w3.eth.contract.side_effect = Exception("should not be called")
        result = executor.discover_proxy_wallet()
        self.assertEqual(result, self.PROXY_ADDR)

    def test_falls_through_to_safe_factory(self):
        """When legacy factory yields nothing, checks Safe factory events."""
        executor = self._make_executor()
        mock_factory = MagicMock()
        # All legacy function calls return zero address
        zero = "0x" + "0" * 40
        mock_factory.functions.getProxy.return_value.call.return_value = zero
        mock_factory.functions.proxies.return_value.call.return_value = zero
        mock_factory.functions.proxyFor.return_value.call.return_value = zero
        executor.w3.eth.contract.side_effect = None
        executor.w3.eth.contract.return_value = mock_factory
        executor.w3.eth.block_number = 80_000_000  # must exceed start_block (70M)

        # Legacy event logs empty, Safe factory logs return a result
        safe_addr = self.PROXY_ADDR
        safe_log_data = bytes.fromhex("00" * 12 + safe_addr[2:].lower())

        def get_logs_side_effect(params):
            addr = params.get("address", "").lower()
            if addr == bot.SAFE_PROXY_FACTORY_ADDRESS.lower():
                return [{"data": safe_log_data, "blockNumber": 75_000_000}]
            return []

        executor.w3.eth.get_logs.side_effect = get_logs_side_effect

        result = executor.discover_proxy_wallet()
        self.assertIsNotNone(result)
        self.assertTrue(executor._proxy_is_safe)

    def test_all_methods_fail_returns_none(self):
        """When all discovery methods fail, returns None and caches failure."""
        executor = self._make_executor()
        mock_factory = MagicMock()
        # All function calls fail
        mock_factory.functions.getProxy.return_value.call.side_effect = Exception("fail")
        mock_factory.functions.proxies.return_value.call.side_effect = Exception("fail")
        mock_factory.functions.proxyFor.return_value.call.side_effect = Exception("fail")
        executor.w3.eth.contract.side_effect = None
        executor.w3.eth.contract.return_value = mock_factory
        executor.w3.eth.block_number = 80_000_000
        executor.w3.eth.get_logs.side_effect = Exception("fail")

        result = executor.discover_proxy_wallet()
        self.assertIsNone(result)
        # Should cache the failure to avoid retrying every 5 min
        self.assertTrue(executor._proxy_discovery_done)


class TestProxyBalances(unittest.TestCase):
    """Test proxy USDC and token balance queries."""

    PROXY_ADDR = "0x" + "P" * 40

    def _make_executor(self):
        w3 = MagicMock()
        mock_account = MagicMock()
        mock_account.address = "0x" + "1" * 40
        w3.eth.account.from_key.return_value = mock_account

        mock_usdc = MagicMock()
        mock_conditional_tokens = MagicMock()

        def contract_factory(address, abi):
            addr = address.lower() if hasattr(address, "lower") else address
            if addr == bot.USDC_ADDRESS.lower():
                return mock_usdc
            if addr == bot.CONDITIONAL_TOKENS_ADDRESS.lower():
                return mock_conditional_tokens
            return MagicMock()

        w3.eth.contract.side_effect = contract_factory

        cfg = dict(bot.DEFAULT_CONFIG)
        executor = bot.TradeExecutor(
            w3=w3,
            private_key="0x" + "a" * 64,
            cfg=cfg,
            logger=logging.getLogger("test_proxy_bal"),
        )
        executor.usdc = mock_usdc
        executor.conditional_tokens = mock_conditional_tokens
        return executor

    def test_proxy_usdc_balance(self):
        executor = self._make_executor()
        executor.usdc.functions.balanceOf.return_value.call.return_value = 150_000000
        result = executor.get_proxy_usdc_balance(self.PROXY_ADDR)
        self.assertEqual(result, Decimal("150"))

    def test_proxy_token_balance(self):
        executor = self._make_executor()
        executor.conditional_tokens.functions.balanceOf.return_value.call.return_value = 30_000000
        result = executor.get_proxy_token_balance(self.PROXY_ADDR, "12345")
        self.assertEqual(result, 30_000000)


class TestProxyRedemptionAndWithdrawal(unittest.TestCase):
    """Test proxy redemption via execute and USDC withdrawal."""

    PROXY_ADDR = "0x" + "P" * 40
    TOK1 = "12345678901234567890"

    def _make_executor(self, dry_run=False, proxy_redeem=True, proxy_withdraw=True):
        w3 = MagicMock()
        mock_account = MagicMock()
        mock_account.address = "0x" + "1" * 40
        w3.eth.account.from_key.return_value = mock_account

        mock_usdc = MagicMock()
        mock_ctf_exchange = MagicMock()
        mock_conditional_tokens = MagicMock()
        mock_neg_risk_adapter = MagicMock()
        # Default: neg risk adapter reports 0 balance (no wrapped tokens)
        mock_neg_risk_adapter.functions.balanceOf.return_value.call.return_value = 0

        def contract_factory(address, abi):
            addr = address.lower() if hasattr(address, "lower") else address
            if addr == bot.USDC_ADDRESS.lower():
                return mock_usdc
            if addr == bot.CTF_EXCHANGE_ADDRESS.lower():
                return mock_ctf_exchange
            if addr == bot.CONDITIONAL_TOKENS_ADDRESS.lower():
                return mock_conditional_tokens
            if addr == bot.NEG_RISK_ADAPTER_ADDRESS.lower():
                return mock_neg_risk_adapter
            return MagicMock()

        w3.eth.contract.side_effect = contract_factory

        cfg = dict(bot.DEFAULT_CONFIG)
        cfg["dry_run"] = dry_run
        cfg["proxy_redeem"] = proxy_redeem
        cfg["proxy_withdraw"] = proxy_withdraw

        executor = bot.TradeExecutor(
            w3=w3,
            private_key="0x" + "a" * 64,
            cfg=cfg,
            logger=logging.getLogger("test_proxy_redeem"),
        )
        executor.conditional_tokens = mock_conditional_tokens
        executor.usdc = mock_usdc
        executor.ctf_exchange = mock_ctf_exchange
        executor.neg_risk_adapter = mock_neg_risk_adapter
        executor.clob_client = MagicMock()
        executor.clob_client.clob_sdk = MagicMock()
        executor._sign_and_send = MagicMock()
        executor._base_tx_params = MagicMock(return_value={
            "from": executor.address, "nonce": 0,
            "maxFeePerGas": 100, "maxPriorityFeePerGas": 30,
            "chainId": 137,
        })
        executor.get_usdc_balance = MagicMock(return_value=Decimal("500"))
        return executor

    def test_redeem_via_proxy(self):
        """Test redeem_via_proxy encodes and sends correctly."""
        executor = self._make_executor()
        cond_id = "0x" + "a" * 64
        executor.conditional_tokens.encode_abi = MagicMock(return_value=b"\x01\x02")

        mock_proxy_contract = MagicMock()
        mock_proxy_contract.functions.execute.return_value.build_transaction.return_value = {}
        executor.w3.eth.contract.side_effect = None
        executor.w3.eth.contract.return_value = mock_proxy_contract

        mock_receipt = MagicMock()
        mock_receipt.status = 1
        executor._sign_and_send.return_value = mock_receipt

        result = executor.redeem_via_proxy(self.PROXY_ADDR, cond_id)
        self.assertEqual(result.status, 1)
        executor.conditional_tokens.encode_abi.assert_called_once()
        executor._sign_and_send.assert_called_once()

    def test_withdraw_usdc_from_proxy(self):
        """Test USDC withdrawal from proxy to EOA."""
        executor = self._make_executor()
        executor.usdc.functions.balanceOf.return_value.call.return_value = 200_000000
        executor.usdc.encode_abi = MagicMock(return_value=b"\x03\x04")

        mock_proxy_contract = MagicMock()
        mock_proxy_contract.functions.execute.return_value.build_transaction.return_value = {}
        executor.w3.eth.contract.side_effect = None
        executor.w3.eth.contract.return_value = mock_proxy_contract

        mock_receipt = MagicMock()
        mock_receipt.status = 1
        mock_receipt.transactionHash.hex.return_value = "0xwithdraw123"
        executor._sign_and_send.return_value = mock_receipt

        result = executor.withdraw_usdc_from_proxy(self.PROXY_ADDR)
        self.assertEqual(result.status, 1)
        executor.usdc.encode_abi.assert_called_once()

    def test_withdraw_zero_balance_returns_none(self):
        """No withdrawal when proxy USDC balance is 0."""
        executor = self._make_executor()
        executor.usdc.functions.balanceOf.return_value.call.return_value = 0

        result = executor.withdraw_usdc_from_proxy(self.PROXY_ADDR)
        self.assertIsNone(result)
        executor._sign_and_send.assert_not_called()

    def test_withdraw_specific_amount(self):
        """Withdraw a specific amount rather than full balance."""
        executor = self._make_executor()
        executor.usdc.encode_abi = MagicMock(return_value=b"\x05\x06")

        mock_proxy_contract = MagicMock()
        mock_proxy_contract.functions.execute.return_value.build_transaction.return_value = {}
        executor.w3.eth.contract.side_effect = None
        executor.w3.eth.contract.return_value = mock_proxy_contract

        mock_receipt = MagicMock()
        mock_receipt.status = 1
        executor._sign_and_send.return_value = mock_receipt

        result = executor.withdraw_usdc_from_proxy(self.PROXY_ADDR, amount=Decimal("50"))
        self.assertEqual(result.status, 1)
        # Verify transfer args: amount should be 50 * 1000000
        call_args = executor.usdc.encode_abi.call_args
        self.assertEqual(call_args[1]["args"][1], 50_000000)


class TestScanAndRedeemProxyPortfolio(unittest.TestCase):
    """Test the full proxy portfolio scan, redeem, and withdraw flow."""

    PROXY_ADDR = "0x" + "P" * 40
    TOK_RESOLVED = "11111111111111111111"
    TOK_ACTIVE = "22222222222222222222"
    TOK_EMPTY = "33333333333333333333"

    def _make_executor(self, dry_run=False, proxy_redeem=True, proxy_withdraw=True):
        w3 = MagicMock()
        mock_account = MagicMock()
        mock_account.address = "0x" + "1" * 40
        w3.eth.account.from_key.return_value = mock_account

        mock_usdc = MagicMock()
        mock_ctf_exchange = MagicMock()
        mock_conditional_tokens = MagicMock()
        mock_neg_risk_adapter = MagicMock()
        # Default: neg risk adapter reports 0 balance (no wrapped tokens)
        mock_neg_risk_adapter.functions.balanceOf.return_value.call.return_value = 0

        def contract_factory(address, abi):
            addr = address.lower() if hasattr(address, "lower") else address
            if addr == bot.USDC_ADDRESS.lower():
                return mock_usdc
            if addr == bot.CTF_EXCHANGE_ADDRESS.lower():
                return mock_ctf_exchange
            if addr == bot.CONDITIONAL_TOKENS_ADDRESS.lower():
                return mock_conditional_tokens
            if addr == bot.NEG_RISK_ADAPTER_ADDRESS.lower():
                return mock_neg_risk_adapter
            return MagicMock()

        w3.eth.contract.side_effect = contract_factory

        cfg = dict(bot.DEFAULT_CONFIG)
        cfg["dry_run"] = dry_run
        cfg["proxy_redeem"] = proxy_redeem
        cfg["proxy_withdraw"] = proxy_withdraw

        executor = bot.TradeExecutor(
            w3=w3,
            private_key="0x" + "a" * 64,
            cfg=cfg,
            logger=logging.getLogger("test_proxy_scan"),
        )
        executor.conditional_tokens = mock_conditional_tokens
        executor.usdc = mock_usdc
        executor.ctf_exchange = mock_ctf_exchange
        executor.neg_risk_adapter = mock_neg_risk_adapter
        executor.clob_client = MagicMock()
        executor.clob_client.clob_sdk = MagicMock()
        executor._sign_and_send = MagicMock()
        executor._base_tx_params = MagicMock(return_value={
            "from": executor.address, "nonce": 0,
            "maxFeePerGas": 100, "maxPriorityFeePerGas": 30,
            "chainId": 137,
        })
        executor.get_usdc_balance = MagicMock(return_value=Decimal("500"))
        return executor

    def test_disabled_via_config(self):
        executor = self._make_executor(proxy_redeem=False)
        results = executor.scan_and_redeem_proxy_portfolio()
        self.assertEqual(results, [])

    def test_no_clob_client_skips(self):
        executor = self._make_executor()
        executor.clob_client = None
        results = executor.scan_and_redeem_proxy_portfolio()
        self.assertEqual(results, [])

    def test_no_proxy_wallet_returns_empty(self):
        executor = self._make_executor()
        executor.discover_proxy_wallet = MagicMock(return_value=None)
        results = executor.scan_and_redeem_proxy_portfolio()
        self.assertEqual(results, [])

    def test_redeems_resolved_proxy_position(self):
        """Full flow: discovers proxy, redeems resolved position, withdraws USDC."""
        executor = self._make_executor()
        executor.discover_proxy_wallet = MagicMock(return_value=self.PROXY_ADDR)
        executor.get_proxy_usdc_balance = MagicMock(return_value=Decimal("50"))

        executor.clob_client.get_wallet_token_ids.return_value = {self.TOK_RESOLVED}

        # Mock proxy token balance on conditional tokens contract
        executor.conditional_tokens.functions.balanceOf.return_value.call.return_value = 30_000000

        executor.clob_client.get_market_by_token.return_value = {
            "condition_id": "0x" + "a" * 64,
            "closed": True,
            "active": False,
            "question": "Test resolved?",
        }
        executor.conditional_tokens.functions.payoutDenominator.return_value.call.return_value = 1000000

        # Mock redeem via proxy
        mock_receipt = MagicMock()
        mock_receipt.status = 1
        mock_receipt.transactionHash.hex.return_value = "0xproxyredeem123"
        executor.redeem_via_proxy = MagicMock(return_value=mock_receipt)

        # Mock withdrawal
        withdraw_receipt = MagicMock()
        withdraw_receipt.status = 1
        withdraw_receipt.transactionHash.hex.return_value = "0xproxywithdraw456"
        executor.withdraw_usdc_from_proxy = MagicMock(return_value=withdraw_receipt)
        # After redeem, proxy now has USDC to withdraw
        executor.get_proxy_usdc_balance = MagicMock(
            side_effect=[Decimal("50"), Decimal("80")]
        )

        results = executor.scan_and_redeem_proxy_portfolio()

        redeemed = [r for r in results if r.get("status") == "redeemed"]
        withdrawn = [r for r in results if r.get("status") == "withdrawn"]
        self.assertEqual(len(redeemed), 1)
        self.assertEqual(redeemed[0]["source"], "proxy")
        self.assertEqual(len(withdrawn), 1)
        self.assertEqual(withdrawn[0]["source"], "proxy_withdrawal")

    def test_skips_active_positions_in_proxy(self):
        """Positions not resolved on-chain (payoutDenominator == 0) are not redeemed."""
        executor = self._make_executor()
        executor.discover_proxy_wallet = MagicMock(return_value=self.PROXY_ADDR)
        executor.get_proxy_usdc_balance = MagicMock(return_value=Decimal("0"))
        executor.clob_client.get_wallet_token_ids.return_value = {self.TOK_ACTIVE}
        executor.conditional_tokens.functions.balanceOf.return_value.call.return_value = 10_000000
        executor.clob_client.get_market_by_token.return_value = {
            "condition_id": "0x" + "b" * 64,
            "closed": False,
            "active": True,
        }
        # On-chain: oracle has NOT reported yet
        executor.conditional_tokens.functions.payoutDenominator.return_value.call.return_value = 0

        results = executor.scan_and_redeem_proxy_portfolio()
        # No redemptions, no withdrawals (proxy balance is 0)
        self.assertEqual(results, [])

    def test_dry_run_does_not_execute(self):
        executor = self._make_executor(dry_run=True)
        executor.discover_proxy_wallet = MagicMock(return_value=self.PROXY_ADDR)
        executor.get_proxy_usdc_balance = MagicMock(return_value=Decimal("100"))
        executor.clob_client.get_wallet_token_ids.return_value = {self.TOK_RESOLVED}
        executor.conditional_tokens.functions.balanceOf.return_value.call.return_value = 50_000000
        executor.clob_client.get_market_by_token.return_value = {
            "condition_id": "0x" + "a" * 64,
            "closed": True,
            "active": False,
            "question": "Test?",
        }
        executor.conditional_tokens.functions.payoutDenominator.return_value.call.return_value = 1000000

        results = executor.scan_and_redeem_proxy_portfolio()

        dry_runs = [r for r in results if r.get("status") == "dry_run"]
        self.assertTrue(len(dry_runs) >= 1)
        executor._sign_and_send.assert_not_called()

    def test_withdraw_disabled_skips_withdrawal(self):
        """When proxy_withdraw is False, only redemption happens."""
        executor = self._make_executor(proxy_withdraw=False)
        executor.discover_proxy_wallet = MagicMock(return_value=self.PROXY_ADDR)
        executor.get_proxy_usdc_balance = MagicMock(return_value=Decimal("100"))
        executor.clob_client.get_wallet_token_ids.return_value = {self.TOK_RESOLVED}
        executor.conditional_tokens.functions.balanceOf.return_value.call.return_value = 50_000000
        executor.clob_client.get_market_by_token.return_value = {
            "condition_id": "0x" + "a" * 64,
            "closed": True,
            "active": False,
            "question": "Test?",
        }
        executor.conditional_tokens.functions.payoutDenominator.return_value.call.return_value = 1000000

        mock_receipt = MagicMock()
        mock_receipt.status = 1
        mock_receipt.transactionHash.hex.return_value = "0xredeem789"
        executor.redeem_via_proxy = MagicMock(return_value=mock_receipt)

        results = executor.scan_and_redeem_proxy_portfolio()

        redeemed = [r for r in results if r.get("status") == "redeemed"]
        withdrawn = [r for r in results if r.get("status") == "withdrawn"]
        self.assertEqual(len(redeemed), 1)
        self.assertEqual(len(withdrawn), 0)  # no withdrawal

    def test_no_trade_history_but_proxy_has_usdc(self):
        """When no trades found but proxy holds USDC, still withdraws."""
        executor = self._make_executor()
        executor.discover_proxy_wallet = MagicMock(return_value=self.PROXY_ADDR)
        executor.get_proxy_usdc_balance = MagicMock(return_value=Decimal("75"))
        executor.clob_client.get_wallet_token_ids.return_value = set()

        withdraw_receipt = MagicMock()
        withdraw_receipt.status = 1
        withdraw_receipt.transactionHash.hex.return_value = "0xwithdraw_only"
        executor.withdraw_usdc_from_proxy = MagicMock(return_value=withdraw_receipt)

        results = executor.scan_and_redeem_proxy_portfolio()

        withdrawn = [r for r in results if r.get("status") == "withdrawn"]
        self.assertEqual(len(withdrawn), 1)
        self.assertAlmostEqual(withdrawn[0]["amount_usdc"], 75.0, places=1)


class TestSafeExecution(unittest.TestCase):
    """Test Gnosis Safe transaction execution."""

    SAFE_ADDR = "0x" + "Bb" * 20

    def _make_executor(self, is_safe=True):
        w3 = MagicMock()
        mock_account = MagicMock()
        mock_account.address = "0x" + "1" * 40
        w3.eth.account.from_key.return_value = mock_account

        mock_usdc = MagicMock()
        mock_ctf_exchange = MagicMock()
        mock_conditional_tokens = MagicMock()

        def contract_factory(address, abi):
            addr = address.lower() if hasattr(address, "lower") else address
            if addr == bot.USDC_ADDRESS.lower():
                return mock_usdc
            if addr == bot.CTF_EXCHANGE_ADDRESS.lower():
                return mock_ctf_exchange
            if addr == bot.CONDITIONAL_TOKENS_ADDRESS.lower():
                return mock_conditional_tokens
            return MagicMock()

        w3.eth.contract.side_effect = contract_factory

        cfg = dict(bot.DEFAULT_CONFIG)
        executor = bot.TradeExecutor(
            w3=w3,
            private_key="0x" + "a" * 64,
            cfg=cfg,
            logger=logging.getLogger("test_safe_exec"),
        )
        executor.conditional_tokens = mock_conditional_tokens
        executor.usdc = mock_usdc
        executor._proxy_is_safe = is_safe
        executor._sign_and_send = MagicMock()
        executor._base_tx_params = MagicMock(return_value={
            "from": executor.address, "nonce": 0,
            "maxFeePerGas": 100, "maxPriorityFeePerGas": 30,
            "chainId": 137,
        })
        return executor

    def test_dispatch_to_safe_when_is_safe(self):
        """_execute_via_proxy dispatches to Safe when _proxy_is_safe is True."""
        executor = self._make_executor(is_safe=True)
        mock_safe = MagicMock()
        mock_safe.functions.nonce.return_value.call.return_value = 5
        mock_safe.functions.getTransactionHash.return_value.call.return_value = b"\x00" * 32
        mock_safe.functions.execTransaction.return_value.build_transaction.return_value = {}
        executor.w3.eth.contract.side_effect = None
        executor.w3.eth.contract.return_value = mock_safe
        executor.w3.eth.account.signHash.return_value = MagicMock(r=1, s=2, v=27)

        mock_receipt = MagicMock()
        mock_receipt.status = 1
        executor._sign_and_send.return_value = mock_receipt

        result = executor._execute_via_proxy(self.SAFE_ADDR, bot.USDC_ADDRESS, b"\x01")
        self.assertEqual(result.status, 1)
        mock_safe.functions.execTransaction.assert_called_once()

    def test_dispatch_to_legacy_when_not_safe(self):
        """_execute_via_proxy dispatches to legacy when _proxy_is_safe is False."""
        executor = self._make_executor(is_safe=False)
        mock_proxy = MagicMock()
        mock_proxy.functions.execute.return_value.build_transaction.return_value = {}
        executor.w3.eth.contract.side_effect = None
        executor.w3.eth.contract.return_value = mock_proxy

        mock_receipt = MagicMock()
        mock_receipt.status = 1
        executor._sign_and_send.return_value = mock_receipt

        result = executor._execute_via_proxy(self.SAFE_ADDR, bot.USDC_ADDRESS, b"\x01")
        self.assertEqual(result.status, 1)
        mock_proxy.functions.execute.assert_called_once()

    def test_safe_signs_transaction_hash(self):
        """Safe execution signs the hash and passes signature to execTransaction."""
        executor = self._make_executor(is_safe=True)
        safe_tx_hash = b"\xab" * 32
        mock_safe = MagicMock()
        mock_safe.functions.nonce.return_value.call.return_value = 0
        mock_safe.functions.getTransactionHash.return_value.call.return_value = safe_tx_hash
        mock_safe.functions.execTransaction.return_value.build_transaction.return_value = {}
        executor.w3.eth.contract.side_effect = None
        executor.w3.eth.contract.return_value = mock_safe
        executor.w3.eth.account.unsafe_sign_hash.return_value = MagicMock(r=1, s=2, v=28)
        executor._sign_and_send.return_value = MagicMock(status=1)

        executor._execute_via_safe(self.SAFE_ADDR, bot.USDC_ADDRESS, b"\x01")

        # Verify unsafe_sign_hash (web3 v7+) was called with the Safe tx hash
        executor.w3.eth.account.unsafe_sign_hash.assert_called_once_with(
            safe_tx_hash, executor.private_key,
        )


class TestProxyConfig(unittest.TestCase):
    """Test proxy-related configuration defaults and env var overrides."""

    def test_default_config_has_proxy_settings(self):
        self.assertTrue(bot.DEFAULT_CONFIG.get("proxy_redeem", False))
        self.assertTrue(bot.DEFAULT_CONFIG.get("proxy_withdraw", False))
        self.assertEqual(bot.DEFAULT_CONFIG.get("proxy_address"), "")

    def test_proxy_factory_address_is_set(self):
        self.assertTrue(bot.PROXY_FACTORY_ADDRESS.startswith("0x"))
        self.assertEqual(len(bot.PROXY_FACTORY_ADDRESS), 42)

    def test_safe_proxy_factory_address_is_set(self):
        self.assertTrue(bot.SAFE_PROXY_FACTORY_ADDRESS.startswith("0x"))
        self.assertEqual(len(bot.SAFE_PROXY_FACTORY_ADDRESS), 42)

    def test_gnosis_safe_abi_has_required_functions(self):
        names = [entry.get("name") for entry in bot.GNOSIS_SAFE_ABI]
        self.assertIn("execTransaction", names)
        self.assertIn("getTransactionHash", names)
        self.assertIn("nonce", names)

    def test_proxy_factory_abi_has_multiple_getters(self):
        names = [entry.get("name") for entry in bot.PROXY_FACTORY_ABI]
        self.assertIn("getProxy", names)
        self.assertIn("proxies", names)
        self.assertIn("proxyFor", names)

    def test_proxy_factory_abi_has_deploy_event(self):
        events = [e for e in bot.PROXY_FACTORY_ABI if e.get("type") == "event"]
        self.assertTrue(any(e["name"] == "Deploy" for e in events))

    def test_proxy_wallet_abi_has_execute(self):
        names = [entry.get("name") for entry in bot.PROXY_WALLET_ABI]
        self.assertIn("execute", names)


class TestMartingaleBot(unittest.TestCase):
    """Tests for the MartingaleBot class."""

    def _make_bot(self, cfg_overrides=None):
        cfg = dict(bot.DEFAULT_CONFIG)
        cfg["martingale_enabled"] = True
        cfg["martingale_direction"] = "Up"
        cfg["martingale_start_bet"] = 5.0
        cfg["martingale_max_bet"] = 0
        cfg["martingale_max_streak"] = 0
        cfg["martingale_slug_base"] = "btc-updown-5m"
        cfg["martingale_window"] = 300
        cfg["martingale_poll_seconds"] = 10
        cfg["martingale_price_min"] = 0.01  # wide range for tests
        cfg["martingale_price_max"] = 0.99
        cfg["martingale_max_entry_seconds"] = 9999  # disable window-start guard for tests
        if cfg_overrides:
            cfg.update(cfg_overrides)
        logger = logging.getLogger("test_martingale")
        logger.handlers = [logging.NullHandler()]
        clob = MagicMock()
        clob.get_usdc_balance.return_value = 10000.0
        mb = bot.MartingaleBot.__new__(bot.MartingaleBot)
        mb.clob_client = clob
        mb.cfg = cfg
        mb.logger = logger
        mb._stop_event = MagicMock()
        mb.strategy = cfg_overrides or {}
        mb.strategy_name = mb.strategy.get("name", "default")
        mb.start_bet = cfg["martingale_start_bet"]
        mb.current_bet = cfg["martingale_start_bet"]
        mb.direction = cfg["martingale_direction"]
        mb.consecutive_losses = 0
        mb.session_pnl = 0.0
        mb._active_bet = None
        mb._bet_history = []
        mb._last_window_ts = 0
        mb._next_window_cache = None
        mb._skip_reason = None
        mb._windows_attempted = 0
        mb._windows_no_market = 0
        mb.notify_callback = None
        mb.log_trade_callback = None
        mb._streak_paused = False
        mb._streak_paused_at = None
        mb._recovery_candles = []
        mb._recovery_candle_open = None
        mb._recovery_candle_ts = 0
        mb._skip_price = None
        mb._skip_gap = None
        mb._miss_recorded_for_ts = 0
        mb._missed_windows = []
        return mb

    # -- slug generation --

    def test_generate_slug(self):
        mb = self._make_bot()
        slug = mb._generate_slug()
        self.assertTrue(slug.startswith("btc-updown-5m-"))
        ts_part = slug.split("-")[-1]
        self.assertTrue(ts_part.isdigit())
        # Should be aligned to 300s boundary
        self.assertEqual(int(ts_part) % 300, 0)

    def test_generate_slug_custom_base(self):
        mb = self._make_bot({"martingale_slug_base": "eth-updown-5m"})
        slug = mb._generate_slug()
        self.assertTrue(slug.startswith("eth-updown-5m-"))

    def test_get_window_ts_aligned(self):
        ts = bot.MartingaleBot._get_window_ts(300)
        self.assertEqual(ts % 300, 0)

    def test_get_window_end(self):
        mb = self._make_bot()
        end = mb._get_window_end()
        window_ts = mb._get_window_ts(300)
        self.assertEqual(end, window_ts + 300)

    # -- market fetching --

    def test_fetch_market_parses_event(self):
        mb = self._make_bot()
        mb.clob_client._get_public.return_value = [{
            "markets": [{
                "condition_id": "cid123",
                "question": "BTC Up or Down?",
                "clobTokenIds": '["tok_up", "tok_down"]',
                "outcomes": '["Up", "Down"]',
            }]
        }]
        result = mb._fetch_market("btc-updown-5m-1000")
        self.assertIsNotNone(result)
        self.assertEqual(result["condition_id"], "cid123")
        self.assertEqual(result["up_token"], "tok_up")
        self.assertEqual(result["down_token"], "tok_down")

    def test_fetch_market_returns_none_on_empty(self):
        mb = self._make_bot()
        mb.clob_client._get_public.return_value = []
        result = mb._fetch_market("nonexistent-slug")
        self.assertIsNone(result)

    def test_fetch_market_maps_down_first(self):
        """If outcomes are ['Down', 'Up'], tokens should be correctly mapped."""
        mb = self._make_bot()
        mb.clob_client._get_public.return_value = [{
            "markets": [{
                "condition_id": "cid456",
                "question": "BTC?",
                "clobTokenIds": '["tok_a", "tok_b"]',
                "outcomes": '["Down", "Up"]',
            }]
        }]
        result = mb._fetch_market("test-slug")
        self.assertEqual(result["up_token"], "tok_b")
        self.assertEqual(result["down_token"], "tok_a")

    # -- bet placement --

    def test_try_place_bet_success(self):
        mb = self._make_bot()
        mb.clob_client._get_public.return_value = [{
            "markets": [{
                "condition_id": "cid1",
                "question": "BTC Up?",
                "clobTokenIds": '["tok_up", "tok_down"]',
                "outcomes": '["Up", "Down"]',
            }]
        }]
        mb.clob_client.get_order_book.return_value = {
            "asks": [{"price": "0.50", "size": "100"}],
        }
        mb.clob_client.place_order.return_value = {
            "takingAmount": "10.0",
            "makingAmount": "5.0",
        }
        result = mb._try_place_bet()
        self.assertTrue(result)
        self.assertIsNotNone(mb._active_bet)
        self.assertEqual(mb._active_bet["direction"], "Up")
        self.assertEqual(mb._active_bet["shares"], 10.0)
        self.assertEqual(mb._active_bet["cost"], 5.0)
        mb.clob_client.place_order.assert_called_once()

    def test_try_place_bet_down_direction(self):
        mb = self._make_bot({"martingale_direction": "Down"})
        mb.clob_client._get_public.return_value = [{
            "markets": [{
                "condition_id": "cid1",
                "question": "BTC?",
                "clobTokenIds": '["tok_up", "tok_down"]',
                "outcomes": '["Up", "Down"]',
            }]
        }]
        mb.clob_client.get_order_book.return_value = {
            "asks": [{"price": "0.50", "size": "100"}],
        }
        mb.clob_client.place_order.return_value = {
            "takingAmount": "10.0",
            "makingAmount": "5.0",
        }
        mb._try_place_bet()
        self.assertIsNotNone(mb._active_bet)
        self.assertEqual(mb._active_bet["direction"], "Down")
        # Should have used the down_token
        call_args = mb.clob_client.place_order.call_args
        self.assertEqual(call_args[1]["token_id"], "tok_down")

    def test_try_place_bet_fok_rejected(self):
        mb = self._make_bot()
        mb.clob_client._get_public.return_value = [{
            "markets": [{
                "condition_id": "cid1",
                "question": "BTC?",
                "clobTokenIds": '["tok_up", "tok_down"]',
                "outcomes": '["Up", "Down"]',
            }]
        }]
        mb.clob_client.get_order_book.return_value = {
            "asks": [{"price": "0.50", "size": "100"}],
        }
        mb.clob_client.place_order.return_value = {
            "status": "fok_rejected",
        }
        mb._try_place_bet()
        self.assertIsNone(mb._active_bet)

    def test_try_place_bet_skips_same_window(self):
        mb = self._make_bot()
        mb._last_window_ts = mb._get_window_ts(300)
        mb._try_place_bet()
        # Should not even fetch market
        mb.clob_client._get_public.assert_not_called()

    def test_try_place_bet_max_streak_pauses(self):
        mb = self._make_bot({"martingale_max_streak": 3})
        mb.consecutive_losses = 3
        result = mb._try_place_bet()
        self.assertFalse(result)
        self.assertTrue(mb._streak_paused)
        # Should NOT stop the bot — it pauses and waits for recovery
        mb._stop_event.set.assert_not_called()

    def test_try_place_bet_max_bet_stops(self):
        mb = self._make_bot({"martingale_max_bet": 10.0})
        mb.current_bet = 20.0
        mb._try_place_bet()
        mb._stop_event.set.assert_called_once()

    def test_try_place_bet_price_too_high_rejected(self):
        """Price above price_max should be rejected."""
        mb = self._make_bot({
            "martingale_price_min": 0.40,
            "martingale_price_max": 0.55,
        })
        mb.clob_client._get_public.return_value = [{
            "markets": [{
                "condition_id": "cid1",
                "question": "BTC?",
                "clobTokenIds": '["tok_up", "tok_down"]',
                "outcomes": '["Up", "Down"]',
            }]
        }]
        mb.clob_client.get_order_book.return_value = {
            "asks": [{"price": "0.70", "size": "100"}],  # above max
        }
        result = mb._try_place_bet()
        self.assertFalse(result)
        self.assertIsNone(mb._active_bet)
        # Should NOT mark window as skipped — price might come back
        self.assertEqual(mb._last_window_ts, 0)

    def test_try_place_bet_price_too_low_rejected(self):
        """Price below price_min should be rejected."""
        mb = self._make_bot({
            "martingale_price_min": 0.40,
            "martingale_price_max": 0.55,
        })
        mb.clob_client._get_public.return_value = [{
            "markets": [{
                "condition_id": "cid1",
                "question": "BTC?",
                "clobTokenIds": '["tok_up", "tok_down"]',
                "outcomes": '["Up", "Down"]',
            }]
        }]
        mb.clob_client.get_order_book.return_value = {
            "asks": [{"price": "0.30", "size": "100"}],  # below min
        }
        result = mb._try_place_bet()
        self.assertFalse(result)
        self.assertIsNone(mb._active_bet)
        self.assertEqual(mb._last_window_ts, 0)

    def test_try_place_bet_price_in_range_accepted(self):
        """Price within [price_min, price_max] should be accepted."""
        mb = self._make_bot({
            "martingale_price_min": 0.40,
            "martingale_price_max": 0.55,
        })
        mb.clob_client._get_public.return_value = [{
            "markets": [{
                "condition_id": "cid1",
                "question": "BTC?",
                "clobTokenIds": '["tok_up", "tok_down"]',
                "outcomes": '["Up", "Down"]',
            }]
        }]
        mb.clob_client.get_order_book.return_value = {
            "asks": [{"price": "0.48", "size": "100"}],  # within range
        }
        mb.clob_client.place_order.return_value = {
            "takingAmount": "10.4",
            "makingAmount": "5.0",
        }
        result = mb._try_place_bet()
        self.assertTrue(result)
        self.assertIsNotNone(mb._active_bet)

    def test_try_place_bet_window_too_old(self):
        """Should skip if too far into the current window."""
        mb = self._make_bot({"martingale_max_entry_seconds": 10})
        # Don't set _last_window_ts so the same-window check passes,
        # but the seconds_into check should catch it (we're >10s into
        # the current 300s window unless the test happens to run right
        # on a boundary, so use a very small max_entry to force it).
        result = mb._try_place_bet()
        self.assertFalse(result)
        # Should mark window as skipped so we don't retry
        window_ts = mb._get_window_ts(300)
        self.assertEqual(mb._last_window_ts, window_ts)

    def test_cycle_returns_false_when_no_bet(self):
        """_cycle returns False when no active bet and bet not placed."""
        mb = self._make_bot({"martingale_max_entry_seconds": 0})
        result = mb._cycle()
        self.assertFalse(result)

    def test_cycle_returns_true_when_waiting_for_resolution(self):
        """_cycle returns True when active bet is still pending."""
        mb = self._make_bot()
        mb._active_bet = {
            "slug": "test",
            "condition_id": "cid1",
            "direction": "Up",
            "window_end": int(time.time()) + 300,  # still in window
        }
        result = mb._cycle()
        self.assertTrue(result)  # waiting for window to end

    # -- stale bet / reset --

    def test_load_state_discards_stale_active_bet(self):
        """A stale active_bet from a previous session is discarded."""
        mb = self._make_bot()
        window = 300
        stale_window_end = int(time.time()) - window * 3  # 3 windows ago
        state = {
            "current_bet": 20.0,
            "consecutive_losses": 2,
            "session_pnl": -15.0,
            "direction": "Up",
            "active_bet": {
                "slug": "btc-updown-5m-old",
                "condition_id": "cid_old",
                "direction": "Up",
                "window_end": stale_window_end,
                "bet_size": 10.0,
                "cost": 10.0,
                "shares": 20.0,
                "token_id": "tok",
                "question": "old",
                "ts": "2026-01-01",
            },
            "last_window_ts": stale_window_end - window,
        }
        with open(bot.MartingaleBot.STATE_FILE, "w") as fh:
            json.dump(state, fh)
        try:
            mb._load_state()
            # Stale bet should be discarded, NOT treated as a loss
            self.assertIsNone(mb._active_bet)
            # But the rest of the state (streak, bet size) should be preserved
            self.assertEqual(mb.current_bet, 20.0)
            self.assertEqual(mb.consecutive_losses, 2)
        finally:
            try:
                os.remove(bot.MartingaleBot.STATE_FILE)
            except OSError:
                pass

    def test_reset_state_clears_everything(self):
        """reset_state wipes bet/streak/pnl back to defaults."""
        mb = self._make_bot()
        mb.current_bet = 40.0
        mb.consecutive_losses = 4
        mb.session_pnl = -75.0
        mb._active_bet = {"dummy": True}
        mb._last_window_ts = 999999
        mb.reset_state()
        self.assertEqual(mb.current_bet, mb.start_bet)
        self.assertEqual(mb.consecutive_losses, 0)
        self.assertEqual(mb.session_pnl, 0.0)
        self.assertIsNone(mb._active_bet)
        self.assertEqual(mb._last_window_ts, 0)

    # -- win/loss handling --

    def test_handle_win_resets_bet(self):
        mb = self._make_bot()
        mb.consecutive_losses = 3
        mb.current_bet = 40.0
        bet = {
            "direction": "Up",
            "shares": 10.0,
            "cost": 5.0,
            "bet_size": 40.0,
            "question": "BTC?",
            "token_id": "tok_up",
        }
        mb._handle_win(bet)
        self.assertEqual(mb.current_bet, 5.0)  # reset to start_bet
        self.assertEqual(mb.consecutive_losses, 0)
        self.assertAlmostEqual(mb.session_pnl, 5.0)  # 10 shares - $5 cost
        self.assertIsNone(mb._active_bet)

    def test_handle_loss_doubles_bet(self):
        mb = self._make_bot()
        mb.current_bet = 5.0
        bet = {
            "direction": "Up",
            "shares": 10.0,
            "cost": 5.0,
            "bet_size": 5.0,
            "question": "BTC?",
            "token_id": "tok_up",
        }
        mb._handle_loss(bet)
        self.assertEqual(mb.current_bet, 10.0)
        self.assertEqual(mb.consecutive_losses, 1)
        self.assertAlmostEqual(mb.session_pnl, -5.0)
        self.assertIsNone(mb._active_bet)

    def test_handle_loss_caps_at_max_bet(self):
        mb = self._make_bot({"martingale_max_bet": 15.0})
        mb.current_bet = 10.0
        mb.consecutive_losses = 1  # simulate one prior loss
        bet = {
            "direction": "Up",
            "shares": 20.0,
            "cost": 10.0,
            "bet_size": 10.0,
            "question": "BTC?",
            "token_id": "tok_up",
        }
        mb._handle_loss(bet)
        # geometric: 5.0 * 2^2 = 20.0, capped at 15.0
        self.assertEqual(mb.current_bet, 15.0)

    def test_martingale_sequence(self):
        """Simulate: lose, lose, win — verify correct bet progression."""
        mb = self._make_bot()

        # Loss 1: $5 bet
        mb._handle_loss({
            "direction": "Up", "shares": 10.0, "cost": 5.0,
            "bet_size": 5.0, "question": "BTC?", "token_id": "t1",
        })
        self.assertEqual(mb.current_bet, 10.0)
        self.assertEqual(mb.consecutive_losses, 1)

        # Loss 2: $10 bet
        mb._handle_loss({
            "direction": "Up", "shares": 20.0, "cost": 10.0,
            "bet_size": 10.0, "question": "BTC?", "token_id": "t1",
        })
        self.assertEqual(mb.current_bet, 20.0)
        self.assertEqual(mb.consecutive_losses, 2)

        # Win: $20 bet, 40 shares, cost $20
        mb._handle_win({
            "direction": "Up", "shares": 40.0, "cost": 20.0,
            "bet_size": 20.0, "question": "BTC?", "token_id": "t1",
        })
        self.assertEqual(mb.current_bet, 5.0)  # reset
        self.assertEqual(mb.consecutive_losses, 0)
        # P&L: -5 -10 + (40-20) = +5
        self.assertAlmostEqual(mb.session_pnl, 5.0)

    def test_martingale_geometric_ignores_cost_drift(self):
        """Bet size must follow start_bet × 2^n even when exchange bumps cost."""
        mb = self._make_bot()
        # start_bet = 5.0, but exchange bumped actual cost to 5.20
        mb._handle_loss({
            "direction": "Up", "shares": 10.0, "cost": 5.20,
            "bet_size": 5.0, "question": "BTC?", "token_id": "t1",
        })
        # Should be 5.0 × 2^1 = 10.0, NOT 5.20 × 2 = 10.40
        self.assertEqual(mb.current_bet, 10.0)

        # Second loss: exchange bumps cost again
        mb._handle_loss({
            "direction": "Up", "shares": 20.0, "cost": 10.40,
            "bet_size": 10.0, "question": "BTC?", "token_id": "t1",
        })
        # Should be 5.0 × 2^2 = 20.0, NOT 10.40 × 2 = 20.80
        self.assertEqual(mb.current_bet, 20.0)

    # -- resolution detection --

    def test_check_resolution_not_closed(self):
        mb = self._make_bot()
        mb.clob_client._get_public.return_value = [{
            "closed": False,
            "active": True,
            "outcomes": '["Up", "Down"]',
            "outcomePrices": '[0.55, 0.45]',
        }]
        resolved, won = mb._check_resolution("cid1", "Up")
        self.assertFalse(resolved)

    def test_check_resolution_up_wins(self):
        mb = self._make_bot()
        mb.clob_client._get_public.return_value = [{
            "closed": True,
            "active": False,
            "outcomes": '["Up", "Down"]',
            "outcomePrices": '[1.0, 0.0]',
        }]
        resolved, won = mb._check_resolution("cid1", "Up")
        self.assertTrue(resolved)
        self.assertTrue(won)

    def test_check_resolution_down_wins(self):
        mb = self._make_bot()
        mb.clob_client._get_public.return_value = [{
            "closed": True,
            "active": False,
            "outcomes": '["Up", "Down"]',
            "outcomePrices": '[0.0, 1.0]',
        }]
        resolved, won = mb._check_resolution("cid1", "Up")
        self.assertTrue(resolved)
        self.assertFalse(won)

    def test_check_resolution_bet_on_down_and_down_wins(self):
        mb = self._make_bot()
        mb.clob_client._get_public.return_value = [{
            "closed": True,
            "active": False,
            "outcomes": '["Up", "Down"]',
            "outcomePrices": '[0.0, 1.0]',
        }]
        resolved, won = mb._check_resolution("cid1", "Down")
        self.assertTrue(resolved)
        self.assertTrue(won)

    def test_check_resolution_orderbook_win(self):
        """Orderbook fallback detects a win when ask >= 0.95."""
        mb = self._make_bot()
        mb.clob_client.get_order_book.return_value = {
            "asks": [{"price": "0.97", "size": "50"}],
        }
        bet = {"token_id": "tok_up", "direction": "Up"}
        result = mb._check_resolution_orderbook(bet)
        self.assertTrue(result)

    def test_check_resolution_orderbook_loss(self):
        """Orderbook fallback detects a loss when ask <= 0.05."""
        mb = self._make_bot()
        mb.clob_client.get_order_book.return_value = {
            "asks": [{"price": "0.03", "size": "50"}],
        }
        bet = {"token_id": "tok_up", "direction": "Up"}
        result = mb._check_resolution_orderbook(bet)
        self.assertFalse(result)

    def test_check_resolution_orderbook_inconclusive(self):
        """Orderbook fallback returns None for mid-range prices."""
        mb = self._make_bot()
        mb.clob_client.get_order_book.return_value = {
            "asks": [{"price": "0.55", "size": "50"}],
        }
        bet = {"token_id": "tok_up", "direction": "Up"}
        result = mb._check_resolution_orderbook(bet)
        self.assertIsNone(result)

    def test_cycle_uses_orderbook_fallback(self):
        """Cycle uses orderbook fallback when API says not resolved."""
        mb = self._make_bot()
        # API says market not closed
        mb.clob_client._get_public.return_value = [{
            "closed": False,
            "active": True,
            "outcomes": '["Up", "Down"]',
            "outcomePrices": '[0.55, 0.45]',
        }]
        # But orderbook shows Up won (ask at 0.97)
        mb.clob_client.get_order_book.return_value = {
            "asks": [{"price": "0.97", "size": "50"}],
        }
        mb._active_bet = {
            "slug": "btc-updown-5m-test",
            "condition_id": "cid1",
            "token_id": "tok_up",
            "direction": "Up",
            "bet_size": 10.0,
            "price": 0.50,
            "shares": 20.0,
            "cost": 10.0,
            "question": "BTC?",
            "window_end": int(time.time()) - 120,  # ended 2 min ago
            "ts": "2026-01-01",
        }
        mb.log_trade_callback = MagicMock()
        result = mb._cycle()
        self.assertFalse(result)  # resolved, switch to fast poll
        self.assertIsNone(mb._active_bet)
        self.assertEqual(mb.current_bet, mb.start_bet)  # win resets bet

    # -- cycle state machine --

    def test_cycle_places_bet_when_no_active(self):
        mb = self._make_bot()
        mb.clob_client._get_public.return_value = [{
            "markets": [{
                "condition_id": "cid1",
                "question": "BTC?",
                "clobTokenIds": '["tok_up", "tok_down"]',
                "outcomes": '["Up", "Down"]',
            }]
        }]
        mb.clob_client.get_order_book.return_value = {
            "asks": [{"price": "0.50", "size": "100"}],
        }
        mb.clob_client.place_order.return_value = {
            "takingAmount": "10.0",
            "makingAmount": "5.0",
        }
        mb._cycle()
        self.assertIsNotNone(mb._active_bet)

    def test_cycle_checks_resolution_when_active(self):
        mb = self._make_bot()
        mb._active_bet = {
            "slug": "test-slug",
            "condition_id": "cid1",
            "direction": "Up",
            "shares": 10.0,
            "cost": 5.0,
            "bet_size": 5.0,
            "question": "BTC?",
            "token_id": "tok_up",
            "window_end": 0,  # already expired
        }
        # Market resolved, Up won
        mb.clob_client._get_public.return_value = [{
            "closed": True,
            "active": False,
            "outcomes": '["Up", "Down"]',
            "outcomePrices": '[1.0, 0.0]',
        }]
        mb._cycle()
        # Should have handled win: reset bet, clear active_bet
        self.assertIsNone(mb._active_bet)
        self.assertEqual(mb.current_bet, 5.0)
        self.assertAlmostEqual(mb.session_pnl, 5.0)

    # -- persistence --

    def test_save_and_load_state(self):
        mb = self._make_bot()
        mb.current_bet = 20.0
        mb.consecutive_losses = 3
        mb.session_pnl = -15.0
        mb.direction = "Down"
        with tempfile.TemporaryDirectory() as tmpdir:
            mb.STATE_FILE = os.path.join(tmpdir, "mart_state.json")
            mb._save_state()
            # Create a fresh bot and load
            mb2 = self._make_bot()
            mb2.STATE_FILE = mb.STATE_FILE
            mb2._load_state()
            self.assertEqual(mb2.current_bet, 20.0)
            self.assertEqual(mb2.consecutive_losses, 3)
            self.assertAlmostEqual(mb2.session_pnl, -15.0)
            self.assertEqual(mb2.direction, "Down")

    # -- trade logging --

    def test_log_bet_calls_callback(self):
        mb = self._make_bot()
        callback = MagicMock()
        mb.log_trade_callback = callback
        bet = {
            "direction": "Up",
            "shares": 10.0,
            "cost": 5.0,
            "bet_size": 5.0,
            "question": "BTC?",
            "token_id": "tok_up",
        }
        mb._log_bet(bet, won=True, profit=5.0)
        callback.assert_called_once()
        record = callback.call_args[0][0]
        self.assertEqual(record["outcome"], "won")
        self.assertEqual(record["reason"], "martingale")
        self.assertEqual(record["pnl_usdc"], 5.0)

    # -- status summary --

    def test_get_status_summary(self):
        mb = self._make_bot()
        mb.current_bet = 10.0
        mb.consecutive_losses = 2
        mb.session_pnl = -15.0
        summary = mb.get_status_summary()
        self.assertIn("Down" if mb.direction == "Down" else "Up", summary)
        self.assertIn("$10.00", summary)
        self.assertIn("Streak: 2", summary)


if __name__ == "__main__":
    unittest.main()
