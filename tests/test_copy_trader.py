"""
Unit tests for the Polymarket Copy Trader Bot.

Tests core logic using mocks — no real blockchain or API calls needed.
Run with: python -m pytest tests/ -v
"""

import json
import logging
import os
import tempfile
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

import polymarket_copy_trader as bot


class TestConfigHelpers(unittest.TestCase):
    """Test config load/save and private key management."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.config_path = os.path.join(self.tmpdir, "test_config.json")
        self.pk_path = os.path.join(self.tmpdir, "test_pk")

    def tearDown(self):
        for f in [self.config_path, self.pk_path]:
            if os.path.exists(f):
                os.remove(f)
        os.rmdir(self.tmpdir)

    def test_save_and_load_config(self):
        cfg = dict(bot.DEFAULT_CONFIG)
        cfg["copy_percentage"] = 75
        cfg["watched_addresses"] = ["0x" + "a" * 40]
        bot.save_config(cfg, self.config_path)

        loaded = bot.load_config(self.config_path)
        self.assertEqual(loaded["copy_percentage"], 75)
        self.assertEqual(loaded["watched_addresses"], ["0x" + "a" * 40])

    def test_load_config_creates_default(self):
        cfg = bot.load_config(self.config_path)
        self.assertTrue(os.path.exists(self.config_path))
        self.assertEqual(cfg["copy_percentage"], 50)
        self.assertEqual(cfg["watched_addresses"], [])

    def test_load_config_merges_defaults(self):
        """Config file with missing keys gets defaults merged in."""
        with open(self.config_path, "w") as f:
            json.dump({"rpc_url": "http://test"}, f)
        cfg = bot.load_config(self.config_path)
        self.assertEqual(cfg["rpc_url"], "http://test")
        self.assertEqual(cfg["copy_percentage"], 50)  # default merged

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
    """Test minimum order size enforcement and FOK order placement."""

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
        client.clob_sdk = MagicMock()
        client.clob_sdk.create_market_order.return_value = {"signed": True}
        client.clob_sdk.post_order.return_value = {"orderID": "test123"}
        return client

    def _get_order_amount(self, client):
        """Extract the amount (USDC) kwarg passed to MarketOrderArgs."""
        call_kwargs = bot.MarketOrderArgs.call_args[1]
        return call_kwargs["amount"]

    def _get_order_type(self, client):
        """Extract the order_type kwarg passed to MarketOrderArgs."""
        call_kwargs = bot.MarketOrderArgs.call_args[1]
        return call_kwargs["order_type"]

    def test_normal_size_not_bumped(self):
        """When USDC >= $1 and tokens >= 5, amount is not changed."""
        client = self._make_client()
        # $5 USDC at price 0.50 -> effective = max(5, 2.5, 1) = $5
        client.place_order("token1", "BUY", 5.0, 0.50)
        self.assertEqual(self._get_order_amount(client), 5.0)

    def test_small_tokens_bumped(self):
        """When tokens < 5, USDC amount bumped to cover 5 tokens."""
        client = self._make_client()
        # $2 USDC at price 0.90 -> effective = max(2, 4.5, 1) = $4.50
        client.place_order("token1", "BUY", 2.0, 0.90)
        self.assertEqual(self._get_order_amount(client), 4.5)

    def test_small_notional_bumped(self):
        """When USDC < $1 notional minimum, bumped to $1."""
        client = self._make_client()
        # $0.30 USDC at price 0.05 -> effective = max(0.30, 0.25, 1.0) = $1.0
        client.place_order("token1", "BUY", 0.30, 0.05)
        self.assertEqual(self._get_order_amount(client), 1.0)

    def test_both_minimums_token_wins(self):
        """When both minimums trigger, the larger USDC requirement wins."""
        client = self._make_client()
        # $0.50 at price 0.80 -> effective = max(0.50, 4.0, 1.0) = $4.0
        client.place_order("token1", "BUY", 0.50, 0.80)
        self.assertEqual(self._get_order_amount(client), 4.0)

    def test_both_minimums_notional_wins(self):
        """When notional minimum requires more USDC than token minimum."""
        client = self._make_client()
        # $0.50 at price 0.10 -> effective = max(0.50, 0.50, 1.0) = $1.0
        client.place_order("token1", "BUY", 0.50, 0.10)
        self.assertEqual(self._get_order_amount(client), 1.0)

    def test_zero_price_returns_none(self):
        """Price of 0 should return None, not divide by zero."""
        client = self._make_client()
        result = client.place_order("token1", "BUY", 5.0, 0.0)
        self.assertIsNone(result)
        client.clob_sdk.create_market_order.assert_not_called()

    def test_exact_minimum_not_bumped(self):
        """Exactly 5 tokens and >= $1 USDC should not be bumped."""
        client = self._make_client()
        # $5 USDC at price 1.0 -> effective = max(5, 5, 1) = $5.0
        client.place_order("token1", "BUY", 5.0, 1.0)
        self.assertEqual(self._get_order_amount(client), 5.0)

    def test_order_uses_fok_type(self):
        """Orders should use Fill-or-Kill order type."""
        client = self._make_client()
        client.place_order("token1", "BUY", 5.0, 0.50)
        self.assertEqual(self._get_order_type(client), bot.OrderType.FOK)
        # post_order should also be called with FOK
        post_kwargs = client.clob_sdk.post_order.call_args[1]
        self.assertEqual(post_kwargs["orderType"], bot.OrderType.FOK)


class TestTradeExecutorCopyAmount(unittest.TestCase):
    """Test compute_copy_amount scaling logic."""

    def _make_executor(self, copy_pct=50, max_trade=100, usdc_balance=1000):
        """Create a TradeExecutor with mocked Web3."""
        w3 = MagicMock()
        # Mock account
        mock_account = MagicMock()
        mock_account.address = "0x" + "1" * 40
        w3.eth.account.from_key.return_value = mock_account
        w3.eth.contract.return_value = MagicMock()

        cfg = dict(bot.DEFAULT_CONFIG)
        cfg["copy_percentage"] = copy_pct
        cfg["max_trade_usdc"] = max_trade

        executor = bot.TradeExecutor(
            w3=w3,
            private_key="0x" + "a" * 64,
            cfg=cfg,
            logger=logging.getLogger("test"),
        )
        # Mock USDC balance
        executor.get_usdc_balance = MagicMock(return_value=Decimal(str(usdc_balance)))
        return executor

    def test_basic_scaling(self):
        executor = self._make_executor(copy_pct=50, max_trade=1000, usdc_balance=1000)
        # 50% of 200 USDC = 100 USDC
        amount = executor.compute_copy_amount(Decimal("200"))
        self.assertEqual(amount, Decimal("100"))

    def test_capped_by_max_trade(self):
        executor = self._make_executor(copy_pct=100, max_trade=50, usdc_balance=1000)
        # 100% of 200 = 200, but max is 50
        amount = executor.compute_copy_amount(Decimal("200"))
        self.assertEqual(amount, Decimal("50"))

    def test_capped_by_balance(self):
        executor = self._make_executor(copy_pct=100, max_trade=10000, usdc_balance=100)
        # 100% of 200 = 200, balance cap = 100 * 0.95 = 95
        amount = executor.compute_copy_amount(Decimal("200"))
        self.assertEqual(amount, Decimal("95.0"))

    def test_zero_balance(self):
        executor = self._make_executor(copy_pct=50, max_trade=100, usdc_balance=0)
        amount = executor.compute_copy_amount(Decimal("200"))
        self.assertEqual(amount, Decimal("0"))

    def test_small_trade(self):
        executor = self._make_executor(copy_pct=10, max_trade=100, usdc_balance=1000)
        # 10% of 5 = 0.5
        amount = executor.compute_copy_amount(Decimal("5"))
        self.assertEqual(amount, Decimal("0.5"))

    def test_balance_fetch_failure_uses_max_trade_cap(self):
        executor = self._make_executor(copy_pct=100, max_trade=50, usdc_balance=1000)
        # Make balance fetch fail
        executor.get_usdc_balance = MagicMock(side_effect=Exception("RPC down"))
        # 100% of 200 = 200, capped by max_trade=50 (balance cap skipped)
        amount = executor.compute_copy_amount(Decimal("200"))
        self.assertEqual(amount, Decimal("50"))


class TestTradeExecutorDryRun(unittest.TestCase):
    """Test that dry-run mode prevents execution."""

    def _make_executor(self, dry_run=True):
        w3 = MagicMock()
        mock_account = MagicMock()
        mock_account.address = "0x" + "1" * 40
        w3.eth.account.from_key.return_value = mock_account
        w3.eth.contract.return_value = MagicMock()

        cfg = dict(bot.DEFAULT_CONFIG)
        cfg["dry_run"] = dry_run
        cfg["copy_percentage"] = 50
        cfg["max_trade_usdc"] = 1000

        executor = bot.TradeExecutor(
            w3=w3,
            private_key="0x" + "a" * 64,
            cfg=cfg,
            logger=logging.getLogger("test"),
        )
        executor.get_usdc_balance = MagicMock(return_value=Decimal("1000"))
        executor.ensure_usdc_approval = MagicMock()
        return executor

    def test_dry_run_returns_status(self):
        executor = self._make_executor(dry_run=True)
        result = executor.execute_copy_trade({
            "side": "BUY",
            "size": "100000000",  # 100 USDC in raw
            "asset_id": "token123",
            "price": 0.65,
        })
        self.assertIsNotNone(result)
        self.assertEqual(result["status"], "dry_run")
        self.assertEqual(result["side"], "BUY")
        # ensure_usdc_approval should NOT have been called
        executor.ensure_usdc_approval.assert_not_called()

    def test_non_dry_run_calls_approval(self):
        executor = self._make_executor(dry_run=False)
        result = executor.execute_copy_trade({
            "side": "BUY",
            "size": "100000000",
            "asset_id": "token123",
            "price": 0.65,
        })
        # In non-dry mode, approval should be called
        executor.ensure_usdc_approval.assert_called_once()

    def test_skip_zero_size(self):
        executor = self._make_executor(dry_run=True)
        result = executor.execute_copy_trade({
            "side": "BUY",
            "size": "0",
            "asset_id": "token123",
            "price": 0.5,
        })
        self.assertIsNone(result)

    def test_min_viable_bump_before_balance_check(self):
        """copy_amount is bumped to min viable USDC before order placement."""
        executor = self._make_executor(dry_run=True)
        # Small original trade: 50% of $0.10 = $0.05
        # Price 0.80 → min viable = max(5*0.80, 1.0) = $4.00
        # Balance is $1000, so bump should succeed
        result = executor.execute_copy_trade({
            "side": "BUY",
            "size": "0.10",
            "asset_id": "token123",
            "price": 0.80,
        })
        self.assertIsNotNone(result)
        self.assertEqual(result["status"], "dry_run")
        self.assertAlmostEqual(result["amount_usdc"], 4.0, places=1)

    def test_min_viable_skips_when_balance_insufficient(self):
        """Skip trade when min viable USDC exceeds available balance."""
        # Balance = $2.00 → 95% = $1.90
        # Price 0.80 → min viable = $4.00 > $1.90 → skip
        executor = self._make_executor(dry_run=False)
        executor.get_usdc_balance = MagicMock(return_value=Decimal("2.0"))
        result = executor.execute_copy_trade({
            "side": "BUY",
            "size": "0.10",
            "asset_id": "token123",
            "price": 0.80,
        })
        self.assertIsNone(result)
        executor.ensure_usdc_approval.assert_not_called()


class TestOnChainMonitor(unittest.TestCase):
    """Test on-chain monitor address filtering."""

    def _make_monitor(self, watched):
        w3 = MagicMock()
        w3.eth.contract.return_value = MagicMock()
        # Patch to_checksum_address so it just returns the input
        with patch.object(bot, "Web3") as mock_w3_cls:
            mock_w3_cls.to_checksum_address = lambda x: x
            monitor = bot.OnChainMonitor(w3, watched, logging.getLogger("test"))
        return monitor

    def test_update_watched(self):
        monitor = self._make_monitor(["0xabc"])
        self.assertEqual(monitor.watched, {"0xabc"})
        monitor.update_watched(["0xDEF", "0x123"])
        self.assertEqual(monitor.watched, {"0xdef", "0x123"})

    def test_poll_first_call_sets_baseline(self):
        monitor = self._make_monitor(["0xabc"])
        monitor.w3.eth.block_number = 12345
        trades = monitor.poll_new_blocks()
        self.assertEqual(trades, [])
        self.assertEqual(monitor._last_block, 12345)

    def test_scan_block_filters_by_address(self):
        """Only watched addresses interacting with exchange are reported."""
        w3 = MagicMock()
        w3.eth.contract.return_value = MagicMock()

        watched_addr = "0x" + "a" * 40
        other_addr = "0x" + "b" * 40

        with patch.object(bot, "Web3") as mock_w3_cls:
            mock_w3_cls.to_checksum_address = lambda x: x
            monitor = bot.OnChainMonitor(w3, [watched_addr], logging.getLogger("test"))

        # Mock a block with 2 transactions
        block = {
            "transactions": [
                {  # Watched address -> exchange
                    "from": watched_addr,
                    "to": bot.CTF_EXCHANGE_ADDRESS,
                    "hash": b"\x01" * 32,
                    "input": b"",
                    "blockNumber": 100,
                },
                {  # Other address -> exchange (should be skipped)
                    "from": other_addr,
                    "to": bot.CTF_EXCHANGE_ADDRESS,
                    "hash": b"\x02" * 32,
                    "input": b"",
                    "blockNumber": 100,
                },
            ]
        }
        w3.eth.get_block.return_value = block
        trades = monitor._scan_block(100)
        self.assertEqual(len(trades), 1)


class TestCopyTraderBot(unittest.TestCase):
    """Test the bot orchestration logic."""

    def test_start_stop(self):
        cfg = dict(bot.DEFAULT_CONFIG)
        cfg["watched_addresses"] = ["0x" + "a" * 40]
        cfg["poll_interval_seconds"] = 1
        logger = logging.getLogger("test")
        logger.handlers = [logging.NullHandler()]

        b = bot.CopyTraderBot(cfg, logger)
        self.assertFalse(b.running)

        # Mock out the initialization methods so the loop starts cleanly
        b._init_web3 = MagicMock(return_value=False)
        b._init_clob = MagicMock()

        b.start()
        self.assertTrue(b.running)
        self.assertIsNotNone(b._thread)

        import time
        time.sleep(0.5)

        b.stop()
        self.assertFalse(b.running)

    def test_double_start_warns(self):
        cfg = dict(bot.DEFAULT_CONFIG)
        logger = MagicMock()
        b = bot.CopyTraderBot(cfg, logger)
        b.running = True
        b.start()
        logger.warning.assert_called_with("Bot is already running")


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


if __name__ == "__main__":
    unittest.main()
