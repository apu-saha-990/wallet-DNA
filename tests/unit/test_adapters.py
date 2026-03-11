"""
WalletDNA — Unit Tests: Adapters
Tests address validation, parsing, and chain detection.
No API calls made — all responses mocked.
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

from walletdna.adapters.eth      import EthereumAdapter
from walletdna.adapters.trx      import TronAdapter
from walletdna.adapters.doge     import DogecoinAdapter
from walletdna.adapters.resolver import AddressResolver
from walletdna.engine.models     import Chain


# ─── Address Validation ───────────────────────────────────────────────────────

class TestAddressValidation:

    def test_eth_valid(self):
        adapter = EthereumAdapter(api_key="test")
        assert adapter.is_valid_address("0xD038A997444Db594BBE62AAad8B4735584D8db2d")
        assert adapter.is_valid_address("0x0000000000000000000000000000000000000000")

    def test_eth_invalid(self):
        adapter = EthereumAdapter(api_key="test")
        assert not adapter.is_valid_address("D038A997444Db594BBE62AAad8B4735584D8db2d")  # no 0x
        assert not adapter.is_valid_address("0xshort")
        assert not adapter.is_valid_address("")

    def test_trx_valid(self):
        adapter = TronAdapter()
        assert adapter.is_valid_address("TN3W4H6rK2ce4vX9YnFQHwKENnHjoxb3m9")

    def test_trx_invalid(self):
        adapter = TronAdapter()
        assert not adapter.is_valid_address("0xabc")
        assert not adapter.is_valid_address("Xabc123")

    def test_doge_valid(self):
        adapter = DogecoinAdapter()
        assert adapter.is_valid_address("DH5yaieqoZN36fDVciNyRueRGvGLR3mr7L")

    def test_doge_invalid(self):
        adapter = DogecoinAdapter()
        assert not adapter.is_valid_address("0xabc")
        assert not adapter.is_valid_address("bc1qtest")


# ─── Address Resolver ─────────────────────────────────────────────────────────

class TestAddressResolver:

    def test_ethereum_detected(self):
        result = AddressResolver.detect("0xD038A997444Db594BBE62AAad8B4735584D8db2d")
        assert Chain.ETHEREUM in result.chains
        assert result.method == "deterministic"

    def test_tron_detected(self):
        result = AddressResolver.detect("TN3W4H6rK2ce4vX9YnFQHwKENnHjoxb3m9")
        assert result.chains == [Chain.TRON]
        assert result.method == "deterministic"

    def test_dogecoin_detected(self):
        result = AddressResolver.detect("DH5yaieqoZN36fDVciNyRueRGvGLR3mr7L")
        assert result.chains == [Chain.DOGECOIN]
        assert result.method == "deterministic"

    def test_unknown_address(self):
        result = AddressResolver.detect("xyz_unknown_format_123")
        assert result.chains == []
        assert result.method == "unknown"

    def test_empty_address(self):
        result = AddressResolver.detect("")
        assert result.chains == []

    def test_eth_tx_hash_detected(self):
        tx = "0x66cec61a427ab454a2c4ea0f76d0bdca9d0d78634ec7f1246807d70391b7eb76"
        chain = AddressResolver.detect_from_tx_hash(tx)
        assert chain == Chain.ETHEREUM

    def test_truncate(self):
        addr = "0xD038A997444Db594BBE62AAad8B4735584D8db2d"
        truncated = AddressResolver.truncate(addr)
        assert "..." in truncated
        assert truncated.endswith(addr[-6:])


# ─── ETH Parser ───────────────────────────────────────────────────────────────

class TestEthParser:

    def setup_method(self):
        self.adapter = EthereumAdapter(api_key="test")

    def test_parse_normal_tx(self):
        raw = {
            "hash":         "0xabc123",
            "timeStamp":    "1706054400",   # 2024-01-24
            "from":         "0xD038A997444Db594BBE62AAad8B4735584D8db2d",
            "to":           "0xTargetAddress1234567890123456789012345678",
            "value":        "100000000000000000",  # 0.1 ETH in wei
            "gasPrice":     "20000000000",          # 20 gwei
            "gasUsed":      "21000",
            "gas":          "21000",
            "input":        "0x",
            "blockNumber":  "19000000",
            "confirmations": "100",
        }
        tx = self.adapter._parse_normal_tx(raw, "0xd038a997444db594bbe62aaad8b4735584d8db2d")

        assert tx is not None
        assert tx.chain == Chain.ETHEREUM
        assert tx.tx_hash == "0xabc123"
        assert abs(tx.value_native - 0.1) < 1e-10
        assert tx.gas_price_gwei == 20.0
        assert tx.direction == "out"
        assert tx.is_contract_call is False

    def test_parse_token_tx(self):
        raw = {
            "hash":         "0xdef456",
            "timeStamp":    "1706054400",
            "from":         "0xd038a997444db594bbe62aaad8b4735584d8db2d",
            "to":           "0xtargetaddress",
            "value":        "260000000",     # 260 USDT (6 decimals)
            "tokenDecimal": "6",
            "tokenSymbol":  "USDT",
            "gasPrice":     "15000000000",
            "gasUsed":      "65000",
            "blockNumber":  "19000001",
        }
        tx = self.adapter._parse_token_tx(raw, "0xd038a997444db594bbe62aaad8b4735584d8db2d")

        assert tx is not None
        assert tx.token_symbol == "USDT"
        assert abs(tx.value_native - 260.0) < 0.001
        assert tx.is_contract_call is True
        assert tx.contract_method == "TRANSFER"

    def test_direction_out(self):
        direction = self.adapter._determine_direction(
            "0xsender", "0xsender", "0xreceiver"
        )
        assert direction == "out"

    def test_direction_in(self):
        direction = self.adapter._determine_direction(
            "0xreceiver", "0xsender", "0xreceiver"
        )
        assert direction == "in"

    def test_direction_self(self):
        direction = self.adapter._determine_direction(
            "0xself", "0xself", "0xself"
        )
        assert direction == "self"


# ─── Your Known Wallets ───────────────────────────────────────────────────────

class TestKnownWallets:
    """Validate all 4 of your real wallet addresses are properly recognised."""

    YOUR_ETH_WALLETS = [
        "0xD038A997444Db594BBE62AAad8B4735584D8db2d",
        "0x3B18DD8653EddC873FcFE4601353b5DCAe4Ac85D",
        "0xb4Bf4E2168b8cbEdE6B7ea5eb2334C988d47D0e1",
        "0xD86a53FEDFACCBA2e080C0Ea1DD831E0FCEacd90",
    ]

    def test_all_eth_wallets_valid(self):
        adapter = EthereumAdapter(api_key="test")
        for addr in self.YOUR_ETH_WALLETS:
            assert adapter.is_valid_address(addr), f"Failed for {addr}"

    def test_all_eth_wallets_detected_as_ethereum(self):
        for addr in self.YOUR_ETH_WALLETS:
            result = AddressResolver.detect(addr)
            assert Chain.ETHEREUM in result.chains, f"Failed for {addr}"
