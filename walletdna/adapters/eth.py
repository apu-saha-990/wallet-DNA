"""
WalletDNA — Ethereum Adapter
Fetches transaction history via Etherscan V2 API.
Handles normal txs, ERC-20 token transfers, and internal txs.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import structlog

from walletdna.adapters.base import BaseAdapter, RateLimiter
from walletdna.engine.models import Chain, NormalisedTx, TxDirection

logger = structlog.get_logger(__name__)

# Known contract type signatures (4-byte selectors → label)
CONTRACT_SIGNATURES: dict[str, str] = {
    "0x38ed1739": "DEX",     # swapExactTokensForTokens (Uniswap V2)
    "0x7ff36ab5": "DEX",     # swapExactETHForTokens
    "0x18cbafe5": "DEX",     # swapExactTokensForETH
    "0x5c11d795": "DEX",     # swapExactTokensForTokensSupportingFeeOnTransferTokens
    "0x414bf389": "DEX",     # exactInputSingle (Uniswap V3)
    "0xac9650d8": "DEX",     # multicall (Uniswap V3)
    "0xa9059cbb": "TRANSFER", # ERC-20 transfer
    "0x23b872dd": "TRANSFER", # transferFrom
    "0x095ea7b3": "APPROVE",  # approve
    "0x2e1a7d4d": "BRIDGE",   # withdraw (WETH)
    "0xd0e30db0": "BRIDGE",   # deposit (WETH)
    "0x9169558b": "STAKE",
    "0xe8eda9df": "LENDING",  # deposit (Aave)
}


class EthereumAdapter(BaseAdapter):

    chain = Chain.ETHEREUM
    BASE_URL = "https://api.etherscan.io/v2/api"

    def __init__(
        self,
        api_key:          Optional[str] = None,
        calls_per_second: float         = 4.0,   # Stay under free tier limit of 5/s
    ):
        self.api_key = api_key or os.getenv("ETHERSCAN_API_KEY", "")
        self._rate_limiter = RateLimiter(calls_per_second)
        self._session: Optional[aiohttp.ClientSession] = None

        if not self.api_key:
            logger.warning("eth_adapter_no_api_key",
                           msg="No Etherscan API key — rate limits will be strict")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # ─── Core API Call ────────────────────────────────────────────────────────

    async def _api_call(self, params: dict) -> dict:
        """Make a single Etherscan V2 API call with rate limiting."""
        if self.api_key:
            params["apikey"] = self.api_key

        async def _fetch():
            session = await self._get_session()
            async with session.get(self.BASE_URL, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()
                if data.get("status") == "0" and data.get("message") == "NOTOK":
                    raise RuntimeError(f"Etherscan error: {data.get('result')}")
                return data

        return await self._fetch_with_retry(_fetch)

    # ─── Public Interface ─────────────────────────────────────────────────────

    async def get_transactions(
        self,
        address:     str,
        start_block: Optional[int] = None,
        end_block:   Optional[int] = None,
        max_txs:     int           = 10_000,
    ) -> list[NormalisedTx]:
        """
        Fetch all transactions for address.
        Merges normal txs + ERC-20 token transfers, deduplicates, sorts by time.
        """
        address = address.lower()

        logger.info("eth_fetching_txs", address=address[:12])

        # Fetch normal transactions
        normal_txs = await self._fetch_normal_txs(address, start_block, end_block)

        # Fetch ERC-20 token transfers (USDT etc.)
        token_txs = await self._fetch_token_txs(address, start_block, end_block)

        # Merge and deduplicate by tx_hash
        seen_hashes: set[str] = set()
        merged: list[NormalisedTx] = []

        for tx in normal_txs + token_txs:
            if tx.tx_hash not in seen_hashes:
                seen_hashes.add(tx.tx_hash)
                merged.append(tx)

        # Sort oldest first
        merged.sort(key=lambda t: t.block_time)

        logger.info(
            "eth_fetch_complete",
            address=address[:12],
            normal=len(normal_txs),
            token=len(token_txs),
            merged=len(merged),
        )

        return merged[:max_txs]

    async def resolve_tx_hash(self, tx_hash: str) -> Optional[NormalisedTx]:
        """Resolve a single tx hash to NormalisedTx."""
        params = {
            "chainid": "1",
            "module":  "proxy",
            "action":  "eth_getTransactionByHash",
            "txhash":  tx_hash,
        }
        data = await self._api_call(params)
        result = data.get("result")
        if not result:
            return None
        return self._parse_raw_tx(result, wallet_address=result.get("from", ""))

    def is_valid_address(self, address: str) -> bool:
        return (
            isinstance(address, str)
            and address.startswith("0x")
            and len(address) == 42
        )

    async def get_wallet_age_days(self, address: str) -> Optional[float]:
        params = {
            "chainid": "1",
            "module":  "account",
            "action":  "txlist",
            "address": address,
            "startblock": "0",
            "endblock":   "99999999",
            "page":       "1",
            "offset":     "1",
            "sort":       "asc",
        }
        data = await self._api_call(params)
        txs = data.get("result", [])
        if not txs or not isinstance(txs, list):
            return None
        first_ts = int(txs[0].get("timeStamp", 0))
        if not first_ts:
            return None
        first_dt = datetime.fromtimestamp(first_ts, tz=timezone.utc)
        age = (datetime.now(tz=timezone.utc) - first_dt).total_seconds() / 86400
        return round(age, 2)

    # ─── Internal Fetchers ────────────────────────────────────────────────────

    async def _fetch_normal_txs(
        self,
        address:     str,
        start_block: Optional[int],
        end_block:   Optional[int],
    ) -> list[NormalisedTx]:
        txs = []
        page = 1
        while True:
            params = {
                "chainid":    "1",
                "module":     "account",
                "action":     "txlist",
                "address":    address,
                "startblock": str(start_block or 0),
                "endblock":   str(end_block or 99_999_999),
                "page":       str(page),
                "offset":     "1000",
                "sort":       "asc",
            }
            data  = await self._api_call(params)
            batch = data.get("result", [])

            if not batch or not isinstance(batch, list):
                break

            for raw in batch:
                tx = self._parse_normal_tx(raw, address)
                if tx:
                    txs.append(tx)

            if len(batch) < 1000:
                break  # Last page
            page += 1

        return txs

    async def _fetch_token_txs(
        self,
        address:     str,
        start_block: Optional[int],
        end_block:   Optional[int],
    ) -> list[NormalisedTx]:
        txs = []
        page = 1
        while True:
            params = {
                "chainid":    "1",
                "module":     "account",
                "action":     "tokentx",
                "address":    address,
                "startblock": str(start_block or 0),
                "endblock":   str(end_block or 99_999_999),
                "page":       str(page),
                "offset":     "1000",
                "sort":       "asc",
            }
            data  = await self._api_call(params)
            batch = data.get("result", [])

            if not batch or not isinstance(batch, list):
                break

            for raw in batch:
                tx = self._parse_token_tx(raw, address)
                if tx:
                    txs.append(tx)

            if len(batch) < 1000:
                break
            page += 1

        return txs

    # ─── Parsers ──────────────────────────────────────────────────────────────

    def _parse_normal_tx(self, raw: dict, wallet_address: str) -> Optional[NormalisedTx]:
        try:
            ts         = int(raw.get("timeStamp", 0))
            block_time = datetime.fromtimestamp(ts, tz=timezone.utc)
            from_addr  = raw.get("from", "").lower()
            to_addr    = raw.get("to", "").lower()
            value_wei  = int(raw.get("value", 0))
            gas_price  = int(raw.get("gasPrice", 0))
            gas_used   = int(raw.get("gasUsed", 0))
            gas_limit  = int(raw.get("gas", 0))
            input_data = raw.get("input", "0x")

            method_id       = input_data[:10] if len(input_data) >= 10 else None
            is_contract     = input_data != "0x" and len(input_data) > 2
            contract_method = CONTRACT_SIGNATURES.get(method_id) if method_id else None

            fee_wei  = gas_price * gas_used
            gas_gwei = gas_price / 1e9

            return NormalisedTx(
                tx_hash          = raw.get("hash", ""),
                chain            = Chain.ETHEREUM,
                block_number     = int(raw.get("blockNumber", 0)),
                block_time       = block_time,
                from_address     = from_addr,
                to_address       = to_addr,
                direction        = self._determine_direction(wallet_address, from_addr, to_addr),
                value_native     = self._wei_to_eth(value_wei),
                fee_native       = self._wei_to_eth(fee_wei),
                gas_price_gwei   = gas_gwei,
                gas_used         = gas_used,
                gas_limit        = gas_limit,
                is_contract_call = is_contract,
                contract_method  = contract_method,
                confirmation_blocks = int(raw.get("confirmations", 0)),
            )
        except Exception as e:
            logger.warning("eth_parse_normal_tx_failed", error=str(e))
            return None

    def _parse_token_tx(self, raw: dict, wallet_address: str) -> Optional[NormalisedTx]:
        try:
            ts         = int(raw.get("timeStamp", 0))
            block_time = datetime.fromtimestamp(ts, tz=timezone.utc)
            from_addr  = raw.get("from", "").lower()
            to_addr    = raw.get("to", "").lower()
            decimals   = int(raw.get("tokenDecimal", 18))
            value_raw  = int(raw.get("value", 0))
            value      = value_raw / (10 ** decimals)
            symbol     = raw.get("tokenSymbol", "")
            gas_price  = int(raw.get("gasPrice", 0))
            gas_used   = int(raw.get("gasUsed", 0))

            return NormalisedTx(
                tx_hash          = raw.get("hash", ""),
                chain            = Chain.ETHEREUM,
                block_number     = int(raw.get("blockNumber", 0)),
                block_time       = block_time,
                from_address     = from_addr,
                to_address       = to_addr,
                direction        = self._determine_direction(wallet_address, from_addr, to_addr),
                value_native     = value,
                fee_native       = self._wei_to_eth(gas_price * gas_used),
                gas_price_gwei   = gas_price / 1e9,
                gas_used         = gas_used,
                is_contract_call = True,
                contract_method  = "TRANSFER",
                token_symbol     = symbol,
            )
        except Exception as e:
            logger.warning("eth_parse_token_tx_failed", error=str(e))
            return None

    def _parse_raw_tx(self, raw: dict, wallet_address: str) -> Optional[NormalisedTx]:
        """Parse raw eth_getTransactionByHash response."""
        try:
            from_addr = raw.get("from", "").lower()
            to_addr   = (raw.get("to") or "").lower()
            value_hex = raw.get("value", "0x0")
            gas_hex   = raw.get("gasPrice", "0x0")
            input_data = raw.get("input", "0x")

            value_wei = int(value_hex, 16) if value_hex else 0
            gas_price = int(gas_hex, 16) if gas_hex else 0
            method_id = input_data[:10] if len(input_data) >= 10 else None

            return NormalisedTx(
                tx_hash          = raw.get("hash", ""),
                chain            = Chain.ETHEREUM,
                block_number     = int(raw.get("blockNumber", "0x0"), 16),
                block_time       = datetime.now(tz=timezone.utc),  # need receipt for exact time
                from_address     = from_addr,
                to_address       = to_addr,
                direction        = self._determine_direction(wallet_address, from_addr, to_addr),
                value_native     = self._wei_to_eth(value_wei),
                gas_price_gwei   = gas_price / 1e9,
                is_contract_call = input_data != "0x",
                contract_method  = CONTRACT_SIGNATURES.get(method_id) if method_id else None,
            )
        except Exception as e:
            logger.warning("eth_parse_raw_tx_failed", error=str(e))
            return None
