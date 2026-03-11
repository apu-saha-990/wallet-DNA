"""
WalletDNA — Tron Adapter
Fetches transaction history via TronScan API.
Handles TRX transfers and TRC-20 token transfers (USDT on Tron).
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import structlog

from walletdna.adapters.base import BaseAdapter, RateLimiter
from walletdna.engine.models import Chain, NormalisedTx

logger = structlog.get_logger(__name__)


class TronAdapter(BaseAdapter):

    chain    = Chain.TRON
    BASE_URL = "https://apilist.tronscanapi.com/api"

    # TRC-20 USDT contract on Tron
    USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"

    def __init__(
        self,
        api_key:          Optional[str] = None,
        calls_per_second: float         = 5.0,
    ):
        self.api_key = api_key or os.getenv("TRONSCAN_API_KEY", "")
        self._rate_limiter = RateLimiter(calls_per_second)
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            headers = {}
            if self.api_key:
                headers["TRON-PRO-API-KEY"] = self.api_key
            timeout = aiohttp.ClientTimeout(total=30)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                headers=headers,
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _api_call(self, endpoint: str, params: dict) -> dict:
        url = f"{self.BASE_URL}/{endpoint}"

        async def _fetch():
            session = await self._get_session()
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                return await resp.json()

        return await self._fetch_with_retry(_fetch)

    # ─── Public Interface ─────────────────────────────────────────────────────

    async def get_transactions(
        self,
        address:     str,
        start_block: Optional[int] = None,
        end_block:   Optional[int] = None,
        max_txs:     int           = 10_000,
    ) -> list[NormalisedTx]:
        logger.info("trx_fetching_txs", address=address[:12])

        trx_txs   = await self._fetch_trx_txs(address)
        token_txs = await self._fetch_trc20_txs(address)

        seen: set[str] = set()
        merged: list[NormalisedTx] = []
        for tx in trx_txs + token_txs:
            if tx.tx_hash not in seen:
                seen.add(tx.tx_hash)
                merged.append(tx)

        merged.sort(key=lambda t: t.block_time)

        logger.info(
            "trx_fetch_complete",
            address=address[:12],
            trx=len(trx_txs),
            trc20=len(token_txs),
            merged=len(merged),
        )

        return merged[:max_txs]

    async def resolve_tx_hash(self, tx_hash: str) -> Optional[NormalisedTx]:
        data = await self._api_call("transaction-info", {"hash": tx_hash})
        if not data:
            return None
        return self._parse_tx_info(data)

    def is_valid_address(self, address: str) -> bool:
        return (
            isinstance(address, str)
            and address.startswith("T")
            and len(address) == 34
            and address.isalnum()
        )

    async def get_wallet_age_days(self, address: str) -> Optional[float]:
        data = await self._api_call("accountv2", {"address": address})
        create_time = data.get("date_created")
        if not create_time:
            return None
        create_dt = datetime.fromtimestamp(create_time / 1000, tz=timezone.utc)
        age = (datetime.now(tz=timezone.utc) - create_dt).total_seconds() / 86400
        return round(age, 2)

    # ─── Internal Fetchers ────────────────────────────────────────────────────

    async def _fetch_trx_txs(self, address: str) -> list[NormalisedTx]:
        txs   = []
        start = 0
        limit = 50

        while True:
            params = {
                "address": address,
                "start":   start,
                "limit":   limit,
                "sort":    "-timestamp",
            }
            data  = await self._api_call("transaction", params)
            batch = data.get("data", [])

            if not batch:
                break

            for raw in batch:
                tx = self._parse_trx_tx(raw, address)
                if tx:
                    txs.append(tx)

            if len(batch) < limit:
                break
            start += limit

        return txs

    async def _fetch_trc20_txs(self, address: str) -> list[NormalisedTx]:
        txs   = []
        start = 0
        limit = 50

        while True:
            params = {
                "address":          address,
                "start":            start,
                "limit":            limit,
                "contract_address": self.USDT_CONTRACT,
            }
            data  = await self._api_call("token_trc20/transfers", params)
            batch = data.get("token_transfers", [])

            if not batch:
                break

            for raw in batch:
                tx = self._parse_trc20_tx(raw, address)
                if tx:
                    txs.append(tx)

            if len(batch) < limit:
                break
            start += limit

        return txs

    # ─── Parsers ──────────────────────────────────────────────────────────────

    def _parse_trx_tx(self, raw: dict, wallet_address: str) -> Optional[NormalisedTx]:
        try:
            ts         = raw.get("timestamp", 0)
            block_time = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            from_addr  = raw.get("ownerAddress", "")
            to_addr    = raw.get("toAddress", "")
            value_sun  = int(raw.get("amount", 0) or 0)      # 1 TRX = 1,000,000 SUN
            energy     = raw.get("energyUsage", 0)
            bandwidth  = raw.get("netUsage", 0)

            return NormalisedTx(
                tx_hash        = raw.get("hash", ""),
                chain          = Chain.TRON,
                block_number   = raw.get("block"),
                block_time     = block_time,
                from_address   = from_addr,
                to_address     = to_addr,
                direction      = self._determine_direction(wallet_address, from_addr, to_addr),
                value_native   = value_sun / 1_000_000,
                energy_used    = energy,
                bandwidth_used = bandwidth,
            )
        except Exception as e:
            logger.warning("trx_parse_tx_failed", error=str(e))
            return None

    def _parse_trc20_tx(self, raw: dict, wallet_address: str) -> Optional[NormalisedTx]:
        try:
            ts         = raw.get("block_ts", 0)
            block_time = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            from_addr  = raw.get("from_address", "")
            to_addr    = raw.get("to_address", "")
            decimals   = int(raw.get("tokenInfo", {}).get("tokenDecimal", 6))
            value_raw  = int(raw.get("quant", 0))
            value      = value_raw / (10 ** decimals)
            symbol     = raw.get("tokenInfo", {}).get("tokenAbbr", "")

            return NormalisedTx(
                tx_hash          = raw.get("transaction_id", ""),
                chain            = Chain.TRON,
                block_number     = raw.get("block"),
                block_time       = block_time,
                from_address     = from_addr,
                to_address       = to_addr,
                direction        = self._determine_direction(wallet_address, from_addr, to_addr),
                value_native     = value,
                is_contract_call = True,
                contract_method  = "TRANSFER",
                token_symbol     = symbol,
            )
        except Exception as e:
            logger.warning("trx_parse_trc20_failed", error=str(e))
            return None

    def _parse_tx_info(self, raw: dict) -> Optional[NormalisedTx]:
        try:
            ts = raw.get("timestamp", 0)
            return NormalisedTx(
                tx_hash      = raw.get("hash", ""),
                chain        = Chain.TRON,
                block_time   = datetime.fromtimestamp(ts / 1000, tz=timezone.utc),
                from_address = raw.get("ownerAddress", ""),
                to_address   = raw.get("toAddress", ""),
                direction    = "out",
                value_native = int(raw.get("amount", 0) or 0) / 1_000_000,
            )
        except Exception as e:
            logger.warning("trx_parse_info_failed", error=str(e))
            return None
