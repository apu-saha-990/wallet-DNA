"""
WalletDNA — Dogecoin Adapter
Fetches transaction history via Blockchair API.
UTXO chain — inputs/outputs model, no smart contracts.
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


class DogecoinAdapter(BaseAdapter):

    chain    = Chain.DOGECOIN
    BASE_URL = "https://api.blockchair.com/dogecoin"

    def __init__(
        self,
        api_key:          Optional[str] = None,
        calls_per_minute: float         = 25.0,  # Conservative on free tier
    ):
        self.api_key = api_key or os.getenv("DOGECHAIN_API_KEY", "")
        # Blockchair rate limit is per minute — convert to per second
        self._rate_limiter = RateLimiter(calls_per_minute / 60.0)
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _api_call(self, endpoint: str, params: Optional[dict] = None) -> dict:
        url = f"{self.BASE_URL}/{endpoint}"
        p   = params or {}
        if self.api_key:
            p["key"] = self.api_key

        async def _fetch():
            session = await self._get_session()
            async with session.get(url, params=p) as resp:
                resp.raise_for_status()
                return await resp.json()

        return await self._fetch_with_retry(_fetch)

    # ─── Public Interface ─────────────────────────────────────────────────────

    async def get_transactions(
        self,
        address:     str,
        start_block: Optional[int] = None,
        end_block:   Optional[int] = None,
        max_txs:     int           = 5_000,
    ) -> list[NormalisedTx]:
        logger.info("doge_fetching_txs", address=address[:12])

        txs    = []
        offset = 0
        limit  = 100

        while len(txs) < max_txs:
            data = await self._api_call(
                f"dashboards/address/{address}",
                {
                    "limit":            f"{limit},0",
                    "offset":           f"{offset},0",
                    "transaction_details": "true",
                },
            )

            addr_data = data.get("data", {}).get(address.lower(), {})
            tx_hashes = addr_data.get("transactions", [])

            if not tx_hashes:
                break

            # Fetch tx details in batches of 10 (Blockchair limit)
            batch_size = 10
            for i in range(0, len(tx_hashes), batch_size):
                batch_hashes = tx_hashes[i:i + batch_size]
                batch_txs    = await self._fetch_tx_batch(batch_hashes, address)
                txs.extend(batch_txs)

            if len(tx_hashes) < limit:
                break
            offset += limit

        txs.sort(key=lambda t: t.block_time)

        logger.info(
            "doge_fetch_complete",
            address=address[:12],
            tx_count=len(txs),
        )

        return txs[:max_txs]

    async def _fetch_tx_batch(
        self, tx_hashes: list[str], wallet_address: str
    ) -> list[NormalisedTx]:
        if not tx_hashes:
            return []

        hashes_str = ",".join(tx_hashes)
        data = await self._api_call(f"dashboards/transactions/{hashes_str}")

        txs = []
        for tx_hash, tx_data in data.get("data", {}).items():
            tx = self._parse_utxo_tx(tx_data, wallet_address)
            if tx:
                txs.append(tx)
        return txs

    async def resolve_tx_hash(self, tx_hash: str) -> Optional[NormalisedTx]:
        data = await self._api_call(f"dashboards/transaction/{tx_hash}")
        tx_data = data.get("data", {}).get(tx_hash)
        if not tx_data:
            return None
        return self._parse_utxo_tx(tx_data, "")

    def is_valid_address(self, address: str) -> bool:
        return (
            isinstance(address, str)
            and address.startswith("D")
            and len(address) in (33, 34)
            and address.isalnum()
        )

    async def get_wallet_age_days(self, address: str) -> Optional[float]:
        data      = await self._api_call(f"dashboards/address/{address}")
        addr_data = data.get("data", {}).get(address.lower(), {})
        addr_info = addr_data.get("address", {})

        first_seen = addr_info.get("first_seen_receiving")
        if not first_seen:
            return None

        first_dt = datetime.fromisoformat(first_seen.replace("Z", "+00:00"))
        age = (datetime.now(tz=timezone.utc) - first_dt).total_seconds() / 86400
        return round(age, 2)

    # ─── Parser ───────────────────────────────────────────────────────────────

    def _parse_utxo_tx(self, tx_data: dict, wallet_address: str) -> Optional[NormalisedTx]:
        """
        UTXO transaction parsing.
        Determines direction by checking if wallet address appears in inputs or outputs.
        """
        try:
            tx          = tx_data.get("transaction", {})
            inputs      = tx_data.get("inputs", [])
            outputs     = tx_data.get("outputs", [])

            tx_hash    = tx.get("hash", "")
            time_str   = tx.get("time", "")
            block_id   = tx.get("block_id")
            fee_sat    = tx.get("fee", 0)

            block_time = datetime.fromisoformat(
                time_str.replace("Z", "+00:00")
            ) if time_str else datetime.now(tz=timezone.utc)

            # Determine direction from UTXO perspective
            wallet_lower  = wallet_address.lower()
            input_addrs   = {
                inp.get("recipient", "").lower()
                for inp in inputs
            }
            output_addrs  = {
                out.get("recipient", "").lower()
                for out in outputs
            }

            is_sender   = wallet_lower in input_addrs
            is_receiver = wallet_lower in output_addrs

            if is_sender and is_receiver:
                direction = "self"
            elif is_sender:
                direction = "out"
            else:
                direction = "in"

            # Calculate value relevant to wallet
            if direction == "out":
                # Sum outputs NOT going back to sender (change)
                value_sat = sum(
                    out.get("value", 0)
                    for out in outputs
                    if out.get("recipient", "").lower() != wallet_lower
                )
                # Primary counterparty
                other_addrs = [
                    out.get("recipient", "")
                    for out in outputs
                    if out.get("recipient", "").lower() != wallet_lower
                ]
            else:
                # Sum outputs going to wallet
                value_sat = sum(
                    out.get("value", 0)
                    for out in outputs
                    if out.get("recipient", "").lower() == wallet_lower
                )
                other_addrs = [
                    inp.get("recipient", "")
                    for inp in inputs
                ]

            # Primary counterparty address
            to_addr   = other_addrs[0] if other_addrs else ""
            from_addr = wallet_address if is_sender else (
                inputs[0].get("recipient", "") if inputs else ""
            )

            # 1 DOGE = 100,000,000 satoshis
            value_doge = value_sat / 1e8
            fee_doge   = fee_sat / 1e8

            return NormalisedTx(
                tx_hash      = tx_hash,
                chain        = Chain.DOGECOIN,
                block_number = block_id,
                block_time   = block_time,
                from_address = from_addr.lower(),
                to_address   = to_addr.lower(),
                direction    = direction,
                value_native = value_doge,
                fee_native   = fee_doge,
                # DOGE has no contracts — these stay None/False
                is_contract_call = False,
            )

        except Exception as e:
            logger.warning("doge_parse_tx_failed", error=str(e))
            return None
