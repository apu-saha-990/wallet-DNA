"""
WalletDNA — Dogecoin Adapter
Fetches transaction history via Blockcypher API (free, no key required).
UTXO chain — inputs/outputs model, no smart contracts.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import aiohttp
import structlog

from walletdna.adapters.base import BaseAdapter, RateLimiter
from walletdna.engine.models import Chain, NormalisedTx

logger = structlog.get_logger(__name__)

# Blockcypher free tier: 3 req/sec, 200 req/hr
BASE_URL = "https://api.blockcypher.com/v1/doge/main"


class DogecoinAdapter(BaseAdapter):
    chain = Chain.DOGECOIN

    def __init__(self, calls_per_second: float = 2.0):
        self._rate_limiter = RateLimiter(calls_per_second)
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
        await self._rate_limiter.acquire()
        session = await self._get_session()
        url = f"{BASE_URL}/{endpoint}"
        async with session.get(url, params=params or {}) as resp:
            if resp.status != 200:
                raise aiohttp.ClientResponseError(
                    resp.request_info, resp.history,
                    status=resp.status, message=f"Blockcypher error {resp.status}"
                )
            return await resp.json()

    async def get_transactions(self, address: str) -> list[NormalisedTx]:
        logger.info("doge_fetching_txs", address=address[:12])
        txs: list[NormalisedTx] = []
        before = None  # cursor for pagination

        try:
            while True:
                params = {"limit": 50, "includeHex": "false"}
                if before:
                    params["before"] = before

                data = await self._api_call(f"addrs/{address}/full", params)
                raw_txs = data.get("txs", [])
                if not raw_txs:
                    break

                for raw in raw_txs:
                    parsed = self._parse_tx(raw, address)
                    if parsed:
                        txs.append(parsed)

                # Pagination — Blockcypher returns hasMore flag
                if not data.get("hasMore", False):
                    break

                # Set cursor to block height of last tx for next page
                last = raw_txs[-1]
                before = last.get("block_height", 0)

                # Safety cap — DOGE wallets can be enormous
                if len(txs) >= 2000:
                    logger.info("doge_tx_cap_reached", address=address[:12], count=len(txs))
                    break

        except Exception as e:
            logger.warning("doge_fetch_failed", address=address[:12], error=str(e))

        logger.info("doge_fetch_complete", address=address[:12], tx_count=len(txs))
        return txs

    def is_valid_address(self, address: str) -> bool:
        return address.startswith("D") and 33 <= len(address) <= 34

    async def get_wallet_age_days(self, address: str) -> Optional[float]:
        try:
            data = await self._api_call(f"addrs/{address}")
            txs  = data.get("n_tx", 0)
            return float(txs)  # proxy — not actual age
        except Exception:
            return None

    async def resolve_tx_hash(self, tx_hash: str) -> Optional[NormalisedTx]:
        try:
            data = await self._api_call(f"txs/{tx_hash}")
            return self._parse_tx(data, "")
        except Exception:
            return None

    def _parse_tx(self, raw: dict, wallet_address: str) -> Optional[NormalisedTx]:
        try:
            addr_lower = wallet_address.lower()

            # Timestamp
            confirmed = raw.get("confirmed")
            received  = raw.get("received")
            if confirmed:
                block_time = datetime.fromisoformat(confirmed.replace("Z", "+00:00"))
            elif received:
                block_time = datetime.fromisoformat(received.replace("Z", "+00:00"))
            else:
                block_time = datetime.now(tz=timezone.utc)

            # Determine direction and value from inputs/outputs
            inputs  = raw.get("inputs", [])
            outputs = raw.get("outputs", [])

            # Check if wallet is in inputs (sending)
            sent_from_wallet = any(
                addr_lower in [a.lower() for a in (inp.get("addresses") or [])]
                for inp in inputs
            )

            # Value received by wallet
            received_value = sum(
                int(out.get("value", 0))
                for out in outputs
                if addr_lower in [a.lower() for a in (out.get("addresses") or [])]
            )

            # Value sent from wallet
            sent_value = sum(
                int(inp.get("output_value", 0))
                for inp in inputs
                if addr_lower in [a.lower() for a in (inp.get("addresses") or [])]
            )

            direction    = "out" if sent_from_wallet else "in"
            value_native = (sent_value - received_value) / 1e8 if sent_from_wallet else received_value / 1e8
            value_native = max(0.0, value_native)

            # Fee
            fees = int(raw.get("fees", 0))
            fee_native = fees / 1e8

            # Gas price proxy — DOGE uses flat fees, use fee as proxy
            gas_price_gwei = fee_native * 1e9 if fee_native > 0 else 1.0

            # From/to addresses
            from_addrs = [a for inp in inputs for a in (inp.get("addresses") or [])]
            to_addrs   = [a for out in outputs for a in (out.get("addresses") or [])]

            from_address = from_addrs[0].lower() if from_addrs else ""
            to_address   = to_addrs[0].lower()   if to_addrs   else ""

            return NormalisedTx(
                tx_hash             = raw.get("hash", ""),
                chain               = Chain.DOGECOIN,
                block_number        = int(raw.get("block_height", 0) or 0),
                block_time          = block_time,
                from_address        = from_address,
                to_address          = to_address,
                direction           = direction,
                value_native        = value_native,
                fee_native          = fee_native,
                gas_price_gwei      = gas_price_gwei,
                gas_used            = 0,
                gas_limit           = 0,
                is_contract_call    = False,
                confirmation_blocks = int(raw.get("confirmations", 0) or 0),
            )

        except Exception as e:
            logger.warning("doge_parse_tx_failed", error=str(e))
            return None
