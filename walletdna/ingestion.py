"""
WalletDNA — Ingestion Service
Orchestrates wallet data fetching across all chain adapters.
Stores results in TimescaleDB.
"""

from __future__ import annotations

import time
from typing import Optional

import structlog

from walletdna.adapters import (
    AddressResolver,
    DogecoinAdapter,
    EthereumAdapter,
    TronAdapter,
)
from walletdna.adapters.base import BaseAdapter
from walletdna.engine.models import Chain, IngestionResult, WalletIngestionRequest
from walletdna.monitoring.metrics import (
    API_ERRORS,
    INGESTION_DURATION,
    TRANSACTIONS_INGESTED,
    WALLETS_INGESTED,
    WALLETS_IN_DB,
)
from walletdna.storage.db import Database

logger = structlog.get_logger(__name__)


class IngestionService:

    def __init__(self, db: Database):
        self.db = db
        self._adapters: dict[Chain, BaseAdapter] = {
            Chain.ETHEREUM: EthereumAdapter(),
            Chain.TRON:     TronAdapter(),
            Chain.DOGECOIN: DogecoinAdapter(),
        }

    async def close(self) -> None:
        for adapter in self._adapters.values():
            await adapter.close()

    # ─── Public ───────────────────────────────────────────────────────────────

    async def ingest_wallet(
        self,
        request: WalletIngestionRequest,
    ) -> IngestionResult:
        """
        Full ingestion pipeline for a single wallet:
        1. Upsert wallet record
        2. Fetch all transactions via chain adapter
        3. Store transactions
        4. Log result
        """
        address = request.address.strip()
        chain   = request.chain

        logger.info("ingestion_start", address=address[:12], chain=chain.value)
        t_start = time.monotonic()

        # Validate address format
        adapter = self._adapters[chain]
        if not adapter.is_valid_address(address):
            return IngestionResult(
                address=address,
                chain=chain,
                tx_count=0,
                status="error",
                error=f"Invalid {chain.value} address format",
            )

        try:
            # Upsert wallet
            wallet_id = await self.db.upsert_wallet(
                address=address,
                chain=chain,
                label=request.label,
                is_target=request.is_target,
                is_sender=request.is_sender,
            )

            # Fetch transactions
            txs = await adapter.get_transactions(address)

            # Store
            inserted = await self.db.insert_transactions(wallet_id, txs)

            duration_ms = int((time.monotonic() - t_start) * 1000)

            # Metrics
            WALLETS_INGESTED.labels(chain=chain.value, status="success").inc()
            TRANSACTIONS_INGESTED.labels(chain=chain.value).inc(inserted)
            INGESTION_DURATION.labels(chain=chain.value).observe(
                (time.monotonic() - t_start)
            )

            await self.db.log_ingestion(
                wallet_id=wallet_id,
                chain=chain,
                status="success",
                tx_count=inserted,
                duration_ms=duration_ms,
            )

            logger.info(
                "ingestion_complete",
                address=address[:12],
                chain=chain.value,
                tx_count=inserted,
                duration_ms=duration_ms,
            )

            return IngestionResult(
                address=address,
                chain=chain,
                tx_count=inserted,
                status="success",
                duration_ms=duration_ms,
            )

        except Exception as e:
            duration_ms = int((time.monotonic() - t_start) * 1000)
            error_str   = str(e)

            WALLETS_INGESTED.labels(chain=chain.value, status="error").inc()
            API_ERRORS.labels(chain=chain.value, error_type=type(e).__name__).inc()

            await self.db.log_ingestion(
                wallet_id=None,
                chain=chain,
                status="error",
                error=error_str,
                duration_ms=duration_ms,
            )

            logger.error(
                "ingestion_failed",
                address=address[:12],
                chain=chain.value,
                error=error_str,
            )

            return IngestionResult(
                address=address,
                chain=chain,
                tx_count=0,
                status="error",
                error=error_str,
                duration_ms=duration_ms,
            )

    async def ingest_from_tx_hash(
        self,
        tx_hash:   str,
        chain:     Optional[Chain] = None,
        is_target: bool            = True,
    ) -> Optional[IngestionResult]:
        """
        Resolve a tx hash to its TO address, then ingest that address.
        This is how we go from your Excel tx hashes → BDAG collector wallets.
        """
        # Auto-detect chain if not provided
        if chain is None:
            detected = AddressResolver.detect_from_tx_hash(tx_hash)
            if detected is None:
                logger.warning("tx_hash_chain_unknown", tx_hash=tx_hash[:16])
                return None
            chain = detected

        adapter = self._adapters[chain]

        try:
            tx = await adapter.resolve_tx_hash(tx_hash)
            if not tx:
                logger.warning("tx_hash_not_found", tx_hash=tx_hash[:16])
                return None

            to_address = tx.to_address
            logger.info(
                "tx_hash_resolved",
                tx_hash=tx_hash[:16],
                to_address=to_address[:12],
                chain=chain.value,
            )

            return await self.ingest_wallet(
                WalletIngestionRequest(
                    address=to_address,
                    chain=chain,
                    label=f"Resolved from tx {tx_hash[:12]}",
                    is_target=is_target,
                )
            )

        except Exception as e:
            logger.error("tx_hash_resolution_failed", tx_hash=tx_hash[:16], error=str(e))
            return None

    async def ingest_batch(
        self,
        requests: list[WalletIngestionRequest],
    ) -> list[IngestionResult]:
        """Ingest multiple wallets sequentially with logging."""
        results = []
        for i, req in enumerate(requests, 1):
            logger.info(
                "batch_ingestion_progress",
                current=i,
                total=len(requests),
                address=req.address[:12],
            )
            result = await self.ingest_wallet(req)
            results.append(result)

        success = sum(1 for r in results if r.status == "success")
        logger.info(
            "batch_ingestion_complete",
            total=len(results),
            success=success,
            failed=len(results) - success,
        )
        return results
