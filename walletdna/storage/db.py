"""
WalletDNA — Database Client
Async PostgreSQL client using asyncpg.
All DB operations go through this module.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Optional

import asyncpg
import structlog

from walletdna.engine.models import Chain, DNAProfile, NormalisedTx, WalletClass

logger = structlog.get_logger(__name__)


class Database:

    def __init__(self, dsn: Optional[str] = None):
        self.dsn  = dsn or os.getenv(
            "DATABASE_URL",
            "postgresql://walletdna:walletdna_secret@localhost:5432/walletdna"
        )
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(
            dsn=self.dsn,
            min_size=2,
            max_size=10,
            command_timeout=60,
        )
        logger.info("db_connected", dsn=self.dsn.split("@")[-1])

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            logger.info("db_closed")

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[asyncpg.Connection, None]:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                yield conn

    # ─── Wallets ─────────────────────────────────────────────────────────────

    async def upsert_wallet(
        self,
        address:   str,
        chain:     Chain,
        label:     Optional[str] = None,
        is_target: bool          = False,
        is_sender: bool          = False,
    ) -> int:
        """Insert or update wallet, return wallet_id."""
        row = await self._pool.fetchrow(
            """
            INSERT INTO wallets (address, chain, label, is_target, is_sender)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (address, chain) DO UPDATE
                SET label     = COALESCE($3, wallets.label),
                    is_target = wallets.is_target OR $4,
                    is_sender = wallets.is_sender OR $5,
                    updated_at = NOW()
            RETURNING id
            """,
            address.lower(), chain.value, label, is_target, is_sender,
        )
        return row["id"]

    async def get_wallet_id(self, address: str, chain: Chain) -> Optional[int]:
        row = await self._pool.fetchrow(
            "SELECT id FROM wallets WHERE address = $1 AND chain = $2",
            address.lower(), chain.value,
        )
        return row["id"] if row else None

    async def get_all_target_wallets(self) -> list[dict]:
        rows = await self._pool.fetch(
            "SELECT id, address, chain, label FROM wallets WHERE is_target = TRUE"
        )
        return [dict(r) for r in rows]

    async def get_all_sender_wallets(self) -> list[dict]:
        rows = await self._pool.fetch(
            "SELECT id, address, chain, label FROM wallets WHERE is_sender = TRUE"
        )
        return [dict(r) for r in rows]

    # ─── Transactions ─────────────────────────────────────────────────────────

    async def insert_transactions(
        self,
        wallet_id: int,
        txs:       list[NormalisedTx],
    ) -> int:
        """Bulk insert transactions. Returns count inserted."""
        if not txs:
            return 0

        records = [
            (
                wallet_id,
                tx.tx_hash,
                tx.chain.value,
                tx.block_number,
                tx.block_time,
                tx.from_address,
                tx.to_address,
                tx.direction,
                float(tx.value_native),
                float(tx.value_usd)        if tx.value_usd        else None,
                float(tx.fee_native)       if tx.fee_native        else None,
                float(tx.fee_usd)          if tx.fee_usd           else None,
                float(tx.gas_price_gwei)   if tx.gas_price_gwei    else None,
                tx.gas_used,
                tx.gas_limit,
                tx.energy_used,
                tx.bandwidth_used,
                tx.is_contract_call,
                tx.contract_method,
                tx.token_symbol,
                tx.confirmation_blocks,
            )
            for tx in txs
        ]

        await self._pool.executemany(
            """
            INSERT INTO transactions (
                wallet_id, tx_hash, chain, block_number, block_time,
                from_address, to_address, direction,
                value_native, value_usd, fee_native, fee_usd,
                gas_price, gas_used, gas_limit,
                energy_used, bandwidth_used,
                is_contract_call, contract_method, token_symbol, confirmation_blocks
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
                $13,$14,$15,$16,$17,$18,$19,$20,$21
            )
            ON CONFLICT DO NOTHING
            """,
            records,
        )

        # Update wallet tx count and timestamps
        await self._pool.execute(
            """
            UPDATE wallets SET
                tx_count   = (SELECT COUNT(*) FROM transactions WHERE wallet_id = $1),
                first_seen = (SELECT MIN(block_time) FROM transactions WHERE wallet_id = $1),
                last_seen  = (SELECT MAX(block_time) FROM transactions WHERE wallet_id = $1),
                updated_at = NOW()
            WHERE id = $1
            """,
            wallet_id,
        )

        return len(records)

    async def get_transactions(
        self,
        wallet_id: int,
        limit:     int = 10_000,
    ) -> list[dict]:
        rows = await self._pool.fetch(
            """
            SELECT * FROM transactions
            WHERE wallet_id = $1
            ORDER BY block_time ASC
            LIMIT $2
            """,
            wallet_id, limit,
        )
        return [dict(r) for r in rows]

    async def get_tx_count(self, wallet_id: int) -> int:
        row = await self._pool.fetchrow(
            "SELECT COUNT(*) as cnt FROM transactions WHERE wallet_id = $1",
            wallet_id,
        )
        return row["cnt"]

    # ─── DNA Profiles ─────────────────────────────────────────────────────────

    async def save_dna_profile(self, wallet_id: int, profile: DNAProfile) -> int:
        row = await self._pool.fetchrow(
            """
            INSERT INTO dna_profiles (
                wallet_id,
                gas_mean_gwei, gas_std_gwei, gas_percentile_50, gas_percentile_95,
                active_hour_start, active_hour_end, timing_entropy, median_interval_sec, sleep_gap_hours,
                value_herfindahl, value_fragmentation, round_number_ratio,
                contract_dex_ratio, contract_bridge_ratio, contract_eoa_ratio, top_contract_type,
                mempool_avg_wait_blocks, mempool_instant_ratio,
                burst_score, dormancy_score,
                wallet_class, bot_confidence, bot_signals,
                dna_string, dna_vector,
                tx_count_analysed, confidence_score
            ) VALUES (
                $1,
                $2,$3,$4,$5,
                $6,$7,$8,$9,$10,
                $11,$12,$13,
                $14,$15,$16,$17,
                $18,$19,
                $20,$21,
                $22,$23,$24,
                $25,$26,
                $27,$28
            )
            RETURNING id
            """,
            wallet_id,
            # Gas
            profile.gas.mean_gwei        if profile.gas else None,
            profile.gas.std_gwei         if profile.gas else None,
            profile.gas.percentile_50    if profile.gas else None,
            profile.gas.percentile_95    if profile.gas else None,
            # Timing
            profile.timing.active_hour_start   if profile.timing else None,
            profile.timing.active_hour_end     if profile.timing else None,
            profile.timing.timing_entropy      if profile.timing else None,
            profile.timing.median_interval_sec if profile.timing else None,
            profile.timing.sleep_gap_hours     if profile.timing else None,
            # Value
            profile.value.herfindahl_index  if profile.value else None,
            profile.value.fragmentation.value if profile.value else None,
            profile.value.round_number_ratio  if profile.value else None,
            # Contract
            profile.contract.dex_ratio       if profile.contract else None,
            profile.contract.bridge_ratio    if profile.contract else None,
            profile.contract.eoa_ratio       if profile.contract else None,
            profile.contract.top_type        if profile.contract else None,
            # Mempool
            profile.mempool.avg_wait_blocks  if profile.mempool else None,
            profile.mempool.instant_ratio    if profile.mempool else None,
            # Activity
            profile.activity.burst_score     if profile.activity else None,
            profile.activity.dormancy_score  if profile.activity else None,
            # Classification
            profile.classification.wallet_class.value if profile.classification else "UNKNOWN",
            profile.classification.confidence         if profile.classification else 0.0,
            profile.classification.signals            if profile.classification else [],
            # DNA output
            profile.dna_string,
            profile.dna_vector,
            # Meta
            profile.tx_count,
            profile.confidence_score,
        )
        return row["id"]

    async def get_latest_dna(self, wallet_id: int) -> Optional[dict]:
        row = await self._pool.fetchrow(
            """
            SELECT * FROM dna_profiles
            WHERE wallet_id = $1
            ORDER BY generated_at DESC
            LIMIT 1
            """,
            wallet_id,
        )
        return dict(row) if row else None

    async def get_all_dna_vectors(self) -> list[dict]:
        """Return all wallet DNA vectors for similarity computation."""
        rows = await self._pool.fetch(
            """
            SELECT DISTINCT ON (wallet_id)
                wallet_id, dna_vector, dna_string, wallet_class
            FROM dna_profiles
            WHERE dna_vector IS NOT NULL
            ORDER BY wallet_id, generated_at DESC
            """
        )
        return [dict(r) for r in rows]

    # ─── Similarity ───────────────────────────────────────────────────────────

    async def save_similarity(
        self,
        wallet_a_id: int,
        wallet_b_id: int,
        similarity:  float,
    ) -> None:
        await self._pool.execute(
            """
            INSERT INTO similarity_results (wallet_a_id, wallet_b_id, similarity)
            VALUES ($1, $2, $3)
            ON CONFLICT (wallet_a_id, wallet_b_id) DO UPDATE
                SET similarity = $3, computed_at = NOW()
            """,
            wallet_a_id, wallet_b_id, similarity,
        )

    async def get_top_matches(
        self,
        wallet_id:  int,
        threshold:  float = 0.75,
        limit:      int   = 10,
    ) -> list[dict]:
        rows = await self._pool.fetch(
            """
            SELECT
                CASE WHEN wallet_a_id = $1 THEN wallet_b_id ELSE wallet_a_id END AS matched_wallet_id,
                similarity
            FROM similarity_results
            WHERE (wallet_a_id = $1 OR wallet_b_id = $1)
              AND similarity >= $2
            ORDER BY similarity DESC
            LIMIT $3
            """,
            wallet_id, threshold, limit,
        )
        return [dict(r) for r in rows]

    # ─── Ingestion Log ────────────────────────────────────────────────────────

    async def log_ingestion(
        self,
        wallet_id:   Optional[int],
        chain:       Chain,
        status:      str,
        tx_count:    int   = 0,
        error:       Optional[str] = None,
        duration_ms: int   = 0,
    ) -> None:
        await self._pool.execute(
            """
            INSERT INTO ingestion_log
                (wallet_id, chain, status, tx_count, error_message, duration_ms)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            wallet_id, chain.value, status, tx_count, error, duration_ms,
        )
