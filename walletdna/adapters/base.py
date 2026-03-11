"""
WalletDNA — Base Chain Adapter
Abstract interface every chain adapter must implement.
The DNA engine only ever interacts with this interface.
"""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from collections import deque
from typing import Optional

import structlog

from walletdna.engine.models import Chain, NormalisedTx

logger = structlog.get_logger(__name__)


class RateLimiter:
    """
    Token bucket rate limiter.
    Ensures we never exceed chain API rate limits.
    """

    def __init__(self, calls_per_second: float):
        self.calls_per_second = calls_per_second
        self.min_interval = 1.0 / calls_per_second
        self._last_call = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait = self.min_interval - (now - self._last_call)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_call = time.monotonic()


class BaseAdapter(ABC):
    """
    Abstract base for all chain adapters.

    Each adapter is responsible for:
    - Fetching full transaction history for a wallet
    - Resolving a transaction hash to its TO address
    - Detecting if an address is valid for this chain
    - Normalising chain-native data to NormalisedTx

    The adapter must NOT leak chain-specific data structures
    beyond its own module. Everything returned is NormalisedTx.
    """

    chain: Chain
    _rate_limiter: RateLimiter

    @abstractmethod
    async def get_transactions(
        self,
        address: str,
        start_block: Optional[int] = None,
        end_block:   Optional[int] = None,
        max_txs:     int           = 10_000,
    ) -> list[NormalisedTx]:
        """
        Fetch full transaction history for an address.
        Returns list of NormalisedTx, sorted oldest-first.
        """
        ...

    @abstractmethod
    async def resolve_tx_hash(self, tx_hash: str) -> Optional[NormalisedTx]:
        """
        Resolve a transaction hash to a NormalisedTx.
        Returns None if tx not found.
        """
        ...

    @abstractmethod
    def is_valid_address(self, address: str) -> bool:
        """
        Check if address format is valid for this chain.
        Must be synchronous — format check only, no API call.
        """
        ...

    @abstractmethod
    async def get_wallet_age_days(self, address: str) -> Optional[float]:
        """
        Return wallet age in days (first tx to now).
        Returns None if wallet has no transactions.
        """
        ...

    async def close(self) -> None:
        """Release any resources (HTTP session, etc.)"""
        pass

    # ─── Shared Utilities ─────────────────────────────────────────────────────

    def _determine_direction(
        self, address: str, from_address: str, to_address: str
    ) -> str:
        addr = address.lower()
        frm  = from_address.lower()
        to   = to_address.lower()
        if frm == to == addr:
            return "self"
        elif frm == addr:
            return "out"
        else:
            return "in"

    def _gwei_to_eth(self, gwei: float) -> float:
        return gwei / 1e9

    def _wei_to_gwei(self, wei: int) -> float:
        return wei / 1e9

    def _wei_to_eth(self, wei: int) -> float:
        return wei / 1e18

    async def _fetch_with_retry(
        self,
        fetch_fn,
        max_retries: int = 3,
        backoff_base: float = 2.0,
    ):
        """
        Execute fetch_fn with exponential backoff on failure.
        Respects rate limiter before each attempt.
        """
        last_err = None
        for attempt in range(max_retries):
            try:
                await self._rate_limiter.acquire()
                return await fetch_fn()
            except Exception as e:
                last_err = e
                wait = backoff_base ** attempt
                logger.warning(
                    "adapter_fetch_retry",
                    chain=self.chain,
                    attempt=attempt + 1,
                    wait=wait,
                    error=str(e),
                )
                await asyncio.sleep(wait)

        raise RuntimeError(
            f"[{self.chain}] Max retries exceeded: {last_err}"
        ) from last_err
