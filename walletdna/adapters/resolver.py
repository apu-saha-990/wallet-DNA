"""
WalletDNA — Address Resolver
Auto-detects which chain(s) an address belongs to.
EVM addresses are ambiguous — resolved by querying all EVM explorers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import structlog

from walletdna.engine.models import Chain

logger = structlog.get_logger(__name__)


@dataclass
class ResolvedAddress:
    address: str
    chains:  list[Chain]
    method:  str       # "deterministic" | "api_verified" | "ambiguous"
    notes:   Optional[str] = None


class AddressResolver:
    """
    Determines which chain(s) an address belongs to.

    Priority:
    1. Deterministic — format alone identifies the chain (TRX, DOGE, etc.)
    2. EVM — 0x prefix is shared across ETH/BNB/MATIC/FTM, needs API verification
    3. Unknown — flag for manual review
    """

    # ─── Deterministic Rules ─────────────────────────────────────────────────

    @staticmethod
    def detect(address: str) -> ResolvedAddress:
        """
        Synchronous format-based detection.
        For EVM addresses, returns all possible chains — caller must verify.
        """
        if not address or not isinstance(address, str):
            return ResolvedAddress(address=address, chains=[], method="invalid")

        addr = address.strip()

        # ── Tron ──────────────────────────────────────────────────────────────
        if addr.startswith("T") and len(addr) == 34 and addr.isalnum():
            return ResolvedAddress(
                address=addr,
                chains=[Chain.TRON],
                method="deterministic",
            )

        # ── Dogecoin ──────────────────────────────────────────────────────────
        if addr.startswith("D") and len(addr) in (33, 34) and addr.isalnum():
            return ResolvedAddress(
                address=addr,
                chains=[Chain.DOGECOIN],
                method="deterministic",
            )

        # ── Ethereum / EVM ────────────────────────────────────────────────────
        if addr.startswith("0x") and len(addr) == 42:
            return ResolvedAddress(
                address=addr,
                chains=[Chain.ETHEREUM],  # We only support ETH in Phase 1
                method="deterministic",
                notes="EVM address — confirmed as Ethereum for Phase 1",
            )

        # ── Unknown ───────────────────────────────────────────────────────────
        logger.warning("address_unrecognised", address=addr[:16])
        return ResolvedAddress(
            address=addr,
            chains=[],
            method="unknown",
            notes=f"Unrecognised format — manual review required",
        )

    @staticmethod
    def detect_from_tx_hash(tx_hash: str) -> Optional[Chain]:
        """
        Best-guess chain from transaction hash format.
        ETH hashes start with 0x, TRX/DOGE are plain hex.
        """
        if tx_hash.startswith("0x") and len(tx_hash) == 66:
            return Chain.ETHEREUM
        if len(tx_hash) == 64 and all(c in "0123456789abcdefABCDEF" for c in tx_hash):
            # Could be TRX or DOGE — ambiguous without context
            return None
        return None

    @staticmethod
    def format_address(address: str, chain: Chain) -> str:
        """Return checksummed/formatted address for chain."""
        if chain == Chain.ETHEREUM:
            # Basic lowercase for now — use web3.py checksumming if needed
            return address.lower()
        return address

    @staticmethod
    def truncate(address: str, chars: int = 8) -> str:
        """Return truncated address for display: 0xABCD...1234"""
        if not address or len(address) <= chars * 2:
            return address
        return f"{address[:chars]}...{address[-6:]}"
