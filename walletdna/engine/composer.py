"""
WalletDNA — DNA Composer
Assembles all feature dimensions into:
  1. Human-readable DNA string: G:MED-STABLE | T:0200-0600UTC | ...
  2. Numeric vector for similarity computation

The DNA string is the deliverable. The vector is the engine underneath.
"""

from __future__ import annotations

from typing import Optional

import structlog

from walletdna.engine.classifier import BotClassifier
from walletdna.engine.extractor import FeatureExtractor
from walletdna.engine.models import (
    ActivityFeature,
    BotClassification,
    Chain,
    ContractFeature,
    DNAProfile,
    GasFeature,
    MempoolFeature,
    NormalisedTx,
    TimingFeature,
    ValueFeature,
)

logger = structlog.get_logger(__name__)

# Minimum transactions to attempt DNA generation
MIN_TX_FOR_DNA = 5


class DNAComposer:
    """
    Takes a list of NormalisedTx for a wallet and produces a full DNAProfile.

    Usage:
        composer = DNAComposer()
        profile  = composer.compose(txs, address, chain, label)
    """

    def __init__(self):
        self._classifier = BotClassifier()

    def compose(
        self,
        txs:     list[NormalisedTx],
        address: str,
        chain:   Chain,
        label:   Optional[str] = None,
    ) -> DNAProfile:
        """
        Full pipeline:
        1. Extract all 6 feature dimensions
        2. Run bot classification
        3. Build DNA string
        4. Build numeric vector
        5. Return complete DNAProfile
        """
        n = len(txs)

        if n < MIN_TX_FOR_DNA:
            logger.warning(
                "dna_insufficient_txs",
                address=address[:12],
                tx_count=n,
                minimum=MIN_TX_FOR_DNA,
            )
            return DNAProfile(
                address=address,
                chain=chain,
                label=label,
                tx_count=n,
                confidence_score=0.0,
                error=f"Insufficient transaction history ({n} txs, minimum {MIN_TX_FOR_DNA})",
            )

        logger.info("dna_composing", address=address[:12], chain=chain.value, tx_count=n)

        # ── Feature Extraction ────────────────────────────────────────────────
        extractor = FeatureExtractor(txs, address, chain)

        gas      = extractor.extract_gas()
        timing   = extractor.extract_timing()
        value    = extractor.extract_value()
        contract = extractor.extract_contract()
        mempool  = extractor.extract_mempool()
        activity = extractor.extract_activity()

        # ── Bot Classification ────────────────────────────────────────────────
        classification = self._classifier.classify(
            gas=gas,
            timing=timing,
            value=value,
            contract=contract,
            mempool=mempool,
            activity=activity,
            tx_count=n,
        )

        # ── DNA String ────────────────────────────────────────────────────────
        dna_string = self._build_dna_string(
            gas, timing, value, contract, mempool, activity, classification
        )

        # ── DNA Vector ────────────────────────────────────────────────────────
        dna_vector = self._build_dna_vector(
            gas, timing, value, contract, mempool, activity
        )

        # ── Confidence ────────────────────────────────────────────────────────
        confidence = extractor.overall_confidence()

        profile = DNAProfile(
            address=address,
            chain=chain,
            label=label,
            gas=gas,
            timing=timing,
            value=value,
            contract=contract,
            mempool=mempool,
            activity=activity,
            classification=classification,
            dna_string=dna_string,
            dna_vector=dna_vector,
            tx_count=n,
            confidence_score=confidence,
        )

        logger.info(
            "dna_composed",
            address=address[:12],
            chain=chain.value,
            wallet_class=classification.wallet_class.value,
            confidence=confidence,
            dna_string=dna_string,
        )

        return profile

    # ─── DNA String Builder ───────────────────────────────────────────────────

    def _build_dna_string(
        self,
        gas:            Optional[GasFeature],
        timing:         Optional[TimingFeature],
        value:          Optional[ValueFeature],
        contract:       Optional[ContractFeature],
        mempool:        Optional[MempoolFeature],
        activity:       Optional[ActivityFeature],
        classification: BotClassification,
    ) -> str:
        """
        Produces the canonical DNA string.
        Format: G:MED-STABLE | T:0200-0600UTC | V:SPLIT-HIGH | C:DEX-HEAVY | M:INSTANT | A:BURST-SLEEP | X:BOT-HIGH
        """
        parts = []

        # G — Gas Profile
        if gas:
            parts.append(f"G:{gas.label.value}-{gas.stability.value}")
        else:
            parts.append("G:N/A")

        # T — Timing Pattern
        if timing:
            parts.append(f"T:{timing.active_window_utc}")
        else:
            parts.append("T:N/A")

        # V — Value Fragmentation
        if value:
            frag = value.fragmentation.value
            round_hint = "ROUND" if value.round_number_ratio > 0.5 else "PRECISE"
            parts.append(f"V:SPLIT-{frag}-{round_hint}")
        else:
            parts.append("V:N/A")

        # C — Contract Preference
        if contract:
            if contract.not_applicable:
                parts.append("C:N/A-UTXO")
            else:
                parts.append(f"C:{contract.top_type}")
        else:
            parts.append("C:N/A")

        # M — Mempool Behaviour
        if mempool:
            if mempool.not_applicable:
                parts.append("M:N/A-UTXO")
            else:
                parts.append(f"M:{mempool.label}")
        else:
            parts.append("M:N/A")

        # A — Activity Cycle
        if activity:
            parts.append(f"A:{activity.label.value}")
        else:
            parts.append("A:N/A")

        # X — Bot Classification (7th dimension)
        cls   = classification.wallet_class.value
        conf  = classification.confidence
        level = "HIGH" if conf > 0.65 else "MED" if conf > 0.40 else "LOW"
        parts.append(f"X:{cls}-{level}")

        return " | ".join(parts)

    # ─── DNA Vector Builder ───────────────────────────────────────────────────

    def _build_dna_vector(
        self,
        gas:      Optional[GasFeature],
        timing:   Optional[TimingFeature],
        value:    Optional[ValueFeature],
        contract: Optional[ContractFeature],
        mempool:  Optional[MempoolFeature],
        activity: Optional[ActivityFeature],
    ) -> list[float]:
        """
        Produces a numeric vector for cosine similarity comparison.
        Dimension weights reflect how stable/reliable each signal is.

        Vector layout (10 dimensions):
        [0]  gas_score           (weight: 0.10)
        [1]  gas_stability_norm  (weight: 0.05)
        [2]  timing_entropy      (weight: 0.15)
        [3]  timing_window_norm  (weight: 0.10)
        [4]  value_hhi           (weight: 0.10)
        [5]  round_number_ratio  (weight: 0.10)
        [6]  contract_dex_ratio  (weight: 0.08)
        [7]  mempool_instant     (weight: 0.07)
        [8]  burst_score         (weight: 0.13)
        [9]  dormancy_score      (weight: 0.12)
        """
        def safe(val, default=0.5):
            return float(val) if val is not None else default

        # Gas stability: STABLE=0.0, MODERATE=0.5, ERRATIC=1.0
        gas_stability_map = {"STABLE": 0.0, "MODERATE": 0.5, "ERRATIC": 1.0}

        vector = [
            safe(gas.score if gas else None),
            gas_stability_map.get(gas.stability.value, 0.5) if gas else 0.5,
            safe(timing.timing_entropy if timing else None),
            # Normalise active window: 0h window=0.0, 24h window=1.0
            safe(
                ((timing.active_hour_end - timing.active_hour_start) % 24) / 24
                if timing else None
            ),
            safe(value.herfindahl_index if value else None),
            safe(value.round_number_ratio if value else None),
            safe(contract.dex_ratio if contract and not contract.not_applicable else None, default=0.0),
            safe(mempool.instant_ratio if mempool and not mempool.not_applicable else None),
            safe(activity.burst_score if activity else None),
            safe(activity.dormancy_score if activity else None),
        ]

        return [round(v, 6) for v in vector]
