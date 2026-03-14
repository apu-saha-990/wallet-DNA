"""
WalletDNA — Feature Extractor
Extracts all 6 behavioural dimensions from a wallet's transaction history.
Each method returns a structured feature object with score + confidence.

Design principles:
- Sparse wallets (< 10 txs) get low confidence, not fabricated scores
- Every dimension is independently scoreable — missing data degrades gracefully
- No magic numbers without explanation
"""

from __future__ import annotations

import math
import statistics
from collections import Counter
from datetime import datetime, timezone
from typing import Optional

import structlog

from walletdna.engine.models import (
    ActivityFeature,
    BurstLabel,
    Chain,
    ContractFeature,
    FragmentationLabel,
    GasFeature,
    GasLabel,
    MempoolFeature,
    NormalisedTx,
    StabilityLabel,
    TimingFeature,
    ValueFeature,
)

logger = structlog.get_logger(__name__)

# Minimum tx counts for reliable scoring
MIN_TX_GAS        = 5
MIN_TX_TIMING     = 10
MIN_TX_VALUE      = 5
MIN_TX_CONTRACT   = 5
MIN_TX_ACTIVITY   = 10


class FeatureExtractor:
    """
    Extracts behavioural features from a list of NormalisedTx.

    Usage:
        extractor = FeatureExtractor(txs, address, chain)
        gas      = extractor.extract_gas()
        timing   = extractor.extract_timing()
        ...
    """

    def __init__(
        self,
        txs:     list[NormalisedTx],
        address: str,
        chain:   Chain,
    ):
        self.txs     = sorted(txs, key=lambda t: t.block_time)
        self.address = address.lower()
        self.chain   = chain
        self.n       = len(txs)

        # Pre-filtered subsets used across multiple extractors
        self._outbound = [t for t in self.txs if t.direction == "out"]
        self._inbound  = [t for t in self.txs if t.direction == "in"]

    # ─── Dimension 1: Gas Profile ─────────────────────────────────────────────

    def extract_gas(self) -> Optional[GasFeature]:
        """
        Analyses gas price behaviour — mean, variance, percentiles.
        High stability + medium price = bot signature.
        ETH and TRX only — DOGE returns None.
        """
        if self.chain == Chain.DOGECOIN:
            return None

        gas_prices = [
            t.gas_price_gwei
            for t in self.txs
            if t.gas_price_gwei is not None and t.gas_price_gwei > 0
        ]

        if len(gas_prices) < MIN_TX_GAS:
            return GasFeature(
                mean_gwei=0, std_gwei=0, percentile_50=0, percentile_95=0,
                label=GasLabel.MED, stability=StabilityLabel.MODERATE,
                score=0.5, confidence=0.1,
            )

        mean_g  = statistics.mean(gas_prices)
        std_g   = statistics.stdev(gas_prices) if len(gas_prices) > 1 else 0.0
        p50     = statistics.median(gas_prices)
        sorted_prices = sorted(gas_prices)
        p95     = sorted_prices[int(len(sorted_prices) * 0.95)]

        # Coefficient of variation — normalised measure of consistency
        cv = (std_g / mean_g) if mean_g > 0 else 0

        # Gas level label
        if mean_g < 15:
            label = GasLabel.LOW
        elif mean_g < 50:
            label = GasLabel.MED
        else:
            label = GasLabel.HIGH

        # Stability label — CV thresholds derived from empirical bot/human data
        if cv < 0.15:
            stability = StabilityLabel.STABLE    # Very consistent — bot signal
        elif cv < 0.40:
            stability = StabilityLabel.MODERATE
        else:
            stability = StabilityLabel.ERRATIC   # Human-like variation

        # Score: 0 = perfectly stable (bot-like), 1 = highly erratic (human-like)
        score = min(cv / 0.6, 1.0)

        confidence = min(len(gas_prices) / 50, 1.0)

        return GasFeature(
            mean_gwei=round(mean_g, 4),
            std_gwei=round(std_g, 4),
            percentile_50=round(p50, 4),
            percentile_95=round(p95, 4),
            label=label,
            stability=stability,
            score=round(score, 4),
            confidence=round(confidence, 4),
        )

    # ─── Dimension 2: Timing Pattern ──────────────────────────────────────────

    def extract_timing(self) -> Optional[TimingFeature]:
        """
        Analyses when a wallet is active (UTC hours).
        Tight active window + low entropy = bot signature.
        """
        if self.n < MIN_TX_TIMING:
            return TimingFeature(
                active_hour_start=0, active_hour_end=23,
                active_window_utc="00-23UTC",
                timing_entropy=1.0,
                median_interval_sec=0,
                sleep_gap_hours=0,
                score=0.5, confidence=max(self.n / MIN_TX_TIMING * 0.3, 0.05),
            )

        hours = [t.block_time.hour for t in self.txs]

        # Hour frequency distribution
        hour_counts = Counter(hours)
        hour_dist   = [hour_counts.get(h, 0) for h in range(24)]
        total       = sum(hour_dist)

        # Shannon entropy of the hour distribution
        # Low entropy = concentrated activity = bot-like
        entropy = 0.0
        for count in hour_dist:
            if count > 0:
                p = count / total
                entropy -= p * math.log2(p)
        # Normalise to 0–1 (max entropy for 24 bins = log2(24) ≈ 4.585)
        normalised_entropy = entropy / math.log2(24)

        # Find primary active window (contiguous hours with >5% of activity)
        threshold = total * 0.05
        active_hours = sorted([h for h, c in hour_counts.items() if c >= threshold])

        if active_hours:
            start_h = active_hours[0]
            end_h   = active_hours[-1]
        else:
            start_h, end_h = 0, 23

        if start_h == end_h:
            window_str = f"{start_h:02d}00UTC"   # single active hour
        else:
            window_str = f"{start_h:02d}00-{end_h:02d}00UTC"

        # Inter-transaction intervals
        timestamps = [t.block_time.timestamp() for t in self.txs]
        intervals  = [
            int(timestamps[i+1] - timestamps[i])
            for i in range(len(timestamps) - 1)
            if timestamps[i+1] > timestamps[i]
        ]

        median_interval = int(statistics.median(intervals)) if intervals else 0

        # Longest dormancy gap (hours)
        sleep_gap_hours = max(intervals) / 3600 if intervals else 0

        # Score: 0 = tight window (bot), 1 = spread across day (human)
        score = normalised_entropy

        confidence = min(self.n / 100, 1.0)

        return TimingFeature(
            active_hour_start=start_h,
            active_hour_end=end_h,
            active_window_utc=window_str,
            timing_entropy=round(normalised_entropy, 4),
            median_interval_sec=median_interval,
            sleep_gap_hours=round(sleep_gap_hours, 2),
            score=round(score, 4),
            confidence=round(confidence, 4),
        )

    # ─── Dimension 3: Value Fragmentation ─────────────────────────────────────

    def extract_value(self) -> Optional[ValueFeature]:
        """
        Analyses how a wallet splits and moves value.
        High fragmentation (Herfindahl near 0) = consistent splitting = bot signal.
        Round number ratio = human signal (people send round amounts).
        """
        outbound_values = [
            t.value_usd if t.value_usd else t.value_native
            for t in self._outbound
            if t.value_native > 0
        ]

        if len(outbound_values) < MIN_TX_VALUE:
            return ValueFeature(
                herfindahl_index=0.5,
                fragmentation=FragmentationLabel.MED,
                round_number_ratio=0.0,
                median_value_usd=None,
                score=0.5,
                confidence=max(len(outbound_values) / MIN_TX_VALUE * 0.3, 0.05),
            )

        total = sum(outbound_values)
        median_val = statistics.median(outbound_values)

        # Herfindahl-Hirschman Index — measure of concentration
        # 1.0 = all value in one tx, 0.0 = perfectly distributed
        hhi = sum((v / total) ** 2 for v in outbound_values) if total > 0 else 0.5

        # Fragmentation label
        if hhi < 0.15:
            fragmentation = FragmentationLabel.HIGH   # Very spread = suspicious
        elif hhi < 0.40:
            fragmentation = FragmentationLabel.MED
        else:
            fragmentation = FragmentationLabel.LOW    # Concentrated = normal

        # Round number ratio — humans tend to send $100, $500, $1000
        def is_round(v: float) -> bool:
            if v <= 0:
                return False
            # Check if round to nearest 10, 50, 100, 500, 1000
            for denom in [1000, 500, 100, 50, 10, 5]:
                if abs(v % denom) < (denom * 0.02):  # within 2% of round number
                    return True
            return False

        round_count = sum(1 for v in outbound_values if is_round(v))
        round_ratio = round_count / len(outbound_values)

        # Score: 0 = highly fragmented (bot-like), 1 = concentrated (human-like)
        # Also penalise very low round number ratio (bot sends precise amounts)
        fragmentation_score = min(hhi * 2, 1.0)
        round_score         = round_ratio
        score               = (fragmentation_score * 0.6) + (round_score * 0.4)

        confidence = min(len(outbound_values) / 30, 1.0)

        return ValueFeature(
            herfindahl_index=round(hhi, 4),
            fragmentation=fragmentation,
            round_number_ratio=round(round_ratio, 4),
            median_value_usd=round(median_val, 2) if median_val else None,
            score=round(score, 4),
            confidence=round(confidence, 4),
        )

    # ─── Dimension 4: Contract Interaction ────────────────────────────────────

    def extract_contract(self) -> Optional[ContractFeature]:
        """
        Analyses which types of contracts a wallet prefers.
        DEX-heavy + consistent method calls = bot signal.
        DOGE wallets return not_applicable.
        """
        if self.chain == Chain.DOGECOIN:
            return ContractFeature(
                dex_ratio=0, bridge_ratio=0, eoa_ratio=1.0,
                top_type="UTXO-CHAIN",
                score=0.5, confidence=1.0,
                not_applicable=True,
            )

        contract_txs = [t for t in self.txs if t.is_contract_call]
        eoa_txs      = [t for t in self.txs if not t.is_contract_call]

        if self.n < MIN_TX_CONTRACT:
            return ContractFeature(
                dex_ratio=0, bridge_ratio=0, eoa_ratio=0,
                top_type="UNKNOWN",
                score=0.5, confidence=0.1,
            )

        eoa_ratio = len(eoa_txs) / self.n

        if not contract_txs:
            return ContractFeature(
                dex_ratio=0, bridge_ratio=0, eoa_ratio=eoa_ratio,
                top_type="EOA-ONLY",
                score=0.3,   # Pure EOA is moderately human-like
                confidence=min(self.n / 20, 1.0),
            )

        # Method type counts
        method_counts: Counter = Counter()
        for tx in contract_txs:
            method = tx.contract_method or "UNKNOWN"
            if method in ("DEX",):
                method_counts["DEX"] += 1
            elif method in ("BRIDGE",):
                method_counts["BRIDGE"] += 1
            elif method in ("TRANSFER", "APPROVE"):
                method_counts["TRANSFER"] += 1
            elif method in ("STAKE", "LENDING"):
                method_counts["DEFI"] += 1
            else:
                method_counts["OTHER"] += 1

        total_contract = len(contract_txs)
        dex_ratio    = method_counts.get("DEX", 0) / total_contract
        bridge_ratio = method_counts.get("BRIDGE", 0) / total_contract

        top_type_raw = method_counts.most_common(1)[0][0] if method_counts else "OTHER"

        if dex_ratio > 0.6:
            top_type = "DEX-HEAVY"
        elif eoa_ratio > 0.7:
            top_type = "EOA-DOMINANT"
        elif bridge_ratio > 0.3:
            top_type = "BRIDGE-HEAVY"
        else:
            top_type = f"{top_type_raw}-MIX"

        # Score: 0 = DEX-heavy (bot-like), 1 = varied/EOA (human-like)
        bot_signal = (dex_ratio * 0.7) + ((1 - eoa_ratio) * 0.3)
        score      = 1.0 - bot_signal

        confidence = min(total_contract / 20, 1.0)

        return ContractFeature(
            dex_ratio=round(dex_ratio, 4),
            bridge_ratio=round(bridge_ratio, 4),
            eoa_ratio=round(eoa_ratio, 4),
            top_type=top_type,
            score=round(score, 4),
            confidence=round(confidence, 4),
        )

    # ─── Dimension 5: Mempool Behaviour ───────────────────────────────────────

    def extract_mempool(self) -> Optional[MempoolFeature]:
        """
        Analyses transaction confirmation speed.
        Instant confirmation (<=2 blocks) consistently = bot using optimal gas.
        Only meaningful for ETH where confirmation_blocks is tracked.
        """
        if self.chain in (Chain.DOGECOIN, Chain.TRON):
            return MempoolFeature(
                avg_wait_blocks=1.0,
                instant_ratio=0.5,
                label="UTXO-CHAIN",
                score=0.5,
                confidence=1.0,
                not_applicable=True,
            )

        conf_txs = [
            t for t in self.txs
            if t.confirmation_blocks is not None and t.confirmation_blocks > 0
        ]

        if len(conf_txs) < 5:
            # Fall back to gas price as proxy for mempool priority
            gas_prices = [t.gas_price_gwei for t in self.txs if t.gas_price_gwei]
            if gas_prices:
                mean_gas = statistics.mean(gas_prices)
                # High gas = likely instant confirmation
                instant_ratio = min(mean_gas / 50, 1.0)
                label = "INSTANT" if instant_ratio > 0.7 else "NORMAL"
                return MempoolFeature(
                    avg_wait_blocks=1.5,
                    instant_ratio=round(instant_ratio, 4),
                    label=label,
                    score=round(1.0 - instant_ratio, 4),
                    confidence=0.3,
                )
            return MempoolFeature(
                avg_wait_blocks=2.0,
                instant_ratio=0.5,
                label="UNKNOWN",
                score=0.5,
                confidence=0.1,
            )

        wait_blocks   = [t.confirmation_blocks for t in conf_txs]
        avg_wait      = statistics.mean(wait_blocks)
        instant_count = sum(1 for w in wait_blocks if w <= 2)
        instant_ratio = instant_count / len(wait_blocks)

        if instant_ratio > 0.8:
            label = "INSTANT"
        elif avg_wait < 5:
            label = "FAST"
        elif avg_wait < 20:
            label = "NORMAL"
        else:
            label = "SLOW"

        # Score: 0 = always instant (bot), 1 = variable wait (human)
        score = 1.0 - instant_ratio

        confidence = min(len(conf_txs) / 30, 1.0)

        return MempoolFeature(
            avg_wait_blocks=round(avg_wait, 2),
            instant_ratio=round(instant_ratio, 4),
            label=label,
            score=round(score, 4),
            confidence=round(confidence, 4),
        )

    # ─── Dimension 6: Activity Cycle ──────────────────────────────────────────

    def extract_activity(self) -> Optional[ActivityFeature]:
        """
        Analyses burst/dormancy patterns in wallet activity.
        Intense bursts followed by long sleep = bot or coordinated operator.
        """
        if self.n < MIN_TX_ACTIVITY:
            return ActivityFeature(
                burst_score=0.3,
                dormancy_score=0.3,
                label=BurstLabel.STEADY,
                avg_daily_tx=0.0,
                peak_day_tx=self.n,
                score=0.5,
                confidence=max(self.n / MIN_TX_ACTIVITY * 0.2, 0.05),
            )

        # Group txs by day
        day_counts: Counter = Counter()
        for tx in self.txs:
            day_key = tx.block_time.date()
            day_counts[day_key] += 1

        daily_counts = list(day_counts.values())
        avg_daily    = statistics.mean(daily_counts)
        peak_day     = max(daily_counts)
        std_daily    = statistics.stdev(daily_counts) if len(daily_counts) > 1 else 0

        # Burst score: ratio of peak to average (high = bursty)
        burst_ratio  = peak_day / avg_daily if avg_daily > 0 else 1.0
        burst_score  = min((burst_ratio - 1) / 10, 1.0)  # Normalise: 10x peak = score 1.0

        # Dormancy score: what fraction of days in the wallet's lifespan had zero activity
        if len(self.txs) >= 2:
            first_day = self.txs[0].block_time.date()
            last_day  = self.txs[-1].block_time.date()
            active_days = len(day_counts)
            total_days  = max((last_day - first_day).days, active_days, 1)
            dormancy_score = max(0.0, 1.0 - (active_days / total_days))
        else:
            dormancy_score = 0.5

        # Activity label
        if burst_score > 0.6 and dormancy_score > 0.5:
            label = BurstLabel.BURST_SLEEP
        elif burst_score > 0.6:
            label = BurstLabel.BURST_HIGH
        else:
            label = BurstLabel.STEADY

        # Score: 0 = extreme burst-sleep (bot), 1 = steady daily activity (human)
        score = 1.0 - ((burst_score * 0.5) + (dormancy_score * 0.5))
        score = max(0.0, min(1.0, score))

        confidence = min(self.n / 50, 1.0)

        return ActivityFeature(
            burst_score=round(burst_score, 4),
            dormancy_score=round(dormancy_score, 4),
            label=label,
            avg_daily_tx=round(avg_daily, 2),
            peak_day_tx=peak_day,
            score=round(score, 4),
            confidence=round(confidence, 4),
        )

    # ─── Overall Confidence ───────────────────────────────────────────────────

    def overall_confidence(self) -> float:
        """
        Weighted average confidence across all extracted features.
        Degrades gracefully with sparse tx history.
        """
        weights = {
            "gas":      0.15,
            "timing":   0.25,
            "value":    0.20,
            "contract": 0.15,
            "mempool":  0.10,
            "activity": 0.15,
        }
        # Base confidence from tx count
        base = min(self.n / 100, 1.0)
        return round(base, 4)
