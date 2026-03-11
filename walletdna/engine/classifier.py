"""
WalletDNA — Bot Classifier
Multi-signal rule engine for bot vs human classification.
Deliberately rule-based (not ML) — keeps it auditable and explainable.
Every classification comes with the signals that triggered it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from walletdna.engine.models import (
    ActivityFeature,
    BotClassification,
    ContractFeature,
    GasFeature,
    MempoolFeature,
    StabilityLabel,
    TimingFeature,
    ValueFeature,
    WalletClass,
)


@dataclass
class Signal:
    name:        str
    triggered:   bool
    weight:      float       # contribution to bot score
    description: str


class BotClassifier:
    """
    Classifies a wallet as BOT / LIKELY_BOT / LIKELY_HUMAN / HUMAN
    based on weighted signals across all 6 feature dimensions.

    Bot score: 0.0 = definitely human, 1.0 = definitely bot
    Thresholds:
        >= 0.75 → BOT
        >= 0.55 → LIKELY_BOT
        >= 0.35 → LIKELY_HUMAN
        <  0.35 → HUMAN
    """

    BOT_THRESHOLD         = 0.75
    LIKELY_BOT_THRESHOLD  = 0.40
    LIKELY_HUMAN_THRESHOLD= 0.25

    def classify(
        self,
        gas:      Optional[GasFeature],
        timing:   Optional[TimingFeature],
        value:    Optional[ValueFeature],
        contract: Optional[ContractFeature],
        mempool:  Optional[MempoolFeature],
        activity: Optional[ActivityFeature],
        tx_count: int = 0,
    ) -> BotClassification:

        signals = self._evaluate_signals(
            gas, timing, value, contract, mempool, activity, tx_count
        )

        triggered_signals = [s for s in signals if s.triggered]
        bot_score = self._compute_score(signals)

        # Classify
        if bot_score >= self.BOT_THRESHOLD:
            wallet_class = WalletClass.BOT
        elif bot_score >= self.LIKELY_BOT_THRESHOLD:
            wallet_class = WalletClass.LIKELY_BOT
        elif bot_score >= self.LIKELY_HUMAN_THRESHOLD:
            wallet_class = WalletClass.LIKELY_HUMAN
        else:
            wallet_class = WalletClass.HUMAN

        explanation = self._explain(wallet_class, triggered_signals, bot_score)

        return BotClassification(
            wallet_class=wallet_class,
            confidence=round(bot_score, 4),
            signals=[s.name for s in triggered_signals],
            explanation=explanation,
        )

    # ─── Signal Evaluation ────────────────────────────────────────────────────

    def _evaluate_signals(
        self,
        gas:      Optional[GasFeature],
        timing:   Optional[TimingFeature],
        value:    Optional[ValueFeature],
        contract: Optional[ContractFeature],
        mempool:  Optional[MempoolFeature],
        activity: Optional[ActivityFeature],
        tx_count: int,
    ) -> list[Signal]:

        signals: list[Signal] = []

        # ── Gas Signals ───────────────────────────────────────────────────────

        if gas:
            # Very consistent gas price (low CV) = automated fee setting
            signals.append(Signal(
                name="STABLE_GAS_PRICE",
                triggered=gas.stability == StabilityLabel.STABLE,
                weight=0.12,
                description="Gas price variance is extremely low — consistent with automated fee management",
            ))

            # Very high gas consistently = MEV or front-running bot
            signals.append(Signal(
                name="CONSISTENTLY_HIGH_GAS",
                triggered=(
                    gas.label.value == "HIGH"
                    and gas.stability == StabilityLabel.STABLE
                ),
                weight=0.10,
                description="Consistently high gas prices — MEV or priority transaction bot",
            ))

        # ── Timing Signals ────────────────────────────────────────────────────

        if timing:
            # Very tight active window (< 6 hours)
            window_hours = (timing.active_hour_end - timing.active_hour_start) % 24
            signals.append(Signal(
                name="NARROW_ACTIVE_WINDOW",
                triggered=window_hours <= 6 and timing.confidence > 0.3,
                weight=0.15,
                description=f"Active only within a {window_hours}h UTC window — bot-scheduled activity",
            ))

            # Low entropy = highly predictable timing
            signals.append(Signal(
                name="LOW_TIMING_ENTROPY",
                triggered=timing.timing_entropy < 0.35 and timing.confidence > 0.3,
                weight=0.18,
                description="Transaction timing follows a highly predictable pattern",
            ))

            # Very short median interval = rapid-fire transactions
            signals.append(Signal(
                name="RAPID_TX_INTERVAL",
                triggered=(
                    timing.median_interval_sec > 0
                    and timing.median_interval_sec < 120   # Under 2 minutes
                    and timing.confidence > 0.3
                ),
                weight=0.12,
                description="Median interval between transactions is under 2 minutes",
            ))

        # ── Value Signals ─────────────────────────────────────────────────────

        if value:
            # Very low round number ratio = precise amounts = bot
            signals.append(Signal(
                name="NON_ROUND_AMOUNTS",
                triggered=value.round_number_ratio < 0.10 and value.confidence > 0.3,
                weight=0.10,
                description="Rarely sends round USD amounts — precise automated amounts",
            ))

            # High fragmentation = systematic splitting
            signals.append(Signal(
                name="HIGH_VALUE_FRAGMENTATION",
                triggered=(
                    value.fragmentation.value == "HIGH"
                    and value.confidence > 0.3
                ),
                weight=0.08,
                description="Consistently splits value into many small transactions",
            ))

        # ── Contract Signals ──────────────────────────────────────────────────

        if contract and not contract.not_applicable:
            # DEX-heavy = arbitrage or trading bot
            signals.append(Signal(
                name="DEX_HEAVY",
                triggered=contract.dex_ratio > 0.65 and contract.confidence > 0.3,
                weight=0.10,
                description="Over 65% of interactions are DEX swaps — trading bot pattern",
            ))

        # ── Mempool Signals ───────────────────────────────────────────────────

        if mempool and not mempool.not_applicable:
            # Always confirmed instantly = bot using precise gas targeting
            signals.append(Signal(
                name="INSTANT_CONFIRMATION",
                triggered=mempool.instant_ratio > 0.85 and mempool.confidence > 0.3,
                weight=0.08,
                description="85%+ of transactions confirmed in ≤2 blocks — optimal gas targeting",
            ))

        # ── Activity Signals ──────────────────────────────────────────────────

        if activity:
            # Extreme burst + long dormancy = scheduled operation
            signals.append(Signal(
                name="BURST_SLEEP_PATTERN",
                triggered=(
                    activity.burst_score > 0.6
                    and activity.dormancy_score > 0.5
                    and activity.confidence > 0.3
                ),
                weight=0.12,
                description="Intense activity bursts followed by complete dormancy — scheduled operation",
            ))

            # Very high peak/average ratio
            signals.append(Signal(
                name="EXTREME_BURST",
                triggered=(
                    activity.peak_day_tx > 50
                    and activity.burst_score > 0.8
                ),
                weight=0.10,
                description=f"Peak of {activity.peak_day_tx if activity else 0} tx/day — far beyond normal human pace",
            ))

        # ── Volume Signal ─────────────────────────────────────────────────────

        signals.append(Signal(
            name="HIGH_TX_VOLUME",
            triggered=tx_count > 500,
            weight=0.05,
            description=f"Total of {tx_count} transactions — high volume consistent with automation",
        ))

        return signals

    def _compute_score(self, signals: list[Signal]) -> float:
        """
        Weighted sum of triggered signals, normalised to 0–1.
        """
        total_weight    = sum(s.weight for s in signals)
        triggered_weight = sum(s.weight for s in signals if s.triggered)

        if total_weight == 0:
            return 0.5

        return min(triggered_weight / total_weight, 1.0)

    def _explain(
        self,
        wallet_class:      WalletClass,
        triggered_signals: list[Signal],
        score:             float,
    ) -> str:
        if not triggered_signals:
            return f"No strong bot signals detected. Score: {score:.2f}"

        signal_descs = "; ".join(s.description for s in triggered_signals[:3])
        return f"{wallet_class.value} (score: {score:.2f}). Key signals: {signal_descs}"
