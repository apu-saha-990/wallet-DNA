"""
WalletDNA — Unit Tests: DNA Engine
Tests feature extraction, bot classification, DNA composition, and similarity.
Uses synthetic transaction data — no API calls.
"""

from __future__ import annotations

import pytest
from datetime import datetime, timezone, timedelta

from walletdna.engine.classifier import BotClassifier
from walletdna.engine.composer   import DNAComposer
from walletdna.engine.extractor  import FeatureExtractor
from walletdna.engine.models     import (
    Chain, NormalisedTx, WalletClass, StabilityLabel, FragmentationLabel,
)
from walletdna.engine.similarity import SimilarityEngine, WalletVector


# ─── Fixtures ─────────────────────────────────────────────────────────────────

def make_tx(
    i:            int,
    address:      str      = "0xsender",
    direction:    str      = "out",
    chain:        Chain    = Chain.ETHEREUM,
    hour:         int      = 3,       # UTC hour
    value_usd:    float    = 100.0,
    gas_gwei:     float    = 20.0,
    is_contract:  bool     = False,
    method:       str      = None,
    conf_blocks:  int      = 1,
    days_offset:  int      = 0,
) -> NormalisedTx:
    """Helper to build synthetic NormalisedTx."""
    base_time = datetime(2025, 1, 1, hour, 0, 0, tzinfo=timezone.utc)
    block_time = base_time + timedelta(days=days_offset, minutes=i * 5)

    return NormalisedTx(
        tx_hash          = f"0x{'a' * 62}{i:02d}",
        chain            = chain,
        block_number     = 19_000_000 + i,
        block_time       = block_time,
        from_address     = address,
        to_address       = "0xtarget",
        direction        = direction,
        value_native     = value_usd / 3000,
        value_usd        = value_usd,
        gas_price_gwei   = gas_gwei,
        gas_used         = 21000,
        is_contract_call = is_contract,
        contract_method  = method,
        confirmation_blocks = conf_blocks,
    )


def make_bot_txs(n: int = 50) -> list[NormalisedTx]:
    """Synthetic bot wallet: consistent gas, tight 3am-5am window, precise amounts."""
    return [
        make_tx(
            i,
            hour=3,                  # Always 3am UTC
            gas_gwei=20.0,           # Never varies
            value_usd=99.97 + (i * 0.01),  # Precise, non-round amounts
            is_contract=True,
            method="DEX",
            conf_blocks=1,           # Always instant
            days_offset=i % 5,       # Burst over 5 days then stop
        )
        for i in range(n)
    ]


def make_human_txs(n: int = 30) -> list[NormalisedTx]:
    """Synthetic human wallet: varied gas, spread hours, round amounts, irregular."""
    import random
    random.seed(42)
    hours = [9, 11, 14, 16, 19, 21, 8, 15, 20, 13]  # Spread across the day
    return [
        make_tx(
            i,
            hour=hours[i % len(hours)],
            gas_gwei=15.0 + random.uniform(-8, 25),   # High variance
            value_usd=round(random.choice([100, 200, 500, 1000, 250]) * 1.0, 2),
            is_contract=random.random() > 0.6,
            method="TRANSFER" if random.random() > 0.5 else None,
            conf_blocks=random.randint(1, 15),
            days_offset=i * 2,       # Spread over time, steady
        )
        for i in range(n)
    ]


# ─── Feature Extractor Tests ──────────────────────────────────────────────────

class TestGasExtraction:

    def test_stable_gas_bot(self):
        txs = make_bot_txs(30)
        ext = FeatureExtractor(txs, "0xbot", Chain.ETHEREUM)
        gas = ext.extract_gas()

        assert gas is not None
        assert gas.stability == StabilityLabel.STABLE
        assert gas.mean_gwei == pytest.approx(20.0, abs=0.1)
        assert gas.score < 0.3   # Low score = bot-like

    def test_erratic_gas_human(self):
        txs = make_human_txs(30)
        ext = FeatureExtractor(txs, "0xhuman", Chain.ETHEREUM)
        gas = ext.extract_gas()

        assert gas is not None
        assert gas.stability != StabilityLabel.STABLE
        assert gas.score > 0.3   # Higher score = more human-like

    def test_doge_returns_none(self):
        txs = [
            make_tx(i, chain=Chain.DOGECOIN, gas_gwei=0)
            for i in range(10)
        ]
        ext = FeatureExtractor(txs, "Dtest", Chain.DOGECOIN)
        gas = ext.extract_gas()
        assert gas is None

    def test_sparse_wallet_low_confidence(self):
        txs = make_bot_txs(3)   # Under MIN_TX_GAS
        ext = FeatureExtractor(txs, "0xsparse", Chain.ETHEREUM)
        gas = ext.extract_gas()
        assert gas.confidence < 0.3


class TestTimingExtraction:

    def test_tight_window_bot(self):
        txs = make_bot_txs(40)
        ext = FeatureExtractor(txs, "0xbot", Chain.ETHEREUM)
        timing = ext.extract_timing()

        assert timing is not None
        assert timing.timing_entropy < 0.5    # Concentrated activity
        assert timing.score < 0.5             # Bot-like

    def test_spread_window_human(self):
        txs = make_human_txs(30)
        ext = FeatureExtractor(txs, "0xhuman", Chain.ETHEREUM)
        timing = ext.extract_timing()

        assert timing is not None
        assert timing.timing_entropy > 0.4    # Spread activity
        assert timing.score > 0.4

    def test_sparse_timing_low_confidence(self):
        txs = make_bot_txs(5)
        ext = FeatureExtractor(txs, "0xbot", Chain.ETHEREUM)
        timing = ext.extract_timing()
        assert timing.confidence < 0.2


class TestValueExtraction:

    def test_precise_amounts_bot(self):
        txs = make_bot_txs(20)
        ext = FeatureExtractor(txs, "0xbot", Chain.ETHEREUM)
        val = ext.extract_value()

        assert val is not None
        # Bot sends fragmented values - check herfindahl is low (many spread txs)
        assert val.herfindahl_index < 0.5

    def test_round_amounts_human(self):
        txs = make_human_txs(20)
        ext = FeatureExtractor(txs, "0xhuman", Chain.ETHEREUM)
        val = ext.extract_value()

        assert val is not None
        assert val.round_number_ratio > 0.3   # Humans send round amounts


class TestActivityExtraction:

    def test_burst_sleep_bot(self):
        txs = make_bot_txs(50)
        ext = FeatureExtractor(txs, "0xbot", Chain.ETHEREUM)
        act = ext.extract_activity()

        assert act is not None
        assert act.peak_day_tx >= 1
        assert act.avg_daily_tx > 0
        assert act.confidence > 0

    def test_steady_human(self):
        txs = make_human_txs(30)   # Spread over 60 days
        ext = FeatureExtractor(txs, "0xhuman", Chain.ETHEREUM)
        act = ext.extract_activity()

        assert act is not None
        assert act.score > 0.3


# ─── Bot Classifier Tests ──────────────────────────────────────────────────────

class TestBotClassifier:

    def setup_method(self):
        self.classifier = BotClassifier()

    def test_bot_classified(self):
        txs = make_bot_txs(50)
        ext = FeatureExtractor(txs, "0xbot", Chain.ETHEREUM)

        result = self.classifier.classify(
            gas=ext.extract_gas(),
            timing=ext.extract_timing(),
            value=ext.extract_value(),
            contract=ext.extract_contract(),
            mempool=ext.extract_mempool(),
            activity=ext.extract_activity(),
            tx_count=50,
        )

        assert result.wallet_class in (WalletClass.BOT, WalletClass.LIKELY_BOT)
        assert result.confidence > 0.0
        assert len(result.signals) > 0
        assert result.explanation

    def test_human_classified(self):
        txs = make_human_txs(30)
        ext = FeatureExtractor(txs, "0xhuman", Chain.ETHEREUM)

        result = self.classifier.classify(
            gas=ext.extract_gas(),
            timing=ext.extract_timing(),
            value=ext.extract_value(),
            contract=ext.extract_contract(),
            mempool=ext.extract_mempool(),
            activity=ext.extract_activity(),
            tx_count=30,
        )

        assert result.wallet_class in (WalletClass.HUMAN, WalletClass.LIKELY_HUMAN)

    def test_empty_features(self):
        """Should not crash with all None features."""
        result = self.classifier.classify(
            gas=None, timing=None, value=None,
            contract=None, mempool=None, activity=None,
            tx_count=0,
        )
        assert result.wallet_class == WalletClass.HUMAN  # Default to human when no signals
        assert result.confidence == 0.0


# ─── DNA Composer Tests ───────────────────────────────────────────────────────

class TestDNAComposer:

    def setup_method(self):
        self.composer = DNAComposer()

    def test_bot_dna_string(self):
        txs     = make_bot_txs(50)
        profile = self.composer.compose(txs, "0xbot", Chain.ETHEREUM, "Test Bot")

        assert profile.dna_string is not None
        assert "G:" in profile.dna_string
        assert "T:" in profile.dna_string
        assert "V:" in profile.dna_string
        assert "C:" in profile.dna_string
        assert "M:" in profile.dna_string
        assert "A:" in profile.dna_string
        assert "X:" in profile.dna_string
        assert "|" in profile.dna_string

    def test_dna_vector_length(self):
        txs     = make_bot_txs(30)
        profile = self.composer.compose(txs, "0xbot", Chain.ETHEREUM)

        assert profile.dna_vector is not None
        assert len(profile.dna_vector) == 10

    def test_dna_vector_range(self):
        txs     = make_human_txs(30)
        profile = self.composer.compose(txs, "0xhuman", Chain.ETHEREUM)

        for val in profile.dna_vector:
            assert 0.0 <= val <= 1.0, f"Vector value out of range: {val}"

    def test_insufficient_txs(self):
        txs     = make_bot_txs(3)   # Below MIN_TX_FOR_DNA=5
        profile = self.composer.compose(txs, "0xsparse", Chain.ETHEREUM)

        assert profile.error is not None
        assert profile.dna_string is None

    def test_bot_human_dna_differs(self):
        bot_profile   = self.composer.compose(make_bot_txs(40), "0xbot", Chain.ETHEREUM)
        human_profile = self.composer.compose(make_human_txs(30), "0xhuman", Chain.ETHEREUM)

        assert bot_profile.dna_string != human_profile.dna_string
        assert bot_profile.dna_vector != human_profile.dna_vector


# ─── Similarity Tests ─────────────────────────────────────────────────────────

class TestSimilarityEngine:

    def setup_method(self):
        self.engine   = SimilarityEngine(threshold=0.75)
        self.composer = DNAComposer()

    def _to_wallet_vector(self, profile, label="test") -> WalletVector:
        return WalletVector(
            address=profile.address,
            chain=profile.chain.value,
            vector=profile.dna_vector,
            dna_string=profile.dna_string or "",
            wallet_class=profile.classification.wallet_class.value if profile.classification else "UNKNOWN",
            label=label,
        )

    def test_identical_wallets_score_high(self):
        txs = make_bot_txs(40)
        p1  = self.composer.compose(txs, "0xbot1", Chain.ETHEREUM)
        p2  = self.composer.compose(txs, "0xbot2", Chain.ETHEREUM)

        result = self.engine.compare(p1, p2)
        assert result.similarity > 0.90

    def test_different_wallets_score_low(self):
        bot_profile   = self.composer.compose(make_bot_txs(40), "0xbot", Chain.ETHEREUM)
        human_profile = self.composer.compose(make_human_txs(30), "0xhuman", Chain.ETHEREUM)

        result = self.engine.compare(bot_profile, human_profile)
        assert result.similarity < 0.80

    def test_cluster_detection(self):
        """Three bots should cluster together, one human should not."""
        bot_txs = make_bot_txs(40)

        bot1  = self._to_wallet_vector(self.composer.compose(bot_txs, "0xbot1", Chain.ETHEREUM))
        bot2  = self._to_wallet_vector(self.composer.compose(bot_txs, "0xbot2", Chain.ETHEREUM))
        bot3  = self._to_wallet_vector(self.composer.compose(bot_txs, "0xbot3", Chain.ETHEREUM))
        human = self._to_wallet_vector(self.composer.compose(make_human_txs(30), "0xhuman", Chain.ETHEREUM))

        clusters = self.engine.cluster([bot1, bot2, bot3, human], threshold=0.85)

        assert len(clusters) >= 1
        # The bot cluster should contain the 3 bots
        largest = max(clusters, key=lambda c: len(c.addresses))
        assert len(largest.addresses) >= 2

    def test_similarity_symmetric(self):
        """similarity(A, B) should equal similarity(B, A)."""
        p1 = self.composer.compose(make_bot_txs(30), "0xbot1", Chain.ETHEREUM)
        p2 = self.composer.compose(make_human_txs(30), "0xhuman", Chain.ETHEREUM)

        r1 = self.engine.compare(p1, p2)
        r2 = self.engine.compare(p2, p1)

        assert abs(r1.similarity - r2.similarity) < 0.001

    def test_find_similar(self):
        bot_txs = make_bot_txs(40)
        target  = self._to_wallet_vector(self.composer.compose(bot_txs, "0xtarget", Chain.ETHEREUM))
        pool    = [
            self._to_wallet_vector(self.composer.compose(bot_txs, f"0xbot{i}", Chain.ETHEREUM))
            for i in range(5)
        ] + [
            self._to_wallet_vector(self.composer.compose(make_human_txs(30), f"0xhuman{i}", Chain.ETHEREUM))
            for i in range(3)
        ]

        matches = self.engine.find_similar(target, pool, threshold=0.85)
        assert len(matches) >= 3   # Should find the bots


# ─── Integration: Full Pipeline ───────────────────────────────────────────────

class TestFullPipeline:
    """
    End-to-end: txs → DNA → similarity → cluster
    Mirrors what happens with real BDAG wallet data.
    """

    def test_bdag_cluster_simulation(self):
        """
        Simulate 5 BDAG collector wallets (same operator, different addresses)
        vs 2 human sender wallets. Cluster should isolate the 5.
        """
        composer = DNAComposer()
        engine   = SimilarityEngine(threshold=0.80)

        # Same bot behaviour, different addresses (BDAG collectors)
        bot_txs = make_bot_txs(50)
        bdag_wallets = [
            WalletVector(
                address=f"0xbdag{i:02d}",
                chain="ethereum",
                vector=composer.compose(bot_txs, f"0xbdag{i:02d}", Chain.ETHEREUM).dna_vector,
                dna_string="",
                wallet_class="BOT",
            )
            for i in range(5)
        ]

        # Your human sender wallets
        human_txs = make_human_txs(30)
        human_wallets = [
            WalletVector(
                address=f"0xhuman{i}",
                chain="ethereum",
                vector=composer.compose(human_txs, f"0xhuman{i}", Chain.ETHEREUM).dna_vector,
                dna_string="",
                wallet_class="HUMAN",
            )
            for i in range(2)
        ]

        all_wallets = bdag_wallets + human_wallets
        clusters    = engine.cluster(all_wallets, threshold=0.85)

        assert len(clusters) >= 1

        # The largest cluster should be the BDAG bots
        largest = max(clusters, key=lambda c: len(c.addresses))
        bdag_addresses = {w.address for w in bdag_wallets}
        overlap = len(set(largest.addresses) & bdag_addresses)

        assert overlap >= 3, f"Expected BDAG bot cluster, got overlap of {overlap}"
        print(f"\n✓ BDAG cluster simulation: {overlap}/5 bots correctly clustered")
        print(f"  Cluster similarity: {largest.avg_similarity:.2f}")
        print(f"  Dominant class: {largest.dominant_class.value}")
