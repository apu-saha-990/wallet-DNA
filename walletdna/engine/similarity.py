"""
WalletDNA — Similarity Engine
Computes cosine similarity between DNA vectors.
Identifies clusters of wallets sharing the same behavioural fingerprint.

This is where the BDAG cluster gets exposed.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional

import structlog

from walletdna.engine.models import ClusterResult, DNAProfile, SimilarityResult, WalletClass

logger = structlog.get_logger(__name__)

# Dimension weights for weighted cosine similarity
# Higher weight = this dimension is more reliable / harder to fake
DIMENSION_WEIGHTS = [
    0.10,   # [0]  gas_score
    0.05,   # [1]  gas_stability
    0.15,   # [2]  timing_entropy       ← most reliable, hardest to fake
    0.10,   # [3]  timing_window
    0.10,   # [4]  value_hhi
    0.10,   # [5]  round_number_ratio
    0.08,   # [6]  contract_dex_ratio
    0.07,   # [7]  mempool_instant
    0.13,   # [8]  burst_score          ← very reliable
    0.12,   # [9]  dormancy_score       ← very reliable
]


@dataclass
class WalletVector:
    address:      str
    chain:        str
    vector:       list[float]
    dna_string:   str
    wallet_class: str
    label:        Optional[str] = None


class SimilarityEngine:
    """
    Computes pairwise similarity between wallet DNA vectors.
    Uses weighted cosine similarity.

    Key methods:
        compare(a, b)               → SimilarityResult
        find_similar(target, pool)  → ranked matches above threshold
        cluster(vectors)            → ClusterResult list
    """

    def __init__(self, threshold: float = 0.75):
        self.threshold = threshold

    # ─── Core Similarity ──────────────────────────────────────────────────────

    def compare(
        self,
        profile_a: DNAProfile,
        profile_b: DNAProfile,
    ) -> SimilarityResult:
        """
        Compute weighted cosine similarity between two DNA profiles.
        Returns SimilarityResult with score and interpretation.
        """
        if not profile_a.dna_vector or not profile_b.dna_vector:
            return SimilarityResult(
                wallet_a=profile_a.address,
                wallet_b=profile_b.address,
                similarity=0.0,
                interpretation="INSUFFICIENT_DATA",
            )

        score = self._weighted_cosine(profile_a.dna_vector, profile_b.dna_vector)

        return SimilarityResult(
            wallet_a=profile_a.address,
            wallet_b=profile_b.address,
            similarity=round(score, 4),
            interpretation=self._interpret(score),
        )

    def compare_vectors(
        self,
        vec_a: list[float],
        vec_b: list[float],
    ) -> float:
        """Raw vector comparison — returns similarity score 0.0–1.0."""
        return round(self._weighted_cosine(vec_a, vec_b), 4)

    # ─── Find Similar ─────────────────────────────────────────────────────────

    def find_similar(
        self,
        target:    WalletVector,
        pool:      list[WalletVector],
        threshold: Optional[float] = None,
        top_n:     int             = 10,
    ) -> list[tuple[WalletVector, float]]:
        """
        Find all wallets in pool with similarity >= threshold to target.
        Returns sorted list of (wallet, similarity) pairs, highest first.
        """
        cutoff  = threshold or self.threshold
        matches = []

        for candidate in pool:
            if candidate.address == target.address:
                continue
            score = self.compare_vectors(target.vector, candidate.vector)
            if score >= cutoff:
                matches.append((candidate, score))

        matches.sort(key=lambda x: x[1], reverse=True)
        return matches[:top_n]

    # ─── Clustering ───────────────────────────────────────────────────────────

    def cluster(
        self,
        vectors:   list[WalletVector],
        threshold: Optional[float] = None,
    ) -> list[ClusterResult]:
        """
        Simple greedy clustering — groups wallets by behavioural similarity.
        Not DBSCAN, but sufficient and auditable for this scale.

        Algorithm:
        1. Start with highest-similarity pair
        2. Add wallets within threshold of cluster centroid
        3. Repeat until no more merges
        """
        cutoff = threshold or self.threshold

        if len(vectors) < 2:
            return []

        # Build similarity matrix
        n     = len(vectors)
        sim_matrix: dict[tuple[int, int], float] = {}

        for i in range(n):
            for j in range(i + 1, n):
                score = self.compare_vectors(vectors[i].vector, vectors[j].vector)
                sim_matrix[(i, j)] = score

        # Greedy cluster assignment
        assigned   = set()
        clusters:  list[list[int]] = []

        # Sort pairs by similarity descending
        sorted_pairs = sorted(sim_matrix.items(), key=lambda x: x[1], reverse=True)

        for (i, j), score in sorted_pairs:
            if score < cutoff:
                break

            # Find existing clusters for i and j
            ci = next((ci for ci, c in enumerate(clusters) if i in c), None)
            cj = next((ci for ci, c in enumerate(clusters) if j in c), None)

            if ci is None and cj is None:
                # New cluster
                clusters.append([i, j])
                assigned.update([i, j])
            elif ci is not None and cj is None:
                clusters[ci].append(j)
                assigned.add(j)
            elif ci is None and cj is not None:
                clusters[cj].append(i)
                assigned.add(i)
            elif ci != cj:
                # Merge clusters
                clusters[ci].extend(clusters[cj])
                clusters.pop(cj)

        if not clusters:
            return []

        # Build ClusterResult objects
        results = []
        for cluster_id, indices in enumerate(clusters):
            if len(indices) < 2:
                continue

            cluster_vectors = [vectors[i] for i in indices]
            addresses       = [v.address for v in cluster_vectors]

            # Average pairwise similarity within cluster
            pairs = [
                (i, j)
                for i in indices
                for j in indices
                if i < j
            ]
            avg_sim = (
                sum(sim_matrix.get((i, j) if i < j else (j, i), 0.0) for i, j in pairs)
                / len(pairs)
                if pairs else 0.0
            )

            # Dominant class
            class_counts: dict[str, int] = {}
            for v in cluster_vectors:
                class_counts[v.wallet_class] = class_counts.get(v.wallet_class, 0) + 1
            dominant_class_str = max(class_counts, key=class_counts.get)
            try:
                dominant_class = WalletClass(dominant_class_str)
            except ValueError:
                dominant_class = WalletClass.UNKNOWN

            # Auto-label based on characteristics
            label = self._auto_label(cluster_vectors, avg_sim)

            results.append(ClusterResult(
                cluster_id=cluster_id,
                label=label,
                addresses=addresses,
                avg_similarity=round(avg_sim, 4),
                dominant_class=dominant_class,
            ))

        # Sort by avg similarity
        results.sort(key=lambda c: c.avg_similarity, reverse=True)

        logger.info(
            "clustering_complete",
            total_wallets=len(vectors),
            clusters_found=len(results),
            threshold=cutoff,
        )

        return results

    # ─── Internal ─────────────────────────────────────────────────────────────

    def _weighted_cosine(self, vec_a: list[float], vec_b: list[float]) -> float:
        """
        Weighted cosine similarity.
        Applies DIMENSION_WEIGHTS to emphasise more reliable dimensions.
        """
        if len(vec_a) != len(vec_b):
            # Pad shorter vector with 0.5 (neutral)
            length = max(len(vec_a), len(vec_b))
            vec_a  = vec_a + [0.5] * (length - len(vec_a))
            vec_b  = vec_b + [0.5] * (length - len(vec_b))

        weights = DIMENSION_WEIGHTS[:len(vec_a)]
        # Normalise weights in case vector is shorter
        weight_sum = sum(weights)
        weights    = [w / weight_sum for w in weights]

        # Weighted dot product
        dot     = sum(w * a * b for w, a, b in zip(weights, vec_a, vec_b))
        mag_a   = math.sqrt(sum(w * a * a for w, a in zip(weights, vec_a)))
        mag_b   = math.sqrt(sum(w * b * b for w, b in zip(weights, vec_b)))

        if mag_a == 0 or mag_b == 0:
            return 0.0

        similarity = dot / (mag_a * mag_b)
        return max(0.0, min(1.0, similarity))

    def _interpret(self, score: float) -> str:
        if score >= 0.92:
            return "LIKELY SAME OPERATOR"
        elif score >= 0.85:
            return "HIGHLY SIMILAR BEHAVIOUR"
        elif score >= 0.75:
            return "SIMILAR BEHAVIOUR"
        elif score >= 0.60:
            return "SOME SIMILARITY"
        else:
            return "DISTINCT BEHAVIOUR"

    def _auto_label(self, vectors: list[WalletVector], avg_sim: float) -> str:
        """Generate a descriptive label for a cluster."""
        classes = [v.wallet_class for v in vectors]

        if all(c in ("BOT", "LIKELY_BOT") for c in classes):
            prefix = "BOT-CLUSTER"
        elif avg_sim >= 0.90:
            prefix = "HIGH-SIM-CLUSTER"
        else:
            prefix = "BEHAVIOUR-CLUSTER"

        return f"{prefix}-{len(vectors)}W-{int(avg_sim * 100)}SIM"
