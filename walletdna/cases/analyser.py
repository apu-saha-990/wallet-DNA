"""
WalletDNA — Case Analyser
=========================
Batch analysis engine.  Respects cache TTL, runs concurrent ingestion,
builds cluster results across all profiles in a case.

Cache logic per wallet:
    - Fresh profile (<24h)  → use cache, no API call
    - Stale / missing       → ingest live, save to case profiles/
    - force=True            → always re-fetch

Progress callback signature: (completed: int, total: int, address: str, status: str) → None
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Callable, Optional

import structlog

from walletdna.cases.manager import CaseManager, detect_chain

logger = structlog.get_logger(__name__)

# Concurrency cap — respect per-chain rate limits
# ETH 4 req/s, TRX 5 req/s, DOGE 2 req/s (Blockcypher)
MAX_CONCURRENT = 3

# USD price cache per run (avoid hammering CoinGecko)
_usd_price_cache: dict[str, float] = {}


async def _fetch_usd_price(chain: str) -> float:
    global _usd_price_cache
    if chain in _usd_price_cache:
        return _usd_price_cache[chain]
    coin_id = {"ETH": "ethereum", "TRX": "tron", "DOGE": "dogecoin"}.get(chain.upper(), "ethereum")
    try:
        import urllib.request, json as _json
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"
        with urllib.request.urlopen(url, timeout=6) as r:
            price = float(_json.loads(r.read())[coin_id]["usd"])
        _usd_price_cache[chain] = price
        return price
    except Exception:
        return 0.0


async def _ingest_single(address: str, chain: str) -> Optional[dict]:
    """
    Run live ingestion for one address.  Returns display-ready profile dict or None.
    Isolates all import / adapter errors — caller gets None, never an exception.
    """
    try:
        from walletdna.engine.composer import DNAComposer
        from walletdna.engine.models import Chain as ChainEnum

        chain_map = {
            "ETH":  ChainEnum.ETHEREUM,
            "TRX":  ChainEnum.TRON,
            "DOGE": ChainEnum.DOGECOIN,
        }
        chain_enum = chain_map.get(chain.upper())
        if not chain_enum:
            return None

        if chain.upper() == "ETH":
            from walletdna.adapters.eth import EthereumAdapter
            adapter = EthereumAdapter()
        elif chain.upper() == "TRX":
            from walletdna.adapters.trx import TronAdapter
            adapter = TronAdapter()
        else:
            from walletdna.adapters.doge import DogecoinAdapter
            adapter = DogecoinAdapter()

        txs = await adapter.get_transactions(address)
        await adapter.close()

        if not txs:
            return None

        composer = DNAComposer()
        profile  = composer.compose(txs, address, chain_enum, label=None)

        if profile.error:
            return None

        # Volume: native outbound + stablecoin face value; other tokens excluded
        from walletdna.engine.models import TxDirection
        STABLECOINS = {'USDT','USDC','BUSD','DAI','USDD','TUSD','USDP','GUSD'}

        total_native = sum(
            float(t.value_native)
            for t in txs
            if t.value_native
            and not t.token_symbol
            and (
                t.direction == TxDirection.OUT
                or t.from_address.lower() == address.lower()
            )
        )
        if total_native == 0:
            total_native = sum(
                float(t.value_native) for t in txs if t.value_native and not t.token_symbol
            )

        # For stablecoins use from_address match — direction field can be unreliable
        # for token transfers depending on how Etherscan returns them
        # For collection wallets (TRX/ETH) count all stablecoin volume through wallet
        # Incoming USDT is what matters for fraud investigation — total throughput
        stable_usd = sum(
            float(t.value_native)
            for t in txs
            if t.value_native
            and t.token_symbol
            and t.token_symbol.upper() in STABLECOINS
        )

        api_limit_hit = len(txs) >= 9999
        usd_price     = await _fetch_usd_price(chain)
        total_usd     = total_native * usd_price + stable_usd

        chain_sym = chain.upper()
        if total_usd >= 1_000_000_000:
            value_str = f"{total_native:,.2f} {chain_sym} (${total_usd / 1_000_000_000:.1f}B USD)"
        elif total_usd >= 1_000_000:
            value_str = f"{total_native:,.2f} {chain_sym} (${total_usd / 1_000_000:.1f}M USD)"
        elif total_usd >= 1_000:
            value_str = f"{total_native:,.2f} {chain_sym} (${total_usd / 1_000:.1f}K USD)"
        elif total_usd > 0:
            value_str = f"{total_native:,.4f} {chain_sym} (${total_usd:.0f} USD)"
        elif total_native > 0:
            value_str = f"{total_native:,.4f} {chain_sym}"
        else:
            value_str = "live"

        # Wallet type classification (exchange / mixer / aggregator)
        wallet_type = _classify_wallet_type(
            tx_count=profile.tx_count,
            total_usd=total_usd,
            wallet_class=profile.classification.wallet_class.value if profile.classification else "UNKNOWN",
            dna_string=profile.dna_string or "",
        )

        from walletdna.dashboard.terminal import _parse_dna_string
        dna_display = _parse_dna_string(profile.dna_string or "")

        return {
            "address":          address,
            "chain":            chain.upper(),
            "label":            None,
            "tx_count":         profile.tx_count,
            "total_native":     round(total_native, 4),
            "total_usd":        round(total_usd, 2),
            "api_limit_hit":    api_limit_hit,
            "value_display":    value_str,
            "wallet_class":     profile.classification.wallet_class.value if profile.classification else "UNKNOWN",
            "bot_confidence":   profile.classification.confidence if profile.classification else 0.0,
            "confidence_score": profile.confidence_score,
            "dna_string":       profile.dna_string,
            "dna_vector":       profile.dna_vector,
            "dna":              dna_display,
            "wallet_type":      wallet_type,
            "source":           "live",
        }

    except Exception as e:
        logger.warning("ingest_single_failed", address=address[:12], error=str(e))
        return None


def _classify_wallet_type(
    tx_count: int,
    total_usd: float,
    wallet_class: str,
    dna_string: str,
) -> Optional[str]:
    """
    Wallet type auto-labelling removed — labels were inaccurate for
    high-volume collection wallets (e.g. scam deposit addresses hit
    10,000 tx cap and got mislabelled as exchange hot wallets).
    DNA + cluster detection tells the real story.
    """
    return None


# ─── Cluster Engine ───────────────────────────────────────────────────────────

def compute_clusters(profiles: list[dict]) -> list[dict]:
    """
    Greedy O(n²) clustering.
    Returns list of cluster dicts:
        {cluster_id, label, addresses, avg_similarity, member_count, interpretation}
    """
    if len(profiles) < 2:
        return []

    try:
        from walletdna.engine.similarity import SimilarityEngine
        engine = SimilarityEngine(threshold=0.75)
    except Exception:
        return []

    vecs = [p.get("dna_vector") for p in profiles]
    n    = len(profiles)

    # Build similarity matrix — skip profiles without vectors
    sim: list[list[float]] = [[0.0] * n for _ in range(n)]
    for i in range(n):
        sim[i][i] = 1.0
        for j in range(i + 1, n):
            if vecs[i] and vecs[j]:
                try:
                    s = engine.compare_vectors(vecs[i], vecs[j])
                except Exception:
                    s = 0.0
            else:
                s = 0.0
            sim[i][j] = s
            sim[j][i] = s

    # Store pairwise similarity on profiles for display
    for i, p in enumerate(profiles):
        p["_sim_row"] = sim[i]

    # Greedy clustering
    assigned  = [-1] * n
    cluster_id = 0
    clusters  = []

    for i in range(n):
        if assigned[i] != -1:
            continue
        members = [i]
        for j in range(i + 1, n):
            if assigned[j] == -1 and sim[i][j] >= 0.75:
                members.append(j)
        if len(members) >= 2:
            for m in members:
                assigned[m] = cluster_id
            # Compute cluster stats
            pair_scores = [
                sim[members[a]][members[b]]
                for a in range(len(members))
                for b in range(a + 1, len(members))
            ]
            avg_sim = sum(pair_scores) / len(pair_scores) if pair_scores else 0.0
            addrs   = [profiles[m]["address"] for m in members]
            classes = [profiles[m].get("wallet_class", "UNKNOWN") for m in members]
            bot_count = sum(1 for c in classes if "BOT" in c)
            interp = (
                "LIKELY SAME OPERATOR" if avg_sim >= 0.92
                else "SIMILAR BEHAVIOUR"
            )
            clusters.append({
                "cluster_id":   cluster_id,
                "label":        f"CLUSTER-{chr(65 + cluster_id)}",
                "addresses":    addrs,
                "avg_similarity": round(avg_sim, 3),
                "member_count": len(members),
                "bot_count":    bot_count,
                "interpretation": interp,
            })
            cluster_id += 1
        else:
            assigned[i] = -1  # singleton, no cluster

    # Stamp cluster membership onto profiles
    label_map = {}
    for cl in clusters:
        for addr in cl["addresses"]:
            label_map[addr.lower()] = cl["label"]
    for p in profiles:
        p["cluster_label"] = label_map.get(p["address"].lower(), "—")

    return clusters


# ─── Batch Analyser ───────────────────────────────────────────────────────────

class CaseAnalyser:
    """
    Runs DNA analysis for all wallets in a case with caching and progress reporting.
    """

    def __init__(self, case_name: str, manager: Optional[CaseManager] = None):
        self.case_name = case_name
        self.manager   = manager or CaseManager()

    async def run(
        self,
        force: bool = False,
        progress_cb: Optional[Callable[[int, int, str, str], None]] = None,
    ) -> list[dict]:
        """
        Analyse all wallets in the case.

        Args:
            force:       Ignore cache, re-fetch everything.
            progress_cb: Called after each wallet completes.

        Returns:
            List of fully-resolved profile dicts (cached or live).
            Includes cluster_label field after clustering.
        """
        wallets = self.manager.get_wallets(self.case_name)
        if not wallets:
            return []

        total    = len(wallets)
        profiles = []
        sem      = asyncio.Semaphore(MAX_CONCURRENT)

        async def analyse_one(w: dict, idx: int) -> dict:
            addr  = w["address"]
            chain = w.get("chain") or detect_chain(addr) or "ETH"
            label = w.get("label", addr[:10])

            # Cache check
            if not force and self.manager.is_profile_fresh(self.case_name, addr):
                profile = self.manager.load_profile(self.case_name, addr)
                profile["label"] = label
                profile["source"] = "cache"
                if progress_cb:
                    progress_cb(idx + 1, total, addr, "cache")
                return profile

            # Live fetch
            async with sem:
                if progress_cb:
                    progress_cb(idx, total, addr, "fetching")
                profile = await _ingest_single(addr, chain)

            if profile:
                profile["label"]  = label
                profile["chain"]  = chain
                self.manager.save_profile(self.case_name, profile)
                status = "live"
            else:
                # Insufficient data — preserve wallet entry without DNA
                profile = {
                    "address":       addr,
                    "chain":         chain,
                    "label":         label,
                    "tx_count":      0,
                    "wallet_class":  "UNKNOWN",
                    "bot_confidence": 0.0,
                    "confidence_score": 0.0,
                    "dna_string":    None,
                    "dna_vector":    None,
                    "source":        "insufficient_data",
                    "total_native":  0,
                    "total_usd":     0,
                    "value_display": "—",
                }
                status = "insufficient"

            if progress_cb:
                progress_cb(idx + 1, total, addr, status)
            return profile

        tasks = [analyse_one(w, i) for i, w in enumerate(wallets)]
        profiles = list(await asyncio.gather(*tasks))

        # Cluster all profiles that have DNA vectors
        compute_clusters(profiles)

        # Stamp last_run on case
        self.manager.touch_last_run(self.case_name)

        return profiles

    def run_sync(
        self,
        force: bool = False,
        progress_cb: Optional[Callable[[int, int, str, str], None]] = None,
    ) -> list[dict]:
        """Synchronous wrapper for use from terminal.py."""
        return asyncio.run(self.run(force=force, progress_cb=progress_cb))
