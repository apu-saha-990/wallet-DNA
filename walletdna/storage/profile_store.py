"""
WalletDNA — Profile Store
Git-tracked JSON persistence layer for DNA profiles.

Survives Docker wipes, re-clones, and environment rebuilds.
DB is the hot cache. This folder is the cold store.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import structlog

logger = structlog.get_logger(__name__)

PROFILES_DIR = Path(__file__).parent.parent.parent / "profiles"


class ProfileStore:
    """
    Reads and writes DNA profiles to/from the git-tracked profiles/ folder.

    Priority logic (used by dashboard):
        1. Check DB (hot cache, <24h)
        2. Check profiles/ folder (cold store, git-tracked)
        3. Fetch from live API (expensive, rate-limited)
    """

    def __init__(self, profiles_dir: Optional[Path] = None):
        self.dir = profiles_dir or PROFILES_DIR
        self.dir.mkdir(parents=True, exist_ok=True)

    # ─── Read ─────────────────────────────────────────────────────────────────

    def load(self, address: str) -> Optional[dict]:
        """Load a profile from disk. Returns None if not found."""
        path = self._path(address)
        if not path.exists():
            return None
        try:
            with open(path) as f:
                profile = json.load(f)
            logger.info("profile_loaded_from_disk", address=address[:12])
            return profile
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("profile_load_failed", address=address[:12], error=str(e))
            return None

    def exists(self, address: str) -> bool:
        return self._path(address).exists()

    def list_all(self) -> list[str]:
        """Return list of all addresses with saved profiles."""
        return [
            f.stem
            for f in self.dir.glob("*.json")
            if f.stem != "README"
        ]

    def load_all(self) -> list[dict]:
        """Load all profiles from disk."""
        profiles = []
        for address in self.list_all():
            p = self.load(address)
            if p:
                profiles.append(p)
        return profiles

    # ─── Write ────────────────────────────────────────────────────────────────

    def save(self, profile: dict) -> None:
        """Save a profile to disk. Overwrites existing."""
        address = profile.get("address", "unknown")
        path    = self._path(address)

        # Stamp the save time
        profile["saved_at"] = datetime.now(timezone.utc).isoformat()

        try:
            with open(path, "w") as f:
                json.dump(profile, f, indent=2, default=str)
            logger.info("profile_saved_to_disk", address=address[:12], path=str(path))
        except OSError as e:
            logger.error("profile_save_failed", address=address[:12], error=str(e))

    def save_from_dna(
        self,
        address:    str,
        chain:      str,
        label:      Optional[str],
        dna_string: str,
        dna_vector: list[float],
        wallet_class: str,
        bot_confidence: float,
        tx_count:   int,
        confidence_score: float,
    ) -> None:
        """Convenience method — build profile dict and save."""
        self.save({
            "address":          address,
            "chain":            chain,
            "label":            label,
            "dna_string":       dna_string,
            "dna_vector":       dna_vector,
            "wallet_class":     wallet_class,
            "bot_confidence":   bot_confidence,
            "tx_count":         tx_count,
            "confidence_score": confidence_score,
            "analysed_at":      datetime.now(timezone.utc).isoformat(),
        })

    def add_to_watchlist(
        self,
        address: str,
        label:   str,
        wallets_json_path: Optional[Path] = None,
    ) -> bool:
        """
        Add an address to wallets.json suspect_wallets list.
        Called when user confirms a high-similarity match.
        Returns True if added, False if already present.
        """
        wj_path = wallets_json_path or (
            Path(__file__).parent.parent.parent / "wallets.json"
        )

        try:
            with open(wj_path) as f:
                config = json.load(f)

            # Check if already present (any section)
            all_addresses = [
                w["address"].lower()
                for section in config.values()
                for w in section
                if isinstance(w, dict) and "address" in w
            ]

            if address.lower() in all_addresses:
                logger.info("watchlist_already_present", address=address[:12])
                return False

            # Add to suspect_wallets
            if "suspect_wallets" not in config:
                config["suspect_wallets"] = []

            config["suspect_wallets"].append({
                "address": address,
                "label":   label,
            })

            with open(wj_path, "w") as f:
                json.dump(config, f, indent=2)

            logger.info("watchlist_added", address=address[:12], label=label)
            return True

        except (OSError, json.JSONDecodeError) as e:
            logger.error("watchlist_add_failed", address=address[:12], error=str(e))
            return False

    # ─── Internal ─────────────────────────────────────────────────────────────

    def _path(self, address: str) -> Path:
        # Normalise: lowercase, strip 0x prefix for filename safety
        safe_name = address.lower().replace("0x", "0x")
        return self.dir / f"{safe_name}.json"
