"""
WalletDNA — Main Entrypoint
CLI interface for running ingestion, DNA generation, and analysis.
"""

from __future__ import annotations

import asyncio
import os
import sys

import structlog
from dotenv import load_dotenv

load_dotenv()

# Configure structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)

logger = structlog.get_logger(__name__)


async def cmd_ingest(args: list[str]) -> None:
    """Ingest a wallet address or tx hash."""
    from walletdna.adapters.resolver import AddressResolver
    from walletdna.engine.models import Chain, WalletIngestionRequest
    from walletdna.ingestion import IngestionService
    from walletdna.storage.db import Database

    if not args:
        print("Usage: walletdna ingest <address_or_txhash> [chain] [--target|--sender]")
        return

    target  = args[0]
    chain_str = args[1] if len(args) > 1 and not args[1].startswith("--") else None
    is_target = "--target" in args
    is_sender = "--sender" in args

    db = Database()
    await db.connect()
    svc = IngestionService(db)

    try:
        if chain_str:
            chain = Chain(chain_str.lower())
        else:
            resolved = AddressResolver.detect(target)
            chain = resolved.chains[0] if resolved.chains else None

        if chain is None:
            print(f"Could not detect chain for: {target}")
            return

        from walletdna.engine.models import WalletIngestionRequest
        result = await svc.ingest_wallet(
            WalletIngestionRequest(
                address=target,
                chain=chain,
                is_target=is_target,
                is_sender=is_sender,
            )
        )
        print(f"\n✓ Ingestion complete")
        print(f"  Address:  {result.address}")
        print(f"  Chain:    {result.chain.value}")
        print(f"  Txs:      {result.tx_count}")
        print(f"  Status:   {result.status}")
        if result.error:
            print(f"  Error:    {result.error}")

    finally:
        await svc.close()
        await db.close()


async def cmd_ingest_all_senders() -> None:
    """Ingest all 4 of your sender wallets from the Excel data."""
    from walletdna.engine.models import Chain, WalletIngestionRequest
    from walletdna.ingestion import IngestionService
    from walletdna.storage.db import Database

    YOUR_WALLETS = [
        WalletIngestionRequest(
            address="0xD038A997444Db594BBE62AAad8B4735584D8db2d",
            chain=Chain.ETHEREUM,
            label="Purchased Wallet (Primary ETH)",
            is_sender=True,
        ),
        WalletIngestionRequest(
            address="0x3B18DD8653EddC873FcFE4601353b5DCAe4Ac85D",
            chain=Chain.ETHEREUM,
            label="Large DoubleUp ETH",
            is_sender=True,
        ),
        WalletIngestionRequest(
            address="0xb4Bf4E2168b8cbEdE6B7ea5eb2334C988d47D0e1",
            chain=Chain.ETHEREUM,
            label="Medium Size Wallet ETH",
            is_sender=True,
        ),
        WalletIngestionRequest(
            address="0xD86a53FEDFACCBA2e080C0Ea1DD831E0FCEacd90",
            chain=Chain.ETHEREUM,
            label="Tiny Wasted Wallet ETH",
            is_sender=True,
        ),
    ]

    db = Database()
    await db.connect()
    svc = IngestionService(db)

    try:
        print(f"\n{'='*60}")
        print(f"  WalletDNA — Ingesting {len(YOUR_WALLETS)} sender wallets")
        print(f"{'='*60}\n")

        results = await svc.ingest_batch(YOUR_WALLETS)

        print(f"\n{'='*60}")
        print(f"  Results:")
        for r in results:
            status_icon = "✓" if r.status == "success" else "✗"
            print(f"  {status_icon} {r.address[:16]}... | {r.tx_count} txs | {r.status}")
        print(f"{'='*60}\n")

    finally:
        await svc.close()
        await db.close()


async def cmd_health() -> None:
    """Check system health."""
    from walletdna.storage.db import Database

    print("\nWalletDNA — Health Check")
    print("─" * 40)

    # DB check
    try:
        db = Database()
        await db.connect()
        rows = await db.get_all_sender_wallets()
        print(f"✓ Database     connected ({len(rows)} sender wallets)")
        await db.close()
    except Exception as e:
        print(f"✗ Database     FAILED: {e}")

    # API key check
    eth_key = os.getenv("ETHERSCAN_API_KEY", "")
    print(f"{'✓' if eth_key else '!'} Etherscan    {'API key set' if eth_key else 'No API key — rate limited'}")

    print("─" * 40)


def main() -> None:
    """CLI entrypoint."""
    args = sys.argv[1:]

    if not args or args[0] == "health":
        asyncio.run(cmd_health())

    elif args[0] == "ingest":
        if len(args) > 1:
            asyncio.run(cmd_ingest(args[1:]))
        else:
            print("Usage: walletdna ingest <address> [chain] [--target|--sender]")

    elif args[0] == "ingest-senders":
        asyncio.run(cmd_ingest_all_senders())

    elif args[0] == "serve":
        from walletdna.monitoring.metrics import start_metrics_server
        port = int(os.getenv("PROMETHEUS_PORT", "8000"))
        start_metrics_server(port)
        print(f"Metrics server running on :{port}")
        asyncio.get_event_loop().run_forever()

    elif args[0] == "dashboard":
        from walletdna.dashboard.terminal import main as run_dashboard
        run_dashboard()

    elif args[0] == "export":
        asyncio.run(cmd_export())

    elif args[0] == "import":
        asyncio.run(cmd_import())


    else:
        print(f"""
WalletDNA — Behavioural Wallet Intelligence

Commands:
  health              Check system health
  ingest <address>    Ingest a single wallet
  ingest-senders      Ingest all your known sender wallets
  serve               Start metrics server
  dashboard           Launch live terminal dashboard
  export              Export all DB profiles to profiles/ (git-tracked)
  import              Rebuild DB from profiles/ (use after fresh clone)

Examples:
  walletdna health
  walletdna ingest 0xD038A997... ethereum --sender
  walletdna ingest-senders
  walletdna dashboard
  walletdna export
  walletdna import
        """)


if __name__ == "__main__":
    main()


# ---------------------------------------------------------------------------
# Export / Import commands
# ---------------------------------------------------------------------------

async def cmd_export() -> None:
    """
    Export all DNA profiles from DB to profiles/ folder.
    Run before wiping Docker volumes or pushing to GitHub.
    """
    import json
    from pathlib import Path
    from walletdna.storage.db import Database

    profiles_dir = Path(__file__).parent.parent / "profiles"
    profiles_dir.mkdir(parents=True, exist_ok=True)

    db = Database()
    try:
        await db.connect()
        rows = await db.get_all_dna_vectors()
        count = 0
        for row in rows:
            # Fetch full profile
            wallet_row = await db._pool.fetchrow(
                "SELECT address, chain, label FROM wallets WHERE id = $1",
                row["wallet_id"],
            )
            if not wallet_row:
                continue

            profile = {
                "address":        wallet_row["address"],
                "chain":          wallet_row["chain"],
                "label":          wallet_row["label"],
                "dna_string":     row["dna_string"],
                "dna_vector":     list(row["dna_vector"]) if row["dna_vector"] else [],
                "wallet_class":   row["wallet_class"],
                "bot_confidence": 0.0,
                "tx_count":       0,
                "confidence_score": 0.0,
                "exported_at":    __import__("datetime").datetime.now(
                    __import__("datetime").timezone.utc
                ).isoformat(),
            }

            path = profiles_dir / f"{wallet_row['address'].lower()}.json"
            with open(path, "w") as f:
                json.dump(profile, f, indent=2, default=str)
            count += 1

        print(f"✓  Exported {count} profiles to profiles/")
        print(f"   Run: git add profiles/ && git push")
    except Exception as e:
        print(f"✗  Export failed: {e}")
        print("   Is the DB running? docker compose up -d postgres")
    finally:
        await db.close()


async def cmd_import() -> None:
    """
    Rebuild DB from profiles/ folder.
    Run after fresh clone to restore all known profiles instantly.
    """
    import json
    from pathlib import Path
    from walletdna.storage.db import Database

    profiles_dir = Path(__file__).parent.parent / "profiles"
    profile_files = [f for f in profiles_dir.glob("*.json") if f.stem != "README"]

    if not profile_files:
        print("No profiles found in profiles/ folder.")
        return

    db = Database()
    try:
        await db.connect()
        count = 0
        for pf in profile_files:
            try:
                with open(pf) as f:
                    profile = json.load(f)

                from walletdna.engine.models import Chain
                chain_map = {
                    "ethereum": Chain.ETHEREUM,
                    "tron":     Chain.TRON,
                    "dogecoin": Chain.DOGECOIN,
                    "ETH":      Chain.ETHEREUM,
                    "TRX":      Chain.TRON,
                    "DOGE":     Chain.DOGECOIN,
                }
                chain_str = profile.get("chain", "ethereum")
                chain     = chain_map.get(chain_str, Chain.ETHEREUM)

                await db.upsert_wallet(
                    address=profile["address"],
                    chain=chain,
                    label=profile.get("label"),
                )
                count += 1
            except Exception as e:
                print(f"  ⚠  Skipped {pf.name}: {e}")

        print(f"✓  Imported {count} profiles into DB")
    except Exception as e:
        print(f"✗  Import failed: {e}")
        print("   Is the DB running? docker compose up -d postgres")
    finally:
        await db.close()
