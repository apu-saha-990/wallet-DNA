"""
WalletDNA — Prometheus Metrics
All metrics exported to /metrics endpoint on port 8000.
"""

from prometheus_client import Counter, Gauge, Histogram, Info, start_http_server
import structlog

logger = structlog.get_logger(__name__)

# ─── Ingestion ────────────────────────────────────────────────────────────────

WALLETS_INGESTED = Counter(
    "walletdna_wallets_ingested_total",
    "Total wallets ingested",
    ["chain", "status"],
)

TRANSACTIONS_INGESTED = Counter(
    "walletdna_transactions_ingested_total",
    "Total transactions ingested",
    ["chain"],
)

INGESTION_DURATION = Histogram(
    "walletdna_ingestion_duration_seconds",
    "Time to ingest a wallet",
    ["chain"],
    buckets=[1, 5, 10, 30, 60, 120, 300],
)

API_ERRORS = Counter(
    "walletdna_api_errors_total",
    "Chain API errors",
    ["chain", "error_type"],
)

# ─── DNA Engine ───────────────────────────────────────────────────────────────

DNA_GENERATED = Counter(
    "walletdna_dna_generated_total",
    "DNA profiles generated",
    ["chain", "wallet_class"],
)

DNA_GENERATION_DURATION = Histogram(
    "walletdna_dna_generation_seconds",
    "Time to generate a DNA profile",
    ["chain"],
    buckets=[0.1, 0.5, 1, 5, 10, 30],
)

DNA_CONFIDENCE = Histogram(
    "walletdna_dna_confidence",
    "DNA profile confidence scores",
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
)

# ─── Similarity ───────────────────────────────────────────────────────────────

SIMILARITY_COMPUTED = Counter(
    "walletdna_similarity_computed_total",
    "Similarity comparisons computed",
)

SIMILARITY_MATCHES = Counter(
    "walletdna_similarity_matches_total",
    "Wallets matched above threshold",
    ["threshold_band"],   # 0.75-0.85, 0.85-0.95, 0.95+
)

CLUSTERS_DETECTED = Gauge(
    "walletdna_clusters_detected",
    "Active clusters detected",
)

# ─── System ───────────────────────────────────────────────────────────────────

WALLETS_IN_DB = Gauge(
    "walletdna_wallets_in_db",
    "Total wallets in database",
    ["chain", "type"],  # type: sender | target
)

DNA_PROFILES_IN_DB = Gauge(
    "walletdna_dna_profiles_in_db",
    "Total DNA profiles in database",
)

BOT_CLASSIFICATIONS = Gauge(
    "walletdna_bot_classifications",
    "Wallet classifications breakdown",
    ["wallet_class"],
)

BUILD_INFO = Info(
    "walletdna_build",
    "WalletDNA build information",
)


def start_metrics_server(port: int = 8000) -> None:
    """Start Prometheus metrics HTTP server."""
    BUILD_INFO.info({
        "version":  "1.0.0",
        "project":  "WalletDNA",
        "chains":   "ethereum,tron,dogecoin",
    })
    start_http_server(port)
    logger.info("metrics_server_started", port=port)
