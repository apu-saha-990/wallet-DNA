"""
WalletDNA — Core Data Models
All data structures used across the system.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


# ─── Enums ────────────────────────────────────────────────────────────────────

class Chain(str, Enum):
    ETHEREUM = "ethereum"
    TRON     = "tron"
    DOGECOIN = "dogecoin"


class TxDirection(str, Enum):
    IN   = "in"
    OUT  = "out"
    SELF = "self"


class WalletClass(str, Enum):
    BOT          = "BOT"
    LIKELY_BOT   = "LIKELY_BOT"
    LIKELY_HUMAN = "LIKELY_HUMAN"
    HUMAN        = "HUMAN"
    UNKNOWN      = "UNKNOWN"


class GasLabel(str, Enum):
    LOW    = "LOW"
    MED    = "MED"
    HIGH   = "HIGH"


class StabilityLabel(str, Enum):
    STABLE   = "STABLE"
    MODERATE = "MODERATE"
    ERRATIC  = "ERRATIC"


class FragmentationLabel(str, Enum):
    LOW  = "LOW"
    MED  = "MED"
    HIGH = "HIGH"


class BurstLabel(str, Enum):
    STEADY      = "STEADY"
    BURST_SLEEP = "BURST-SLEEP"
    BURST_HIGH  = "BURST-HIGH"


# ─── Normalised Transaction ────────────────────────────────────────────────────

class NormalisedTx(BaseModel):
    """
    Chain-agnostic transaction representation.
    Every adapter outputs this — the DNA engine never sees raw chain data.
    """
    tx_hash:           str
    chain:             Chain
    block_number:      Optional[int]   = None
    block_time:        datetime
    from_address:      str
    to_address:        str
    direction:         TxDirection
    value_native:      float           = 0.0
    value_usd:         Optional[float] = None
    fee_native:        Optional[float] = None
    fee_usd:           Optional[float] = None

    # ETH-specific
    gas_price_gwei:    Optional[float] = None
    gas_used:          Optional[int]   = None
    gas_limit:         Optional[int]   = None

    # TRX-specific
    energy_used:       Optional[int]   = None
    bandwidth_used:    Optional[int]   = None

    # Classification hints
    is_contract_call:  bool            = False
    contract_method:   Optional[str]   = None
    token_symbol:      Optional[str]   = None
    confirmation_blocks: Optional[int] = None

    @field_validator("from_address", "to_address", mode="before")
    @classmethod
    def lowercase_address(cls, v: str) -> str:
        return v.lower() if v else v


# ─── DNA Feature Dimensions ───────────────────────────────────────────────────

class GasFeature(BaseModel):
    mean_gwei:       float
    std_gwei:        float
    percentile_50:   float
    percentile_95:   float
    label:           GasLabel
    stability:       StabilityLabel
    score:           float = Field(ge=0.0, le=1.0)
    confidence:      float = Field(ge=0.0, le=1.0)


class TimingFeature(BaseModel):
    active_hour_start:   int    = Field(ge=0, le=23)
    active_hour_end:     int    = Field(ge=0, le=23)
    active_window_utc:   str                          # "0200-0600UTC"
    timing_entropy:      float  = Field(ge=0.0, le=1.0)
    median_interval_sec: int
    sleep_gap_hours:     float
    score:               float  = Field(ge=0.0, le=1.0)
    confidence:          float  = Field(ge=0.0, le=1.0)


class ValueFeature(BaseModel):
    herfindahl_index:    float  = Field(ge=0.0, le=1.0)
    fragmentation:       FragmentationLabel
    round_number_ratio:  float  = Field(ge=0.0, le=1.0)
    median_value_usd:    Optional[float]
    score:               float  = Field(ge=0.0, le=1.0)
    confidence:          float  = Field(ge=0.0, le=1.0)


class ContractFeature(BaseModel):
    dex_ratio:       float = Field(ge=0.0, le=1.0)
    bridge_ratio:    float = Field(ge=0.0, le=1.0)
    eoa_ratio:       float = Field(ge=0.0, le=1.0)
    top_type:        str                              # "DEX-HEAVY", "EOA-ONLY", etc.
    score:           float = Field(ge=0.0, le=1.0)
    confidence:      float = Field(ge=0.0, le=1.0)
    not_applicable:  bool  = False                   # DOGE has no contracts


class MempoolFeature(BaseModel):
    avg_wait_blocks:     float
    instant_ratio:       float = Field(ge=0.0, le=1.0)  # confirmed in <=2 blocks
    label:               str                              # "INSTANT", "NORMAL", "SLOW"
    score:               float = Field(ge=0.0, le=1.0)
    confidence:          float = Field(ge=0.0, le=1.0)
    not_applicable:      bool  = False


class ActivityFeature(BaseModel):
    burst_score:     float = Field(ge=0.0, le=1.0)
    dormancy_score:  float = Field(ge=0.0, le=1.0)
    label:           BurstLabel
    avg_daily_tx:    float
    peak_day_tx:     int
    score:           float = Field(ge=0.0, le=1.0)
    confidence:      float = Field(ge=0.0, le=1.0)


# ─── Bot Classification ───────────────────────────────────────────────────────

class BotClassification(BaseModel):
    wallet_class:    WalletClass
    confidence:      float = Field(ge=0.0, le=1.0)
    signals:         list[str]          # which signals triggered
    explanation:     str


# ─── Full DNA Profile ─────────────────────────────────────────────────────────

class DNAProfile(BaseModel):
    address:         str
    chain:           Chain
    label:           Optional[str]      = None

    # Feature dimensions
    gas:             Optional[GasFeature]       = None
    timing:          Optional[TimingFeature]    = None
    value:           Optional[ValueFeature]     = None
    contract:        Optional[ContractFeature]  = None
    mempool:         Optional[MempoolFeature]   = None
    activity:        Optional[ActivityFeature]  = None

    # Classification
    classification:  Optional[BotClassification] = None

    # Output
    dna_string:      Optional[str]      = None
    dna_vector:      Optional[list[float]] = None

    # Metadata
    tx_count:        int                = 0
    analysis_window_days: Optional[int] = None
    confidence_score: float             = 0.0
    generated_at:    datetime           = Field(default_factory=datetime.utcnow)
    error:           Optional[str]      = None


# ─── Similarity ───────────────────────────────────────────────────────────────

class SimilarityResult(BaseModel):
    wallet_a:        str
    wallet_b:        str
    similarity:      float = Field(ge=0.0, le=1.0)
    interpretation:  str   # "LIKELY SAME OPERATOR", "SIMILAR BEHAVIOUR", etc.


class ClusterResult(BaseModel):
    cluster_id:      int
    label:           str
    addresses:       list[str]
    avg_similarity:  float
    dominant_class:  WalletClass
    notes:           Optional[str] = None


# ─── Ingestion ────────────────────────────────────────────────────────────────

class WalletIngestionRequest(BaseModel):
    address:     str
    chain:       Chain
    label:       Optional[str]  = None
    is_target:   bool           = False
    is_sender:   bool           = False


class IngestionResult(BaseModel):
    address:     str
    chain:       Chain
    tx_count:    int
    status:      str            # success | error | partial
    error:       Optional[str]  = None
    duration_ms: int            = 0
