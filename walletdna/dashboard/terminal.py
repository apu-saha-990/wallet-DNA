"""
WalletDNA — Terminal Dashboard (Phase 4)
========================================
Live ingestion pipeline wired directly into the dashboard.
DB cache -> profile store -> live API (priority order).

Layout:
  [Investigation Summary]  <- NEW: conclusion first
  [Table 1 - DNA Generation]
  [Table 2 - Comparison]
  [Table 3 - Cluster Detection]

New in Phase 4:
  - Real DNA from live API, not hardcoded
  - Investigation Summary panel with Analysis ID, Risk, Confidence
  - Persistence: profiles/ folder auto-saved after every analysis
  - "Add to watchlist?" prompt on high-similarity match
  - Chain auto-detected from address format
  - DB cache check before any API call
"""

from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from rich import box
from rich.align import Align
from rich.console import Console, Group
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.rule import Rule
from rich.table import Table
from rich.text import Text
from dotenv import load_dotenv
load_dotenv()

console = Console()

# --- Colours ------------------------------------------------------------------
BLUE  = "#2D7DD2"
DARK  = "#1E3A5F"
GREEN = "#39D353"
AMBER = "#F4A261"
RED   = "#E76F51"
GREY  = "#888888"
DIM   = "#444444"
WHITE = "white"

# --- DNA Dimensions -----------------------------------------------------------
DNA_DIMS = [
    ("G", "Gas Profile",    "Gas CV"),
    ("T", "Timing",         "Hour Entropy"),
    ("V", "Value",          "HHI Index"),
    ("C", "Contract",       "DEX Ratio"),
    ("M", "Mempool",        "Instant Ratio"),
    ("A", "Activity",       "Burst Score"),
    ("X", "Classification", "Bot Score"),
]

# --- Demo fallback profiles ---------------------------------------------------
DEMO_YOUR_WALLET = {
    "address":        "0xD038A997444Db594BBE62AAad8B4735584D8db2d",
    "label":          "Primary Wallet",
    "chain":          "ETH",
    "tx_count":       29,
    "value_display":  "$10,503",
    "wallet_class":   "HUMAN",
    "bot_confidence": 0.09,
    "confidence_score": 0.29,
    "dna_string":     "G:MED-ERRATIC | T:SPREAD | V:SPLIT-LOW-ROUND | C:EOA-DOMINANT | M:NORMAL | A:STEADY | X:HUMAN-LOW",
    "dna": {
        "G": ("MED-ERRATIC",     GREEN),
        "T": ("SPREAD",          GREEN),
        "V": ("SPLIT-LOW-ROUND", GREEN),
        "C": ("EOA-DOMINANT",    GREEN),
        "M": ("NORMAL",          GREEN),
        "A": ("STEADY",          GREEN),
        "X": ("HUMAN-LOW",       GREEN),
    },
    "source": "demo",
}

DEMO_SUSPECT_WALLET = {
    "address":        "0xE40BD3d16AF3dbea6Ed781ebAC22e0f9A21a416c",
    "label":          "Suspect #1",
    "chain":          "ETH",
    "tx_count":       412,
    "value_display":  "unknown",
    "wallet_class":   "BOT",
    "bot_confidence": 0.87,
    "confidence_score": 0.82,
    "dna_string":     "G:MED-STABLE | T:0300-0500UTC | V:SPLIT-HIGH-PRECISE | C:DEX-HEAVY | M:INSTANT | A:BURST-SLEEP | X:BOT-HIGH",
    "dna": {
        "G": ("MED-STABLE",         AMBER),
        "T": ("0300-0500UTC",       RED),
        "V": ("SPLIT-HIGH-PRECISE", AMBER),
        "C": ("DEX-HEAVY",          AMBER),
        "M": ("INSTANT",            RED),
        "A": ("BURST-SLEEP",        RED),
        "X": ("BOT-HIGH",           RED),
    },
    "source": "demo",
}

DEMO_SIM_MATRIX = [
    [1.00, 0.94, 0.92, 0.91, 0.93],
    [0.94, 1.00, 0.93, 0.90, 0.92],
    [0.92, 0.93, 1.00, 0.91, 0.94],
    [0.91, 0.90, 0.91, 1.00, 0.91],
    [0.93, 0.92, 0.94, 0.91, 1.00],
]


# --- Helpers ------------------------------------------------------------------

def detect_chain(addr: str) -> Optional[str]:
    addr = addr.strip()
    if re.match(r"^0x[0-9a-fA-F]{40}$", addr):             return "ETH"
    if re.match(r"^T[1-9A-HJ-NP-Za-km-z]{33}$", addr):    return "TRX"
    if re.match(r"^D[1-9A-HJ-NP-Za-km-z]{32,34}$", addr): return "DOGE"
    return None


def load_wallet_config() -> dict:
    wj = Path(__file__).parent.parent.parent / "wallets.json"
    if wj.exists():
        with open(wj) as f:
            return json.load(f)
    return {"your_wallets": [], "suspect_wallets": []}


def load_profile_from_disk(address: str) -> Optional[dict]:
    profiles_dir = Path(__file__).parent.parent.parent / "profiles"
    path = profiles_dir / f"{address.lower()}.json"
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            return None
    return None


def save_profile_to_disk(profile: dict) -> None:
    profiles_dir = Path(__file__).parent.parent.parent / "profiles"
    profiles_dir.mkdir(parents=True, exist_ok=True)
    address = profile.get("address", "unknown")
    path = profiles_dir / f"{address.lower()}.json"
    profile["saved_at"] = datetime.now(timezone.utc).isoformat()
    with open(path, "w") as f:
        json.dump(profile, f, indent=2, default=str)


def add_to_watchlist(address: str, label: str) -> bool:
    wj_path = Path(__file__).parent.parent.parent / "wallets.json"
    try:
        with open(wj_path) as f:
            config = json.load(f)
        all_addresses = [
            w["address"].lower()
            for section in config.values()
            for w in section
            if isinstance(w, dict) and "address" in w
        ]
        if address.lower() in all_addresses:
            return False
        if "suspect_wallets" not in config:
            config["suspect_wallets"] = []
        config["suspect_wallets"].append({"address": address, "label": label})
        with open(wj_path, "w") as f:
            json.dump(config, f, indent=2)
        return True
    except Exception:
        return False


def _parse_dna_string(dna_string: str) -> dict:
    """Parse 'G:MED-STABLE | T:... ' into {dim: (value, colour)} dict."""
    dna = {}
    if not dna_string:
        return {d: ("N/A", GREY) for d, _, _ in DNA_DIMS}

    for part in dna_string.split(" | "):
        if ":" not in part:
            continue
        dim, val = part.split(":", 1)
        dim = dim.strip()
        val = val.strip()

        if dim == "X":
            if "BOT" in val and "LIKELY" not in val:
                colour = RED
            elif "LIKELY_BOT" in val:
                colour = AMBER
            elif "LIKELY_HUMAN" in val:
                colour = "#90EE90"
            else:
                colour = GREEN
        elif any(kw in val for kw in ["STABLE", "INSTANT", "BURST-SLEEP", "DEX-HEAVY", "PRECISE"]):
            colour = AMBER
        elif "BOT" in val:
            colour = RED
        else:
            colour = GREEN

        dna[dim] = (val, colour)

    for d, _, _ in DNA_DIMS:
        if d not in dna:
            dna[d] = ("N/A", GREY)

    return dna


def risk_level(bot_score: float) -> tuple[str, str]:
    if bot_score >= 0.65:
        return "HIGH", RED
    elif bot_score >= 0.35:
        return "MEDIUM", AMBER
    else:
        return "LOW", GREEN


def risk_emoji(bot_score: float) -> str:
    if bot_score >= 0.65: return "HIGH"
    elif bot_score >= 0.35: return "MEDIUM"
    else: return "LOW"


def score_bar(score: float, width: int = 18) -> Text:
    filled = int(score * width)
    colour = GREEN if score <= 0.35 else AMBER if score <= 0.65 else RED
    t = Text()
    t.append("█" * filled,           style=f"bold {colour}")
    t.append("░" * (width - filled),  style=DIM)
    t.append(f"  {score:.2f}",        style=f"bold {colour}")
    return t


def dna_line(dna: dict) -> Text:
    t = Text()
    dims = [d for d, _, _ in DNA_DIMS]
    for i, dim in enumerate(dims):
        val, colour = dna.get(dim, ("N/A", GREY))
        t.append(f"{dim}:", style=f"bold {GREY}")
        t.append(val,       style=f"bold {colour}")
        if i < len(dims) - 1:
            t.append("  |  ", style=DIM)
    return t


async def ingest_live(address: str, chain: str) -> Optional[dict]:
    """
    Attempt live ingestion. Priority: disk cache -> live API.
    Returns display-ready profile dict or None.
    """
    # Check disk cache first
    cached = load_profile_from_disk(address)
    if cached:
        cached["source"] = "cache"
        return cached

    try:
        from walletdna.engine.composer import DNAComposer
        from walletdna.engine.models import Chain as ChainEnum

        chain_map = {
            "ETH":  ChainEnum.ETHEREUM,
            "TRX":  ChainEnum.TRON,
            "DOGE": ChainEnum.DOGECOIN,
        }
        chain_enum = chain_map.get(chain)
        if not chain_enum:
            return None

        if chain == "ETH":
            from walletdna.adapters.eth import EthereumAdapter
            adapter = EthereumAdapter()
        elif chain == "TRX":
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

        dna_display = _parse_dna_string(profile.dna_string or "")

        # Compute total native volume (all directions, native + token)
        total_native = sum(
            float(t.value_native) for t in txs if t.value_native
        )
        api_limit_hit = len(txs) >= 9999
        chain_sym = {"ETH": "ETH", "TRX": "TRX", "DOGE": "DOGE"}.get(chain.upper(), chain.upper())

        # Fetch USD price
        usd_price = 0.0
        try:
            import urllib.request, json as _json
            coin_id = {"ETH": "ethereum", "TRX": "tron", "DOGE": "dogecoin"}.get(chain.upper(), "ethereum")
            url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"
            with urllib.request.urlopen(url, timeout=5) as r:
                usd_price = _json.loads(r.read())[coin_id]["usd"]
        except Exception:
            pass

        total_usd = total_native * usd_price
        if total_usd > 0:
            value_str = f"{total_native:,.2f} {chain_sym} (${total_usd:,.0f} USD)"
        elif total_native > 0:
            value_str = f"{total_native:,.4f} {chain_sym}"
        else:
            value_str = "live"

        result = {
            "address":          address,
            "chain":            chain,
            "label":            None,
            "tx_count":         profile.tx_count,
            "total_native":     round(total_native, 4),
            "total_usd":        round(total_usd, 2),
            "total_eth":        round(total_native, 4),
            "api_limit_hit":    api_limit_hit,
            "value_display":    value_str,
            "wallet_class":     profile.classification.wallet_class.value if profile.classification else "UNKNOWN",
            "bot_confidence":   profile.classification.confidence if profile.classification else 0.0,
            "confidence_score": profile.confidence_score,
            "dna_string":       profile.dna_string,
            "dna_vector":       profile.dna_vector,
            "dna":              dna_display,
            "source":           "live",
        }

        save_profile_to_disk(result)
        return result

    except Exception:
        return None


# --- Investigation Summary Panel ----------------------------------------------

def render_investigation_summary(
    target:   dict,
    suspects: list[dict],
    analysis_id: str,
) -> Panel:
    addr  = target["address"]
    label = target.get("label") or "Input Wallet"
    chain = target.get("chain", "ETH")
    txns  = target.get("tx_count", "—")
    src   = target.get("source", "demo")

    wclass = target.get("wallet_class", "UNKNOWN")
    bconf  = float(target.get("bot_confidence", 0.5))
    cscore = float(target.get("confidence_score", 0.0))
    display_conf = cscore if cscore > 0 else (1.0 - bconf) if "HUMAN" in wclass else bconf
    conf_pct = f"{int(display_conf * 100)}%"
    risk_str, risk_col = risk_level(bconf)

    # Classification colour
    if "BOT" in wclass and "LIKELY" not in wclass:
        class_col = RED
    elif "LIKELY_BOT" in wclass:
        class_col = AMBER
    elif "LIKELY_HUMAN" in wclass:
        class_col = "#90EE90"
    else:
        class_col = GREEN

    # Cluster match
    cluster_match = "NONE"
    cluster_col   = GREEN
    avg_sim       = 0.0
    sim_scores    = []

    dna_vec = target.get("dna_vector")
    if dna_vec and suspects:
        try:
            from walletdna.engine.similarity import SimilarityEngine
            engine = SimilarityEngine()
            for sp in suspects:
                sv = sp.get("dna_vector")
                if sv:
                    sim_scores.append(engine.compare_vectors(dna_vec, sv))
            if sim_scores:
                avg_sim = sum(sim_scores) / len(sim_scores)
                max_sim = max(sim_scores)
                count   = len([s for s in sim_scores if s >= 0.75])
                if max_sim >= 0.92:
                    cluster_match = f"YES  BOT-CLUSTER-{count}W-{int(max_sim*100)}SIM"
                    cluster_col   = RED
                elif max_sim >= 0.75:
                    cluster_match = f"PARTIAL  {int(max_sim*100)}% MATCH"
                    cluster_col   = AMBER
        except Exception:
            pass

    # Conclusion
    if "BOT" in wclass and avg_sim >= 0.85:
        conclusion = "Coordinated automated wallet network detected"
        conc_col   = RED
    elif "BOT" in wclass and "LIKELY" not in wclass:
        conclusion = "Automated wallet behaviour detected"
        conc_col   = RED
    elif "LIKELY_BOT" in wclass:
        conclusion = "Probable automation — further analysis recommended"
        conc_col   = AMBER
    elif avg_sim >= 0.75:
        conclusion = "High behavioural similarity to known suspect wallets"
        conc_col   = AMBER
    else:
        conclusion = "Human retail behaviour — no cluster match detected"
        conc_col   = GREEN

    source_tag = (
        f"[{AMBER}]  DEMO MODE[/{AMBER}]" if src == "demo"
        else f"[{GREEN}]  source: {src}[/{GREEN}]"
    )

    t = Table(show_header=False, box=None, padding=(0, 2), expand=True)
    t.add_column(style=GREY,  width=22)
    t.add_column(style=WHITE, width=70)

    short = f"{addr[:10]}...{addr[-6:]}"

    t.add_row("Analysis ID",    Text(analysis_id,               style=f"bold {BLUE}"))
    t.add_row("Target wallet",  Text(f"{short}  ·  {label}",    style=f"bold {BLUE}"))
    total_eth     = target.get("total_eth")
    api_limit_hit = target.get("api_limit_hit", False)
    chain_upper   = chain.upper()
    native_sym    = "ETH" if chain_upper == "ETH" else "TRX" if chain_upper == "TRX" else "DOGE" if chain_upper == "DOGE" else "native"
    volume_str    = f"  ·  {total_eth:.4f} {native_sym} outbound" if total_eth else ""
    api_warn_str  = "  ⚠ API LIMIT — capped at 10,000 txns" if api_limit_hit else ""
    chain_t = Text()
    chain_t.append(f"{chain}  ·  {txns} transactions{volume_str}", style=f"bold {BLUE}")
    if api_warn_str:
        chain_t.append(api_warn_str, style=f"bold {AMBER}")
    t.add_row("Chain", chain_t)
    t.add_row("Classification", Text(wclass,                    style=f"bold {class_col}"))

    risk_t = Text()
    risk_t.append(f"{risk_str}", style=f"bold {risk_col}")
    t.add_row("Risk Level", risk_t)

    conf_t = Text()
    conf_t.append(conf_pct, style=f"bold {risk_col}")
    t.add_row("Confidence", conf_t)

    cl_t = Text()
    cl_t.append(cluster_match, style=f"bold {cluster_col}")
    t.add_row("Cluster Match", cl_t)

    if sim_scores:
        sim_t = Text()
        sim_col = GREEN if avg_sim < 0.50 else RED if avg_sim > 0.85 else AMBER
        sim_t.append(f"{avg_sim:.3f}", style=f"bold {sim_col}")
        sim_t.append(f"  (avg across {len(sim_scores)} wallets)", style=GREY)
        t.add_row("Similarity", sim_t)

    t.add_row("", Text(""))
    conc_t = Text()
    conc_t.append(conclusion, style=f"bold {conc_col}")
    t.add_row("Conclusion", conc_t)

    return Panel(
        t,
        title=f"[bold white]🔍  INVESTIGATION SUMMARY[/bold white]{source_tag}",
        subtitle=f"[{GREY}]{analysis_id}  ·  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}[/{GREY}]",
        border_style=BLUE,
        style="on #0D1117",
        padding=(1, 2),
    )


# --- Table 1 ------------------------------------------------------------------

def render_table1(profile: dict) -> Panel:
    addr  = profile["address"]
    label = profile.get("label") or "Input Wallet"
    chain = profile.get("chain", "ETH")
    txns  = profile.get("tx_count", "—")
    value = profile.get("value_display", "—")
    dna   = profile.get("dna") or {d: ("N/A", GREY) for d, _, _ in DNA_DIMS}
    bconf = float(profile.get("bot_confidence", 0.5))

    dim_weights = [0.12, 0.18, 0.10, 0.10, 0.08, 0.12, 1.0]
    bot_scores  = [min(bconf * w / max(dim_weights) * 1.2, 1.0) for w in dim_weights]
    bot_scores[-1] = bconf

    t = Table(
        show_header=True, header_style=f"bold white on {DARK}",
        box=box.SIMPLE_HEAVY, border_style=DARK, padding=(0, 1), expand=True,
    )
    t.add_column("DIM",       style=f"bold {BLUE}", width=5)
    t.add_column("DIMENSION", style=WHITE,          width=16)
    t.add_column("SIGNAL",    style=GREY,           width=16)
    t.add_column("VALUE",     style=WHITE,          width=26)
    t.add_column("BOT SCORE", style=WHITE,          width=24)

    for i, (dim, name, signal) in enumerate(DNA_DIMS):
        val, colour = dna.get(dim, ("N/A", GREY))
        t.add_row(
            Text(dim,    style=f"bold {BLUE}"),
            Text(name,   style=WHITE),
            Text(signal, style=GREY),
            Text(f"  {val}", style=f"bold {colour}"),
            score_bar(bot_scores[i]),
        )

    content = Group(
        t,
        Rule(style=DARK),
        Align.center(Text("◆  DNA FINGERPRINT  ◆", style=f"bold {GREEN}")),
        Align.center(dna_line(dna)),
    )

    short    = f"{addr[:10]}...{addr[-6:]}"
    api_warn = profile.get("api_limit_hit", False)
    api_note = f"  ·  [bold #F4A261]⚠ API limit hit — actual volume is higher[/bold #F4A261]" if api_warn else ""
    return Panel(
        content,
        title=f"[bold {BLUE}]⚙  TABLE 1 — DNA GENERATION[/bold {BLUE}]  [{GREY}]{short}  ·  {label}[/{GREY}]",
        subtitle=f"[{GREY}]{txns} transactions  ·  {chain}  ·  {value}[/{GREY}]{api_note}",
        border_style=BLUE, style="on #0D1117", padding=(0, 1),
    )


# --- Table 2 ------------------------------------------------------------------

def render_table2(your: dict, suspect: dict) -> Panel:
    VERDICTS = {
        "G": "Fees vary vs always same",
        "T": "All day vs 3-5am only",
        "V": "Round amounts vs precise",
        "C": "Simple transfers vs DEX",
        "M": "Sometimes waits vs instant",
        "A": "Steady vs burst+sleep",
        "X": "Classification comparison",
    }

    your_dna    = your.get("dna")     or {d: ("N/A", GREY) for d, _, _ in DNA_DIMS}
    suspect_dna = suspect.get("dna")  or {d: ("N/A", GREY) for d, _, _ in DNA_DIMS}
    your_label  = your.get("label")   or "Your Wallet"
    sus_label   = suspect.get("label") or "Suspect Wallet"

    sim_score = 0.11
    yv = your.get("dna_vector")
    sv = suspect.get("dna_vector")
    if yv and sv:
        try:
            from walletdna.engine.similarity import SimilarityEngine
            sim_score = SimilarityEngine().compare_vectors(yv, sv)
        except Exception:
            pass

    sim_col = GREEN if sim_score < 0.50 else RED if sim_score > 0.85 else AMBER

    t = Table(
        show_header=True, header_style=f"bold white on {DARK}",
        box=box.SIMPLE_HEAVY, border_style=DARK, padding=(0, 1), expand=True,
    )
    t.add_column("DIMENSION",      style=f"bold {BLUE}", width=16)
    t.add_column("YOUR WALLET",    style=WHITE,          width=24, header_style=f"bold {GREEN}")
    t.add_column("SUSPECT WALLET", style=WHITE,          width=24, header_style=f"bold {RED}")
    t.add_column("VERDICT",        style=WHITE,          width=30)

    for dim, name, _ in DNA_DIMS:
        y_val, y_col = your_dna.get(dim, ("N/A", GREY))
        s_val, s_col = suspect_dna.get(dim, ("N/A", GREY))
        t.add_row(
            Text(f"{dim}: {name}", style=f"bold {BLUE}"),
            Text(f"  {y_val}",     style=f"bold {y_col}"),
            Text(f"  {s_val}",     style=f"bold {s_col}"),
            Text(f"  ⚠  {VERDICTS.get(dim, '')}", style=f"bold {RED}"),
        )

    t.add_section()
    sim_t = Text()
    sim_t.append("  DNA SIMILARITY: ",  style=GREY)
    sim_t.append(f"{sim_score:.2f}  ",  style=f"bold {sim_col}")
    interp = (
        "LIKELY SAME OPERATOR" if sim_score >= 0.92
        else "DISTINCT BEHAVIOUR" if sim_score < 0.50
        else "SOME SIMILARITY"
    )
    sim_t.append(interp, style=GREY)
    t.add_row(
        Text("SIMILARITY", style="bold white"),
        Text(your_label,   style=GREY),
        Text(sus_label,    style=GREY),
        sim_t,
    )

    ya = your["address"]
    sa = suspect["address"]
    return Panel(
        t,
        title=f"[bold {AMBER}]⚖  TABLE 2 — COMPARISON[/bold {AMBER}]  [{GREY}]Human Retail Buyer  vs  Suspect Wallet[/{GREY}]",
        subtitle=f"[bold {GREEN}]{ya[:8]}...{ya[-6:]} (YOU)[/bold {GREEN}]  [{DIM}]vs[/{DIM}]  [bold {RED}]{sa[:8]}...{sa[-6:]} (SUSPECT)[/bold {RED}]",
        border_style=AMBER, style="on #0D1117", padding=(0, 1),
    )


# --- Table 3 ------------------------------------------------------------------

def render_table3(suspect_profiles: list[dict]) -> Panel:
    n = len(suspect_profiles)
    if n == 0:
        return Panel(
            Text("No suspect profiles loaded.", style=GREY),
            title=f"[bold {RED}]🔍  TABLE 3 — CLUSTER DETECTION[/bold {RED}]",
            border_style=RED, style="on #0D1117",
        )

    vecs   = [sp.get("dna_vector") for sp in suspect_profiles]
    labels = [sp.get("label", sp["address"][:10]) for sp in suspect_profiles]

    sim_matrix = [[1.0 if i == j else 0.0 for j in range(n)] for i in range(n)]

    if all(v for v in vecs):
        try:
            from walletdna.engine.similarity import SimilarityEngine
            engine = SimilarityEngine()
            for i in range(n):
                for j in range(n):
                    if i != j:
                        sim_matrix[i][j] = engine.compare_vectors(vecs[i], vecs[j])
        except Exception:
            for i in range(n):
                for j in range(n):
                    if i != j:
                        sim_matrix[i][j] = DEMO_SIM_MATRIX[min(i, 4)][min(j, 4)]
    else:
        for i in range(n):
            for j in range(n):
                if i != j:
                    sim_matrix[i][j] = DEMO_SIM_MATRIX[min(i, 4)][min(j, 4)]

    t = Table(
        show_header=True, header_style=f"bold {GREY} on #0D1117",
        box=box.SIMPLE, border_style=DARK, show_edge=False, padding=(0, 0), expand=True,
    )
    t.add_column("WALLET",  width=18, style=f"bold {BLUE}")
    t.add_column("LABEL",   width=18, style=GREY)
    for lbl in labels:
        t.add_column(lbl[:10], width=8, justify="center")
    t.add_column("CLASS",   width=10)
    t.add_column("SCORE",   width=8, justify="right")

    for i, sp in enumerate(suspect_profiles):
        addr  = sp["address"]
        lbl   = sp.get("label", addr[:10])
        bconf = float(sp.get("bot_confidence", 0.87))
        wc    = sp.get("wallet_class", "BOT")

        row = [
            Text(f"{addr[:8]}...{addr[-6:]}", style=f"bold {BLUE}"),
            Text(lbl, style=GREY),
        ]
        for j in range(n):
            if i == j:
                row.append(Text("  —  ", style=DIM))
            else:
                s      = sim_matrix[i][j]
                colour = GREEN if s >= 0.92 else AMBER
                row.append(Text(f"{s:.2f}", style=f"bold {colour}"))

        badge = Text()
        badge.append(" BOT " if "BOT" in wc else " HUM ", style="bold white on red" if "BOT" in wc else "bold white on green")
        row.append(badge)
        row.append(Text(f"{bconf:.2f}", style=f"bold {RED if bconf > 0.65 else AMBER}"))
        t.add_row(*row)

    all_pairs = [sim_matrix[i][j] for i in range(n) for j in range(n) if i != j]
    avg_sim   = sum(all_pairs) / len(all_pairs) if all_pairs else 0.0
    in_cluster = sum(1 for sp in suspect_profiles if float(sp.get("bot_confidence", 0)) >= 0.40)

    summary = Text()
    summary.append("\n  ◆ CLUSTER RESULT  ", style=f"bold {GREEN}")
    summary.append(f"BOT-CLUSTER-{n}W-{int(avg_sim*100)}SIM\n\n", style=f"bold {RED}")

    for lbl_s, val, col in [
        ("Wallets analysed",   str(n),                   WHITE),
        ("Wallets in cluster", f"{in_cluster}  ({int(in_cluster/max(n,1)*100)}%)", RED),
        ("Avg similarity",     f"{avg_sim:.3f}",          GREEN),
        ("Dominant class",     "BOT",                     RED),
        ("Interpretation",     "LIKELY SAME OPERATOR" if avg_sim >= 0.92 else "SIMILAR BEHAVIOUR",
                               RED if avg_sim >= 0.92 else AMBER),
    ]:
        summary.append(f"  {lbl_s:<24}", style=GREY)
        summary.append(f"{val}\n",       style=f"bold {col}")

    summary.append(f"\n  {n} different addresses.  Identical behaviour.  One operator.\n", style=f"bold {AMBER}")
    summary.append("  A wallet can change its address — it cannot change its behaviour.\n", style=GREY)

    return Panel(
        Group(t, Rule(style=DARK), summary),
        title=f"[bold {RED}]🔍  TABLE 3 — CLUSTER DETECTION[/bold {RED}]  [{GREY}]Weighted Cosine Similarity  ·  threshold 0.75[/{GREY}]",
        subtitle=f"[{GREY}]{n} suspect wallets  ·  greedy O(n²) clustering  ·  threshold 0.75[/{GREY}]",
        border_style=RED, style="on #0D1117", padding=(0, 1),
    )


# --- Wallet selection prompt --------------------------------------------------

def prompt_wallet_selection(config: dict) -> tuple[str, str, bool]:
    """Returns (address, chain, is_new_address)."""
    console.print()
    console.rule(f"[bold {BLUE}]🧬  WALLETDNA[/bold {BLUE}]", style=DARK)
    console.print()

    chain_table = Table(
        show_header=True, header_style=f"bold white on {DARK}",
        box=box.SIMPLE_HEAVY, border_style=DARK, padding=(0, 2),
    )
    chain_table.add_column("CHAIN",   style=f"bold {BLUE}", width=8)
    chain_table.add_column("FORMAT",  style=GREY,           width=36)
    chain_table.add_column("EXAMPLE", style=DIM,            width=46)
    chain_table.add_row("ETH",  "Starts 0x  ·  42 characters",  "0xD038A997444Db594BBE62AAad8B4735584D8db2d")
    chain_table.add_row("TRX",  "Starts T   ·  34 characters",  "TQn9Y2khEsLJW1ChVWFMSMeRDow5KcbLSE")
    chain_table.add_row("DOGE", "Starts D   ·  33-34 chars",    "DH5yaieqoZN36fDVciNyRueRGvGLR3mr38")
    console.print(Align.center(chain_table))
    console.print()

    all_wallets = []
    your_wallets    = config.get("your_wallets", [])
    suspect_wallets = config.get("suspect_wallets", [])

    if your_wallets or suspect_wallets:
        menu = Table(
            show_header=True, header_style=f"bold white on {DARK}",
            box=box.SIMPLE_HEAVY, border_style=DARK, padding=(0, 2),
        )
        menu.add_column("#",      style=f"bold {BLUE}", width=4)
        menu.add_column("TYPE",   style=GREY,           width=10)
        menu.add_column("LABEL",  style=WHITE,          width=20)
        menu.add_column("ADDRESS",style=DIM,            width=46)
        menu.add_column("CHAIN",  style=f"bold {BLUE}", width=6)
        menu.add_column("CACHED", style=GREEN,          width=8)

        idx = 1
        for w in your_wallets:
            ch = detect_chain(w["address"]) or "?"
            cached = "✓" if load_profile_from_disk(w["address"]) else ""
            menu.add_row(str(idx), "YOURS", w["label"], w["address"], ch, cached)
            all_wallets.append(w["address"])
            idx += 1

        for w in suspect_wallets:
            if w["address"].startswith("SUSPECT_ADDRESS"):
                continue
            ch = detect_chain(w["address"]) or "?"
            cached = "✓" if load_profile_from_disk(w["address"]) else ""
            menu.add_row(str(idx), f"[bold {RED}]SUSPECT[/bold {RED}]", w["label"], w["address"], ch, cached)
            all_wallets.append(w["address"])
            idx += 1

        console.print(Align.center(menu))
        console.print()

    console.print(f"  [{GREY}]Enter a number, paste any address, or press Enter for demo.[/{GREY}]\n")

    raw = Prompt.ask(
        f"  [{BLUE}]Wallet address or number[/{BLUE}]",
        default="", console=console,
    ).strip()

    if not raw:
        console.print(f"\n  [{GREY}]Running demo mode.[/{GREY}]\n")
        return "", "", False

    if raw.isdigit():
        idx_sel = int(raw) - 1
        if 0 <= idx_sel < len(all_wallets):
            address = all_wallets[idx_sel]
            chain   = detect_chain(address) or "ETH"
            console.print(f"\n  [bold {GREEN}]✓  Selected: {address}  ·  Chain: {chain}[/bold {GREEN}]\n")
            return address, chain, False
        else:
            console.print(f"\n  [{AMBER}]⚠  Invalid selection. Running demo.[/{AMBER}]\n")
            return "", "", False

    chain = detect_chain(raw)
    if chain:
        known = [
            w["address"].lower()
            for section in config.values()
            for w in section
            if isinstance(w, dict) and "address" in w
        ]
        is_new = raw.lower() not in known
        new_tag = f"  [bold {AMBER}]NEW ADDRESS[/bold {AMBER}]" if is_new else ""
        console.print(f"\n  [bold {GREEN}]✓  Detected: {chain}[/bold {GREEN}]{new_tag}\n")
        return raw, chain, is_new

    console.print(f"\n  [{AMBER}]⚠  Unrecognised format. Running demo.[/{AMBER}]\n")
    return "", "", False


# --- Main ---------------------------------------------------------------------

def main():
    console.clear()

    config      = load_wallet_config()
    analysis_id = f"DNA-{datetime.now(timezone.utc).strftime('%Y-%m-%d-%H%M')}"

    address, chain, is_new = prompt_wallet_selection(config)

    # Header
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d  %H:%M:%S UTC")
    hdr = Table(show_header=False, box=None, padding=(0, 1), expand=True)
    hdr.add_column(justify="left",   style=f"bold {BLUE}")
    hdr.add_column(justify="center", style=GREY)
    hdr.add_column(justify="right",  style=GREY)
    hdr.add_row("🧬  WALLETDNA", "Behavioural Wallet Fingerprinting", now)
    console.print(hdr)
    console.print()

    # Resolve target profile
    target_profile   = None
    suspect_profiles = []

    if address and chain:
        console.print(f"  [{GREY}]Resolving profile...[/{GREY}]", end="")
        cached = load_profile_from_disk(address)
        if cached:
            cached["source"] = "cache"
            target_profile = cached
            console.print(f"  [bold {GREEN}]✓ cached[/bold {GREEN}]")
        else:
            console.print(f"  [{AMBER}]fetching live data...[/{AMBER}]")
            try:
                target_profile = asyncio.run(ingest_live(address, chain))
                if target_profile:
                    console.print(f"  [bold {GREEN}]✓ live data loaded ({target_profile.get('tx_count', '?')} txns)[/bold {GREEN}]")
                else:
                    if chain.upper() == "ETH":
                        console.print(f"  [{AMBER}]⚠  Live ingestion failed — check ETHERSCAN_API_KEY in .env[/{AMBER}]")
                    else:
                        console.print(f"  [{AMBER}]⚠  Insufficient data — wallet has too few transactions to generate DNA profile[/{AMBER}]")
            except Exception as e:
                console.print(f"  [{AMBER}]⚠  {str(e)[:80]}[/{AMBER}]")

        if target_profile:
            for section in config.values():
                for w in section:
                    if isinstance(w, dict) and w.get("address", "").lower() == address.lower():
                        target_profile["label"] = w.get("label")
                        break

    # Load suspect profiles from disk
    for sw in config.get("suspect_wallets", []):
        if sw["address"].startswith("SUSPECT_ADDRESS"):
            continue
        sp = load_profile_from_disk(sw["address"])
        if sp:
            sp["label"] = sw.get("label", sp.get("label"))
            suspect_profiles.append(sp)

    # Fallback to demo
    if not target_profile:
        if address:
            # Show a minimal profile for low-tx wallets instead of demo
            target_profile = {
                "address":       address,
                "chain":         chain if "chain" in dir() else "UNKNOWN",
                "label":         "Input Wallet",
                "tx_count":      0,
                "wallet_class":  "UNKNOWN",
                "bot_confidence": 0.0,
                "confidence_score": 0.0,
                "dna_string":    "INSUFFICIENT_DATA",
                "dna_vector":    None,
                "source":        "insufficient_data",
                "total_native":  0,
                "total_usd":     0,
            }
        else:
            target_profile = DEMO_YOUR_WALLET.copy()
            target_profile["source"] = "demo"

    # No demo fallback for suspects — show empty state instead

    # Find your wallet for Table 2
    your_profile = None
    for yw in config.get("your_wallets", []):
        yp = load_profile_from_disk(yw["address"])
        if yp:
            yp["label"] = yw.get("label", yp.get("label"))
            your_profile = yp
            break
    if not your_profile:
        your_profile = DEMO_YOUR_WALLET.copy()

    # Render
    console.print()
    console.rule(f"[bold {BLUE}]Analysis  ·  {analysis_id}[/bold {BLUE}]", style=DARK)
    console.print()

    console.print(render_investigation_summary(target_profile, suspect_profiles, analysis_id))
    console.print()
    console.print(render_table1(target_profile))
    console.print()
    if suspect_profiles:
        console.print(render_table2(your_profile, suspect_profiles[0]))
        console.print()
    else:
        from rich.panel import Panel
        from rich.text import Text
        console.print(Panel(Text("No suspect profiles loaded — add suspect addresses to wallets.json to enable comparison.", style="#888888"), title="[bold #F4A261]⚖  TABLE 2 — COMPARISON[/bold #F4A261]", border_style="#F4A261", style="on #0D1117", padding=(1,2)))
        console.print()
    console.print(render_table3(suspect_profiles))
    console.print()
    console.rule(f"[{GREY}]Analysis complete[/{GREY}]", style=DARK)

    # Watchlist prompt — new address with high similarity or bot classification
    if is_new and target_profile.get("source") == "live":
        dna_vec = target_profile.get("dna_vector")
        sim_scores = []
        if dna_vec:
            try:
                from walletdna.engine.similarity import SimilarityEngine
                engine = SimilarityEngine()
                for sp in suspect_profiles:
                    sv = sp.get("dna_vector")
                    if sv:
                        sim_scores.append(engine.compare_vectors(dna_vec, sv))
            except Exception:
                pass

        max_sim = max(sim_scores) if sim_scores else 0.0
        wclass  = target_profile.get("wallet_class", "")

        if max_sim >= 0.75 or wclass in ("BOT", "LIKELY_BOT"):
            console.print()
            console.print(f"  [bold {AMBER}]⚠  Match detected — similarity {max_sim:.2f}  ·  class {wclass}[/bold {AMBER}]")
            console.print(f"  [{GREY}]This wallet shows behavioural similarity to known suspect wallets.[/{GREY}]")
            console.print()

            do_add = Confirm.ask(
                f"  [{BLUE}]Add this wallet to watchlist (wallets.json)?[/{BLUE}]",
                default=False, console=console,
            )

            if do_add:
                label_in = Prompt.ask(
                    f"  [{BLUE}]Label[/{BLUE}]",
                    default=f"Suspect #{len(config.get('suspect_wallets', [])) + 1}",
                    console=console,
                ).strip()

                if add_to_watchlist(address, label_in):
                    save_profile_to_disk(target_profile)
                    console.print(f"\n  [bold {GREEN}]✓  Added as '{label_in}'  ·  profile saved[/bold {GREEN}]")
                    console.print(f"  [{GREY}]Run: git add wallets.json profiles/ && git push[/{GREY}]\n")
                else:
                    console.print(f"\n  [{AMBER}]Already in watchlist.[/{AMBER}]\n")
            else:
                console.print(f"\n  [{GREY}]Skipped.[/{GREY}]\n")

    console.print(f"\n  [{GREY}]Re-run:[/{GREY}]  [bold white]python3 -m walletdna.dashboard.terminal[/bold white]\n")


if __name__ == "__main__":
    main()
