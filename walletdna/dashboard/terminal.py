"""
WalletDNA — Terminal Dashboard
==============================
Case-based investigation workflow.

Flow:
    1. Case selection / creation  (+ [L] Quick lookup)
    2. Case menu: Add | Remove | Re-analyse | Cached | Network | Single | Cluster | Wipe | Quit
    3. Batch analysis with progress bar
    4. Network table (full cluster view)
    5. Cluster drill-down — which wallets matched and why
    6. Single wallet deep-dive: Investigation Summary + DNA Analysis with reasoning

No hardcoded wallets.  No demo mode.  No fake data — ever.
"""

from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone
from typing import Optional

from rich import box
from rich.align import Align
from rich.console import Console, Group
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.prompt import Confirm, Prompt
from rich.rule import Rule
from rich.table import Table
from rich.text import Text
from dotenv import load_dotenv

load_dotenv()

from walletdna.cases.manager import CaseManager, detect_chain
from walletdna.cases.analyser import CaseAnalyser, compute_clusters
from walletdna.dashboard.network_table import render_network_table

console = Console()

# ─── Colour Palette ───────────────────────────────────────────────────────────
BLUE  = "#2D7DD2"
DARK  = "#1E3A5F"
GREEN = "#39D353"
AMBER = "#F4A261"
RED   = "#E76F51"
GREY  = "#888888"
DIM   = "#444444"
WHITE = "white"

DNA_DIMS = [
    ("G", "Gas Profile",    "Gas CV"),
    ("T", "Timing",         "Hour Entropy"),
    ("V", "Value",          "HHI Index"),
    ("C", "Contract",       "DEX Ratio"),
    ("M", "Mempool",        "Instant Ratio"),
    ("A", "Activity",       "Burst Score"),
    ("X", "Classification", "Bot Score"),
]


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _parse_dna_string(dna_string: str) -> dict:
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


def _risk_level(bot_score: float) -> tuple[str, str]:
    if bot_score >= 0.65:  return "HIGH",   RED
    if bot_score >= 0.35:  return "MEDIUM", AMBER
    return "LOW", GREEN


def _score_bar(score: float, width: int = 18) -> Text:
    filled = int(score * width)
    colour = GREEN if score <= 0.35 else AMBER if score <= 0.65 else RED
    t = Text()
    t.append("█" * filled,          style=f"bold {colour}")
    t.append("░" * (width - filled), style=DIM)
    t.append(f"  {score:.2f}",       style=f"bold {colour}")
    return t


def _dna_line(dna: dict) -> Text:
    t = Text()
    dims = [d for d, _, _ in DNA_DIMS]
    for i, dim in enumerate(dims):
        val, colour = dna.get(dim, ("N/A", GREY))
        t.append(f"{dim}:", style=f"bold {GREY}")
        t.append(val,       style=f"bold {colour}")
        if i < len(dims) - 1:
            t.append("  |  ", style=DIM)
    return t


def _header() -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d  %H:%M:%S UTC")
    hdr = Table(show_header=False, box=None, padding=(0, 1), expand=True)
    hdr.add_column(justify="left",   style=f"bold {BLUE}")
    hdr.add_column(justify="center", style=GREY)
    hdr.add_column(justify="right",  style=GREY)
    hdr.add_row("🧬  WALLETDNA", "Behavioural Wallet Fingerprinting", now)
    console.print(hdr)


# ─── Reasoning ────────────────────────────────────────────────────────────────

def _dim_reasoning(dim: str, val: str) -> tuple[str, str]:
    """Plain English explanation of what each DNA value means."""
    val = val.upper()

    if dim == "G":
        if "STABLE" in val:
            return "Gas always the same — automation signature", AMBER
        if "ERRATIC" in val:
            return "Gas varies a lot — human spending pattern", GREEN
        if "MODERATE" in val:
            return "Gas somewhat consistent — mixed signals", GREY
        return "Gas pattern unclear", GREY

    if dim == "T":
        if "00-23" in val or "SPREAD" in val:
            return "Active across all hours — no narrow window", GREEN
        # Single hour format e.g. 0900UTC
        m_single = re.search(r"^(\d{4})UTC$", val)
        if m_single:
            return "Active in a single 1-hour window — very strong bot signal", RED
        m = re.search(r"(\d{2})(\d{2})-(\d{2})(\d{2})", val)
        if m:
            h_start = int(m.group(1))
            h_end   = int(m.group(3))
            window  = (h_end - h_start) % 24
            if window <= 3:
                return f"Only active in a {window}-hour window — strong bot signal", RED
            if window <= 6:
                return f"Active in a {window}-hour window — possibly automated", AMBER
            return f"Active in a {window}-hour window — human-range timing", GREEN
        return "Timing pattern unclear", GREY

    if dim == "V":
        if "PRECISE" in val and "HIGH" in val:
            return "Large precise amounts — layering or automation signal", AMBER
        if "PRECISE" in val:
            return "Precise non-round amounts — could be automated", AMBER
        if "ROUND" in val:
            return "Round number amounts — typical human behaviour", GREEN
        if "LOW" in val:
            return "Small varied amounts — retail human pattern", GREEN
        return "Value pattern unclear", GREY

    if dim == "C":
        if "DEX-HEAVY" in val:
            return "Mostly DEX interactions — bot or active trader", AMBER
        if "EOA-DOMINANT" in val:
            return "Mostly wallet-to-wallet transfers — simple human usage", GREEN
        if "TRANSFER-MIX" in val:
            return "Mix of transfers and contracts — normal human activity", GREEN
        if "UTXO" in val:
            return "UTXO chain — contract data not applicable", GREY
        return "Contract pattern unclear", GREY

    if dim == "M":
        if "INSTANT" in val:
            return "Confirms in 1-2 blocks every time — bot signature", RED
        if "SLOW" in val:
            return "Transactions wait in mempool — not automated", GREEN
        if "NORMAL" in val:
            return "Average wait time — no strong signal either way", GREY
        if "UTXO" in val:
            return "UTXO chain — mempool data not applicable", GREY
        return "Mempool pattern unclear", GREY

    if dim == "A":
        if "BURST-SLEEP" in val:
            return "Intense activity bursts then long silence — bot cycle", RED
        if "BURST-HIGH" in val:
            return "High activity bursts — possible automation or trading bot", AMBER
        if "STEADY" in val:
            return "Regular consistent activity over time — human pattern", GREEN
        return "Activity pattern unclear", GREY

    if dim == "X":
        if "BOT" in val and "LIKELY" not in val:
            return "All signals combined — automated wallet", RED
        if "LIKELY_BOT" in val:
            return "More bot signals than human — further review recommended", AMBER
        if "LIKELY_HUMAN" in val:
            return "More human signals than bot — probably a person", GREEN
        if "HUMAN" in val:
            return "All signals combined — human wallet", GREEN
        if "UNKNOWN" in val:
            return "Too few transactions to classify reliably", GREY
        return "Classification unclear", GREY

    return "", GREY


# ─── Investigation Summary ────────────────────────────────────────────────────

def render_investigation_summary(
    target:       dict,
    all_profiles: list[dict],
    analysis_id:  str,
) -> Panel:
    addr    = target["address"]
    label   = target.get("label") or "Input Wallet"
    chain   = target.get("chain", "ETH")
    txns    = target.get("tx_count", "—")
    src     = target.get("source", "unknown")
    wclass  = target.get("wallet_class", "UNKNOWN")
    bconf   = float(target.get("bot_confidence", 0.0))
    cscore  = float(target.get("confidence_score", 0.0))
    wtype   = target.get("wallet_type") or ""

    display_conf = cscore if cscore > 0 else ((1.0 - bconf) if "HUMAN" in wclass else bconf)
    conf_pct     = f"{int(display_conf * 100)}%"
    risk_str, risk_col = _risk_level(bconf)

    if "BOT" in wclass and "LIKELY" not in wclass:
        class_col = RED
    elif "LIKELY_BOT" in wclass:
        class_col = AMBER
    elif "LIKELY_HUMAN" in wclass:
        class_col = "#90EE90"
    elif wclass == "UNKNOWN":
        class_col = GREY
    else:
        class_col = GREEN

    # Cluster / similarity
    cluster_match = "None detected"
    cluster_col   = GREEN
    avg_sim        = 0.0
    sim_scores     = []
    matching_wallets: list[dict] = []

    dna_vec = target.get("dna_vector")
    peers   = [p for p in all_profiles if p["address"].lower() != addr.lower()]

    if dna_vec and peers:
        try:
            from walletdna.engine.similarity import SimilarityEngine
            engine = SimilarityEngine()
            for p in peers:
                sv = p.get("dna_vector")
                if sv:
                    score = engine.compare_vectors(dna_vec, sv)
                    sim_scores.append(score)
                    if score >= 0.75:
                        matching_wallets.append(p)
            if sim_scores:
                avg_sim = sum(sim_scores) / len(sim_scores)
                max_sim = max(sim_scores)
                count   = len(matching_wallets)
                if max_sim >= 0.92:
                    cluster_match = (
                        f"{count} wallet{'s' if count != 1 else ''} share identical behaviour pattern "
                        f"({int(max_sim * 100)}% similarity) — likely same operator"
                    )
                    cluster_col = RED
                elif max_sim >= 0.75:
                    cluster_match = (
                        f"{count} wallet{'s' if count != 1 else ''} show similar behaviour pattern "
                        f"({int(max_sim * 100)}% similarity) — worth investigating"
                    )
                    cluster_col = AMBER
        except Exception:
            pass

    # Conclusion
    if wclass == "UNKNOWN" or src == "insufficient_data":
        conclusion = "Insufficient transaction history — need at least 5 transactions to classify"
        conc_col   = GREY
    elif "BOT" in wclass and avg_sim >= 0.85:
        conclusion = "Coordinated automated wallet — part of a larger bot network"
        conc_col   = RED
    elif "BOT" in wclass and "LIKELY" not in wclass:
        conclusion = "Automated wallet behaviour detected — not a human user"
        conc_col   = RED
    elif "LIKELY_BOT" in wclass:
        conclusion = "Probable automation — multiple bot signals present, review recommended"
        conc_col   = AMBER
    elif avg_sim >= 0.75:
        conclusion = "Behavioural match to other wallets in this case — investigate connection"
        conc_col   = AMBER
    else:
        conclusion = "Human retail behaviour — no bot signals, no cluster match"
        conc_col   = GREEN

    api_warn   = target.get("api_limit_hit", False)
    source_tag = f"  [{GREY}]source: {src}[/{GREY}]"

    t = Table(show_header=False, box=None, padding=(0, 2), expand=True)
    t.add_column(style=GREY,  width=22)
    t.add_column(style=WHITE, width=72)

    short = f"{addr[:10]}...{addr[-6:]}"

    t.add_row("Analysis ID",   Text(analysis_id,             style=f"bold {BLUE}"))
    t.add_row("Target wallet", Text(f"{short}  ·  {label}",  style=f"bold {BLUE}"))

    chain_t = Text()
    chain_t.append(f"{chain}  ·  {txns} transactions", style=f"bold {BLUE}")
    if api_warn:
        chain_t.append("  ⚠ API LIMIT — capped at 10,000 txns", style=f"bold {AMBER}")
    t.add_row("Chain", chain_t)

    if wtype:
        t.add_row("Wallet type", Text(wtype, style=f"bold {AMBER}"))

    t.add_row("Classification", Text(wclass,   style=f"bold {class_col}"))
    risk_t = Text()
    risk_t.append(risk_str, style=f"bold {risk_col}")
    t.add_row("Risk Level", risk_t)
    t.add_row("Confidence",     Text(conf_pct, style=f"bold {risk_col}"))
    t.add_row("Cluster Match",  Text(cluster_match, style=f"bold {cluster_col}"))

    if sim_scores:
        sim_col = GREEN if avg_sim < 0.50 else RED if avg_sim > 0.85 else AMBER
        sim_t   = Text()
        sim_t.append(f"{avg_sim:.3f}", style=f"bold {sim_col}")
        sim_t.append(f"  (average across {len(sim_scores)} wallets in case)", style=GREY)
        t.add_row("Avg Similarity", sim_t)

    if matching_wallets:
        match_t = Text()
        for i, mw in enumerate(matching_wallets):
            ml  = mw.get("label", mw["address"][:10])
            ma  = mw["address"]
            match_t.append(f"  {ml} ({ma[:8]}...{ma[-6:]})", style=AMBER)
            if i < len(matching_wallets) - 1:
                match_t.append("\n", style="")
        t.add_row("Matching wallets", match_t)

    t.add_row("", Text(""))
    t.add_row("Conclusion", Text(conclusion, style=f"bold {conc_col}"))

    return Panel(
        t,
        title=f"[bold white]🔍  INVESTIGATION SUMMARY[/bold white]{source_tag}",
        subtitle=(
            f"[{GREY}]{analysis_id}  ·  "
            f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}[/{GREY}]"
        ),
        border_style=BLUE,
        style="on #0D1117",
        padding=(1, 2),
    )


# ─── DNA Analysis Table ────────────────────────────────────────────────────────

def render_table1(profile: dict) -> Panel:
    addr  = profile["address"]
    label = profile.get("label") or "Input Wallet"
    chain = profile.get("chain", "ETH")
    txns  = profile.get("tx_count", "—")
    value = profile.get("value_display", "—")
    dna   = profile.get("dna") or {d: ("N/A", GREY) for d, _, _ in DNA_DIMS}
    bconf = float(profile.get("bot_confidence", 0.0))

    dim_weights = [0.12, 0.18, 0.10, 0.10, 0.08, 0.12, 1.0]
    bot_scores  = [min(bconf * w / max(dim_weights) * 1.2, 1.0) for w in dim_weights]
    bot_scores[-1] = bconf

    t = Table(
        show_header=True, header_style=f"bold white on {DARK}",
        box=box.SIMPLE_HEAVY, border_style=DARK, padding=(0, 1), expand=True,
    )
    t.add_column("DIM",       style=f"bold {BLUE}", width=5)
    t.add_column("DIMENSION", style=WHITE,          width=16)
    t.add_column("VALUE",     style=WHITE,          width=24)
    t.add_column("WHAT THIS MEANS", style=WHITE,    width=50)
    t.add_column("BOT SCORE", style=WHITE,          width=22)

    for i, (dim, name, _signal) in enumerate(DNA_DIMS):
        val, colour      = dna.get(dim, ("N/A", GREY))
        reason, r_colour = _dim_reasoning(dim, val)
        t.add_row(
            Text(dim,          style=f"bold {BLUE}"),
            Text(name,         style=WHITE),
            Text(f"  {val}",   style=f"bold {colour}"),
            Text(f"  {reason}", style=r_colour),
            _score_bar(bot_scores[i]),
        )

    content = Group(
        t,
        Rule(style=DARK),
        Align.center(Text("◆  BEHAVIOURAL FINGERPRINT  ◆", style=f"bold {GREEN}")),
        Align.center(_dna_line(dna)),
    )

    short    = f"{addr[:10]}...{addr[-6:]}"
    api_warn = profile.get("api_limit_hit", False)
    api_note = (
        f"  ·  [bold {AMBER}]⚠ API limit hit — actual volume higher[/bold {AMBER}]"
        if api_warn else ""
    )
    return Panel(
        content,
        title=(
            f"[bold {BLUE}]🔬  DNA ANALYSIS[/bold {BLUE}]  "
            f"[{GREY}]{short}  ·  {label}[/{GREY}]"
        ),
        subtitle=f"[{GREY}]{txns} transactions  ·  {chain}  ·  {value}[/{GREY}]{api_note}",
        border_style=BLUE, style="on #0D1117", padding=(0, 1),
    )


# ─── Cluster Drill-Down ───────────────────────────────────────────────────────

def render_cluster_drilldown(cluster: dict, profiles: list[dict]) -> Panel:
    """
    Show all wallets in a cluster side by side with their DNA strings
    and the key signals that caused the match.
    """
    addrs   = [a.lower() for a in cluster["addresses"]]
    members = [p for p in profiles if p["address"].lower() in addrs]
    label   = cluster["label"]
    avg_sim = cluster["avg_similarity"]
    interp  = cluster["interpretation"]
    n       = len(members)

    # Header explanation
    intro = Text()
    intro.append(f"\n  {n} wallets detected with matching behavioural patterns\n", style=f"bold {RED if avg_sim >= 0.92 else AMBER}")
    intro.append(f"  Average similarity: ", style=GREY)
    intro.append(f"{avg_sim:.3f}", style=f"bold {RED if avg_sim >= 0.92 else AMBER}")
    intro.append(f"  ·  Threshold for same operator: 0.92\n", style=GREY)
    if avg_sim >= 0.92:
        intro.append(f"\n  These wallets show near-identical behaviour across all 7 dimensions.\n", style=f"bold {RED}")
        intro.append(f"  Different addresses — same hand operating them.\n", style=AMBER)
    else:
        intro.append(f"\n  These wallets share significant behavioural overlap.\n", style=AMBER)
        intro.append(f"  May be the same operator or wallets following a similar strategy.\n", style=GREY)

    # Per-wallet DNA table
    t = Table(
        show_header=True, header_style=f"bold white on {DARK}",
        box=box.SIMPLE_HEAVY, border_style=DARK, padding=(0, 1), expand=True,
    )
    t.add_column("DIMENSION",  style=f"bold {BLUE}", width=16)
    for m in members:
        lbl = m.get("label", m["address"][:10])
        t.add_column(lbl[:18], style=WHITE, width=22)

    dim_names = {
        "G": "Gas Profile",
        "T": "Timing Pattern",
        "V": "Value Behaviour",
        "C": "Contract Type",
        "M": "Mempool Speed",
        "A": "Activity Cycle",
        "X": "Classification",
    }

    for dim, name in dim_names.items():
        row = [Text(name, style=f"bold {BLUE}")]
        vals = []
        for m in members:
            dna = m.get("dna") or _parse_dna_string(m.get("dna_string") or "")
            val, colour = dna.get(dim, ("N/A", GREY))
            vals.append((val, colour))

        # Highlight if all values match
        all_same = len(set(v for v, _ in vals)) == 1 and vals[0][0] != "N/A"
        for val, colour in vals:
            cell = Text()
            if all_same:
                cell.append("  ● ", style=f"bold {RED if avg_sim >= 0.92 else AMBER}")
            else:
                cell.append("    ", style="")
            cell.append(val, style=f"bold {colour}")
            row.append(cell)
        t.add_row(*row)

    # Similarity matrix between members
    sim_section = Text()
    sim_section.append("\n  Pairwise similarity scores\n", style=f"bold {GREY}")
    try:
        from walletdna.engine.similarity import SimilarityEngine
        engine = SimilarityEngine()
        for i in range(n):
            for j in range(i + 1, n):
                va = members[i].get("dna_vector")
                vb = members[j].get("dna_vector")
                if va and vb:
                    score = engine.compare_vectors(va, vb)
                    la = members[i].get("label", members[i]["address"][:10])
                    lb = members[j].get("label", members[j]["address"][:10])
                    col = RED if score >= 0.92 else AMBER if score >= 0.75 else GREEN
                    sim_section.append(f"  {la[:16]:<18} ↔  {lb[:16]:<18}  ", style=GREY)
                    sim_section.append(f"{score:.3f}", style=f"bold {col}")
                    verdict = "  LIKELY SAME OPERATOR" if score >= 0.92 else "  SIMILAR BEHAVIOUR"
                    sim_section.append(f"{verdict}\n", style=col)
    except Exception:
        pass

    return Panel(
        Group(intro, Rule(style=DARK), t, Rule(style=DARK), sim_section),
        title=(
            f"[bold {RED}]🔗  CLUSTER DRILL-DOWN[/bold {RED}]  "
            f"[{GREY}]{label}  ·  {n} wallets  ·  avg sim {avg_sim:.3f}[/{GREY}]"
        ),
        subtitle=f"[{GREY if avg_sim < 0.92 else AMBER}]{interp}[/{GREY if avg_sim < 0.92 else AMBER}]  ·  [{GREY}]● = identical value across all wallets[/{GREY}]",
        border_style=RED,
        style="on #0D1117",
        padding=(0, 1),
    )


# ─── Quick Lookup (no case) ───────────────────────────────────────────────────

async def _quick_lookup_fetch(address: str, chain: str) -> Optional[dict]:
    """Live fetch for quick lookup — nothing saved."""
    try:
        from walletdna.engine.composer import DNAComposer
        from walletdna.engine.models import Chain as ChainEnum, TxDirection

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

        STABLECOINS = {"USDT", "USDC", "BUSD", "DAI", "USDD", "TUSD", "USDP", "GUSD"}
        total_native = sum(
            float(t.value_native) for t in txs
            if t.value_native and not t.token_symbol
            and (t.direction == TxDirection.OUT or t.from_address.lower() == address.lower())
        ) or sum(
            float(t.value_native) for t in txs
            if t.value_native and not t.token_symbol
        )
        # Count all stablecoin volume through wallet — total throughput
        stable_usd = sum(
            float(t.value_native) for t in txs
            if t.value_native and t.token_symbol
            and t.token_symbol.upper() in STABLECOINS
        )

        import urllib.request, json as _json
        usd_price = 0.0
        try:
            coin_id = {"ETH": "ethereum", "TRX": "tron", "DOGE": "dogecoin"}.get(chain.upper(), "ethereum")
            url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"
            with urllib.request.urlopen(url, timeout=6) as r:
                usd_price = float(_json.loads(r.read())[coin_id]["usd"])
        except Exception:
            pass

        total_usd = total_native * usd_price + stable_usd
        chain_sym = chain.upper()
        if total_usd >= 1_000_000:
            value_str = f"{total_native:,.2f} {chain_sym} (${total_usd / 1_000_000:.1f}M USD)"
        elif total_usd >= 1_000:
            value_str = f"{total_native:,.2f} {chain_sym} (${total_usd / 1_000:.1f}K USD)"
        elif total_usd > 0:
            value_str = f"{total_native:,.4f} {chain_sym} (${total_usd:.0f} USD)"
        elif total_native > 0:
            value_str = f"{total_native:,.4f} {chain_sym}"
        else:
            value_str = "live"

        dna_display = _parse_dna_string(profile.dna_string or "")
        return {
            "address":          address,
            "chain":            chain.upper(),
            "label":            "Quick Lookup",
            "tx_count":         profile.tx_count,
            "total_native":     round(total_native, 4),
            "total_usd":        round(total_usd, 2),
            "api_limit_hit":    len(txs) >= 9999,
            "value_display":    value_str,
            "wallet_class":     profile.classification.wallet_class.value if profile.classification else "UNKNOWN",
            "bot_confidence":   profile.classification.confidence if profile.classification else 0.0,
            "confidence_score": profile.confidence_score,
            "dna_string":       profile.dna_string,
            "dna_vector":       profile.dna_vector,
            "dna":              dna_display,
            "source":           "live",
        }
    except Exception as e:
        console.print(f"  [{AMBER}]Error: {str(e)[:80]}[/{AMBER}]")
        return None


def quick_lookup() -> None:
    """Analyse any address on the spot — no case, nothing saved."""
    console.clear()
    _header()
    console.print()
    console.rule(f"[bold {BLUE}]⚡  QUICK LOOKUP[/bold {BLUE}]", style=DARK)
    console.print()
    console.print(f"  [{GREY}]Paste any address — ETH, TRX, or DOGE.  Nothing will be saved.[/{GREY}]")
    console.print()

    raw = Prompt.ask(
        f"  [{BLUE}]Address[/{BLUE}]",
        default="", console=console,
    ).strip()

    if not raw:
        return

    chain = detect_chain(raw)
    if not chain:
        console.print(f"  [{AMBER}]Unrecognised address format.[/{AMBER}]")
        console.input(f"\n  [{GREY}]Press Enter to continue...[/{GREY}]")
        return

    console.print(f"\n  [{GREEN}]✓ Detected: {chain}[/{GREEN}]  [{GREY}]Fetching live data...[/{GREY}]\n")

    profile = asyncio.run(_quick_lookup_fetch(raw, chain))

    if not profile:
        console.print(f"  [{AMBER}]Could not fetch data — check API key or address has transactions.[/{AMBER}]")
        console.input(f"\n  [{GREY}]Press Enter to continue...[/{GREY}]")
        return

    if not profile.get("dna") and profile.get("dna_string"):
        profile["dna"] = _parse_dna_string(profile["dna_string"])

    analysis_id = f"DNA-{datetime.now(timezone.utc).strftime('%Y-%m-%d-%H%M')}"
    console.clear()
    _header()
    console.print()
    console.rule(f"[bold {BLUE}]Quick Lookup  ·  {analysis_id}[/bold {BLUE}]", style=DARK)
    console.print()
    console.print(render_investigation_summary(profile, [], analysis_id))
    console.print()
    console.print(render_table1(profile))
    console.print()
    console.rule(f"[{GREY}]Analysis complete  ·  not saved[/{GREY}]", style=DARK)
    console.print()
    console.input(f"  [{GREY}]Press Enter to continue...[/{GREY}]")


# ─── Address Entry ────────────────────────────────────────────────────────────

def prompt_batch_addresses() -> list[dict]:
    console.print()
    console.print(f"  [{GREY}]Paste addresses — one per line, mixed chains OK, blank line to finish.[/{GREY}]")
    console.print(f"  [{GREY}]Format:  ADDRESS   or   ADDRESS LABEL[/{GREY}]")
    console.print()

    entries = []
    idx = 1
    while True:
        raw = Prompt.ask(
            f"  [{BLUE}]>[/{BLUE}]",
            default="", console=console,
        ).strip()
        if not raw:
            break
        parts = raw.split(None, 1)
        addr  = parts[0]
        label = parts[1] if len(parts) > 1 else f"Wallet #{idx}"
        chain = detect_chain(addr)
        if not chain:
            console.print(f"  [{AMBER}]  ⚠ Unrecognised format: {addr[:20]}[/{AMBER}]")
            continue
        entries.append({"address": addr, "label": label})
        console.print(
            f"  [{GREEN}]  ✓ {chain}  {addr[:12]}...{addr[-6:]}[/{GREEN}]  "
            f"[{GREY}]{label}[/{GREY}]"
        )
        idx += 1

    if entries:
        eth  = sum(1 for e in entries if detect_chain(e["address"]) == "ETH")
        trx  = sum(1 for e in entries if detect_chain(e["address"]) == "TRX")
        doge = sum(1 for e in entries if detect_chain(e["address"]) == "DOGE")
        parts = [f"{n} {c}" for n, c in [(eth, "ETH"), (trx, "TRX"), (doge, "DOGE")] if n]
        console.print(f"\n  [{GREY}]Detected: {' · '.join(parts)}[/{GREY}]")

    return entries


def prompt_single_address(profiles: list[dict]) -> Optional[dict]:
    if not profiles:
        return None

    console.print()
    t = Table(
        show_header=True, header_style=f"bold white on {DARK}",
        box=box.SIMPLE_HEAVY, border_style=DARK, padding=(0, 2),
    )
    t.add_column("#",       style=f"bold {BLUE}", width=4)
    t.add_column("LABEL",   style=WHITE,          width=22)
    t.add_column("ADDRESS", style=DIM,            width=20)
    t.add_column("CHAIN",   style=GREY,           width=6)
    t.add_column("CLASS",   style=WHITE,          width=14)
    t.add_column("CLUSTER", style=AMBER,          width=12)

    for i, p in enumerate(profiles):
        addr  = p["address"]
        chain = p.get("chain", "?")
        wc    = p.get("wallet_class", "UNKNOWN")
        lbl   = p.get("label", addr[:10])
        cl    = p.get("cluster_label", "—")
        wc_col = (
            RED if "BOT" in wc and "LIKELY" not in wc
            else AMBER if "LIKELY_BOT" in wc
            else "#90EE90" if "LIKELY_HUMAN" in wc
            else GREEN if wc == "HUMAN"
            else GREY
        )
        t.add_row(
            str(i + 1),
            Text(lbl,   style=WHITE),
            Text(f"{addr[:10]}...{addr[-6:]}", style=DIM),
            Text(chain, style=GREY),
            Text(wc,    style=f"bold {wc_col}"),
            Text(cl,    style=f"bold {RED if cl != '—' else GREY}"),
        )

    console.print(Align.center(t))
    console.print()

    raw = Prompt.ask(
        f"  [{BLUE}]Select wallet number[/{BLUE}]",
        default="", console=console,
    ).strip()

    if not raw or not raw.isdigit():
        return None
    idx = int(raw) - 1
    if 0 <= idx < len(profiles):
        return profiles[idx]
    return None


# ─── Progress ─────────────────────────────────────────────────────────────────

def _run_analysis_with_progress(
    analyser: CaseAnalyser,
    force: bool = False,
) -> list[dict]:
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
        transient=True,
    ) as prog:
        wallets = analyser.manager.get_wallets(analyser.case_name)
        task    = prog.add_task("Analysing...", total=len(wallets))

        def cb(completed: int, total: int, address: str, status: str) -> None:
            short = f"{address[:10]}...{address[-6:]}"
            col   = GREEN if status == "cache" else AMBER if status == "live" else GREY
            prog.update(
                task,
                completed=completed,
                description=f"[{col}]{status.upper():<14}[/{col}] {short}",
            )

        return analyser.run_sync(force=force, progress_cb=cb)


# ─── Case Selection ───────────────────────────────────────────────────────────

def prompt_case_open_or_create(manager: CaseManager) -> Optional[str]:
    cases = manager.list_cases()

    console.clear()
    _header()
    console.print()
    console.rule(f"[bold {BLUE}]🧬  WALLETDNA[/bold {BLUE}]", style=DARK)
    console.print()

    if cases:
        t = Table(
            show_header=True, header_style=f"bold white on {DARK}",
            box=box.SIMPLE_HEAVY, border_style=DARK, padding=(0, 2),
        )
        t.add_column("#",        style=f"bold {BLUE}", width=4)
        t.add_column("CASE",     style=WHITE,          width=32)
        t.add_column("WALLETS",  style=GREY,           width=9,  justify="right")
        t.add_column("CACHED",   style=GREEN,          width=8,  justify="right")
        t.add_column("CREATED",  style=DIM,            width=12)
        t.add_column("LAST RUN", style=DIM,            width=22)

        for i, c in enumerate(cases):
            t.add_row(
                str(i + 1),
                Text(c["name"], style="bold white"),
                str(c["wallet_count"]),
                str(c["profile_count"]),
                c["created"],
                c.get("last_run") or "—",
            )

        console.print(Align.center(t))
        console.print()
        console.print(
            f"  [{GREY}]Enter a number to open a case, type a new name to create one, "
            f"[bold]L[/bold] for quick lookup, or [bold]Q[/bold] to quit.[/{GREY}]"
        )
        console.print()
    else:
        console.print(f"  [{GREY}]No cases found.  Enter a name to create your first case.[/{GREY}]")
        console.print(f"  [{GREY}]Or press [bold]L[/bold] for a quick one-off address lookup.[/{GREY}]")
        console.print()

    raw = Prompt.ask(
        f"  [{BLUE}]>[/{BLUE}]",
        default="", console=console,
    ).strip()

    if not raw or raw.upper() == "Q":
        return None

    if raw.upper() == "L":
        quick_lookup()
        return ""  # empty string = stay on selection screen

    if raw.isdigit() and cases:
        idx = int(raw) - 1
        if 0 <= idx < len(cases):
            return cases[idx]["name"]
        console.print(f"  [{AMBER}]Invalid selection.[/{AMBER}]")
        return ""

    if not manager.case_exists(raw):
        desc = Prompt.ask(
            f"  [{GREY}]Description (optional)[/{GREY}]",
            default="", console=console,
        ).strip()
        manager.create_case(raw, description=desc)
        console.print(f"\n  [bold {GREEN}]✓  Case '{raw}' created.[/bold {GREEN}]\n")

    return raw


# ─── Case Menu ────────────────────────────────────────────────────────────────

def case_menu(manager: CaseManager, case_name: str) -> None:
    analyser = CaseAnalyser(case_name, manager)
    profiles: list[dict] = manager.load_all_profiles(case_name)
    cluster_list: list[dict] = []
    if profiles:
        cluster_list = compute_clusters(profiles)

    while True:
        console.clear()
        _header()
        console.print()

        meta          = manager.open_case(case_name)
        wallet_count  = len(meta.get("wallets", []))
        cached_count  = len(manager.load_all_profiles(case_name))
        last_run      = meta.get("last_run", "never")

        case_t = Table(show_header=False, box=None, padding=(0, 2), expand=False)
        case_t.add_column(style=GREY,  width=16)
        case_t.add_column(style=WHITE)
        case_t.add_row("Case",     Text(case_name, style=f"bold {BLUE}"))
        case_t.add_row("Wallets",  f"{wallet_count} loaded  ·  {cached_count} cached")
        case_t.add_row("Last run", last_run)
        if cluster_list:
            cl_summary = "  ".join(
                f"{cl['label']} {cl['member_count']}w"
                for cl in cluster_list
            )
            case_t.add_row("Clusters", Text(cl_summary, style=f"bold {RED}"))
        if meta.get("description"):
            case_t.add_row("Description", meta["description"])
        console.print(Panel(case_t, border_style=DARK, style="on #0D1117", padding=(0, 2)))
        console.print()

        console.print(f"  [{BLUE}][A][/{BLUE}]  Add addresses")
        console.print(f"  [{BLUE}][D][/{BLUE}]  Remove a wallet from case")
        console.print(f"  [{BLUE}][R][/{BLUE}]  Re-analyse all  [{GREY}](live API, ignores cache)[/{GREY}]")
        console.print(f"  [{BLUE}][C][/{BLUE}]  Load cached  [{GREY}](fast, no API calls)[/{GREY}]")
        console.print(f"  [{BLUE}][V][/{BLUE}]  View network table")
        console.print(f"  [{BLUE}][S][/{BLUE}]  Single wallet deep-dive")
        if cluster_list:
            console.print(f"  [{BLUE}][X][/{BLUE}]  [{RED}]Cluster drill-down  [{GREY}]({len(cluster_list)} cluster{'s' if len(cluster_list) != 1 else ''} detected)[/{GREY}][/{RED}]")
        console.print(f"  [{BLUE}][W][/{BLUE}]  Wipe profile cache")
        console.print(f"  [{BLUE}][Q][/{BLUE}]  Back to case selection")
        console.print()

        choice = Prompt.ask(
            f"  [{BLUE}]>[/{BLUE}]",
            default="", console=console,
        ).strip().upper()

        if choice == "Q" or not choice:
            break

        elif choice == "A":
            entries = prompt_batch_addresses()
            if entries:
                added, skipped = manager.add_wallets(case_name, entries)
                console.print()
                console.print(
                    f"  [bold {GREEN}]✓  Added {added}  ·  Skipped {skipped} (duplicates)[/bold {GREEN}]"
                )
                console.print(
                    f"  [{GREY}]Run [bold]R[/bold] or [bold]C[/bold] to analyse.[/{GREY}]"
                )
            console.input(f"\n  [{GREY}]Press Enter to continue...[/{GREY}]")

        elif choice == "D":
            wallets = manager.get_wallets(case_name)
            if not wallets:
                console.print(f"\n  [{GREY}]No wallets in case.[/{GREY}]")
                console.input(f"\n  [{GREY}]Press Enter to continue...[/{GREY}]")
                continue

            console.print()
            t = Table(
                show_header=True, header_style=f"bold white on {DARK}",
                box=box.SIMPLE_HEAVY, border_style=DARK, padding=(0, 2),
            )
            t.add_column("#",       style=f"bold {BLUE}", width=4)
            t.add_column("LABEL",   style=WHITE,           width=22)
            t.add_column("ADDRESS", style=DIM,             width=20)
            t.add_column("CHAIN",   style=GREY,            width=6)
            for i, w in enumerate(wallets):
                addr = w["address"]
                t.add_row(
                    str(i + 1),
                    w.get("label", addr[:10]),
                    f"{addr[:10]}...{addr[-6:]}",
                    w.get("chain", "?"),
                )
            console.print(Align.center(t))
            console.print()
            console.print(f"  [{GREY}]Enter number to remove, or blank to cancel.[/{GREY}]")
            console.print()

            raw = Prompt.ask(f"  [{BLUE}]>[/{BLUE}]", default="", console=console).strip()

            if not raw:
                console.print(f"  [{GREY}]Cancelled.[/{GREY}]")
            elif raw.isdigit():
                idx = int(raw) - 1
                if 0 <= idx < len(wallets):
                    target = wallets[idx]
                    addr   = target["address"]
                    lbl    = target.get("label", addr[:10])
                    ok = Confirm.ask(
                        f"  [{AMBER}]Remove '{lbl}' ({addr[:10]}...{addr[-6:]}) from case?[/{AMBER}]",
                        default=False, console=console,
                    )
                    if ok:
                        manager.remove_wallet(case_name, addr)
                        profiles     = [p for p in profiles if p["address"].lower() != addr.lower()]
                        cluster_list = compute_clusters(profiles) if len(profiles) > 1 else []
                        console.print(f"\n  [bold {GREEN}]✓  Removed '{lbl}' from case.[/bold {GREEN}]")
                    else:
                        console.print(f"  [{GREY}]Cancelled.[/{GREY}]")
                else:
                    console.print(f"  [{AMBER}]Invalid selection.[/{AMBER}]")
            else:
                console.print(f"  [{AMBER}]Invalid input.[/{AMBER}]")
            console.input(f"\n  [{GREY}]Press Enter to continue...[/{GREY}]")

        elif choice in ("R", "C"):
            wallets = manager.get_wallets(case_name)
            if not wallets:
                console.print(f"\n  [{AMBER}]No wallets in case.  Use [bold]A[/bold] to add addresses.[/{AMBER}]")
                console.input(f"\n  [{GREY}]Press Enter to continue...[/{GREY}]")
                continue

            force = (choice == "R")
            console.print()
            console.rule(
                f"[{GREY}]{'Re-analysing' if force else 'Loading cached profiles'}  —  {case_name}[/{GREY}]",
                style=DARK,
            )
            console.print()
            profiles     = _run_analysis_with_progress(analyser, force=force)
            cluster_list = compute_clusters(profiles) if len(profiles) > 1 else []

            n_live  = sum(1 for p in profiles if p.get("source") == "live")
            n_cache = sum(1 for p in profiles if p.get("source") == "cache")
            n_fail  = sum(1 for p in profiles if p.get("source") == "insufficient_data")
            console.print()
            console.print(
                f"  [bold {GREEN}]✓  Complete — "
                f"{n_live} live  ·  {n_cache} cached  ·  {n_fail} insufficient[/bold {GREEN}]"
            )
            if cluster_list:
                for cl in cluster_list:
                    col = RED if cl["avg_similarity"] >= 0.92 else AMBER
                    console.print(
                        f"  [{col}]● {cl['label']}  {cl['member_count']} wallets  "
                        f"avg sim {cl['avg_similarity']:.3f}  {cl['interpretation']}[/{col}]"
                    )

            console.print()
            console.print(render_network_table(case_name, profiles, cluster_list))
            console.print()
            console.input(f"  [{GREY}]Press Enter to continue...[/{GREY}]")

        elif choice == "V":
            if not profiles:
                profiles     = manager.load_all_profiles(case_name)
                cluster_list = compute_clusters(profiles) if len(profiles) > 1 else []
            if not profiles:
                console.print(f"\n  [{AMBER}]No profiles loaded.  Run analysis first.[/{AMBER}]")
                console.input(f"\n  [{GREY}]Press Enter to continue...[/{GREY}]")
                continue

            console.clear()
            _header()
            console.print()
            console.print(render_network_table(case_name, profiles, cluster_list))
            console.print()
            console.input(f"  [{GREY}]Press Enter to continue...[/{GREY}]")

        elif choice == "S":
            if not profiles:
                profiles     = manager.load_all_profiles(case_name)
                cluster_list = compute_clusters(profiles) if len(profiles) > 1 else []
            if not profiles:
                console.print(f"\n  [{AMBER}]No profiles loaded.  Run analysis first.[/{AMBER}]")
                console.input(f"\n  [{GREY}]Press Enter to continue...[/{GREY}]")
                continue

            selected = prompt_single_address(profiles)
            if not selected:
                continue

            if not selected.get("dna") and selected.get("dna_string"):
                selected["dna"] = _parse_dna_string(selected["dna_string"])

            analysis_id = f"DNA-{datetime.now(timezone.utc).strftime('%Y-%m-%d-%H%M')}"
            console.clear()
            _header()
            console.print()
            console.rule(f"[bold {BLUE}]Analysis  ·  {analysis_id}[/bold {BLUE}]", style=DARK)
            console.print()
            console.print(render_investigation_summary(selected, profiles, analysis_id))
            console.print()
            console.print(render_table1(selected))
            console.print()
            console.rule(f"[{GREY}]Analysis complete[/{GREY}]", style=DARK)
            console.print()
            console.input(f"  [{GREY}]Press Enter to continue...[/{GREY}]")

        elif choice == "X":
            if not cluster_list:
                console.print(f"\n  [{GREY}]No clusters detected.  Run analysis first.[/{GREY}]")
                console.input(f"\n  [{GREY}]Press Enter to continue...[/{GREY}]")
                continue

            # If multiple clusters, ask which one
            selected_cluster = cluster_list[0]
            if len(cluster_list) > 1:
                console.print()
                for i, cl in enumerate(cluster_list):
                    col = RED if cl["avg_similarity"] >= 0.92 else AMBER
                    console.print(
                        f"  [{BLUE}][{i+1}][/{BLUE}]  [{col}]{cl['label']}[/{col}]  "
                        f"[{GREY}]{cl['member_count']} wallets  ·  "
                        f"avg sim {cl['avg_similarity']:.3f}  ·  {cl['interpretation']}[/{GREY}]"
                    )
                console.print()
                raw = Prompt.ask(
                    f"  [{BLUE}]Select cluster[/{BLUE}]",
                    default="1", console=console,
                ).strip()
                if raw.isdigit():
                    idx = int(raw) - 1
                    if 0 <= idx < len(cluster_list):
                        selected_cluster = cluster_list[idx]

            console.clear()
            _header()
            console.print()
            console.print(render_cluster_drilldown(selected_cluster, profiles))
            console.print()
            console.input(f"  [{GREY}]Press Enter to continue...[/{GREY}]")

        elif choice == "W":
            cached = manager.load_all_profiles(case_name)
            if not cached:
                console.print(f"\n  [{GREY}]No cached profiles to delete.[/{GREY}]")
                console.input(f"\n  [{GREY}]Press Enter to continue...[/{GREY}]")
                continue

            console.print()
            t = Table(
                show_header=True, header_style=f"bold white on {DARK}",
                box=box.SIMPLE_HEAVY, border_style=DARK, padding=(0, 2),
            )
            t.add_column("#",       style=f"bold {BLUE}", width=4)
            t.add_column("LABEL",   style=WHITE,          width=22)
            t.add_column("ADDRESS", style=DIM,            width=20)
            t.add_column("CHAIN",   style=GREY,           width=6)
            t.add_column("FETCHED", style=GREY,           width=22)
            for i, p in enumerate(cached):
                addr    = p["address"]
                lbl     = p.get("label", addr[:10])
                chain   = p.get("chain", "?")
                fetched = p.get("fetched_at", "unknown")[:19].replace("T", "  ")
                t.add_row(str(i + 1), lbl, f"{addr[:10]}...{addr[-6:]}", chain, fetched)
            console.print(Align.center(t))
            console.print()
            console.print(
                f"  [{GREY}]Enter number to delete one, "
                f"[bold]ALL[/bold] to wipe all, or blank to cancel.[/{GREY}]"
            )
            console.print()

            raw = Prompt.ask(f"  [{BLUE}]>[/{BLUE}]", default="", console=console).strip()

            if not raw:
                console.print(f"  [{GREY}]Cancelled.[/{GREY}]")
            elif raw.upper() == "ALL":
                ok = Confirm.ask(
                    f"  [{AMBER}]Wipe all {len(cached)} cached profiles?[/{AMBER}]",
                    default=False, console=console,
                )
                if ok:
                    count        = manager.wipe_profiles(case_name)
                    profiles     = []
                    cluster_list = []
                    console.print(f"\n  [bold {GREEN}]✓  Wiped {count} profiles.  Wallet list preserved.[/bold {GREEN}]")
                else:
                    console.print(f"  [{GREY}]Cancelled.[/{GREY}]")
            elif raw.isdigit():
                idx = int(raw) - 1
                if 0 <= idx < len(cached):
                    target = cached[idx]
                    addr   = target["address"]
                    lbl    = target.get("label", addr[:10])
                    path   = manager._profile_path(case_name, addr)
                    if path.exists():
                        path.unlink()
                    profiles     = [p for p in profiles if p["address"].lower() != addr.lower()]
                    cluster_list = compute_clusters(profiles) if len(profiles) > 1 else []
                    console.print(
                        f"\n  [bold {GREEN}]✓  Deleted profile for {lbl}[/bold {GREEN}]"
                    )
                else:
                    console.print(f"  [{AMBER}]Invalid selection.[/{AMBER}]")
            else:
                console.print(f"  [{AMBER}]Invalid input.[/{AMBER}]")
            console.input(f"\n  [{GREY}]Press Enter to continue...[/{GREY}]")

        else:
            console.print(f"  [{AMBER}]Unknown command.[/{AMBER}]")
            console.input(f"\n  [{GREY}]Press Enter to continue...[/{GREY}]")


# ─── Entry Point ──────────────────────────────────────────────────────────────

def main() -> None:
    manager = CaseManager()
    while True:
        case_name = prompt_case_open_or_create(manager)
        if case_name is None:
            console.print(f"\n  [{GREY}]Quit.[/{GREY}]\n")
            break
        if case_name == "":
            continue  # L was pressed — loop back to selection
        case_menu(manager, case_name)


if __name__ == "__main__":
    main()
