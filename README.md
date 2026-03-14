# WalletDNA

A forensic intelligence tool that generates a behavioural fingerprint for any blockchain wallet.

---

## What It Does

You and your friend both have wallets. You can't tell them apart by address alone — but you can tell them apart by behaviour.

WalletDNA looks at how a wallet actually operates:

- **When it's active** — what hours, what timezone, how wide the window
- **How it sends transactions** — round numbers or precise amounts, fragmented or consolidated
- **Gas fee patterns** — consistent to the gwei (automation) or varied (human)
- **Mempool behaviour** — instant confirmation every time, or does it wait
- **Activity cycle** — steady usage over time, or intense bursts followed by silence
- **Contract interactions** — DEX-heavy, wallet-to-wallet, or mixed

Each wallet gets a 7-dimension DNA string:

```
G:MED-STABLE | T:0300-0500UTC | V:SPLIT-HIGH-PRECISE | C:DEX-HEAVY | M:INSTANT | A:BURST-SLEEP | X:BOT-HIGH
```

Load a batch of wallets into a case. The engine compares every wallet against every other wallet using weighted cosine similarity. Wallets that behave the same get clustered together. If the similarity score hits 0.92 or above, the tool flags them as likely controlled by the same operator — different addresses, same hand.

**Supported chains:** ETH · TRX · DOGE

---

## Why This Project Matters

This was built during a real investigation into a BDAG presale fraud. A group of collection wallets across ETH and TRX — different addresses, different chains. On a block explorer they look unrelated.

When you run WalletDNA across them, the clusters emerge.

Four TRX collection wallets scored 1.000 similarity — perfect behavioural match across all 7 dimensions. Same gas pattern. Same 1-hour active window. Same value fragmentation. Same activity cycle. Different addresses. Same hand operating them.

Multiple ETH wallets scored above 0.92 — several pairs hitting 0.997 and 0.993. That is not a coincidence.

The tool exists because block explorers show you what happened. WalletDNA shows you who is behind it.

---

## How I Built This

I'm a career changer from a manufacturing background. No CS degree. No bootcamp.

I use AI (Claude) throughout development — as a learning tool, code reviewer, and debugging partner. Every terminal error went back to Claude. The decisions are mine. The systems run.

I decided what to build based on a real investigation I was involved in. I decided the 7 dimensions to measure. I decided the case-based architecture so investigations stay organised and portable. I decided what to cut — Grafana and Prometheus were removed because this is a forensic CLI tool, not a 24/7 monitoring service.

The similarity algorithm uses weighted cosine on a 10-dimensional vector. I asked Claude to propose the approach and the weights based on which signals are hardest to fake. I tested it on real wallets. The results made sense. I kept it.

The systems run. The tests pass. I can demo everything live.

---

## What I Learned

- How blockchain adapters work across different chains — ETH uses Etherscan V2, TRX uses TronScan, DOGE uses Blockcypher. Each one has a different API shape, different rate limits, different quirks
- UTXO chains (DOGE) have no gas, no contracts, no mempool — those dimensions don't apply and need to be handled separately
- ERC-20 token transfers store the token amount in the value field, not the ETH equivalent — if you treat token amounts as ETH you get completely wrong numbers
- Smart contract payments like presale contracts don't attribute the transaction to the user's wallet — the contract appears as the sender, so volume figures will always be lower than actual spending
- Cosine similarity only works cleanly when the feature vectors are on the same scale — if one dimension dominates, the whole score gets skewed
- Caching logic sounds simple until you handle every state — missing profile, stale profile, fresh profile, and force-refresh all need separate paths
- asyncio with a semaphore cap is the right way to run multiple chain adapters in parallel without hitting rate limits

---

## Post-Mortem

**Bug: wallet volumes showing millions of dollars for wallets with only a handful of transactions**

**What happened:** The tool was adding up all token transfer amounts and treating them as ETH. A wallet that sent 16,000 USDT was showing up as 16,000 ETH — which at current prices came out as over $16 million. Completely wrong.

**Root cause:** ERC-20 token transfers store the token amount in the same field as native ETH amounts. 16,000 USDT and 16,000 ETH look identical in the raw data. The tool was multiplying everything by the ETH price without checking whether it was actually ETH.

**Fix:** Split the calculation into two parts. Native ETH transactions get multiplied by the ETH spot price. Stablecoins like USDT and USDC get counted at face value — $1 each. Everything else gets excluded because there is no reliable way to price other tokens without a historical price feed per token per date.

**Remaining limitation:** Payments routed through smart contracts — like presale contracts that use transferFrom — don't appear as outbound from the user's wallet. The contract shows as the sender, not you. So the volume figure will always be lower than what was actually spent through contracts. This is how Ethereum works, not a bug in the tool.

**Lesson:** Raw blockchain data doesn't label itself. A number in a value field could be ETH, USDT, or anything else. You have to check what you are handling before doing any maths on it.

---

## Running It

```bash
# Clone
git clone https://github.com/apu-saha-990/Project04-wallet-DNA.git
cd Project04-wallet-DNA

# Environment
cp .env.example .env
# Add your ETHERSCAN_API_KEY to .env

# Install
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Start database
docker compose up -d postgres

# Run dashboard
python3 -m walletdna.dashboard.terminal
```

---

## Environment Variables

```bash
ETHERSCAN_API_KEY=       # Required for ETH — get from etherscan.io
POSTGRES_PASSWORD=       # Optional — defaults to walletdna_secret
```

TRX (TronScan) and DOGE (Blockcypher) require no API keys.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.12 |
| ETH data | Etherscan V2 API |
| TRX data | TronScan public API |
| DOGE data | Blockcypher API |
| Price feed | CoinMarketCap API |
| Database | PostgreSQL + TimescaleDB |
| Terminal UI | Python Rich |
| Async | asyncio |
| Containerisation | Docker |
| CI/CD | GitHub Actions |
| Linting | ruff |

---

## Project Structure

```
walletdna/
├── adapters/          # ETH, TRX, DOGE chain adapters
├── cases/             # Case manager + batch analyser
├── dashboard/         # Terminal UI — network table, drill-down, panels
├── engine/            # Feature extractor, classifier, composer, similarity
└── storage/           # TimescaleDB schema

cases/                 # Local investigation cases — gitignored
docker-compose.yml     # Postgres only
.github/workflows/     # CI/CD pipeline
```

---

## Roadmap

- Behaviour timeline — split a wallet's history into time windows and detect when its pattern changed
- Operator probability score — weighted formula combining similarity, cluster density, and behaviour consistency
- Behaviour signature hash — hash the DNA string so you can search for wallets by fingerprint
- Architecture diagram in repo

---

## Why Not Use An Existing Tool

Chainalysis and similar tools cluster wallets by address proximity — who sent to who. WalletDNA clusters by behaviour. A wallet can change its address. It cannot easily change how it operates. This approach is different, not better — it catches things address-graph tools miss, and misses things they catch.

---

*Career changer from manufacturing. Learning in public. Building real things.*
