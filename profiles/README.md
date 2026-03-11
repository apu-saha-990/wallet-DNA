# profiles/

This directory stores computed DNA profiles as JSON files.

Each file is named `{address}.json` and contains the full DNA profile
computed from live on-chain data.

## Why this exists

TimescaleDB data is lost when Docker volumes are wiped or the repo is re-cloned.
This folder provides a git-tracked persistence layer — profiles survive across
machines, re-clones, and environment rebuilds.

## How it works

1. User analyses a wallet → DNA computed from live API data
2. Profile auto-saved to `profiles/{address}.json`
3. On next run → dashboard checks this folder first (cache hit = no API call)
4. `git push` → profiles travel with the repo permanently

## Import/Export

```bash
# Export all DB profiles to this folder
python3 -m walletdna export

# Rebuild DB from this folder (after fresh clone)
python3 -m walletdna import
```

## File format

```json
{
  "address": "0x...",
  "chain": "ethereum",
  "label": "Suspect #1",
  "dna_string": "G:MED-STABLE | T:0300-0500UTC | ...",
  "dna_vector": [0.12, 0.0, 0.28, ...],
  "wallet_class": "BOT",
  "bot_confidence": 0.87,
  "tx_count": 412,
  "confidence_score": 0.82,
  "analysed_at": "2026-03-11T09:00:00Z"
}
```
