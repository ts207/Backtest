# Backtest: Alpha Discovery Engine

A specialized, spec-driven quantitative research platform for Binance perpetual and spot markets. 

The system's sole objective is to discover statistically validated trading edges and export them as **Strategy Blueprints** (`blueprints.jsonl`) for execution in production engines like NautilusTrader.

---

## Core Pipeline

1. **Ingest & Clean:** Raw Binance OHLCV + microstructure.
2. **Features:** PIT-safe vol regime, carry, and liquidity states.
3. **Events:** Detection of 57 canonical event types (e.g., VOL_SHOCK, LIQUIDITY_VACUUM).
4. **Discovery (Phase 2):** Hierarchical James-Stein shrinkage + BH-FDR multiplicity control.
5. **Bridge:** Cost-stressed expectancy stress-test.
6. **Compile:** Export to `blueprints.jsonl`.

---

## Quick Start

```bash
# Setup
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
export BACKTEST_DATA_ROOT=$(pwd)/data

# Run Discovery (Produces blueprints.jsonl)
make discover-blueprints \
  RUN_ID=alpha_run_01 \
  SYMBOLS=BTCUSDT,ETHUSDT \
  START=2022-01-01 \
  END=2025-01-01
```

---

## Key Research Metrics

| Metric | Value |
|--------|-------|
| Event Types | 57 |
| Canonical Families | 10 |
| Statistical Gates | Hierarchical Shrinkage (JS) + FDR (BH) |
| Default FDR | q ≤ 0.05 |
| Output Artifact | `blueprints.jsonl` |
| Primary Horizon | 15m |

---

## Documentation

| Document | Contents |
|----------|---------|
| [CONCEPTS.md](docs/CONCEPTS.md) | PIT safety, shrinkage math, and blueprint schema. |
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | Research pipeline detail and module map. |
| [SPEC_FIRST.md](docs/SPEC_FIRST.md) | Defining new events and alpha hypotheses in `spec/`. |
| [PERFORMANCE.md](docs/PERFORMANCE.md) | Parallel discovery and pipeline tuning. |

---

## System Contracts

1. **Spec is Source of Truth** — Gate thresholds and event definitions live in `spec/`.
2. **PIT-Safe Joins Only** — All features join using `merge_asof(direction="backward")`.
3. **Alpha-as-Artifact** — The research pipeline is successful if it produces validated Blueprints.
4. **No Future Leaks** — Entry always lags detection by $\ge 1$ bar.
