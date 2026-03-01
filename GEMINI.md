# Backtest Project Context

## What This Repo Is
Backtest is a specialized **Research & Alpha Discovery** engine for Binance perp/spot data. Its primary objective is to convert raw market data into statistically validated **Strategy Blueprints** using rigorous FDR control and hierarchical shrinkage.

Downstream execution, high-fidelity backtesting, and live trading are handled by the `nautilus_trader` engine.

Core entrypoint: `project/pipelines/run_all.py`

## Runtime Structure
- **Ingest & Clean:** 5m bar research datasets for Binance.
- **Feature Generation:** PIT-safe features (Vol, Carry, Microstructure).
- **Phase 1 Event Detection:** 57 canonical event types across 10 families.
- **Phase 2 Discovery:** Statistical validation using James-Stein shrinkage and Benjamini-Hochberg FDR.
- **Bridge Evaluation:** Fast, cost-stressed tradability check.
- **Blueprint Compilation:** Final artifact generation (`blueprints.jsonl`).

## Contracts That Matter
- **Blueprint-as-Artifact:** The pipeline ends at `blueprints.jsonl`. This is the "Alpha DNA" for external execution.
- **Spec is Truth:** All event and gate definitions live in `spec/`.
- **Statistical Rigor:** No signal is promoted to a blueprint without passing the FDR and shrinkage gates.
- **PIT Safety:** All joins must use `merge_asof(direction="backward")`.
- **Lagged Entry:** Always assume `entry_lag_bars >= 1` for research consistency.

## Working Commands
```bash
export BACKTEST_DATA_ROOT=$(pwd)/data
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt

make run
make discover-edges
make test-fast
./.venv/bin/python project/pipelines/run_all.py --help
```

## Doc Index
- `README.md`
- `docs/GETTING_STARTED.md`
- `docs/ARCHITECTURE.md`
- `docs/CONCEPTS.md`
- `docs/SPEC_FIRST.md`
- `docs/AUDIT.md`
