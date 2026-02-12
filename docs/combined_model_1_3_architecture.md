# Combined Model 1 + Model 3 Architecture (v1)

This repository is patched to support an institutional, reproducible crypto systematic trading research stack that combines:

- **Model 3**: deterministic data model, service boundaries, auditability, event-driven backtesting, accounting completeness, validation & robustness battery.
- **Model 1**: deeper alpha research layer: multi-horizon momentum, stateful mean reversion, funding/basis/OI carry, on-chain flow, regime gating, orthogonalization, ridge combination.

## What changed in this repo

### New specs
- `project/specs/combined_model_1_3_spec_v1.yaml` — authoritative combined spec (schemas + alpha bundle contract).

### New pipeline modules (scaffolding)
- `project/pipelines/alpha_bundle/`
  - `build_universe_snapshot.py`
  - `build_cross_section_features.py`
  - `build_alpha_signals_v2.py`
  - `fit_orth_and_ridge.py`
  - `apply_alpha_bundle.py`

These modules are written to be **deterministic** (stable sorting, explicit time blocks) and **point-in-time safe** (event-time only).

## Data expectations

The repo already supports Binance UM ingestion (funding, open interest, OHLCV). The alpha bundle assumes:
- `timestamp` is UTC and used as `ts_event` (the pipelines normalize naming).
- funding is 8h cadence; open interest is provided at a regular cadence (or forward-filled with PIT rules).

## Recommended workflow

1. Ingest raw data (existing pipelines).
2. Build cleaned bars (existing).
3. Build universe snapshots (new).
4. Build cross-sectional aggregates and CS features (new).
5. Build raw alpha signals (new).
6. Fit OrthSpec + Ridge combination offline (new).
7. Apply AlphaBundle to produce `score`, `mu`, and diagnostics (new).
8. Use existing research scripts or extend to portfolio/risk/execution backtests.

## Open items

This patch focuses on repo structure, contracts, and core scaffolding. Production-grade completion typically adds:
- stream watermarks and late-event policy for live/backfill parity
- calibration options for score->mu (ridge / isotonic / binning)
- liquidation/margin stress module for perps
- capacity estimation outputs
