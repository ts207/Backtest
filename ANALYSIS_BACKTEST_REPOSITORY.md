# Analysis of the Backtest Repository

## Repository Overview

The `ts207/Backtest` repository is organized around a modular cryptocurrency futures backtesting pipeline.

Top-level structure:

- `pipelines/`: Orchestrates ingestion, cleaning, feature engineering, backtesting, and reporting. The one-command driver (`pipelines/run_all.py`) executes these stages in sequence.
- `engine/`: Core portfolio/trade logic (including per-trade PnL and multi-symbol aggregation).
- `strategies/`: Strategy implementations. `vol_compression_v1.py` implements a volatility compression → expansion setup. A registry maps strategy names to implementations.
- `pipelines/_lib/`: Shared utilities for config loading, I/O, HTTP helpers, validation, and manifests.
- Runtime outputs: `lake/`, `runs/`, and `reports/` directories generated during execution.
- `tests/`: Unit tests for determinism and expected pipeline artifacts.

Configuration is primarily controlled through `pipeline.yaml` and `fees.yaml`.

## Backtest Pipeline

### 1) Ingest OHLCV and funding data

- `pipelines/ingest/` pulls Binance UM 15-minute OHLCV bars and funding rates.
- Raw data lands in `lake/raw`, then canonicalized outputs are written to `lake/cleaned`.
- Validation checks include timestamp monotonicity, funding bounds, and funding alignment diagnostics.

### 2) Clean 15-minute bars

- `build_cleaned_15m.py` aligns bars with funding events, drops invalid rows, and records quality metrics.
- Manifests include diagnostics like monthly missing bar percentages.

### 3) Feature engineering

- `build_features_v1.py` builds features such as realized volatility, rolling high/low ranges (including 96-bar constructs), funding features, and range-based measures.
- Features are written as Parquet under `lake/features/perp/<symbol>/15m/`.

### 4) Backtest

- `backtest_vol_compression_v1.py` loads cleaned bars + features and invokes the engine to simulate strategy returns.
- Outputs include:
  - `runs/<run_id>/engine/strategy_returns_<strategy>.csv`
  - `runs/<run_id>/engine/portfolio_returns.csv`
  - `lake/trades/backtests/vol_compression_expansion_v1/<run_id>/trades_<symbol>.csv`
  - `lake/trades/backtests/vol_compression_expansion_v1/<run_id>/equity_curve.csv`
  - `runs/<run_id>/engine/metrics.json`
  - `runs/<run_id>/engine/fee_sensitivity.json`

### 5) Report generation

- `make_report.py` aggregates metrics, fee sensitivity, and data quality diagnostics.
- Produces `summary.md` and `summary.json` in `reports/vol_compression_expansion_v1/<run_id>/`.
- Includes fallback logic when explicit trade files are unavailable.

## Examined Run: `20240101_120000`

Sample report location:

- `project/reports/vol_compression_expansion_v1/20240101_120000/summary.md`
- `project/reports/vol_compression_expansion_v1/20240101_120000/summary.json`

Key outcomes:

- Date range: `2024-06-01` to `2025-06-01`
- Total trades: `6`
  - BTCUSDT: `4`
  - ETHUSDT: `2`
- Net return (6 bps per-side total costs): `-3.150%`
- Average R-multiple (combined): `-0.2018`
- Max drawdown: `-4.35%`

Fee sensitivity (0–10 bps per side) indicates worsening results as costs increase:

- Net return approximately degrades from `-2.49%` (0 bps) to `-3.59%` (10 bps).

## Data Quality Diagnostics

- Missing OHLCV bars are near zero overall, with a small monthly blip around October 2024 (~0.13%).
- Funding alignment is effectively complete.
- Funding missing events were not observed in the sample diagnostics.
- Gap stats indicate very limited interruptions (single short gap events).

## Interpretation

- **Performance is negative across all fee scenarios**, including zero-fee cases. This suggests the core entry/exit logic under current parameters is not achieving positive expectancy.
- **Trade count is low** (6 trades over ~1 year across 2 symbols), making outcomes highly sensitive to a few events.
- **Drawdown is moderate** relative to many directional systems, but profitability remains negative.
- **Fees worsen results**, but do not explain the negative edge on their own.
- **Data quality appears strong**, so data issues are unlikely to be the primary cause of poor results.

## Potential Improvements

1. **Refine strategy logic**
   - Revisit compression definition, breakout confirmation, and stop/target behavior.
   - Consider adaptive/trailing exits.
2. **Parameter exploration**
   - Run systematic sweeps for thresholds, stop/target multipliers, and cost assumptions.
3. **Expand symbol universe**
   - Add liquid perpetuals beyond BTC/ETH for broader robustness checks.
4. **Feature enrichment**
   - Test additional predictors (e.g., open interest deltas, funding momentum, microstructure proxies).
5. **Robustness testing**
   - Add scenario tests for trade timing assumptions and edge persistence across regimes.

## Conclusion

The repository provides a clear, modular pipeline with strong data-quality checks and deterministic testing patterns. The included sample run demonstrates end-to-end functionality but shows underperformance for the volatility compression strategy over the tested period. The next practical step is a structured research loop: hypothesis-driven strategy adjustments, broader cross-symbol validation, and walk-forward robustness checks.
