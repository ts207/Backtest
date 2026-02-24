# Concept Playbook: C_STRATEGY_BACKTEST

## Definition
The simulation of a candidate edge's performance across long-horizon historical data. Backtesting incorporates parameter sweeps to find robust regions and stress tests to simulate microstructure failure (spread widening, venue outages). It is the final gate before an edge is considered for portfolio inclusion.

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `blueprints.jsonl` | `conditions`, `action` | Strategy-level | N/A |
| `cleaned_5m` | All OHLCV | 5m | 2+ years |

## Metrics
*   **`backtest_sharpe`**: Annualized reward-to-risk ratio of the strategy's equity curve.
*   **`max_drawdown_pct`**: Largest peak-to-trough decline in simulated portfolio value.
*   **`edge_half_life`**: The time window over which the strategy's expectancy remains significant in OOS data.
*   **`stress_degradation_ratio`**: Ratio of Sharpe in the stress scenario to Sharpe in the base scenario.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_BTEST_01` | Neighborhood Robustness | Strategy must remain profitable in 80%+ of its +/- 5% parameter neighborhood. |
| `T_BTEST_02` | Stress Survival | Sharpe must remain > 0 during a simulated 3x spread expansion stress test. |
| `T_BTEST_03` | Delay Sensitivity | Strategy must remain profitable if entry is delayed by 1 additional minute. |

## Artifacts
*   **`project/pipelines/backtest/backtest_strategies.py`**: The core strategy execution engine.
*   **`project/strategies/vol_compression_v1.py`**: A template implementation of a backtestable strategy.
*   **`data/reports/backtest/equity_curves.parquet`**: Timeseries of returns per strategy.
