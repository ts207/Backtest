# Concept Playbook: C_PORTFOLIO

## Definition
The management of multiple strategy equity curves to achieve a target risk profile and maximize risk-adjusted returns. Portfolio construction handles allocation across strategies, instruments, and venues, while enforcing deterministic risk gates (exposure caps, liquidity limits).

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `equity_curves.parquet` | `strategy_id`, `pnl` | 5m | 365 days |
| `universe_snapshots.parquet` | `symbol`, `status` | Daily | N/A |

## Metrics
*   **`portfolio_sharpe`**: Total portfolio annualized reward-to-risk ratio.
*   **`diversification_ratio`**: Ratio of weighted average volatility of individual strategies to portfolio volatility.
*   **`active_risk_contribution`**: Marginal contribution of each strategy to total portfolio variance.
*   **`turnover_per_day`**: Total notional traded per day as a percentage of total portfolio NAV.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_PORT_01` | Exposure Caps | No single strategy may contribute more than 25% of total portfolio risk. |
| `T_PORT_02` | Liquidity Gate | Portfolio allocation must be reduced by 50% if the aggregate spread exceeds the 90th percentile. |
| `T_PORT_03` | Correlation Cap | Average pairwise correlation between promoted strategies must be < 0.4. |

## Artifacts
*   **`project/pipelines/report/capital_allocation.py`**: Allocation and sizing logic.
*   **`project/pipelines/alpha_bundle/build_universe_snapshots.py`**: Historical membership logic.
*   **`data/reports/portfolio/daily_stats.json`**: Portfolio-level performance and risk metrics.
