# Concept Playbook: C_CONTEXT_INTERACTIONS

## Definition
The study of edges that only manifest when multiple independent conditions align (e.g., low volatility AND a session open AND a level sweep). This concept assumes that single-factor indicator research is largely "noise" and that true signal is conditional on the global market state.

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `perp_ohlcv_5m` | All core | 5m | 180 days |
| `event_flags` | All signal columns | 5m | 180 days |

## Metrics
*   **`interaction_lift`**: The incremental expectancy of combining condition A and B compared to A alone.
*   **`conditional_probability_matrix`**: Probability of return > X given state vector S.
*   **`regime_concordance`**: Degree to which different volatility or liquidity proxies agree on the current state.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_INT_01` | Synergistic Signal | The combination of `vol_compression` and `session_open` must show 2x higher Sharpe than either factor in isolation. |
| `T_INT_02` | State Independence | Input features used in interactions (e.g., funding and vol) must have a rank correlation < 0.3. |
| `T_INT_03` | Sample Sufficiency | Each interaction bucket (cell in the state matrix) must contain >= 30 samples. |

## Artifacts
*   **`project/pipelines/research/phase2_candidate_discovery.py`**: Main engine for testing multi-factor interactions.
*   **`data/reports/interactions/lift_analysis.parquet`**: Results of conditional expectancy tests.
