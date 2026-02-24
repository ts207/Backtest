# Concept Playbook: C_VALIDATION

## Definition
The formal process of converting conditional returns into statistically valid candidate edges. Validation uses purged/embargoed cross-validation and regime-based partitioning to ensure that an edge's expectancy remains positive after accounting for realistic execution and selection bias.

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `events.parquet` | All | Event-driven | 365 days |
| `forward_labels` | All | Multi-horizon | 365 days |

## Metrics
*   **`after_cost_expectancy_bps`**: Mean return per trade after subtracting fees, slippage, and impact.
*   **`regime_stability_score`**: Percentage of market regimes (vol, liquidity, trend) where the edge has positive expectancy.
*   **`parameter_neighborhood_stability`**: Ratio of successful parameter combinations within a +/- 10% range of the optimum.
*   **`t_stat_adjusted`**: Multiplicity-adjusted t-statistic for the edge's mean return.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_VAL_01` | Edge Contract | Edge must have positive expectancy in at least 3 distinct regime partitions. |
| `T_VAL_02` | Purging Check | Training data must not contain any samples whose label window overlaps with the test window. |
| `T_VAL_03` | P-Hacking Guard | All parameter bounds must be pre-registered in the run manifest before validation. |

## Artifacts
*   **`project/pipelines/research/phase2_candidate_discovery.py`**: The core validation pipeline script.
*   **`project/pipelines/research/evaluate_naive_entry.py`**: Baseline comparison for event significance.
*   **`data/reports/validation/candidate_ranking.csv`**: Ranked list of edges passing all validation gates.
