# Concept Playbook: C_MULTIPLICITY_CONTROL

## Definition
The statistical correction layer required to prevent false discoveries when testing a large number of hypotheses or parameter sweeps on the same dataset. Multiplicity control ensures that the probability of at least one "false positive" edge being promoted is bounded and reported.

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `validation_results` | `p_value`, `expectancy` | Event-driven | N/A |

## Metrics
*   **`q_value_bh`**: Adjusted p-value using the Benjamini-Hochberg (FDR) procedure.
*   **`deflated_sharpe_ratio`**: A Sharpe ratio adjusted for the number of trials and the variance of returns across trials.
*   **`reality_check_p_value`**: Bootstrap-based significance of the "best" edge relative to a random benchmark.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_MULT_01` | FDR Gate | Candidate must have a `q_value_bh` <= 0.05 to pass to the Bridge stage. |
| `T_MULT_02` | Trial Independence | Hypotheses in the same "multiplicity family" must be clearly identified in the manifest. |
| `T_MULT_03` | Monotonic Penalty | The multiplicity penalty must increase as the number of tested parameter combinations increases. |

## Artifacts
*   **`project/eval/multiplicity.py`**: Implementation of FDR, Deflated Sharpe, and Reality Check.
*   **`data/lake/reports/validation/multiplicity_report.json`**: Adjustment results for every batch of research trials.
