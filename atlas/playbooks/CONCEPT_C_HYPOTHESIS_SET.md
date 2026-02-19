# Concept Playbook: C_HYPOTHESIS_SET

## Definition
The structured collection of testable "if-then" behaviors that drive the research pipeline. A hypothesis set maps mechanisms (e.g., deleveraging) to observable events and expected distributional shifts in forward returns. It acts as the research backlog and guides feature engineering.

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `research_backlog.csv` | `claim_id`, `mechanism` | N/A | N/A |
| `phase1_hypotheses` | `hypothesis_id`, `priority` | N/A | N/A |

## Metrics
*   **`hypothesis_yield_rate`**: Percentage of hypotheses that result in a promoted candidate edge.
*   **`mechanism_coverage`**: Ratio of Atlas-defined mechanisms currently active in the research queue.
*   **`novelty_score`**: Measures the distance of a new hypothesis from previously tested ones in feature space.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_HYP_01` | Falsifiability | Every hypothesis must specify a "Confirm" and "Disconfirm" trigger. |
| `T_HYP_02` | Operationalizable | 80%+ of Rank 1-3 hypotheses must have a defined data requirement available in the lake. |
| `T_HYP_03` | No Outcome Leak | Hypothesis descriptions must not contain references to the outcome variables (returns/PnL) during the discovery phase. |

## Artifacts
*   **`project/pipelines/research/generate_hypothesis_queue.py`**: Automated queue generator.
*   **`data/lake/reports/research_backlog.csv`**: The master work queue for the research system.
