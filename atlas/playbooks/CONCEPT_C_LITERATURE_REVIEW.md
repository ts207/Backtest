# Concept Playbook: C_LITERATURE_REVIEW

## Definition
The ongoing synthesis of external research, papers, and bibliometric analysis to identify new mechanisms and validate existing assumptions. In the context of the Atlas, literature review acts as the "source of claims" that are then converted into testable hypotheses and backlogged for implementation.

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `sources` | `source_id`, `title`, `author` | N/A | N/A |
| `fragments.jsonl` | `text`, `locator`, `strength` | N/A | N/A |

## Metrics
*   **`evidence_strength_ratio`**: Percentage of Atlas claims supported by "high" strength evidence.
*   **`claim_to_code_conversion_rate`**: Percentage of literature claims that have been operationalized into playbooks or scripts.
*   **`source_diversity_index`**: Number of independent research groups or organizations contributing to the Atlas.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_LIT_01` | Claim Traceability | Every claim in the Atlas must have at least one valid locator in `fragments.jsonl`. |
| `T_LIT_02` | Contradiction Tracking | All detected contradictions must be linked to at least two claims with opposing evidence. |
| `T_LIT_03` | Paper Coverage | Top 10 most cited papers in the "Crypto Systematic" domain must be present in the `sources` list. |

## Artifacts
*   **`knowledge_atlas (3).md`**: The master markdown record of the literature review.
*   **`fragments (1).jsonl`**: The raw evidence extracted from the literature.
*   **`project/pipelines/research/summarize_discovery_quality.py`**: Reporting on research throughput.
