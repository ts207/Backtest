# Concept Playbook: C_EVENT_DEFINITIONS

## Definition
The technical implementation details for every event type in the registry. This includes exact threshold values (e.g., "funding_zscore > 2.0"), required feature dependencies, and the lookback windows used for context. Event definitions must be versioned to track how changes in triggers affect edge performance.

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `event_registry` | `trigger_logic`, `version` | N/A | N/A |
| `feature_vectors` | All inputs for triggers | 1m/15m | N/A |

## Metrics
*   **`event_frequency_per_day`**: Average number of occurrences per symbol per day.
*   **`co-occurrence_rate`**: Degree to which different event types trigger at the same time.
*   **`trigger_drift_score`**: Change in event frequency across different dataset versions or exchanges.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_EDEF_01` | Non-Degeneracy | Event must trigger at least 50 times across the training universe. |
| `T_EDEF_02` | Trigger Robustness | Minor changes (+/- 5%) in thresholds must not change the event set by more than 10%. |
| `T_EDEF_03` | Dependency Check | All features required by the trigger logic must be present in the `feature_vectors` table. |

## Artifacts
*   **`project/events/registry.py`**: Implementation of `EVENT_REGISTRY_SPECS`.
*   **`project/pipelines/research/build_event_registry.py`**: Script to detect and persist events.
*   **`data/lake/events/registry_manifest.json`**: Versioned metadata for the current event set.
