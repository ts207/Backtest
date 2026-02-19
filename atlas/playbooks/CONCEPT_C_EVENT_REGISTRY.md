# Concept Playbook: C_EVENT_REGISTRY

## Definition
A deterministic, versioned taxonomy of all tradable market situations. Every event in the registry must have a formal trigger logic, required data fields for its detection, and a deterministic labeling rule for forward outcomes. It serves as the single source of truth for "what we test."

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `event_registry` | `event_type`, `trigger_logic` | N/A | N/A |
| `events.parquet` | `timestamp`, `symbol`, `event_type` | Event-driven | N/A |

## Metrics
*   **`event_prevalence_bps`**: Number of event occurrences per 10,000 bars.
*   **`trigger_determinism_ratio`**: Percentage of events that can be perfectly reproduced from the raw data plane.
*   **`registry_coverage_pct`**: % of total price action (by time) covered by at least one active event type.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_REG_01` | No Lookahead | Event triggers must only use data with timestamp <= t0. |
| `T_REG_02` | Cooldown Enforcement | No two events of the same type for the same symbol can occur within the specified `cooldown_bars`. |
| `T_REG_03` | Schema Compliance | Every entry must contain `event_id`, `trigger_logic`, and `context_fields`. |

## Artifacts
*   **`project/events/registry.py`**: The canonical Python implementation of the registry.
*   **`data/lake/events/events.parquet`**: The unified table of detected events across all symbols.
