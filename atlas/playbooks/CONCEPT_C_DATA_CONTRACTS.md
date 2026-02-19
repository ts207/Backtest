# Concept Playbook: C_DATA_CONTRACTS

## Definition
The formal specification of venue-specific mechanics and data schemas. Data contracts ensure that raw exchange feeds (funding schedules, mark-index rules, liquidation formats) are correctly translated into the normalized data plane used by the research framework.

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `perp_ohlcv_1m` | All core | 1m | N/A |
| `um_funding_rates`| `funding_rate`, `funding_time` | 8h | N/A |
| `mark_price_1m` | `mark_price` | 1m | N/A |

## Metrics
*   **`schema_conformance_rate`**: Percentage of ingested rows matching the target schema (type and nullability).
*   **`timestamp_monotonicity_score`**: Ratio of rows with strictly increasing UTC timestamps.
*   **`funding_alignment_offset`**: Difference between the expected funding event time and the observed timestamp in the feed.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_CON_01` | OHLCV Schema | Every OHLCV row must contain non-null `open`, `high`, `low`, `close`, and `volume`. |
| `T_CON_02` | Funding Grid | Injected funding rates must occur at UTC 00:00, 08:00, and 16:00 for Binance perps. |
| `T_CON_03` | Mark-Spot Parity | The distance between Mark and Index price must be < 5% during all non-liquidating periods. |

## Artifacts
*   **`project/schemas/feature_schema_v1.json`**: Formal schema definitions for all research inputs.
*   **`project/pipelines/_lib/sanity.py`**: Implementation of data contract validation checks.
*   **`project/pipelines/ingest/`**: Ingestion scripts that enforce contracts at the entry point.
