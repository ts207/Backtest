# Concept Playbook: C_DATA_SCHEMA

## Definition
The minimal universal labeling schema required to evaluate all hypotheses from a single dataset pass. It defines the structure for Point-in-Time (PIT) features, forward labels, and detected events, ensuring consistency across symbols and timeframes while preventing "silent" schema drift.

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `cleaned_bars` | `open`, `high`, `low`, `close`, `volume` | 1m/15m | N/A |
| `feature_vectors` | All PIT features | 1m/15m | N/A |
| `label_vectors` | `ret_fwd`, `range_fwd`, `rv_fwd` | Multi-horizon | N/A |

## Metrics
*   **`null_rate_per_column`**: Percentage of missing values in the schema.
*   **`schema_hash_stability`**: Measures if the column set or data types have changed relative to the manifest.
*   **`storage_efficiency_ratio`**: Parquet file size relative to row count and column entropy.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_SCH_01` | Core Column Presence | Every feature table must contain `timestamp`, `symbol`, and `run_id`. |
| `T_SCH_02` | Type Enforcement | Price columns must be `float64`, and flag columns must be `bool`. |
| `T_SCH_03` | Partition Logic | Data must be partitioned by `year` and `month` for scalable retrieval. |

## Artifacts
*   **`project/schemas/feature_schema_v1.json`**: The technical schema definition.
*   **`project/pipelines/_lib/io_utils.py`**: Parquet reading/writing logic with schema checks.
*   **`data/lake/cleaned/`**: The physical storage of normalized data.
