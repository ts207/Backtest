# Concept Playbook: C_PROVENANCE_REPLAY

## Definition
The requirement that every research result must be perfectly reproducible from its inputs. Provenance tracks the exact dataset hash, code commit, configuration digest, and RNG seeds used to generate an artifact. Replay ensures that re-running the pipeline with identical provenance yields identical output tables and metrics.

## Data Requirements
| Dataset | Columns | Granularity | Min History |
|---------|---------|-------------|-------------|
| `run_manifest.json` | `dataset_hash`, `code_commit`, `config_hash` | Per run | N/A |
| `lake/runs/` | All artifacts | Per run | N/A |

## Metrics
*   **`reproducibility_match_rate`**: Percentage of bytes identical between a fresh run and its historical manifest.
*   **`provenance_completeness`**: Ratio of required metadata fields present in the `run_manifest.json`.
*   **`config_drift_score`**: Measures deviation between the current environment and the one recorded in provenance.

## Tests
| Test ID | Objective | Acceptance Criteria |
|---------|-----------|---------------------|
| `T_PROV_01` | Manifest Integrity | Every run must produce a `run_manifest.json` containing a valid 40-character Git SHA and SHA-256 data hashes. |
| `T_PROV_02` | Replay Match | Re-running a "Smoke" run ID with the same manifest must result in a 100% hash match for all output Parquet files. |
| `T_PROV_03` | Input Validation | Pipeline must fail if input data hashes do not match the expected values in the manifest. |

## Artifacts
*   **`project/pipelines/_lib/run_manifest.py`**: Logic for reading and writing execution provenance.
*   **`tests/test_manifest_provenance.py`**: Unit tests for manifest correctness.
*   **`data/runs/<run_id>/run_manifest.json`**: The immutable record of each research execution.
