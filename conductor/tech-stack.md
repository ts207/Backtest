# Tech Stack

## Runtime

- Python 3.12
- Subprocess-driven orchestration via `project/pipelines/run_all.py`

## Data and Artifacts

- Pandas / NumPy for tabular and numeric processing
- Parquet for high-volume research artifacts
- Local data root controlled by `BACKTEST_DATA_ROOT`

## Configuration and Contracts

- YAML specs under `spec/`
- JSON-based run/stage manifests for execution provenance
- Feature schema contract at `project/schemas/feature_schema_v1.json`

## Testing and Validation

- `pytest`
- repository hygiene script (`project/scripts/check_repo_hygiene.sh`)
- Make targets (`make test-fast`, `make check-hygiene`, `make test`)
