# Spec-First Development

Specs are the source of truth for research configuration and event registry wiring.

## Key Spec Areas

- `spec/events/*.yaml`
  - Defines `event_type`, `reports_dir`, `events_file`, `signal_column`.
  - Used by registry loading and event flag construction.

- `spec/gates.yaml`
  - Phase2 and downstream gating thresholds.

- `spec/multiplicity/families.yaml`
  - Per-family templates/horizons and search budgets.

- `project/schemas/feature_schema_v1.json`
  - Required output columns for features/context datasets.

## Implementation Rules

1. Add/modify event family in `spec/events` first.
2. Ensure analyzer emits fields needed for normalization (`enter_ts` or anchor equivalent).
3. Keep `PHASE2_EVENT_CHAIN` and registry specs consistent.
4. Run tests that cover registry and phase2 integrity.

## Adding a New Event Family

1. Add `spec/events/<event_type>.yaml`.
2. Implement or map Phase1 analyzer script in `project/pipelines/research/`.
3. Add entry to `PHASE2_EVENT_CHAIN` in `run_all.py`.
4. Verify registry output columns in `data/events/<run_id>/event_flags.parquet`.
5. Run Phase2 for that event and check `phase2_candidates.csv`.

## Why This Matters

Spec-first avoids hidden hardcoded behavior and gives reproducible, auditable runs.
