# Core Concepts

## 1) Point-in-Time (PIT)

All joins and labels must be valid at decision time.

- Backward/asof joins with explicit staleness windows.
- No same-bar fill leakage for Phase2 (`entry_lag_bars >= 1`).
- No future timestamps in features or event alignment.

## 2) Spec-First Contracts

Behavior is defined in specs before implementation.

- Event and registry contracts: `spec/events/*.yaml`
- Gates and thresholds: `spec/gates.yaml`
- Multiplicity and taxonomy: `spec/multiplicity/*`
- State registry and ontology linkage: `spec/states/*`, `spec/multiplicity/*`

## 3) Event Semantics

Each family can produce:

- Impulse flags (`*_event`) at anchor timestamps.
- Active-window flags (`*_active`) across enter/exit intervals.

This allows impulse-triggered and window-filtered logic.

## 4) Discovery Pipeline Semantics

- Phase1 analyzers detect event candidates.
- Registry normalizes analyzer outputs.
- Phase2 evaluates hypotheses with multiplicity control.
- Bridge and promotion gates enforce economic viability.

## 5) Run Trace and Auditability

Run manifests track both plan and execution details.

- Logical plan: `planned_stages`
- Concrete execution plan: `planned_stage_instances`
- Timings: `stage_timings_sec` and `stage_instance_timings_sec`
- Session token: `pipeline_session_id`
- Terminal audit fields: `artifact_cutoff_utc`, `late_artifact_count`, `late_artifact_examples`

## 6) Checklist and Execution Guard

The checklist decision controls execution transitions.

- `PROMOTE`: execution stages may proceed.
- `KEEP_RESEARCH`: execution stages are fail-closed unless explicitly bypassed by safe, allowed flows.

Execution guard behavior is intentionally conservative to protect measurement integrity.
