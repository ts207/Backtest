# Spec-First Development

Specs are the ultimate source of truth for event logic, deterministic gating, and ontology-aligned discovery behavior across the entirety of the pipeline. Implementations simply interpret what the specs demand.

## Authoritative Spec Areas

- `spec/events/*.yaml`: Event families and registry-facing output contracts.
- `spec/events/canonical_event_registry.yaml`: Canonical family/event constraints and hierarchy mapping.
- `spec/gates.yaml`: Discovery, promotion, and evaluation thresholds determining candidate viability.
- `spec/multiplicity/*`: Taxonomy mapping and combinatorial multiplicity bounding.
- `spec/states/state_registry.yaml`: Market state regimes linking structural conditions.
- `spec/hypotheses/template_verb_lexicon.yaml`: DSL verb/operator controls mapping actionable strategy parameters.
- `project/schemas/feature_schema_v1.json` & `v2.json`: Evolving data contracts enforcing structural typing on engineered feature stages.

## Change Workflow

1. Update the relevant spec files first to outline intent.
2. Implement backend systems to parse and validate against the new spec contracts.
3. Update specific testing matrices for handling parsing limits.
4. Execute validation architectures (`make check-hygiene`, `python scripts/ontology_consistency_audit.py`).

## Event Family Additions

1. Define topology internally via `spec/events/<EVENT>.yaml`.
2. Add corresponding isolated analyzers in `project/pipelines/research/analyze_*.py`
3. Guarantee `PHASE2_EVENT_CHAIN` in the core pipeline orchestrator `run_all.py` includes the explicit target.
4. Verify event outputs translate to correct artifact registers (`events.parquet`).

## Run Manifest Binding

Every system execution binds the exact spec snapshot utilized via the orchestrator to its final artifacts. Hashed fields track mutations dynamically linking exactly what code/specs ran to generation artifacts, guaranteeing deep structural reproducibility.

## Non-Negotiable Pipeline Invariants

- **No Future Data Leakage:** Lookahead biases are structurally invalid.
- **Spec Subservience:** Hidden runtime behavior that bypasses explicitly written spec rules constitutes a critical break.
- **Fail-Closed Execution:** The execution flow securely defaults to `KEEP_RESEARCH`, shielding production evaluation workflows against non-promoted experimental artifacts.
