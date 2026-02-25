# Spec-First Development

Specs are the source of truth for event logic, gating, and ontology-aligned discovery behavior.

## Authoritative Spec Areas

- `spec/events/*.yaml`: event families and registry-facing contracts
- `spec/events/canonical_event_registry.yaml`: canonical family/event constraints
- `spec/gates.yaml`: discovery/promotion/evaluation thresholds
- `spec/multiplicity/*`: taxonomy, families, multiplicity controls
- `spec/states/state_registry.yaml`: state definitions and source linkage
- `spec/hypotheses/template_verb_lexicon.yaml`: template verb/operator controls
- `project/schemas/feature_schema_v1.json`: feature output schema contract

## Change Workflow

1. Update relevant spec(s) first.
2. Update implementation to match spec contracts.
3. Update tests for new/changed behavior.
4. Run validation (`make check-hygiene`, `make test-fast`).

## Event Family Additions

1. Add `spec/events/<EVENT>.yaml`.
2. Ensure analyzer support in `project/pipelines/research/`.
3. Keep `PHASE2_EVENT_CHAIN` aligned with registry support.
4. Validate event registry outputs and Phase2 candidate outputs.

## Run Manifest Binding

Each run records spec and ontology hashes in manifest fields, enabling traceability between artifacts and the exact spec snapshot used.

## Non-Negotiable Invariants

- No lookahead leakage.
- No hidden runtime behavior that bypasses spec contracts.
- No silent fallback paths in protected evaluation flows.
