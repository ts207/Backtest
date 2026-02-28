# Core Concepts

## 1) Point-in-Time (PIT) & Integrity Checks
All joins and labels must be valid at decision time to protect out-of-sample discovery.
- **Backward (as-of) joins:** Applied with explicit staleness tolerance windows (e.g., funding, spot, Open Interest, orderbook snapshots).
- **Missing Data Realism:** Gaps in market data are rigorously identified; prices are **never** forward-filled, establishing invalid bar masks that prevent synthetic stability across data outages.
- **Discrete Funding:** Yield models apply funding discretely at the exact extraction hour, preventing false intra-bar smoothing leakage in PnL attribution.
- **Decision Phase Shifts:** Phase 2 logic mandates `entry_lag_bars >= 1` ensuring trade execution operates distinctly behind discovery logic.

## 2) Spec-First Contracts
Behavior is formally declared in YAML specifications or JSON schemas before runtime execution.
- Event tracking contracts: `spec/events/*.yaml`
- Multiplicity control logic: `spec/multiplicity/*`
- Thresholds for promotion & expectancy gates: `spec/gates.yaml`
- Verb lexicon definitions: `spec/hypotheses/template_verb_lexicon.yaml`
- Schema structure definitions: `schemas/feature_schema_v1.json` & `feature_schema_v2.json`

## 3) Event & Dislocation Semantics
Each analyzer maps market telemetry into distinct topological states:
- **Impulse flags** (`*_event`) trigger exactly at anchor timestamps.
- **Active-window flags** (`*_active`) mark duration bounds over structurally significant regimes.
This bifurcation enforces clean separation between immediate triggers and regime context filters.

## 4) Exhaustive Discovery Pipeline Model
- **Phase 1 Detectors:** Emit core candidate events across disparate domains (liquidity vacuums, OI sweeps, volatility clustering).
- **Phase 2 Conditionals:** Applies combinatorial hypotheses evaluating delays, parameter curvature, and expected utility under Benjamini-Hochberg False Discovery Rate (BH-FDR) constraints.
- **Evaluation Stages:** Aggregates bridge metrics, cost survival bounds, and expectancy robustness dynamically over diverse structural conditions.
- **Candidate Promotion:** Evaluates survival artifacts to compile strictly executable Strategy Blueprints.

## 5) Auditable Run Traces
Run manifests perfectly encode dataset provenance and pipeline states:
- `planned_stages` vs `planned_stage_instances` to model scaling complexities.
- Multi-dimensional timings logging via `stage_timings_sec`.
- Snapshot isolation mechanisms like `artifact_cutoff_utc` and strict Git commit binds ensuring reproducibility.
- **Ontology consistency pre-flights** (`ontology_consistency_audit.py`) safely halt runs missing structural components before computation heavily loads.

## 6) Checklist and Execution Guards
The `checklist.json` artifact dynamically acts as a runtime gatekeeper.
- Models resolving to `PROMOTE` unleash execution stages for full backtesting workflows.
- Models evaluating as `KEEP_RESEARCH` defensively block naive out-of-sample execution routes minimizing look-ahead decay and restricting p-hacking.
Execution guards are intentionally hyper-conservative.
