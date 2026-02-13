# Report-Derived Requirements

## Assumptions

- A1. Scope is the repository at `/workspace/Backtest` using static code inspection only.
- A2. “Implementation evidence” means concrete code paths; “usage evidence” means orchestration/runtime invocation and/or tests.
- A3. Where report guidance is conceptual (for example “aggressive validation”), acceptance criteria are translated into measurable artifacts in this repo.
- A4. If no file-level proof exists, requirement is treated as unmet in downstream matrix.

## Requirements

### R1. Net-of-cost performance accounting
- **Statement:** Backtests must report gross alpha, explicit costs, and net alpha.
- **Rationale (report):** `deep-research-report.md` lines 5, 14, 171-179 (net-of-cost edge definition; realistic costs/impact requirement).
- **Acceptance Criteria:**
  1. Output artifact includes gross, fees, slippage, impact, net.
  2. Cost components are computed from executed position changes.
  3. Backtest orchestration exposes cost parameters.
- **Verification Method:** Inspect `project/pipelines/backtest/backtest_strategies.py` cost computation + metrics payload and `run_all.py` argument passthrough.
- **Dependencies:** None.

### R2. Time-respecting out-of-sample validation
- **Statement:** Research evaluation must separate train/validation/test in time (walk-forward or equivalent) and report OOS results.
- **Rationale (report):** lines 5, 14, 374-375, 405 (time-respecting split and walk-forward gating).
- **Acceptance Criteria:**
  1. Explicit temporal split definitions (train/val/test windows) in code artifacts.
  2. Reported metrics tagged by split role.
  3. Leakage prevention includes embargo/overlap policy.
- **Verification Method:** Inspect research pipeline modules and generated schema fields for split identifiers.
- **Dependencies:** R1.

### R3. Multiple-testing / data-snooping controls
- **Statement:** Candidate promotion must include multiplicity-aware correction.
- **Rationale (report):** lines 5, 14, 195-197, 405.
- **Acceptance Criteria:**
  1. Test-count metadata persisted.
  2. Adjusted significance metric (for example corrected p-value / DSR / PBO surrogate).
  3. Promotion gate depends on adjusted (not only raw) metric.
- **Verification Method:** Inspect phase2/expectancy outputs and promotion logic.
- **Dependencies:** R2.

### R4. Data QA and provenance gating
- **Statement:** Input pipelines must enforce timestamp/schema integrity and persist run-level provenance.
- **Rationale (report):** lines 369, 199-203.
- **Acceptance Criteria:**
  1. Timestamp monotonicity/unit checks and schema validation.
  2. Missingness/gap statistics persisted.
  3. Run manifest records inputs/outputs and failure status.
- **Verification Method:** Inspect clean/features pipelines + manifest helper usage.
- **Dependencies:** None.

### R5. Mechanism-grounded feature coverage
- **Statement:** Features should cover carry/funding/basis and related regime signals with explicit lag handling for revisable sources.
- **Rationale (report):** lines 9-12, 225-231, 262-277, 355-357, 387-390.
- **Acceptance Criteria:**
  1. Funding/basis-like fields present in feature store.
  2. OI/liquidation/on-chain regime fields integrated or explicitly modeled as optional inputs.
  3. Revisable sources use lag/confirmation controls.
- **Verification Method:** Inspect feature builders and alpha-bundle modules.
- **Dependencies:** R4.

### R6. Derivatives realism: funding + borrow in PnL
- **Statement:** Derivatives/carry strategies must include funding transfers (and borrow where applicable) in PnL.
- **Rationale (report):** lines 9, 225-231, 256-257.
- **Acceptance Criteria:**
  1. PnL model has separate funding component.
  2. Borrow/financing cost is represented for hedged spot leg when relevant.
  3. Metrics expose these components.
- **Verification Method:** Inspect engine PnL and backtest metrics decomposition.
- **Dependencies:** R1, R5.

### R7. Reproducibility + config/code/data versioning
- **Statement:** Runs must be reproducible via config digest, code revision, and data snapshot IDs.
- **Rationale (report):** lines 199-203.
- **Acceptance Criteria:**
  1. Backtest artifact includes config digest + code revision + snapshot IDs.
  2. Deterministic seed policy is explicit for stochastic components.
  3. Research run captures hypothesis/test-count diary.
- **Verification Method:** Inspect backtest metadata and research manifests.
- **Dependencies:** R4.

### R8. Robustness suite (regime, cost stress, parameter stability, capacity)
- **Statement:** Validation must include adversarial robustness, not only baseline expectancy.
- **Rationale (report):** lines 5, 205-211, 375, 405.
- **Acceptance Criteria:**
  1. Regime-partition and tail diagnostics generated.
  2. Cost stress scenarios generated.
  3. Parameter stability and capacity checks exist.
- **Verification Method:** Inspect robustness scripts and outputs.
- **Dependencies:** R1, R2, R3.

### R9. Deployment gates, monitoring, and kill-switch controls
- **Statement:** Promotion/deployment requires enforceable monitoring gates.
- **Rationale (report):** lines 185-189, 377-378, 405.
- **Acceptance Criteria:**
  1. Machine-readable go/no-go gate output exists.
  2. Feed health, execution drift, cost drift, risk drift checks are represented.
  3. Non-pass state can block downstream deployment stage.
- **Verification Method:** Inspect checklist/report pipeline and run orchestration behavior.
- **Dependencies:** R8.

### R10. Case-study strategy coverage
- **Statement:** Implement runnable strategy templates aligned to report case studies (carry, on-chain flow, microstructure, MEV-aware risk filter).
- **Rationale (report):** lines 223-345.
- **Acceptance Criteria:**
  1. Registry includes templates for carry/on-chain/microstructure families.
  2. MEV-aware execution-quality filter exists as strategy/overlay gate.
  3. Templates have tests proving instantiation and bounded outputs.
- **Verification Method:** Inspect strategy registry, adapters, alpha bundle, tests.
- **Dependencies:** R5, R8.

### R11. Survivorship-bias controls
- **Statement:** Backtests must account for changing tradable universe and delistings.
- **Rationale (report):** line 24 (“survivorship” pitfall).
- **Acceptance Criteria:**
  1. Universe construction is timestamped and auditable.
  2. Delisted/inactive assets are represented in historical eligibility.
  3. Reports expose universe membership by period.
- **Verification Method:** Inspect ingest/universe modules and report artifacts for membership history.
- **Dependencies:** R4.
