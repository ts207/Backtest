# Comprehensive Audit Packet: Backtest

Date: 2026-02-13
Repo: `Backtest/`
Baseline commit: `41a60d7`
Audit mode: deterministic local-first (no live network runs)

## 1) Executive Summary
This repository has solid overall structure and broad test coverage, but the current baseline is not release-ready because strategy-candidate generation is broken and CI is red. The most urgent issue is a runtime crash in strategy routing that breaks `build_strategy_candidates` and causes `8/102` tests to fail. Beyond that, there are high-impact consistency gaps in strategy ID routing, deterministic seed orchestration, and enforcement semantics for checklist gating.

## 2) Scope and Method
In scope:
- Static inspection of orchestration, ingest/clean/features/research/backtest/report paths.
- Deterministic validation via local pytest.
- Evidence-backed risk analysis and remediation plan.

Out of scope:
- Live exchange ingestion runs.
- Production deployment or runtime infra changes.

Commands run:
- `./.venv/bin/python -m pytest -q`
- `./.venv/bin/python -m pytest --collect-only -q`
- targeted source inspection using `rg`, `nl`, `sed`.

## 3) System Understanding
### 3.1 High-level architecture
- Orchestrator: `project/pipelines/run_all.py`
- Stage groups: `project/pipelines/{ingest,clean,features,research,backtest,report,alpha_bundle}`
- Execution/runtime: `project/engine/runner.py`, `project/engine/pnl.py`
- Strategy layer: `project/strategies/*` and adapters in `project/strategies/adapters.py`
- Run artifacts: `data/runs/<run_id>`, `data/reports/...`, `data/lake/...`

Codebase footprint snapshot:
- Tests: `36` files
- Pipeline modules: `50` files
- Strategy modules: `9` files
- Engine modules: `3` files
- Pytest collection: `102` tests

### 3.2 Canonical runtime contract (observed)
Core stage ordering and contracts are implemented in `project/pipelines/run_all.py:15`, `project/pipelines/run_all.py:241`, `project/pipelines/run_all.py:388`, `project/pipelines/run_all.py:435`, `project/pipelines/run_all.py:485`, `project/pipelines/run_all.py:494`, `project/pipelines/run_all.py:514`, `project/pipelines/run_all.py:530`.

Key orchestration characteristics:
- Cross-venue spot auto-enablement when phase2 includes `cross_venue_desync`: `project/pipelines/run_all.py:131`.
- Per-stage logging contract via optional `--log_path`: `project/pipelines/run_all.py:33`, `project/pipelines/run_all.py:47`.
- Checklist stage is explicitly non-fatal when it returns `1`: `project/pipelines/run_all.py:51`.

## 4) Validation Baseline
Baseline command:
- `./.venv/bin/python -m pytest -q`

Result:
- Passed: `94`
- Failed: `8`
- Total: `102`

Failure cluster:
- All failing tests are in `tests/test_build_strategy_candidates.py`.
- Primary crash: `NameError: EVENT_BASE_STRATEGY_MAP is not defined` from `project/pipelines/research/build_strategy_candidates.py:148`.

## 5) Findings (Ordered by Severity)

### Critical
1. Strategy builder crashes at runtime due to undefined routing map constant.
- Evidence: `project/pipelines/research/build_strategy_candidates.py:148`
- Impact: `build_strategy_candidates` fails hard, which breaks default pipeline flows where strategy builder is enabled (`project/pipelines/run_all.py:101`, `project/pipelines/run_all.py:494`).
- Verification: `./.venv/bin/python -m pytest -q tests/test_build_strategy_candidates.py`

2. Route function has invalid fallback type and unreachable logic.
- Evidence: `project/pipelines/research/build_strategy_candidates.py:150`, `project/pipelines/research/build_strategy_candidates.py:151`, `project/pipelines/research/build_strategy_candidates.py:185`
- Impact: even after fixing constant name, returning a string where dict is expected will break indexing (`route["execution_family"]`). Unreachable return indicates dead code and inconsistent intended behavior.
- Verification: add/execute a unit test for unknown event family route type and candidate output schema.

3. Strategy routing map is inconsistent with registry-backed executable IDs.
- Evidence: routing map uses `carry_imbalance_v1` and `spread_dislocation_v1` in `project/pipelines/research/build_strategy_candidates.py:49`, `project/pipelines/research/build_strategy_candidates.py:57`; registry publishes `carry_funding_v1` and `spread_desync_v1` in `project/strategies/registry.py:15`, `project/strategies/registry.py:17`; adapter-backed strategy IDs are in `project/strategies/adapters.py:57`.
- Impact: incorrect `base_strategy` generation, wrong `backtest_ready`, and invalid manual command suggestions.
- Verification: `./.venv/bin/python -m pytest -q tests/test_build_strategy_candidates.py`

4. Strategy-builder tests contain internal contradictions and one invalid test body.
- Evidence: undefined `payload` in `tests/test_build_strategy_candidates.py:226`; conflicting expected values in `tests/test_build_strategy_candidates.py:261` and `tests/test_build_strategy_candidates.py:266`.
- Impact: noisy/incorrect CI signals and ambiguous intended fail-closed behavior for unknown events.
- Verification: same targeted test run after test repairs.

### High
1. Checklist go/no-go is intentionally non-blocking at orchestration layer.
- Evidence: non-fatal allowlist in `project/pipelines/run_all.py:51`; checklist returns non-zero for `KEEP_RESEARCH` in `project/pipelines/research/generate_recommendations_checklist.py:243`; behavior codified in `tests/test_run_all_phase2.py:34`.
- Impact: runs can continue to strategy-builder/backtest paths even when checklist says do not promote.
- Verification: `./.venv/bin/python -m pytest -q tests/test_run_all_phase2.py::test_recommendations_checklist_keep_research_is_non_fatal`

2. Deterministic seed policy is not centralized in orchestrator CLI.
- Evidence: stage-level seed usage exists (for example `project/pipelines/research/phase2_conditional_hypotheses.py:909`, `project/pipelines/research/analyze_vol_shock_relaxation.py:323`) but `run_all.py` has no top-level seed argument in CLI contract (`project/pipelines/run_all.py:66`).
- Impact: reproducibility depends on per-stage defaults; hard to re-run with globally pinned stochastic settings.
- Verification: static check plus run contract review.

3. Strategy-feature contract mismatch for spread strategy inputs.
- Evidence: spread strategy requires `basis_zscore` in `project/strategies/spread_desync_v1.py:18`, while `features_v1` output columns omit basis/spread z-score fields (`project/pipelines/features/build_features_v1.py:269`).
- Impact: strategy may degrade to fallback zero-signal behavior; diagnostics track missing features but execution still proceeds (`project/engine/runner.py:287`, `project/engine/runner.py:325`).
- Verification: backtest run with `spread_desync_v1` and inspect `missing_feature_columns` diagnostics.

### Medium
1. Requirement-level gap remains for explicit MEV-aware overlay and on-chain strategy template routing.
- Evidence: current strategy registry entries in `project/strategies/registry.py:13`; overlay registry validates edge specs but has no dedicated MEV overlay behavior (`project/strategies/overlay_registry.py:146` onward).
- Impact: strategy coverage remains partial relative to desired case-study breadth.
- Verification: inspect registry + adapter tests in `tests/test_strategy_adapters.py:42`.

2. Reproducibility metadata is strong but still misses orchestrator-wide deterministic random policy.
- Evidence: reproducibility metadata exists (`project/pipelines/backtest/backtest_strategies.py:350`, `project/pipelines/backtest/backtest_strategies.py:593`), but orchestration has no global seed propagation (`project/pipelines/run_all.py:66`).
- Impact: metadata captures config/code/data snapshot IDs, but not a single source of truth for RNG controls across stages.
- Verification: contract review + stage arg audit.

## 6) Requirement Coverage Snapshot
- R1 Net-of-cost accounting: Met (`project/pipelines/backtest/backtest_strategies.py:587`, `project/engine/pnl.py:55`)
- R2 OOS/walk-forward split handling: Met (`project/pipelines/research/analyze_conditional_expectancy.py:153`, `project/pipelines/research/phase2_conditional_hypotheses.py:1034`)
- R3 Multiplicity controls: Met (`project/pipelines/research/analyze_conditional_expectancy.py:398`, `project/pipelines/research/phase2_conditional_hypotheses.py:1081`)
- R4 Provenance and QA gating: Met (`project/pipelines/_lib/run_manifest.py:39`, `project/pipelines/features/build_features_v1.py:322`)
- R5 Mechanism-grounded features: Partial (spread basis mismatch)
- R6 Funding/borrow in PnL: Met (`project/engine/pnl.py:51`, `project/pipelines/backtest/backtest_strategies.py:414`)
- R7 Reproducibility/versioning: Partial (no global seed contract)
- R8 Robustness diagnostics: Met (`project/pipelines/research/validate_expectancy_traps.py:682`)
- R9 Enforceable deployment gating: Partial (checklist non-fatal)
- R10 Case-study strategy coverage: Partial (MEV/on-chain template gap)
- R11 Survivorship controls: Met (`project/pipelines/ingest/build_universe_snapshots.py:74`, `project/engine/runner.py:141`, `project/pipelines/report/make_report.py:386`)

## 7) Triage Table for Current Test Failures
| Test group | Root cause | Severity | Fix target |
|---|---|---|---|
| `test_build_strategy_candidates_*` (7 tests) | Runtime crash from undefined routing constant and invalid route fallback logic | Critical | `project/pipelines/research/build_strategy_candidates.py` |
| `test_build_strategy_candidates_unknown_event_does_not_fallback_to_breakout` | Test uses undefined local variable (`payload`) and does not execute main flow | Critical | `tests/test_build_strategy_candidates.py` |

## 8) Remediation Backlog (Decision-Complete)
### Phase 0: Restore correctness and CI immediately
1. Replace `EVENT_BASE_STRATEGY_MAP` references with a single canonical mapping object and return type `Optional[Dict[str, str]]` from `_route_event_family`.
- Files: `project/pipelines/research/build_strategy_candidates.py`
- Acceptance: no route-type runtime errors; unknown events return `None` and produce fail-closed unmapped candidate payload.

2. Align event-family base strategy IDs to actual executable/adapter IDs from registry.
- Files: `project/pipelines/research/build_strategy_candidates.py`, `project/strategies/registry.py`, `project/strategies/adapters.py`
- Acceptance: generated `base_strategy` values are registry-resolvable where intended.

3. Repair and normalize strategy-builder tests to assert one coherent unknown-event contract.
- Files: `tests/test_build_strategy_candidates.py`
- Acceptance: no undefined variables; no contradictory assertions; explicit fail-closed expectations.

4. Run focused and full validation.
- Commands:
  - `./.venv/bin/python -m pytest -q tests/test_build_strategy_candidates.py`
  - `./.venv/bin/python -m pytest -q`
- Acceptance: all tests green.

### Phase 1: Reliability hardening
1. Add optional strict checklist enforcement in `run_all.py`.
- Introduce CLI flag (default preserves existing behavior) to fail pipeline on checklist `KEEP_RESEARCH`.
- Files: `project/pipelines/run_all.py`, tests in `tests/test_run_all_phase2.py`.

2. Add orchestrator-level seed contract.
- Add `--seed` in `run_all.py` and propagate to all stochastic stages that accept seed.
- Files: `project/pipelines/run_all.py`, relevant research stage scripts/tests.

3. Resolve spread feature contract gap.
- Either compute basis/spread z-score fields in features pipeline or adjust spread strategy required_features to match guaranteed upstream fields.
- Files: `project/pipelines/features/build_features_v1.py`, `project/strategies/spread_desync_v1.py`, tests.

### Phase 2: Strategic coverage and governance
1. Add MEV-aware execution overlay with explicit binding behavior and tests.
- Files: `project/strategies/overlay_registry.py` and/or concrete overlay implementation path, tests.

2. Add on-chain strategy template route (not only signal generation).
- Files: `project/strategies/registry.py`, adapter layer, strategy-builder routing, tests.

## 9) Verification Plan
Required commands after Phase 0:
- `./.venv/bin/python -m pytest -q tests/test_build_strategy_candidates.py`
- `./.venv/bin/python -m pytest -q`

Required commands after Phase 1:
- `./.venv/bin/python -m pytest -q tests/test_run_all_phase2.py`
- `./.venv/bin/python -m pytest -q tests/test_strategy_adapters.py`

Optional smoke check after fixes:
- `./.venv/bin/python project/pipelines/run_all.py --run_id audit_smoke --symbols BTCUSDT,ETHUSDT --start 2024-01-01 --end 2024-01-07 --run_phase2_conditional 1 --phase2_event_type vol_shock_relaxation --run_strategy_builder 1 --run_backtest 0 --run_make_report 0`

## 10) Assumptions
- Existing local change in `gap_matrix.md` is unrelated and was not modified by this audit packet.
- No live data ingestion was executed; all conclusions are from static analysis + deterministic local tests.
- Requirement status labels are based on repository evidence as of commit `41a60d7` and test baseline run on 2026-02-13.
