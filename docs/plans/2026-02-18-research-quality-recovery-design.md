# Research Quality Recovery Design

**Context**
- Current pipeline already has core safeguards (walk-forward splits, multiplicity control, cost-aware gates), but strategy performance is still poor in discovery, candidate building, and downstream testing.
- `tradingresource.md` suggests proven quant tooling we can use to close gaps in diagnostics and selection quality (for example `empyrical-reloaded`, `alphalens-reloaded`, standardized factor/performance reporting).

**Primary Goal**
- Raise the quality of promoted strategies by improving candidate diagnostics, tightening OOS quality gates, and strengthening test contracts around discovery and strategy selection.

**Non-Goals**
- Rewriting the backtest engine.
- Adding live-trading adapters.
- Expanding to new exchanges before quality recovery on current Binance perp/spot flow.

**Approach Options Considered**
1. Quality-gate first (recommended): tighten discovery and selection contracts, then iterate on features.
2. Runtime-speed first: optimize throughput before research quality changes.
3. External benchmark first: integrate external frameworks before internal gate changes.

**Chosen Approach**
- Quality-gate first, with a small test-iteration speed track.
- Rationale: poor strategy outcomes are mostly a selection/validation problem. Better gating and diagnostics improve both model quality and engineering iteration quality.

**Architecture**
- Add a discovery-quality summary stage after phase2 to produce deterministic diagnostics (`gate_pass` rate, fail-reason distribution, event-family health, candidate concentration).
- Add composite quality scoring and stricter OOS gate checks in `phase2_conditional_hypotheses.py`.
- Update strategy candidate builder to enforce quality-ranked and diversity-aware selection.
- Extend walk-forward outputs with standardized metrics from `empyrical-reloaded` (fallback-safe if missing) for better promotion decisions.
- Improve test ergonomics and speed via explicit fast-test targets and focused regression contracts.

**Data Flow**
1. `run_all.py` executes phase2.
2. New `summarize_discovery_quality.py` reads phase2 artifacts and writes `data/reports/phase2/<run_id>/discovery_quality_summary.json`.
3. Strategy candidate builder consumes phase2 results with new `quality_score` and strict OOS fields.
4. Walk-forward emits standardized performance metrics consumed by promotion/reporting.
5. Tests assert contract invariants for each new artifact and gate.

**Error Handling**
- Missing phase2 artifacts: summary stage writes explicit status and exits non-zero with actionable message.
- Missing optional analytics dependency: walk-forward writes metrics using fallback calculations and sets provenance flag.
- Invalid metric columns: builder falls back to safe defaults and records reason in fail metadata.

**Testing Strategy**
- Test-first for every quality contract:
  - discovery summary contract tests,
  - phase2 quality score/gate tests,
  - strategy builder ranking/diversity tests,
  - walk-forward metric contract tests.
- Add fast test profile for local iteration; keep full `pytest -q` for final validation.

**Acceptance Criteria**
- Discovery summary produced for every run with deterministic schema.
- Phase2 output includes composite quality score and strict OOS gate flags.
- Strategy builder selects fewer but higher-quality candidates under explicit caps.
- Walk-forward output includes standardized return/risk metrics.
- New tests pass and prevent regression in these contracts.

