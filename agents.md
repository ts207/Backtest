# Agents Requirements

## Assumptions

- A1. “agents.md” means an agent-ownership spec that operationalizes `report_requirements.md`.
- A2. Agent names are logical roles and may map to one or more concrete modules or teams.
- A3. Verification commands reuse existing repository entry points and test suite.
- A4. Dependencies may reference requirements in this file and/or `report_requirements.md`.

## Requirements

### R1. Orchestrator Agent contract enforcement
- Short statement: The Orchestrator Agent shall block pipeline progression unless run contract fields are complete and valid.
- Acceptance criteria:
  - Run initialization validates symbols, venues, instruments, and date bounds.
  - Missing required contract fields fail with actionable errors.
  - Resolved run contract is persisted to run artifacts.
- Verification method (commands/tests):
  - `make run RUN_ID=agent_r1 SYMBOLS=BTCUSDT START=2024-01-01 END=2024-01-07`
  - `./.venv/bin/python -m pytest -q`
- Dependencies:
  - `report_requirements.md` R1.

### R2. Data QA Agent provenance gate
- Short statement: The Data QA Agent shall validate timestamp units/timezones, continuity gaps, and schema drift before feature generation.
- Acceptance criteria:
  - Run-scoped QA report includes unit, timezone, gap, and schema checks.
  - Critical QA errors stop downstream stages.
  - Data source provenance is recorded per dataset.
- Verification method (commands/tests):
  - `make run RUN_ID=agent_r2 SYMBOLS=BTCUSDT,ETHUSDT START=2024-01-01 END=2024-01-31`
  - `./.venv/bin/python -m pytest -q tests`
- Dependencies:
  - R1
  - `report_requirements.md` R2.

### R3. Feature Agent mechanism-first feature pack
- Short statement: The Feature Agent shall produce mechanism-grounded features (funding, basis, OI/liquidation, regime flags) with explicit lag controls.
- Acceptance criteria:
  - Features are generated for funding/basis/OI/liquidation where data is available.
  - Revision-lag parameters are configurable and logged in metadata.
  - Feature generation fails fast on missing required upstream fields.
- Verification method (commands/tests):
  - `make run RUN_ID=agent_r3 SYMBOLS=BTCUSDT,ETHUSDT START=2024-01-01 END=2024-02-01`
  - `./.venv/bin/python -m pytest -q`
- Dependencies:
  - R1, R2
  - `report_requirements.md` R3.

### R4. Execution Model Agent cost realism
- Short statement: The Execution Model Agent shall model maker/taker fees, slippage, and impact in net performance.
- Acceptance criteria:
  - Net PnL decomposition exposes fees/slippage/impact and gross PnL.
  - Venue-tier fee configuration is supported.
  - Stressable cost multipliers are configurable per run.
- Verification method (commands/tests):
  - `make discover-hybrid-backtest RUN_ID=agent_r4 STRATEGIES=vol_compression_v1`
  - `./.venv/bin/python -m pytest -q`
- Dependencies:
  - R3
  - `report_requirements.md` R4.

### R5. Reproducibility Agent deterministic runs
- Short statement: The Reproducibility Agent shall make runs reproducible via immutable inputs, config digests, and fixed seeds.
- Acceptance criteria:
  - Two equivalent runs produce consistent summary metrics within tolerance.
  - Artifacts include config hash, data snapshot ID, and code revision.
  - Run metadata captures tested hypothesis identifiers.
- Verification method (commands/tests):
  - `make discover-hybrid-backtest RUN_ID=agent_r5a STRATEGIES=vol_compression_v1`
  - `make discover-hybrid-backtest RUN_ID=agent_r5b STRATEGIES=vol_compression_v1`
  - `./.venv/bin/python -m pytest -q`
- Dependencies:
  - R2, R3, R4
  - `report_requirements.md` R5.

### R6. Validation Agent time-series integrity
- Short statement: The Validation Agent shall enforce time-respecting split logic and leakage checks.
- Acceptance criteria:
  - Walk-forward or time-series split artifacts are generated for discovery and backtest.
  - Leakage tests detect feature/label misalignment.
  - Out-of-sample metrics are clearly separated in reports.
- Verification method (commands/tests):
  - `make discover-hybrid RUN_ID=agent_r6`
  - `./.venv/bin/python -m pytest -q`
- Dependencies:
  - R3, R5
  - `report_requirements.md` R6.

### R7. Significance Agent multiplicity control
- Short statement: The Significance Agent shall apply multiple-testing adjustments before strategy promotion.
- Acceptance criteria:
  - Discovery output includes test count and adjusted significance fields.
  - Promotion rules reference adjusted metrics, not only raw metrics.
  - Reports show raw vs adjusted outcomes side by side.
- Verification method (commands/tests):
  - `make discover-hybrid RUN_ID=agent_r7`
  - `./.venv/bin/python -m pytest -q`
- Dependencies:
  - R6
  - `report_requirements.md` R7.

### R8. Robustness Agent stress/adversarial suite
- Short statement: The Robustness Agent shall run stress scenarios (fees/spreads/latency/outage/regime) and gate promotion on pass criteria.
- Acceptance criteria:
  - Robustness outputs exist per run and strategy.
  - At least one scenario each for cost widening, delay injection, and regime split is executed.
  - Failing robustness gates sets strategy status to rejected/not promotable.
- Verification method (commands/tests):
  - `make discover-hybrid RUN_ID=agent_r8`
  - `./.venv/bin/python -m pytest -q`
- Dependencies:
  - R4, R6, R7
  - `report_requirements.md` R8.

### R9. Strategy Agent phased edge rollout
- Short statement: The Strategy Agent shall prioritize carry/funding and basis strategies before L2 microstructure and MEV-sensitive strategies.
- Acceptance criteria:
  - Default strategy set reflects phased rollout priorities.
  - Phase progression criteria are documented and testable.
  - Later-phase strategies are disabled by default unless prerequisites pass.
- Verification method (commands/tests):
  - `make discover-hybrid-backtest RUN_ID=agent_r9 STRATEGIES=vol_compression_v1`
  - `./.venv/bin/python -m pytest -q`
- Dependencies:
  - R3, R4, R8
  - `report_requirements.md` R9, R10.

### R10. Risk Agent sizing and limits enforcement
- Short statement: The Risk Agent shall enforce leverage, liquidity, drawdown, and tail-risk caps during simulation and paper/live readiness checks.
- Acceptance criteria:
  - Risk cap breaches are logged with deterministic mitigation action.
  - Reports include cap utilization and breach summary.
  - Strategy gating includes risk-policy pass/fail status.
- Verification method (commands/tests):
  - `make discover-hybrid-backtest RUN_ID=agent_r10 STRATEGIES=vol_compression_v1`
  - `./.venv/bin/python -m pytest -q`
- Dependencies:
  - R4, R5, R6, R8
  - `report_requirements.md` R11, R12.

### R11. Reporting Agent go/no-go gate artifact
- Short statement: The Reporting Agent shall publish a run-scoped gate artifact with final go/no-go decision and failure reasons.
- Acceptance criteria:
  - Gate artifact includes validation, multiplicity, robustness, and risk statuses.
  - Decision is deterministic from recorded metrics and thresholds.
  - Artifact is emitted to run-scoped report path.
- Verification method (commands/tests):
  - `make discover-hybrid-backtest RUN_ID=agent_r11 STRATEGIES=vol_compression_v1`
  - `./.venv/bin/python -m pytest -q`
- Dependencies:
  - R6, R7, R8, R10
  - `report_requirements.md` R12.
