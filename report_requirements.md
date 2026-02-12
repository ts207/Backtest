# Report Requirements

## Assumptions

- A1. `deep-research-report.md` is the source of truth and this document translates it into implementable requirements for the current `Backtest/` repository.
- A2. Phase 1 scope prioritizes derivatives carry/funding and basis strategies before L2 microstructure and MEV-heavy execution.
- A3. Initial implementation targets research and paper-trading readiness, not immediate production capital deployment.
- A4. Verification commands should prefer existing repository contracts (`make run`, `make discover-hybrid`, `make discover-hybrid-backtest`, `make test`).
- A5. External data provider selection (exact vendor mix) is configurable; requirements define capabilities, not mandatory commercial subscriptions.
- A6. Time windows, symbols, and run IDs in commands are examples and may be changed without invalidating requirements.

## Requirements

### R1. Define the trading game contract
- Short statement: The system shall define and persist a strategy contract per run: universe, venues, instruments, leverage/borrow/custody constraints, and PnL realization mechanics.
- Acceptance criteria:
  - A run-scoped config captures all contract fields before feature generation/backtest.
  - Backtest artifacts include a serialized copy of the resolved contract.
  - Missing contract fields fail fast with explicit error messages.
- Verification method (commands/tests):
  - `make run RUN_ID=req_r1 SYMBOLS=BTCUSDT,ETHUSDT START=2024-01-01 END=2024-01-31`
  - `./.venv/bin/python -m pytest -q`
- Dependencies:
  - None.

### R2. Enforce data audit and provenance gates
- Short statement: The pipeline shall validate timestamp units/timezones, sequence continuity, schema versions, and source provenance before downstream stages.
- Acceptance criteria:
  - Audit report exists per run and lists unit/timezone checks, missing intervals, and schema hash/version.
  - Known issues (unit shifts, missing snapshots, malformed rows) are surfaced and categorized.
  - Critical audit failures block backtest execution.
- Verification method (commands/tests):
  - `make run RUN_ID=req_r2 SYMBOLS=BTCUSDT START=2024-01-01 END=2024-01-07`
  - `./.venv/bin/python -m pytest -q tests`
- Dependencies:
  - R1.

### R3. Implement mechanism-grounded feature sets
- Short statement: The feature stage shall compute structurally grounded features (funding, basis, OI/liquidation, cost-aware regime filters) with explicit lag handling.
- Acceptance criteria:
  - Feature outputs include funding/basis/OI/liquidation features for supported symbols.
  - On-chain or vendor-labeled metrics support configurable revision lag and confirmation windows.
  - Feature metadata records source field mappings and transformation parameters.
- Verification method (commands/tests):
  - `make run RUN_ID=req_r3 SYMBOLS=BTCUSDT,ETHUSDT START=2024-01-01 END=2024-02-01`
  - `./.venv/bin/python -m pytest -q`
- Dependencies:
  - R1, R2.

### R4. Model transaction costs and execution frictions
- Short statement: Backtests shall include explicit maker/taker fees, slippage, and impact assumptions, with venue-tier sensitivity.
- Acceptance criteria:
  - PnL decomposition includes gross alpha, fees, slippage, impact, and net alpha.
  - Fee schedules are configurable by venue and tier.
  - Cost stress parameters (spread widening/slippage multiplier) are supported.
- Verification method (commands/tests):
  - `make discover-hybrid-backtest RUN_ID=req_r4 STRATEGIES=vol_compression_v1`
  - `./.venv/bin/python -m pytest -q`
- Dependencies:
  - R1, R2, R3.

### R5. Support deterministic and reproducible backtest runs
- Short statement: Every run shall be deterministic and reproducible from immutable inputs and versioned configs.
- Acceptance criteria:
  - Same run config and seed reproduce identical summary metrics within fixed tolerance.
  - Artifacts include dataset snapshot IDs, config digest, and code revision.
  - Pipeline logs hypothesis/test metadata for research traceability.
- Verification method (commands/tests):
  - `make discover-hybrid-backtest RUN_ID=req_r5a STRATEGIES=vol_compression_v1`
  - `make discover-hybrid-backtest RUN_ID=req_r5b STRATEGIES=vol_compression_v1`
  - `./.venv/bin/python -m pytest -q`
- Dependencies:
  - R1, R2, R3, R4.

### R6. Use time-respecting validation
- Short statement: Strategy evaluation shall use walk-forward/time-series splits with strict no-leakage policies.
- Acceptance criteria:
  - Validation outputs clearly separate train/validation/test windows by time.
  - Leakage checks exist for feature alignment and look-ahead conditions.
  - Reported performance includes out-of-sample metrics.
- Verification method (commands/tests):
  - `make discover-hybrid RUN_ID=req_r6`
  - `./.venv/bin/python -m pytest -q`
- Dependencies:
  - R3, R5.

### R7. Apply multiplicity and data-snooping controls
- Short statement: Discovery and strategy selection shall account for multiple testing and selection bias.
- Acceptance criteria:
  - Discovery outputs include test-count metadata and adjusted significance/gating fields.
  - Strategy promotion requires passing multiplicity-aware thresholds.
  - Reports expose both raw and adjusted decision metrics.
- Verification method (commands/tests):
  - `make discover-hybrid RUN_ID=req_r7`
  - `./.venv/bin/python -m pytest -q`
- Dependencies:
  - R5, R6.

### R8. Run robustness and adversarial stress suite
- Short statement: Candidate edges shall be stress-tested across spread/fee shocks, signal delays, outages, and regime partitions.
- Acceptance criteria:
  - Robustness outputs are generated per strategy/run with pass/fail gates.
  - Stress scenarios include at least cost-widening, latency delay, and regime split cases.
  - Failing stress gates prevents strategy promotion.
- Verification method (commands/tests):
  - `make discover-hybrid RUN_ID=req_r8`
  - `./.venv/bin/python -m pytest -q`
- Dependencies:
  - R4, R6, R7.

### R9. Deliver budget-first edge pipeline
- Short statement: The default research path shall prioritize carry/funding + basis + regime filters before microstructure-heavy strategies.
- Acceptance criteria:
  - A documented default path exists in configs or docs for phased research execution.
  - Phase 1 can run using low-cost/community-access datasets.
  - Phase 2/3 (L2 microstructure, MEV-aware filters) are optional and explicitly gated.
- Verification method (commands/tests):
  - `make discover-hybrid-backtest RUN_ID=req_r9 STRATEGIES=vol_compression_v1`
  - `./.venv/bin/python -m pytest -q`
- Dependencies:
  - R2, R3, R4, R6.

### R10. Add strategy templates from case studies
- Short statement: Repository shall include reusable templates for funding carry, on-chain netflow filter, order-book imbalance scalp, and MEV-risk filter logic.
- Acceptance criteria:
  - Strategy template modules/configs exist with clear parameterization.
  - Templates include documented failure modes and risk controls.
  - At least one template is runnable end-to-end through report generation.
- Verification method (commands/tests):
  - `make discover-hybrid-backtest RUN_ID=req_r10 STRATEGIES=vol_compression_v1`
  - `./.venv/bin/python -m pytest -q`
- Dependencies:
  - R3, R4, R6, R8, R9.

### R11. Enforce risk sizing and portfolio caps
- Short statement: Position sizing shall combine edge strength with volatility/tail-risk controls and liquidity/leverage caps.
- Acceptance criteria:
  - Risk policy config includes max leverage, liquidity participation cap, and drawdown/tail controls.
  - Backtest/report outputs include risk utilization and breach events.
  - Breach handling (reduce size/flat) is deterministic and testable.
- Verification method (commands/tests):
  - `make discover-hybrid-backtest RUN_ID=req_r11 STRATEGIES=vol_compression_v1`
  - `./.venv/bin/python -m pytest -q`
- Dependencies:
  - R4, R5, R6.

### R12. Provide deployment gates and live monitoring hooks
- Short statement: Promotion to paper/live shall require gate checks and monitorable health metrics.
- Acceptance criteria:
  - A formal go/no-go checklist includes walk-forward stability, multiplicity gate, stress pass, and execution parity checks.
  - Monitoring outputs cover feed health, fill drift, cost drift, and risk drift.
  - Gate status is written to run-scoped artifacts.
- Verification method (commands/tests):
  - `make discover-hybrid-backtest RUN_ID=req_r12 STRATEGIES=vol_compression_v1`
  - `./.venv/bin/python -m pytest -q`
- Dependencies:
  - R7, R8, R10, R11.
