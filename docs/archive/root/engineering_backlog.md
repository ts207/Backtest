# Engineering Backlog (Missing + Tier1/2 Partial)

Issue: R2 lacks explicit walk-forward train/validation/test splits and OOS-only promotion reporting.

:::task-stub{title="Add walk-forward split framework and OOS-tagged evaluation outputs"}
1. In `project/pipelines/research/analyze_conditional_expectancy.py`, add a time-based split constructor that emits split labels (`train`, `validation`, `test`) for each symbol/event row.
2. In `project/pipelines/research/phase2_conditional_hypotheses.py`, require candidate scoring to consume split labels and persist per-split metrics in `phase2_candidates.csv` and `phase2_manifests.json`.
3. Add an OOS-only promotion gate: candidate promotion must depend on validation/test metrics, not in-sample-only performance.
4. Update `project/pipelines/research/validate_expectancy_traps.py` to include split-level leakage/overlap diagnostics and explicit embargo window config.
5. Add tests in `tests/test_phase2_conditional_hypotheses.py` and `tests/test_validate_expectancy_traps.py` that fail when split labels are absent or promotion uses in-sample-only metrics.
6. Definition of done: artifacts contain split columns and OOS metrics; tests verify gate behavior.
:::

Issue: R3 has no multiplicity-adjusted significance gate for candidate promotion.

:::task-stub{title="Implement multiplicity-aware promotion metrics in phase2"}
1. In `project/pipelines/research/phase2_conditional_hypotheses.py`, compute adjusted significance from candidate test set (for example BH/FDR-adjusted p-values or equivalent correction field).
2. Persist both raw and adjusted metrics per candidate in `phase2_candidates.csv`.
3. Add new manifest fields in `phase2_manifests.json` for total hypotheses tested and adjusted pass count.
4. Change `gate_all` logic to require adjusted metric threshold pass.
5. Add regression tests in `tests/test_phase2_conditional_hypotheses.py` asserting adjusted fields exist and gate behavior changes when multiplicity increases.
6. Definition of done: promotion depends on adjusted metric; test-count and correction fields are present in outputs.
:::

Issue: R4 provenance is partial (source/vendor/schema version metadata is not complete in run artifacts).

:::task-stub{title="Extend run manifests with source provenance and schema version IDs"}
1. Update stage parameter/input payloads in `project/pipelines/clean/build_cleaned_15m.py` and `project/pipelines/features/build_features_v1.py` to include source identifiers (exchange/vendor), schema version/hash, and extraction window.
2. Extend manifest writing via `project/pipelines/_lib/run_manifest.py` call sites so provenance fields are always populated for inputs.
3. Add validation that missing provenance metadata fails the stage before finalize.
4. Add tests in `tests/` for clean/features stages to assert required provenance keys exist in generated run manifest JSON.
5. Definition of done: manifests under `data/runs/<run_id>/` contain consistent provenance blocks across stages.
:::

Issue: R5 feature coverage is incomplete for OI/liquidation/revision-lag integration.

:::task-stub{title="Integrate OI/liquidation and revision-lag-aware features into core feature pipeline"}
1. Extend `project/pipelines/features/build_features_v1.py` to ingest OI/liquidation inputs (where available) and emit normalized feature columns.
2. Add optional revision-lag parameters for revisable sources and persist the lag used in output metadata.
3. Update downstream consumption in `project/pipelines/research/analyze_conditional_expectancy.py` to include new fields in condition generation.
4. Add schema contract tests in `tests/test_*features*.py` (or create new test file) validating presence and null-handling of new columns.
5. Definition of done: feature artifacts include documented OI/liquidation/revision-lag fields and downstream research code references them.
:::

Issue: R6 derivatives PnL lacks funding-transfer and borrow-cost modeling (Tier 1 validity).

:::task-stub{title="Add funding and borrow components to derivatives PnL engine"}
1. Modify `project/engine/pnl.py` to support optional funding and borrow input series and compute separate PnL components.
2. Update `project/engine/runner.py` to pass aligned funding/borrow series into PnL computation when strategy family is carry/derivatives.
3. Extend `project/pipelines/backtest/backtest_strategies.py` metrics payload (`cost_decomposition`) to include `funding_pnl` and `borrow_cost`.
4. Ensure carry strategies in `project/strategies/` document required inputs and fallback behavior when funding/borrow is missing.
5. Add tests under `tests/` for PnL decomposition correctness with synthetic funding and borrow scenarios.
6. Definition of done: backtest metrics show funding/borrow components and net PnL reconciles component sum.
:::

Issue: R8 robustness lacks parameter-stability and capacity estimation checks.

:::task-stub{title="Add parameter-stability and capacity diagnostics to robustness suite"}
1. In `project/pipelines/research/validate_expectancy_traps.py`, add parameter perturbation sweeps around key thresholds and persist stability metrics (rank consistency / performance decay).
2. In `project/pipelines/backtest/backtest_strategies.py` or a new research stage, add capacity estimation using turnover/participation proxies and depth assumptions.
3. Add failure gates in `generate_recommendations_checklist.py` requiring minimum stability/capacity thresholds.
4. Add tests validating new robustness JSON sections and checklist gate integration.
5. Definition of done: robustness artifact contains stability and capacity sections; checklist decision responds to their pass/fail state.
:::

Issue: R10 lacks explicit MEV-aware risk filter and microstructure execution template coverage.

:::task-stub{title="Add MEV-aware execution-risk overlay and microstructure strategy template"}
1. Implement a new overlay in `project/strategies/overlay_registry.py` that reduces size/increases slippage assumptions when MEV-risk metric exceeds threshold.
2. Add microstructure-oriented strategy template (order-book imbalance family) under `project/strategies/` with clear input contracts.
3. Register new strategy/overlay IDs in `project/strategies/registry.py` and any adapter registry wiring.
4. Update `project/pipelines/research/build_strategy_candidates.py` routing to map relevant event families to new template IDs.
5. Add adapter/registry tests in `tests/test_strategy_adapters.py` for instantiation and bounded outputs.
6. Definition of done: new IDs are routable, tested, and reflected in candidate generation outputs.
:::

Issue: R11 survivorship-bias handling is missing (Tier 1 validity).

:::task-stub{title="Implement historical universe snapshots to prevent survivorship bias"}
1. Add a universe snapshot artifact (timestamped eligible symbols with listing/delisting intervals) under a new ingest/metadata stage in `project/pipelines/ingest/`.
2. Modify `project/pipelines/run_all.py` and backtest load path(s) to join strategy evaluation data with time-valid universe eligibility.
3. Ensure delisted/inactive assets remain represented in historical periods where applicable.
4. Add reporting outputs in `project/pipelines/report/` showing universe membership over time for each run.
5. Add tests ensuring backtest excludes assets outside eligibility windows and includes historical delisted periods correctly.
6. Definition of done: run artifacts include universe history and backtest results are eligibility-aware.
:::
