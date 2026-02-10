# Backtest Repository Recommendations (Actionable Next Steps)

## 1) Immediate stabilization (next 1–2 PRs)

1. **Lock warning regressions in CI**
   - Run a dedicated warning-sensitive test target:
     - `python3 -m pytest -q tests/test_warning_hardening.py`
   - Add a CI gate that fails if this file fails.
   - Goal: prevent reintroduction of pandas warning paths already fixed.

2. **Add a warning budget check for touched modules**
   - On PRs touching `project/pipelines/_lib/validation.py`, `project/engine/runner.py`, or `project/pipelines/ingest/ingest_binance_um_funding.py`, run tests with warnings-as-errors for those code paths.
   - Goal: make deprecation/future drift visible at review time.

3. **Document pandas compatibility expectations**
   - Add a short “Dependency compatibility” section to README that states tested pandas range and warning policy.
   - Goal: reduce upgrade ambiguity for contributors.

## 2) Data/contract hardening (next 2–4 PRs)

1. **Schema contract tests for persisted outputs**
   - Add assertions for required columns and dtypes for:
     - funding raw partitions,
     - context features (`fp_*` columns),
     - engine strategy returns outputs.
   - Goal: prevent silent schema drift across dependency updates.

2. **Manifest quality checks**
   - Add tests that verify manifest fields are always present (`requested_*`, `effective_*`, counts, partitions written/skipped).
   - Goal: maintain auditability and reproducibility guarantees.

3. **Run-scoped-vs-fallback input precedence tests expansion**
   - Expand current run-scoped preference tests to multiple symbols and missing-partition edge cases.
   - Goal: ensure deterministic data source precedence under partial data conditions.

## 3) Research/strategy improvement loop (next milestone)

1. **Increase sample breadth**
   - Expand beyond BTC/ETH in controlled batches (high-liquidity perpetuals first).
   - Goal: verify edge persistence and reduce overfitting risk.

2. **Introduce fixed robustness split pack**
   - Standardize regime/time/symbol split reports for every candidate overlay.
   - Goal: make Phase-2 promotion evidence directly comparable run-to-run.

3. **Prioritize candidate promotion discipline**
   - Keep conditional/action caps strict and require explicit friction-floor plausibility in promotion checks.
   - Goal: preserve scientific gatekeeping before any optimization loops.

## 4) Suggested execution order

1. CI warning gate + warning-hardening test enforcement.
2. Schema/manifest contracts.
3. Multi-symbol robustness expansion.
4. Overlay promotion evidence depth improvements.

## Definition of Done for this recommendation cycle

- Warning-hardening tests remain green in CI.
- Full suite stays green.
- At least one schema contract test added for each critical output family.
- A single dashboard/report artifact summarizes run-over-run stability for candidate overlays.


## 5) Portfolio gate calibration workflow (new)

1. **Split portfolio policy profiles**
   - Keep strict production defaults in `project/configs/portfolio_production.yaml`.
   - Use `project/configs/portfolio_research.yaml` as an overlay for discovery runs.

2. **Run harshness sweeps on completed runs**
   - `python3 project/pipelines/research/sweep_portfolio_harshness.py --run_ids <id1,id2,...>`
   - Review `frontier.csv` and `report.md` under `reports/multi_edge_validation/harshness_sweep`.

3. **Promotion discipline**
   - Discovery can use research overlay.
   - Promotion must re-run `validate_multi_edge_portfolio.py` with strict production settings.


4. **Use 15m-signal / 4h-execution feasibility runs**
   - Keep features/signals on native 15m bars and set `--execution_decision_grid_hours 4` in `strategies_cost_report.py`.
   - Goal: suppress turnover without redefining feature semantics.

5. **Always pair 4h aggregation tests with semantic diagnostics**
   - Inspect `strategies_cost_report_diagnostics_*.json` for aggregation identity mismatch ratios and feature equivalence correlations (`rv_pct_2880`, `range_med_480`).
   - Goal: avoid false conclusions from non-equivalent timeframe transformations.


6. **Use bar-index execution masks for 4h throttle experiments**
   - Prefer `--timeframe 15m --execution_decision_grid_bars 16 --execution_decision_grid_offset_bars 0` for block-start sampling (`t % 16 == 0`).
   - Use `--execution_decision_grid_offset_bars 15` for end-of-block sampling (`t % 16 == 15`).
   - Goal: test execution throttling only, while keeping 15m signal semantics unchanged.
