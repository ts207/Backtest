# RUN_2022_2023 Audit

**Scope:** full project audit (code, pipelines, artifacts, manifest) plus freeze-run validation per AGENTS instructions.

## Execution Evidence
- `./.venv/bin/python project/pipelines/run_all.py --run_id RUN_2022_2023 --symbols BTCUSDT,ETHUSDT --start 2021-01-01 --end 2022-12-31 --run_phase2_conditional 1 --phase2_event_type all --run_backtest 1 --run_walkforward_eval 1 --run_make_report 1 --force 1 --clean_engine_artifacts 1 --walkforward_allow_unexpected_strategy_files 0` produced `data/runs/RUN_2022_2023/run_manifest.json` with `checklist_decision=KEEP_RESEARCH`, `auto_continue_applied=true`, `non_production_overrides=[build_strategy_candidates:--ignore_checklist=1,--allow_non_promoted=1, compile_strategy_blueprints:--ignore_checklist=1,--allow_fallback_blueprints=1]`, and timing data showing walkforward (123s) and promotion (11s) recorded. Manifest still lacks `ended_at` even though `status=success`.
- After auto-continue the same run completed all 55 stages (log summaries around the 14:31 timestamp). Bridge/funnel artifacts materialized at `data/reports/RUN_2022_2023/funnel_summary.json` and `data/reports/phase2/RUN_2022_2023/*/phase2_candidates.csv` with per-family gate counts and expense columns.

## Artifact Compliance
- Funnel contract satisfied: each of the nine families in `data/reports/RUN_2022_2023/funnel_summary.json` records `phase2_candidates`, `phase2_gate_all_pass`, `bridge_evaluable`, `bridge_pass_val`, `compiled_bases`, `compiled_overlays`, `wf_tested`, `wf_survivors`, `top_failure_reasons`. Totals: `phase2_candidates=384`, `bridge_pass_val=171`, `compiled=0`, `wf_survivors=0`.
- Bridge cost sweeps recorded for every non-empty family in `data/reports/phase2/RUN_2022_2023/*/phase2_candidates.csv` and `data/reports/bridge_eval/RUN_2022_2023/*/bridge_candidate_metrics.csv`; zero-row families still produce headers and placeholders.
- Walkforward/promotion evidence lives under `data/reports/eval/RUN_2022_2023/walkforward_summary.json` and `data/reports/promotions/RUN_2022_2023/promotion_report.json` (eight tested strategies, none promoted, fallback evidence not used).

## Code & Policy Findings

> **All three findings resolved as of 2026-02-19.** See commits on `main` branch.

- **FIXED** High: overlay-only rows still build standalone strategies and reach walkforward/promotion. `phase2_conditional_hypotheses.py:779-808` classifies `entry_gate_skip`/`risk_throttle_*` as `candidate_type="overlay"` with `_overlay_base_candidate_id`, yet `export_edge_candidates.py:150-198` writes them unchanged and `build_strategy_candidates.py:942-1040` plus `_build_edge_strategy_candidate` at `build_strategy_candidates.py:505-611` accepts every row without filtering, so overlays can run as full entries. **Fix:** added `candidate_type == "overlay"` guard in `build_strategy_candidates.py` inner loop; `skipped_overlay_count` tracked in `builder_diagnostics`.
- **FIXED** High: non-production override tracking never records flags such as `--allow_naive_entry_fail`, `--strategy_builder_allow_non_promoted`, `--walkforward_allow_unexpected_strategy_files`, `--promotion_allow_fallback_evidence`; only the auto-continue injection appends to `non_production_overrides` (`run_all.py:1097-1110`). Consequently, the manifest audit referenced in `docs/operator_runbook.md` and `README.md` can’t prove who turned off a gate. **Fix:** added startup-time flag scan in `run_all.py` that appends to `non_production_overrides` for all known bypass flags.
- **FIXED** Medium: manifest never records `ended_at`, so run completion is harder to verify (see `run_all.py:1049`, `run_all.py:1167`). **Fix:** `ended_at` initialized to `None` in manifest dict and set on all three exit paths (stage failure, checklist-blocked, success).

## Operational Notes
- Bridge gate enforcement is intact: `bridge_evaluate_phase2.py:21-247` computes `edge_to_cost` and gate status plus cost sweeps, and `summarize_discovery_quality.py:164-175` copies `gate_bridge_tradable` counts into `bridge_pass_val`. The funnel summary lists top failure reasons consistent with bridge gating (e.g., `gate_oos_consistency_strict`, `gate_bridge_edge_cost_ratio`).
- Promotion report contains `tested_count=8`, `survivors_count=0`, `evidence_mode_counts={'walkforward_strategy': 8}`; each strategy in `strategy_candidates.json` includes overlay-style IDs like `dsl_interpreter_v1__...__delay_30_all`.

## Next Steps
1. ~~Prevent overlay candidates from entering the builder/execution stream.~~ **DONE**
2. ~~Record every non-prod override flag in `non_production_overrides`.~~ **DONE**
3. ~~Populate `ended_at` in `run_manifest.json`.~~ **DONE**
4. Re-run the frozen canonical run (`RUN_2022_2023`) after code fixes to confirm no overlay strategies flow through promotions and no silent override usage remains. *(Manual step, ~2h, requires data lake populated.)*
