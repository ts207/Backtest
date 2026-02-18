# Codex Operating Instructions (Backtest Repo)

## Mission
Run bridge-first research-to-execution diagnostics and produce explicit funnel evidence for where candidates fail.

## Pinned Frozen Run (Default)
- `run_id`: `RUN_2022_2023`
- `symbols`: `BTCUSDT,ETHUSDT`
- `start`: `2021-01-01`
- `end`: `2022-12-31`

## Required Funnel Artifact
- Path: `data/reports/<run_id>/funnel_summary.json`
- Per-family required fields:
  - `phase2_candidates`
  - `phase2_gate_all_pass`
  - `bridge_evaluable`
  - `bridge_pass_val`
  - `compiled_bases`
  - `compiled_overlays`
  - `wf_tested`
  - `wf_survivors`
  - `top_failure_reasons`

## Bridge Policy (Canonical)
- Enforce in bridge stage:
  - `edge_to_cost >= 2.0` on validation
- Persist cost sweep columns on validation:
  - `exp_costed_x0_5`
  - `exp_costed_x1_0`
  - `exp_costed_x1_5`
  - `exp_costed_x2_0`
- `bridge_pass_val` must reflect `gate_bridge_tradable`.

## Overlay Policy
- Overlay candidates are delta-vs-base only.
- Overlay-only candidates must not appear as standalone executable strategies in backtest or walkforward.

## Execution Checklist
1. Run `project/pipelines/run_all.py` with bridge, walkforward, and promotion enabled for the frozen window.
2. Confirm `data/reports/<run_id>/funnel_summary.json` exists.
3. Confirm bridge outputs include cost sweep + edge-to-cost ratio.
4. Confirm promotion and walkforward consume only executable base strategies.

## Failure Pivot Rule
If `bridge_pass_val == 0` across all families, treat as “no executed edge at current costs” and pivot to semantic strategy changes (effect horizon, turnover reduction, de-clustering, hold-time alignment), not threshold relaxation.

## Guardrails
- Keep strict defaults (no non-production checklist bypass) for decision runs.
- Keep test split untouched for selection.
