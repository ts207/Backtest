# Dataset-To-Mechanism Hypothesis Generator

This module implements a strict hybrid workflow:

1. Datasets may propose mechanisms.
2. Phase-1/Phase-2 remain the sole validation gate.
3. No outcome-based optimization is allowed in generation.

## Entry Point

```bash
python3 project/pipelines/research/generate_hypothesis_queue.py \
  --run_id 20260210_000001 \
  --symbols BTCUSDT,ETHUSDT \
  --datasets auto \
  --max_fused 24
```

## Design Guarantees

- No return/PnL/sharpe inputs are used.
- Horizon is fixed to coarse buckets: `short` or `medium`.
- Every candidate includes predefined negative controls.
- Only pre-event conditioning is allowed.
- Output is finite and auditable.

## Templates Implemented

- `T1_forced_participation_constraint`
- `T2_latency_synchronization_failure`
- `T3_capacity_saturation_liquidity_discontinuity`
- `T4_crowding_constraint_unwind`
- `T5_information_release_asymmetry`

## Fusion Operators Implemented

- `F1_trigger_plus_context`
- `F2_confirmation`
- `F3_causal_chain`
- `F4_cross_domain_sync`
- `F5_triangulated_gating`

## Output Contract

- `dataset_introspection.json`
- `template_hypotheses.json`
- `fusion_hypotheses.json`
- `hypothesis_candidates.json`
- `phase1_hypothesis_queue.jsonl`
- `phase1_hypothesis_queue.csv`
- `summary.md`

All outputs are written under:

`data/reports/hypothesis_generator/<run_id>/`
