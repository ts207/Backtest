# Hypothesis Generator

`project/pipelines/research/generate_hypothesis_queue.py` builds auditable, pre-event hypotheses from dataset structure.

## Design Constraints

- No PnL/return/sharpe optimization in generation.
- Uses pre-event conditioning only.
- Emits finite candidate sets with explicit controls.
- Intended as proposal layer, not validation layer.

Validation remains in phase-1 and phase-2 stages.
When the queue exists for a run, phase-2 uses queue metadata (`target_phase2_event_types`) to attach proposal lineage to evaluated candidates.

## Command

```bash
./.venv/bin/python project/pipelines/research/generate_hypothesis_queue.py \
  --run_id RUN \
  --symbols BTCUSDT,ETHUSDT \
  --datasets auto \
  --max_fused 24
```

## Output Contract

Writes to:
- `data/reports/hypothesis_generator/<run_id>/`

Typical files:
- `dataset_introspection.json`
- `template_hypotheses.json`
- `fusion_hypotheses.json`
- `hypothesis_candidates.json`
- `phase1_hypothesis_queue.jsonl`
- `phase1_hypothesis_queue.csv`
- `summary.md`

Queue rows include `target_phase2_event_types` so each proposal maps to one or more phase-2 event families.

## Operational Use

1. Generate queue.
2. Run phase-1/phase-2 discovery with same `run_id` and symbols.
3. Compare generated hypotheses against promoted candidates.
