"""Test _choose_event_rows with require_positive_expectancy=False."""
import sys
sys.path.insert(0, 'project')

import pandas as pd
from pathlib import Path
from pipelines.research.compile_strategy_blueprints import (
    _choose_event_rows, _passes_quality_floor, _load_phase2_table
)

DATA_ROOT = Path('data')
run_id = 'atlas_loop_05'
event_type = 'LIQUIDATION_CASCADE'

edge = pd.read_csv(f'data/reports/edge_candidates/{run_id}/edge_candidates_normalized.csv')
event_edge_rows = edge[edge['event'] == event_type].to_dict(orient='records')

phase2_df = _load_phase2_table(run_id=run_id, event_type=event_type)

# Test with require_positive_expectancy=False directly in _passes_quality_floor
for r in event_edge_rows[:3]:
    p1 = _passes_quality_floor(r, strict_cost_fields=False, min_events=50, min_robustness=0.33, require_positive_expectancy=True)
    p2 = _passes_quality_floor(r, strict_cost_fields=False, min_events=50, min_robustness=0.33, require_positive_expectancy=False)
    print(f'require_pos=True: {p1}  require_pos=False: {p2}  rob={r["robustness_score"]:.3f}  n={r["n_events"]}')

print()
print('Calling _choose_event_rows with require_positive_expectancy=False...')
selected, diag, sel_df = _choose_event_rows(
    run_id=run_id,
    event_type=event_type,
    edge_rows=event_edge_rows,
    phase2_df=phase2_df,
    max_per_event=2,
    allow_fallback_blueprints=True,
    strict_cost_fields=False,
    min_events=50,
    min_robustness=0.33,
    require_positive_expectancy=False,
    expected_cost_digest=None,
    naive_validation={},
    allow_naive_entry_fail=True,
)
print(f'selected: {len(selected)} rows')
print(f'diag: {diag}')
if selected:
    for s in selected:
        print(f"  cid={s.get('candidate_id', '')[:60]}  rob={s.get('robustness_score')}  promotion_track={s.get('promotion_track')}")
