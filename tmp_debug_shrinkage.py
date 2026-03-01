import sys
from pathlib import Path
sys.path.insert(0, str(Path("/home/tstuv/workspace/backtest/project")))

from pipelines.research.phase2_candidate_discovery import _apply_hierarchical_shrinkage
import pandas as pd

df = pd.DataFrame(
    [
        {
            "symbol": "BTCUSDT",
            "canonical_family": "POSITIONING_EXTREMES",
            "canonical_event_type": "FUNDING_EXTREME_ONSET",
            "runtime_event_type": "funding_extreme_onset",
            "event_type": "FUNDING_EXTREME_ONSET",
            "template_verb": "mean_reversion",
            "rule_template": "mean_reversion",
            "horizon": "15m",
            "state_id": "CROWDING_STATE",
            "conditioning": "all",
            "expectancy": 0.10,
            "p_value": 0.001,
            "n_events": 10,
            "sample_size": 10,
            "std_return": 0.12,
        },
        {
            "symbol": "BTCUSDT",
            "canonical_family": "POSITIONING_EXTREMES",
            "canonical_event_type": "FUNDING_EXTREME_ONSET",
            "runtime_event_type": "funding_extreme_onset",
            "event_type": "FUNDING_EXTREME_ONSET",
            "template_verb": "mean_reversion",
            "rule_template": "mean_reversion",
            "horizon": "15m",
            "state_id": "POST_EXTREME_CARRY_STATE",
            "conditioning": "all",
            "expectancy": 0.02,
            "p_value": 0.001,
            "n_events": 500,
            "sample_size": 500,
            "std_return": 0.08,
        },
    ]
)

out = _apply_hierarchical_shrinkage(
    df,
    lambda_state=100.0,
    lambda_event=300.0,
    lambda_family=1000.0,
)

for c in ["effect_raw", "shrinkage_weight_state_group", "effect_shrunk_event", "effect_shrunk_state_group", "lambda_symbol_status", "shrinkage_weight_state", "effect_shrunk_state"]:
    print(f"{c}: {out[c].values}")
