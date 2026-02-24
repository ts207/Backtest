from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.research.compile_strategy_blueprints import _build_blueprint


def test_build_blueprint_records_bridge_embargo_in_lineage():
    row = {
        "candidate_id": "cand_1",
        "source_path": "data/reports/phase2/r1/vol_shock_relaxation/phase2_candidates.csv",
        "promotion_track": "standard",
        "bridge_embargo_days_used": 3,
        "n_events": 150,
        "condition": "all",
        "action": "long",
        "event": "vol_shock_relaxation",
    }

    bp = _build_blueprint(
        run_id="r1",
        run_symbols=["BTCUSDT"],
        event_type="vol_shock_relaxation",
        row=row,
        phase2_lookup={},
        stats={},
        fees_bps=2.0,
        slippage_bps=4.0,
        min_events=100,
        cost_config_digest="digest",
    )

    assert bp.lineage.bridge_embargo_days_used == 3
