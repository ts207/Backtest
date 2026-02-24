from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.research import hypothesis_spec_translator as hst


def test_load_active_hypothesis_specs_filters_to_active(tmp_path):
    spec_dir = tmp_path / "spec" / "hypotheses"
    spec_dir.mkdir(parents=True)
    (spec_dir / "a.yaml").write_text(
        yaml.safe_dump(
            {
                "hypothesis_id": "H_ACTIVE",
                "version": 1,
                "status": "active",
                "scope": {"conditioning_features": ["vol_regime"]},
            }
        ),
        encoding="utf-8",
    )
    (spec_dir / "b.yaml").write_text(
        yaml.safe_dump(
            {
                "hypothesis_id": "H_PLANNED",
                "version": 1,
                "status": "planned",
                "scope": {"conditioning_features": ["carry_state"]},
            }
        ),
        encoding="utf-8",
    )
    (spec_dir / "template_verb_lexicon.yaml").write_text("kind: template_verb_lexicon\n", encoding="utf-8")

    specs = hst.load_active_hypothesis_specs(tmp_path)
    assert [s["hypothesis_id"] for s in specs] == ["H_ACTIVE"]
    assert specs[0]["conditioning_features"] == ["vol_regime"]


def test_translate_candidate_hypotheses_emits_executable_row():
    specs = [
        {
            "hypothesis_id": "H_ACTIVE",
            "version": 1,
            "spec_path": "spec/hypotheses/a.yaml",
            "conditioning_features": ["vol_regime", "carry_state"],
            "metric": "lift_bps",
            "output_schema": ["lift_bps", "p_value", "q_value", "n", "effect_ci", "stability_score", "net_after_cost"],
        }
    ]
    side_policy = {"mean_reversion": "contrarian"}
    base = {
        "event_type": "VOL_SHOCK",
        "rule_template": "mean_reversion",
        "horizon": "5m",
        "entry_lag_bars": 0,
        "symbol": "BTCUSDT",
        "conditioning": {"vol_regime": "high"},
        "template_id": "CL_1@VOL_SHOCK",
        "state_id": None,
    }
    rows, audit = hst.translate_candidate_hypotheses(
        base_candidate=base,
        hypothesis_specs=specs,
        available_condition_keys={"vol_regime", "carry_state", "funding_rate_bps"},
        template_side_policy=side_policy,
        strict=True,
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["hypothesis_id"] == "H_ACTIVE"
    assert row["template_id"] == "mean_reversion"
    assert row["horizon_bars"] == 1
    assert row["direction_rule"] == "contrarian"
    assert row["condition_signature"] == "vol_regime=high"
    assert row["candidate_id"]
    assert row["hypothesis_output_schema"] == [
        "lift_bps",
        "p_value",
        "q_value",
        "n",
        "effect_ci",
        "stability_score",
        "net_after_cost",
    ]
    assert audit and audit[0]["status"] == "executed"


def test_translate_candidate_hypotheses_strict_fails_on_missing_spec_condition_key():
    specs = [
        {
            "hypothesis_id": "H_ACTIVE",
            "version": 1,
            "spec_path": "spec/hypotheses/a.yaml",
            "conditioning_features": ["vol_regime", "carry_state"],
            "metric": "lift_bps",
            "output_schema": ["lift_bps"],
        }
    ]
    base = {
        "event_type": "VOL_SHOCK",
        "rule_template": "mean_reversion",
        "horizon": "5m",
        "entry_lag_bars": 0,
        "symbol": "BTCUSDT",
        "conditioning": {"vol_regime": "high"},
        "template_id": "CL_1@VOL_SHOCK",
        "state_id": None,
    }
    with pytest.raises(ValueError, match="missing required conditioning keys"):
        hst.translate_candidate_hypotheses(
            base_candidate=base,
            hypothesis_specs=specs,
            available_condition_keys={"vol_regime"},
            template_side_policy={},
            strict=True,
        )
