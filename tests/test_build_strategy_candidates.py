import json
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines.research import build_strategy_candidates


def _write_edge_inputs(
    tmp_path: Path,
    run_id: str,
    event: str = "vol_shock_relaxation",
    condition: str = "all",
    action: str = "delay_30",
) -> None:
    edge_dir = tmp_path / "reports" / "edge_candidates" / run_id
    phase2_dir = tmp_path / "reports" / "phase2" / run_id / event
    edge_dir.mkdir(parents=True, exist_ok=True)
    phase2_dir.mkdir(parents=True, exist_ok=True)

    source_json = phase2_dir / "promoted_candidates.json"
    source_json.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "event_type": event,
                "candidates": [
                    {
                        "candidate_id": f"{event}_0",
                        "condition": condition,
                        "action": action,
                        "sample_size": 120,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    pd.DataFrame(
        [
            {
                "run_id": run_id,
                "event": event,
                "candidate_id": f"{event}_0",
                "status": "PROMOTED",
                "edge_score": 0.42,
                "expected_return_proxy": 0.01,
                "expectancy_per_trade": 0.01,
                "stability_proxy": 0.9,
                "robustness_score": 0.9,
                "event_frequency": 0.5,
                "capacity_proxy": 1.0,
                "profit_density_score": 0.0045,
                "n_events": 120,
                "source_path": str(source_json),
            }
        ]
    ).to_csv(edge_dir / "edge_candidates_normalized.csv", index=False)


def _write_promotion_inputs(
    tmp_path: Path,
    run_id: str,
    *,
    event: str = "vol_shock_relaxation",
    blueprint_id: str = "bp_promoted",
    candidate_id: str = "promoted_0",
    condition: str = "all",
    action: str = "delay_30",
    validation_score: float = 0.25,
) -> None:
    promo_dir = tmp_path / "reports" / "promotions" / run_id
    promo_dir.mkdir(parents=True, exist_ok=True)
    delay_bars = 0
    overlays = []
    if action.startswith("delay_"):
        delay_bars = int(action.split("_")[-1])
    elif action == "entry_gate_skip":
        overlays = [{"name": "risk_throttle", "params": {"size_scale": 0.0}}]
    elif action.startswith("risk_throttle_"):
        overlays = [{"name": "risk_throttle", "params": {"size_scale": float(action.split("_")[-1])}}]
    row = {
        "id": blueprint_id,
        "run_id": run_id,
        "event_type": event,
        "candidate_id": candidate_id,
        "symbol_scope": {"mode": "multi_symbol", "symbols": ["BTCUSDT", "ETHUSDT"], "candidate_symbol": "ALL"},
        "entry": {"conditions": [condition], "delay_bars": delay_bars},
        "overlays": overlays,
        "evaluation": {"min_trades": 100},
        "lineage": {"source_path": "x"},
    }
    (promo_dir / "promoted_blueprints.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")
    report = {
        "run_id": run_id,
        "tested": [
            {
                "blueprint_id": blueprint_id,
                "trades": 180,
                "split_pnl": {"train": 0.5, "validation": validation_score, "test": 0.1},
                "stressed_split_pnl": {"train": 0.4, "validation": validation_score, "test": 0.05},
                "symbol_pass_rate": 0.8,
            }
        ],
    }
    (promo_dir / "promotion_report.json").write_text(json.dumps(report), encoding="utf-8")


def test_build_strategy_candidates_from_promoted_edges(monkeypatch, tmp_path: Path) -> None:
    run_id = "strategy_builder_run"
    _write_edge_inputs(tmp_path, run_id=run_id)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_strategy_candidates, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_strategy_candidates.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--include_alpha_bundle",
            "0",
            "--ignore_checklist",
            "1",
        ],
    )

    assert build_strategy_candidates.main() == 0

    out_json = tmp_path / "reports" / "strategy_builder" / run_id / "strategy_candidates.json"
    assert out_json.exists()
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert len(payload) == 1
    assert payload[0]["execution_family"] == "breakout_mechanics"
    assert payload[0]["base_strategy"] == "vol_compression_v1"
    assert payload[0]["backtest_ready"] is True
    assert payload[0]["backtest_ready_reason"] == ""
    assert payload[0]["action"] == "delay_30"
    assert payload[0]["risk_controls"]["entry_delay_bars"] == 30
    assert payload[0]["profit_density_score"] == pytest.approx(0.0045)
    assert payload[0]["selection_score"] == pytest.approx(payload[0]["profit_density_score"])
    assert payload[0]["candidate_symbol"] == "ALL"
    assert payload[0]["run_symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert payload[0]["deployment_type"] == "single_symbol"
    assert payload[0]["deployment_symbols"] == ["BTCUSDT"]
    assert payload[0]["strategy_instances"][0]["strategy_id"] == "vol_compression_v1_BTCUSDT"

    out_manifest = tmp_path / "reports" / "strategy_builder" / run_id / "deployment_manifest.json"
    manifest = json.loads(out_manifest.read_text(encoding="utf-8"))
    assert manifest["strategies"][0]["deployment_type"] == "single_symbol"


def test_build_strategy_candidates_can_include_alpha_bundle(monkeypatch, tmp_path: Path) -> None:
    run_id = "strategy_builder_alpha"
    (tmp_path / "reports" / "edge_candidates" / run_id).mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        columns=[
            "run_id",
            "event",
            "candidate_id",
            "status",
            "edge_score",
            "expected_return_proxy",
            "expectancy_per_trade",
            "stability_proxy",
            "robustness_score",
            "event_frequency",
            "capacity_proxy",
            "profit_density_score",
            "n_events",
            "source_path",
        ]
    ).to_csv(tmp_path / "reports" / "edge_candidates" / run_id / "edge_candidates_normalized.csv", index=False)

    alpha_dir = tmp_path / "feature_store" / "alpha_bundle"
    alpha_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2021-01-01", periods=4, freq="1D", tz="UTC"),
            "symbol": ["BTCUSDT", "ETHUSDT", "BTCUSDT", "ETHUSDT"],
            "score": [0.1, -0.2, 0.3, -0.1],
        }
    ).to_csv(alpha_dir / "alpha_bundle_scores.csv", index=False)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_strategy_candidates, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_strategy_candidates.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--include_alpha_bundle",
            "1",
            "--ignore_checklist",
            "1",
        ],
    )

    assert build_strategy_candidates.main() == 0
    out_json = tmp_path / "reports" / "strategy_builder" / run_id / "strategy_candidates.json"
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert len(payload) == 1
    assert payload[0]["source_type"] == "alpha_bundle"
    assert payload[0]["execution_family"] == "onchain_flow"
    assert payload[0]["base_strategy"] == "onchain_flow_v1"
    assert payload[0]["backtest_ready"] is True


def test_builder_prefers_high_quality_and_enforces_per_event_caps(monkeypatch, tmp_path: Path) -> None:
    run_id = "strategy_builder_quality_rank"
    edge_dir = tmp_path / "reports" / "edge_candidates" / run_id
    phase2_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    edge_dir.mkdir(parents=True, exist_ok=True)
    phase2_dir.mkdir(parents=True, exist_ok=True)

    source_csv = phase2_dir / "phase2_candidates.csv"
    pd.DataFrame(
        [
            {"candidate_id": "c1", "condition": "all", "action": "delay_4"},
            {"candidate_id": "c2", "condition": "all", "action": "delay_8"},
            {"candidate_id": "c3", "condition": "all", "action": "delay_16"},
        ]
    ).to_csv(source_csv, index=False)

    pd.DataFrame(
        [
            {
                "run_id": run_id,
                "event": "vol_shock_relaxation",
                "candidate_id": "c1",
                "status": "PROMOTED",
                "edge_score": 10.0,
                "expected_return_proxy": 0.01,
                "expectancy_per_trade": 0.01,
                "expectancy_after_multiplicity": 0.01,
                "stability_proxy": 0.2,
                "robustness_score": 0.2,
                "event_frequency": 0.3,
                "capacity_proxy": 1.0,
                "profit_density_score": 0.0,
                "quality_score": 0.10,
                "n_events": 120,
                "source_path": str(source_csv),
            },
            {
                "run_id": run_id,
                "event": "vol_shock_relaxation",
                "candidate_id": "c2",
                "status": "PROMOTED",
                "edge_score": 1.0,
                "expected_return_proxy": 0.02,
                "expectancy_per_trade": 0.02,
                "expectancy_after_multiplicity": 0.02,
                "stability_proxy": 0.9,
                "robustness_score": 0.9,
                "event_frequency": 0.3,
                "capacity_proxy": 1.0,
                "profit_density_score": 0.0,
                "quality_score": 0.95,
                "n_events": 120,
                "source_path": str(source_csv),
            },
            {
                "run_id": run_id,
                "event": "vol_shock_relaxation",
                "candidate_id": "c3",
                "status": "PROMOTED",
                "edge_score": 2.0,
                "expected_return_proxy": 0.015,
                "expectancy_per_trade": 0.015,
                "expectancy_after_multiplicity": 0.015,
                "stability_proxy": 0.8,
                "robustness_score": 0.8,
                "event_frequency": 0.3,
                "capacity_proxy": 1.0,
                "profit_density_score": 0.0,
                "quality_score": 0.80,
                "n_events": 120,
                "source_path": str(source_csv),
            },
        ]
    ).to_csv(edge_dir / "edge_candidates_normalized.csv", index=False)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_strategy_candidates, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_strategy_candidates.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--include_alpha_bundle",
            "0",
            "--ignore_checklist",
            "1",
            "--top_k_per_event",
            "5",
            "--max_candidates_per_event",
            "2",
        ],
    )

    assert build_strategy_candidates.main() == 0
    out_json = tmp_path / "reports" / "strategy_builder" / run_id / "strategy_candidates.json"
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert len(payload) == 2
    assert [row["action"] for row in payload] == ["delay_8", "delay_16"]


def test_build_strategy_candidates_event_specific_template(monkeypatch, tmp_path: Path) -> None:
    run_id = "strategy_builder_funding_template"
    _write_edge_inputs(
        tmp_path,
        run_id=run_id,
        event="funding_extreme_reversal_window",
        condition="vol_regime_mid",
        action="entry_gate_skip",
    )

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_strategy_candidates, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_strategy_candidates.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--include_alpha_bundle",
            "0",
            "--ignore_checklist",
            "1",
        ],
    )

    assert build_strategy_candidates.main() == 0
    out_json = tmp_path / "reports" / "strategy_builder" / run_id / "strategy_candidates.json"
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert len(payload) == 1
    assert payload[0]["base_strategy"] == "funding_extreme_reversal_v1"
    assert payload[0]["backtest_ready"] is True
    assert "--strategies funding_extreme_reversal_v1" in payload[0]["manual_backtest_command"]


@pytest.mark.parametrize(
    "event,expected_strategy",
    [
        ("funding_extreme_reversal_window", "funding_extreme_reversal_v1"),
        ("directional_exhaustion_after_forced_flow", "forced_flow_exhaustion_v1"),
        ("liquidity_refill_lag_window", "liquidity_refill_lag_v1"),
        ("cross_venue_desync", "cross_venue_desync_v1"),
    ],
)
def test_build_strategy_candidates_maps_event_families(
    monkeypatch,
    tmp_path: Path,
    event: str,
    expected_strategy: str,
) -> None:
    run_id = f"strategy_builder_map_{event}"
    _write_edge_inputs(tmp_path, run_id=run_id, event=event)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_strategy_candidates, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_strategy_candidates.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--include_alpha_bundle",
            "0",
            "--ignore_checklist",
            "1",
        ],
    )

    assert build_strategy_candidates.main() == 0
    out_json = tmp_path / "reports" / "strategy_builder" / run_id / "strategy_candidates.json"
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload[0]["base_strategy"] == expected_strategy


def test_build_strategy_candidates_unknown_event_does_not_fallback_to_breakout(monkeypatch, tmp_path: Path) -> None:
    run_id = "strategy_builder_unknown"
    _write_edge_inputs(tmp_path, run_id=run_id, event="unknown_edge_family")

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_strategy_candidates, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_strategy_candidates.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--include_alpha_bundle",
            "0",
            "--ignore_checklist",
            "1",
        ],
    )

    assert build_strategy_candidates.main() == 0
    out_json = tmp_path / "reports" / "strategy_builder" / run_id / "strategy_candidates.json"
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload[0]["execution_family"] == "unmapped"
    assert payload[0]["base_strategy"] == "unmapped"
    assert payload[0]["backtest_ready"] is False
    assert payload[0]["backtest_ready_reason"] == "Unknown event family `unknown_edge_family`; no strategy routing is defined."
    assert payload[0]["base_strategy"] != "vol_compression_v1"


def test_build_strategy_candidates_unknown_event_fails_closed(monkeypatch, tmp_path: Path) -> None:
    run_id = "strategy_builder_unknown_event"
    _write_edge_inputs(
        tmp_path,
        run_id=run_id,
        event="totally_unknown_event_family",
        condition="vol_regime_high",
        action="no_action",
    )

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_strategy_candidates, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_strategy_candidates.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--include_alpha_bundle",
            "0",
            "--ignore_checklist",
            "1",
        ],
    )

    assert build_strategy_candidates.main() == 0
    out_json = tmp_path / "reports" / "strategy_builder" / run_id / "strategy_candidates.json"
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert len(payload) == 1
    assert payload[0]["execution_family"] == "unmapped"
    assert payload[0]["base_strategy"] == "unmapped"
    assert payload[0]["backtest_ready"] is False
    assert payload[0]["backtest_ready_reason"] == "Unknown event family `totally_unknown_event_family`; no strategy routing is defined."
    assert "Unknown event family `totally_unknown_event_family`" in " ".join(payload[0]["notes"])


def test_build_strategy_candidates_preserves_candidate_symbol_from_edge(monkeypatch, tmp_path: Path) -> None:
    run_id = "strategy_builder_symbol_scope"
    _write_edge_inputs(tmp_path, run_id=run_id)

    edge_csv = tmp_path / "reports" / "edge_candidates" / run_id / "edge_candidates_normalized.csv"
    df = pd.read_csv(edge_csv)
    df["candidate_symbol"] = ["ETHUSDT"]
    df.to_csv(edge_csv, index=False)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_strategy_candidates, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_strategy_candidates.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--include_alpha_bundle",
            "0",
            "--ignore_checklist",
            "1",
        ],
    )

    assert build_strategy_candidates.main() == 0
    out_json = tmp_path / "reports" / "strategy_builder" / run_id / "strategy_candidates.json"
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload[0]["candidate_symbol"] == "ETHUSDT"
    assert payload[0]["deployment_type"] == "single_symbol"
    assert payload[0]["deployment_symbols"] == ["ETHUSDT"]
    assert payload[0]["strategy_instances"][0]["strategy_id"] == "vol_compression_v1_ETHUSDT"


def test_build_strategy_candidates_allows_multi_symbol_rollout_when_similar_scores(monkeypatch, tmp_path: Path) -> None:
    run_id = "strategy_builder_rollout"
    _write_edge_inputs(tmp_path, run_id=run_id)

    edge_csv = tmp_path / "reports" / "edge_candidates" / run_id / "edge_candidates_normalized.csv"
    df = pd.read_csv(edge_csv)
    df["rollout_eligible"] = [True]
    df["symbol_scores"] = [json.dumps({"BTCUSDT": 0.5, "ETHUSDT": 0.48})]
    df.to_csv(edge_csv, index=False)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_strategy_candidates, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_strategy_candidates.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--include_alpha_bundle",
            "0",
            "--ignore_checklist",
            "1",
        ],
    )

    assert build_strategy_candidates.main() == 0
    out_json = tmp_path / "reports" / "strategy_builder" / run_id / "strategy_candidates.json"
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload[0]["deployment_type"] == "multi_symbol"
    assert payload[0]["deployment_symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert len(payload[0]["strategy_instances"]) == 2


def test_build_strategy_candidates_filters_non_promoted_by_default(monkeypatch, tmp_path: Path) -> None:
    run_id = "strategy_builder_non_promoted_filter"
    _write_edge_inputs(tmp_path, run_id=run_id)
    edge_csv = tmp_path / "reports" / "edge_candidates" / run_id / "edge_candidates_normalized.csv"
    edge_df = pd.read_csv(edge_csv)
    edge_df["status"] = ["DRAFT"]
    edge_df.to_csv(edge_csv, index=False)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_strategy_candidates, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_strategy_candidates.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--include_alpha_bundle",
            "0",
            "--ignore_checklist",
            "1",
        ],
    )
    assert build_strategy_candidates.main() == 0
    out_json = tmp_path / "reports" / "strategy_builder" / run_id / "strategy_candidates.json"
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload == []


def test_build_strategy_candidates_detail_lookup_by_candidate_id(monkeypatch, tmp_path: Path) -> None:
    run_id = "strategy_builder_candidate_id_lookup"
    edge_dir = tmp_path / "reports" / "edge_candidates" / run_id
    phase2_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    edge_dir.mkdir(parents=True, exist_ok=True)
    phase2_dir.mkdir(parents=True, exist_ok=True)

    source_csv = phase2_dir / "phase2_candidates.csv"
    pd.DataFrame(
        [
            {"candidate_id": "candidate_b", "condition": "all", "action": "delay_30"},
            {"candidate_id": "candidate_a", "condition": "all", "action": "delay_8"},
        ]
    ).to_csv(source_csv, index=False)

    pd.DataFrame(
        [
            {
                "run_id": run_id,
                "event": "vol_shock_relaxation",
                "candidate_id": "candidate_a",
                "status": "PROMOTED",
                "edge_score": 0.42,
                "expected_return_proxy": 0.01,
                "expectancy_per_trade": 0.01,
                "stability_proxy": 0.9,
                "robustness_score": 0.9,
                "event_frequency": 0.5,
                "capacity_proxy": 1.0,
                "profit_density_score": 0.0045,
                "n_events": 120,
                "source_path": str(source_csv),
            }
        ]
    ).to_csv(edge_dir / "edge_candidates_normalized.csv", index=False)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_strategy_candidates, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_strategy_candidates.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--include_alpha_bundle",
            "0",
            "--ignore_checklist",
            "1",
        ],
    )
    assert build_strategy_candidates.main() == 0
    out_json = tmp_path / "reports" / "strategy_builder" / run_id / "strategy_candidates.json"
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload[0]["action"] == "delay_8"


def test_build_strategy_candidates_missing_detail_fails_by_default(monkeypatch, tmp_path: Path) -> None:
    run_id = "strategy_builder_missing_detail_fail"
    edge_dir = tmp_path / "reports" / "edge_candidates" / run_id
    phase2_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    edge_dir.mkdir(parents=True, exist_ok=True)
    phase2_dir.mkdir(parents=True, exist_ok=True)

    source_csv = phase2_dir / "phase2_candidates.csv"
    pd.DataFrame([{"candidate_id": "candidate_b", "condition": "all", "action": "delay_30"}]).to_csv(source_csv, index=False)
    pd.DataFrame(
        [
            {
                "run_id": run_id,
                "event": "vol_shock_relaxation",
                "candidate_id": "candidate_a",
                "status": "PROMOTED",
                "edge_score": 0.42,
                "expected_return_proxy": 0.01,
                "expectancy_per_trade": 0.01,
                "stability_proxy": 0.9,
                "robustness_score": 0.9,
                "event_frequency": 0.5,
                "capacity_proxy": 1.0,
                "profit_density_score": 0.0045,
                "n_events": 120,
                "source_path": str(source_csv),
            }
        ]
    ).to_csv(edge_dir / "edge_candidates_normalized.csv", index=False)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_strategy_candidates, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_strategy_candidates.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--include_alpha_bundle",
            "0",
            "--ignore_checklist",
            "1",
        ],
    )
    assert build_strategy_candidates.main() == 1


def test_build_strategy_candidates_missing_detail_can_be_skipped(monkeypatch, tmp_path: Path) -> None:
    run_id = "strategy_builder_missing_detail_skip"
    edge_dir = tmp_path / "reports" / "edge_candidates" / run_id
    phase2_dir = tmp_path / "reports" / "phase2" / run_id / "vol_shock_relaxation"
    edge_dir.mkdir(parents=True, exist_ok=True)
    phase2_dir.mkdir(parents=True, exist_ok=True)

    source_csv = phase2_dir / "phase2_candidates.csv"
    pd.DataFrame([{"candidate_id": "candidate_b", "condition": "all", "action": "delay_30"}]).to_csv(source_csv, index=False)
    pd.DataFrame(
        [
            {
                "run_id": run_id,
                "event": "vol_shock_relaxation",
                "candidate_id": "candidate_a",
                "status": "PROMOTED",
                "edge_score": 0.42,
                "expected_return_proxy": 0.01,
                "expectancy_per_trade": 0.01,
                "stability_proxy": 0.9,
                "robustness_score": 0.9,
                "event_frequency": 0.5,
                "capacity_proxy": 1.0,
                "profit_density_score": 0.0045,
                "n_events": 120,
                "source_path": str(source_csv),
            }
        ]
    ).to_csv(edge_dir / "edge_candidates_normalized.csv", index=False)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_strategy_candidates, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_strategy_candidates.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--include_alpha_bundle",
            "0",
            "--ignore_checklist",
            "1",
            "--allow_missing_candidate_detail",
            "1",
        ],
    )
    assert build_strategy_candidates.main() == 0
    out_json = tmp_path / "reports" / "strategy_builder" / run_id / "strategy_candidates.json"
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload == []

    out_manifest = tmp_path / "reports" / "strategy_builder" / run_id / "deployment_manifest.json"
    manifest = json.loads(out_manifest.read_text(encoding="utf-8"))
    assert manifest["builder_diagnostics"]["missing_candidate_detail_count"] == 1
    assert manifest["builder_diagnostics"]["skipped_missing_candidate_detail_count"] == 1


def test_build_strategy_candidates_applies_final_cap_after_alpha_append(monkeypatch, tmp_path: Path) -> None:
    run_id = "strategy_builder_final_cap"
    _write_edge_inputs(tmp_path, run_id=run_id)

    alpha_dir = tmp_path / "feature_store" / "alpha_bundle"
    alpha_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2021-01-01", periods=4, freq="1D", tz="UTC"),
            "symbol": ["BTCUSDT", "ETHUSDT", "BTCUSDT", "ETHUSDT"],
            "score": [0.3, 0.2, 0.1, 0.2],
        }
    ).to_csv(alpha_dir / "alpha_bundle_scores.csv", index=False)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_strategy_candidates, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_strategy_candidates.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--max_candidates",
            "1",
            "--include_alpha_bundle",
            "1",
            "--ignore_checklist",
            "1",
        ],
    )
    assert build_strategy_candidates.main() == 0
    out_json = tmp_path / "reports" / "strategy_builder" / run_id / "strategy_candidates.json"
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert len(payload) == 1


def test_strategy_builder_merges_promotions_and_edge_candidates(monkeypatch, tmp_path: Path) -> None:
    run_id = "strategy_builder_merge_sources"
    _write_edge_inputs(tmp_path, run_id=run_id, event="vol_shock_relaxation", action="delay_30")
    _write_promotion_inputs(
        tmp_path,
        run_id=run_id,
        event="funding_extreme_reversal_window",
        blueprint_id="bp_promoted",
        candidate_id="promoted_1",
        condition="all",
        action="delay_8",
        validation_score=0.3,
    )

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_strategy_candidates, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_strategy_candidates.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--include_alpha_bundle",
            "0",
            "--ignore_checklist",
            "1",
        ],
    )
    assert build_strategy_candidates.main() == 0
    payload = json.loads((tmp_path / "reports" / "strategy_builder" / run_id / "strategy_candidates.json").read_text(encoding="utf-8"))
    assert len(payload) == 2
    assert {row["source_type"] for row in payload} == {"promoted_blueprint", "edge_candidate"}


def test_strategy_builder_prefers_promoted_source_on_conflict(monkeypatch, tmp_path: Path) -> None:
    run_id = "strategy_builder_source_priority"
    _write_edge_inputs(tmp_path, run_id=run_id, event="vol_shock_relaxation", condition="all", action="delay_30")
    _write_promotion_inputs(
        tmp_path,
        run_id=run_id,
        event="vol_shock_relaxation",
        blueprint_id="bp_priority",
        candidate_id="vol_shock_relaxation_0",
        condition="all",
        action="delay_30",
        validation_score=0.25,
    )

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_strategy_candidates, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_strategy_candidates.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--include_alpha_bundle",
            "0",
            "--ignore_checklist",
            "1",
        ],
    )
    assert build_strategy_candidates.main() == 0
    payload = json.loads((tmp_path / "reports" / "strategy_builder" / run_id / "strategy_candidates.json").read_text(encoding="utf-8"))
    assert len(payload) == 1
    assert payload[0]["source_type"] == "promoted_blueprint"


def test_strategy_builder_reports_source_diagnostics(monkeypatch, tmp_path: Path) -> None:
    run_id = "strategy_builder_source_diag"
    _write_edge_inputs(tmp_path, run_id=run_id)
    _write_promotion_inputs(tmp_path, run_id=run_id)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(build_strategy_candidates, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_strategy_candidates.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--include_alpha_bundle",
            "0",
            "--ignore_checklist",
            "1",
        ],
    )
    assert build_strategy_candidates.main() == 0
    manifest = json.loads((tmp_path / "reports" / "strategy_builder" / run_id / "deployment_manifest.json").read_text(encoding="utf-8"))
    seen = manifest["builder_diagnostics"]["source_counts_seen"]
    selected = manifest["builder_diagnostics"]["source_counts_selected"]
    assert seen["promoted_blueprint"] == 1
    assert seen["edge_candidate"] == 1
    assert selected["promoted_blueprint"] >= 1
