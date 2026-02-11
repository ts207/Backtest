import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines import run_all


def test_run_all_includes_hypothesis_generator_stage(monkeypatch) -> None:
    captured = []

    def _fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append((stage, script_path, list(base_args), run_id))
        return True

    monkeypatch.setattr(run_all, "_run_stage", _fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--run_id",
            "run_hypothesis_generator",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_hypothesis_generator",
            "1",
            "--hypothesis_datasets",
            "auto",
            "--hypothesis_max_fused",
            "16",
        ],
    )
    assert run_all.main() == 0

    stage_names = [x[0] for x in captured]
    assert "generate_hypothesis_queue" in stage_names
    assert stage_names.index("build_context_features") < stage_names.index("generate_hypothesis_queue")

    hyp_args = next(base_args for stage, _, base_args, _ in captured if stage == "generate_hypothesis_queue")
    assert "--datasets" in hyp_args
    assert "auto" in hyp_args
    assert "--max_fused" in hyp_args
    assert "16" in hyp_args


def test_run_all_hypothesis_stage_precedes_edge_export(monkeypatch) -> None:
    captured = []

    def _fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append((stage, script_path, list(base_args), run_id))
        return True

    monkeypatch.setattr(run_all, "_run_stage", _fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--run_id",
            "run_hypothesis_and_export",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_hypothesis_generator",
            "1",
            "--run_edge_candidate_universe",
            "1",
        ],
    )
    assert run_all.main() == 0

    stage_names = [x[0] for x in captured]
    assert stage_names.index("build_context_features") < stage_names.index("generate_hypothesis_queue")
    assert stage_names.index("generate_hypothesis_queue") < stage_names.index("export_edge_candidates")


def test_run_all_edge_export_is_aggregate_only(monkeypatch) -> None:
    captured = []

    def _fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append((stage, script_path, list(base_args), run_id))
        return True

    monkeypatch.setattr(run_all, "_run_stage", _fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--run_id",
            "run_edge_export_aggregate",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_edge_candidate_universe",
            "1",
        ],
    )
    assert run_all.main() == 0

    export_args = next(base_args for stage, _, base_args, _ in captured if stage == "export_edge_candidates")
    assert "--execute" in export_args
    assert "0" in export_args
    assert "--run_hypothesis_generator" in export_args


def test_run_all_includes_optional_dataset_ingestors(monkeypatch) -> None:
    captured = []

    def _fake_run_stage(stage: str, script_path: Path, base_args: list[str], run_id: str) -> bool:
        captured.append((stage, script_path, list(base_args), run_id))
        return True

    monkeypatch.setattr(run_all, "_run_stage", _fake_run_stage)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--run_id",
            "run_ingest_extra_datasets",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_ingest_liquidation_snapshot",
            "1",
            "--run_ingest_open_interest_hist",
            "1",
            "--open_interest_period",
            "5m",
        ],
    )
    assert run_all.main() == 0

    stage_names = [x[0] for x in captured]
    assert "ingest_binance_um_liquidation_snapshot" in stage_names
    assert "ingest_binance_um_open_interest_hist" in stage_names
