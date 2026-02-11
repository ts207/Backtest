import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from pipelines import run_all


def test_run_all_includes_multi_edge_chain_when_enabled(monkeypatch) -> None:
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
            "run_multi_edge_chain",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_edge_candidate_universe",
            "1",
            "--run_multi_edge_portfolio",
            "1",
            "--run_multi_edge_validation",
            "1",
            "--multi_edge_symbols",
            "TOP10",
        ],
    )

    assert run_all.main() == 0
    stage_names = [x[0] for x in captured]
    assert "export_edge_candidates" in stage_names
    assert "backtest_multi_edge_portfolio" in stage_names
    assert "validate_multi_edge_portfolio" in stage_names
    assert stage_names.index("export_edge_candidates") < stage_names.index("backtest_multi_edge_portfolio")
    assert stage_names.index("backtest_multi_edge_portfolio") < stage_names.index("validate_multi_edge_portfolio")
    assert stage_names.index("validate_multi_edge_portfolio") < stage_names.index("make_report")


def test_run_all_requires_discovery_first_when_multi_edge_enabled(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--run_id",
            "run_multi_edge_missing_discovery",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_multi_edge_portfolio",
            "1",
            "--run_edge_candidate_universe",
            "0",
            "--enforce_discovery_first",
            "1",
        ],
    )
    try:
        run_all.main()
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("Expected parser error when discovery stage is missing.")


def test_run_all_rejects_make_report_without_baseline(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_all.py",
            "--run_id",
            "run_make_report_without_baseline",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-31",
            "--run_backtest_baseline",
            "0",
            "--run_make_report",
            "1",
        ],
    )
    try:
        run_all.main()
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("Expected parser error when make_report is enabled without baseline backtest.")
