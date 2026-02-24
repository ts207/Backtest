from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

from engine import runner
from pipelines._lib.spec_loader import load_global_defaults
from pipelines._lib.timeframe_constants import (
    BARS_PER_YEAR_BY_TIMEFRAME,
    DEFAULT_EVENT_HORIZON_BARS,
    HORIZON_BARS_BY_TIMEFRAME,
)
from pipelines.research import phase2_candidate_discovery
from pipelines.research import validate_event_quality


def test_runner_uses_canonical_bars_per_year_map():
    assert runner.BARS_PER_YEAR == BARS_PER_YEAR_BY_TIMEFRAME


def test_horizon_lookup_uses_canonical_mapping():
    assert phase2_candidate_discovery._horizon_to_bars("5m") == HORIZON_BARS_BY_TIMEFRAME["5m"]
    assert phase2_candidate_discovery._horizon_to_bars("60m") == HORIZON_BARS_BY_TIMEFRAME["60m"]
    assert phase2_candidate_discovery._horizon_to_bars("unknown_horizon") == 12


def test_default_event_horizon_grid_is_stable():
    assert DEFAULT_EVENT_HORIZON_BARS == [1, 3, 12]


def test_validate_event_quality_default_horizons_uses_canonical_constant():
    assert validate_event_quality._default_horizons_bars_csv() == ",".join(str(x) for x in DEFAULT_EVENT_HORIZON_BARS)


def test_spec_loader_precedence_explicit_over_env_over_default(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    project_root = repo_root / "project"
    spec_root = repo_root / "spec"
    project_root.mkdir(parents=True)
    spec_root.mkdir(parents=True)

    default_path = spec_root / "global_defaults.yaml"
    default_path.write_text("defaults:\n  source: default\n")

    env_path = tmp_path / "env_defaults.yaml"
    env_path.write_text("defaults:\n  source: env\n")
    monkeypatch.setenv("BACKTEST_GLOBAL_DEFAULTS_PATH", str(env_path))

    explicit_path = tmp_path / "explicit_defaults.yaml"
    explicit_path.write_text("defaults:\n  source: explicit\n")

    loaded = load_global_defaults(project_root=project_root, explicit_path=explicit_path)
    assert loaded["source"] == "explicit"


def test_spec_loader_env_over_default(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    project_root = repo_root / "project"
    spec_root = repo_root / "spec"
    project_root.mkdir(parents=True)
    spec_root.mkdir(parents=True)

    (spec_root / "global_defaults.yaml").write_text("defaults:\n  source: default\n")
    env_path = tmp_path / "env_defaults.yaml"
    env_path.write_text("defaults:\n  source: env\n")
    monkeypatch.setenv("BACKTEST_GLOBAL_DEFAULTS_PATH", str(env_path))

    loaded = load_global_defaults(project_root=project_root)
    assert loaded["source"] == "env"


def test_spec_loader_required_raises_when_missing(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    project_root = repo_root / "project"
    project_root.mkdir(parents=True)
    monkeypatch.delenv("BACKTEST_GLOBAL_DEFAULTS_PATH", raising=False)

    with pytest.raises(FileNotFoundError):
        load_global_defaults(project_root=project_root, required=True)


def test_spec_loader_returns_empty_when_optional_and_missing(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    project_root = repo_root / "project"
    project_root.mkdir(parents=True)
    monkeypatch.delenv("BACKTEST_GLOBAL_DEFAULTS_PATH", raising=False)

    assert load_global_defaults(project_root=project_root) == {}
