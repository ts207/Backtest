from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def backtest_data_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "data"
    root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(root))
    return root

