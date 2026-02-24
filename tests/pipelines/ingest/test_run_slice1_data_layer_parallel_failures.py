from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3] / "project"
sys.path.insert(0, str(PROJECT_ROOT))

import pipelines.ingest.run_slice1_data_layer as run_slice1


class _FakeProcess:
    def __init__(self, script_path: str, rc: int):
        self.script_path = script_path
        self._rc = int(rc)

    def wait(self) -> int:
        return self._rc


def test_run_parallel_exits_with_failure_count_and_logs_scripts(monkeypatch, caplog):
    rc_by_script = {
        "script_ok.py": 0,
        "script_fail_a.py": 2,
        "script_fail_b.py": 7,
    }

    def fake_popen(cmd):
        script_path = str(cmd[1])
        return _FakeProcess(script_path=script_path, rc=rc_by_script[script_path])

    monkeypatch.setattr(run_slice1.subprocess, "Popen", fake_popen)

    scripts = [
        ("script_ok.py", ["--x", "1"]),
        ("script_fail_a.py", ["--x", "1"]),
        ("script_fail_b.py", ["--x", "1"]),
    ]

    with pytest.raises(SystemExit) as exc:
        run_slice1.run_parallel(scripts)

    assert int(exc.value.code) == 2
    assert "script_fail_a.py" in caplog.text
    assert "script_fail_b.py" in caplog.text


def test_run_parallel_no_failures_does_not_exit(monkeypatch):
    def fake_popen(cmd):
        return _FakeProcess(script_path=str(cmd[1]), rc=0)

    monkeypatch.setattr(run_slice1.subprocess, "Popen", fake_popen)
    run_slice1.run_parallel([("script_ok.py", [])])
