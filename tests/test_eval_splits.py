import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from eval.splits import build_time_splits


def test_build_time_splits_is_deterministic() -> None:
    kwargs = {
        "start": "2024-01-01",
        "end": "2024-01-31",
        "train_frac": 0.6,
        "validation_frac": 0.2,
        "embargo_days": 1,
    }
    first = [w.to_dict() for w in build_time_splits(**kwargs)]
    second = [w.to_dict() for w in build_time_splits(**kwargs)]
    assert first == second


def test_build_time_splits_applies_embargo() -> None:
    windows = build_time_splits(
        start="2024-01-01",
        end="2024-01-31",
        train_frac=0.6,
        validation_frac=0.2,
        embargo_days=2,
    )
    by_label = {w.label: w for w in windows}
    assert {"train", "validation", "test"}.issubset(set(by_label))

    train_end = by_label["train"].end
    validation_start = by_label["validation"].start
    validation_end = by_label["validation"].end
    test_start = by_label["test"].start

    assert validation_start > train_end
    assert test_start > validation_end
    assert int((validation_start - train_end).total_seconds()) == 2 * 86400 + 1
    assert int((test_start - validation_end).total_seconds()) == 2 * 86400 + 1


def test_build_time_splits_rejects_invalid_fracs() -> None:
    with pytest.raises(ValueError):
        build_time_splits(
            start="2024-01-01",
            end="2024-01-31",
            train_frac=0.8,
            validation_frac=0.3,
        )
