import pytest
import pandas as pd
from eval.splits import build_time_splits, SplitWindow, _normalize_ts

def test_normalize_ts():
    ts = pd.Timestamp("2024-01-01")
    norm = _normalize_ts(ts)
    assert str(norm.tz) == "UTC"

    ts_tz = pd.Timestamp("2024-01-01", tz="America/New_York")
    norm_tz = _normalize_ts(ts_tz)
    assert str(norm_tz.tz) == "UTC"

def test_build_time_splits_basic():
    splits = build_time_splits(
        start="2024-01-01",
        end="2024-01-10",
        train_frac=0.6,
        validation_frac=0.2,
        embargo_days=0
    )
    assert len(splits) == 3
    assert splits[0].label == "train"
    assert splits[1].label == "validation"
    assert splits[2].label == "test"
    
    # Train is 60% of 10 days = 6 days (Jan 1-6)
    assert splits[0].start == pd.Timestamp("2024-01-01", tz="UTC")

def test_build_time_splits_embargo():
    splits = build_time_splits(
        start="2024-01-01",
        end="2024-01-10",
        train_frac=0.5,
        validation_frac=0.2,
        embargo_days=1
    )
    assert len(splits) == 3
    assert splits[0].label == "train"
    assert splits[1].label == "validation"
    assert list(splits[0].to_dict().keys()) == ["label", "start", "end"]

def test_build_time_splits_invalid():
    with pytest.raises(ValueError, match="start must be <= end"):
        build_time_splits(start="2024-01-10", end="2024-01-01")
    
    with pytest.raises(ValueError, match="train_frac must be in"):
        build_time_splits(start="2024-01-01", end="2024-01-10", train_frac=1.5)
        
    with pytest.raises(ValueError, match="validation_frac must be in"):
        build_time_splits(start="2024-01-01", end="2024-01-10", train_frac=0.5, validation_frac=0.0)

    with pytest.raises(ValueError, match="train_frac \\+ validation_frac must be < 1"):
        build_time_splits(start="2024-01-01", end="2024-01-10", train_frac=0.6, validation_frac=0.5)

    with pytest.raises(ValueError, match="embargo_days must be >= 0"):
        build_time_splits(start="2024-01-01", end="2024-01-10", embargo_days=-1)


# --- Tests for build_time_splits_with_purge ---

from eval.splits import build_time_splits_with_purge

def test_purge_shortens_train_end():
    windows = build_time_splits_with_purge(
        start="2023-01-01", end="2023-12-31",
        train_frac=0.6, validation_frac=0.2,
        embargo_days=1, purge_bars=12,
        bar_duration_minutes=5,
    )
    standard = build_time_splits(
        start="2023-01-01", end="2023-12-31",
        train_frac=0.6, validation_frac=0.2, embargo_days=1,
    )
    train_purged = next(w for w in windows if w.label == "train")
    train_std = next(w for w in standard if w.label == "train")
    # purged train must end strictly before standard train
    assert train_purged.end < train_std.end

def test_purge_zero_is_identity():
    windows = build_time_splits_with_purge(
        start="2023-01-01", end="2023-12-31",
        train_frac=0.6, validation_frac=0.2,
        embargo_days=1, purge_bars=0,
        bar_duration_minutes=5,
    )
    standard = build_time_splits(
        start="2023-01-01", end="2023-12-31",
        train_frac=0.6, validation_frac=0.2, embargo_days=1,
    )
    for w, s in zip(windows, standard):
        assert w.label == s.label
        assert w.start == s.start
        assert w.end == s.end

def test_purge_raises_on_negative():
    import pytest
    with pytest.raises(ValueError, match="purge_bars"):
        build_time_splits_with_purge(
            start="2023-01-01", end="2023-12-31",
            train_frac=0.6, validation_frac=0.2,
            embargo_days=1, purge_bars=-1,
            bar_duration_minutes=5,
        )
