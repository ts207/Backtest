import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from engine.runner import _join_context_features
from features.funding_persistence import FP_DEF_VERSION, build_funding_persistence_state


def _sample_frame() -> pd.DataFrame:
    ts = pd.date_range("2024-01-01 00:00", periods=220, freq="15min", tz="UTC")
    funding = [0.0001] * len(ts)
    for i in range(60, 84):
        funding[i] = 0.004
    for i in range(140, 170):
        funding[i] = 0.006
    return pd.DataFrame({"timestamp": ts, "funding_rate_scaled": funding})


def test_funding_persistence_determinism() -> None:
    frame = _sample_frame()
    out1 = build_funding_persistence_state(frame, symbol="BTCUSDT")
    out2 = build_funding_persistence_state(frame, symbol="BTCUSDT")
    pd.testing.assert_frame_equal(out1, out2)


def test_deoverlap_and_monotonic_age() -> None:
    out = build_funding_persistence_state(_sample_frame(), symbol="BTCUSDT")
    assert (out["fp_active"].isin([0, 1])).all()

    active = out[out["fp_active"] == 1].copy()
    for _, grp in active.groupby("fp_event_id", sort=True):
        assert grp["fp_age_bars"].iloc[0] == 1
        assert grp["fp_age_bars"].diff().fillna(1).eq(1).all()

    for idx in range(len(out) - 1):
        if int(out.loc[idx, "fp_active"]) == 1 and int(out.loc[idx + 1, "fp_active"]) == 0:
            assert int(out.loc[idx + 1, "fp_age_bars"]) == 0


def test_no_signal_guard_columns() -> None:
    out = build_funding_persistence_state(_sample_frame(), symbol="BTCUSDT")
    forbidden_exact = {"signal", "side", "direction", "alpha", "entry", "short", "long"}
    forbidden_tokens = {"signal", "side", "direction", "alpha", "entry", "short", "long"}
    for col in out.columns:
        lowered = col.lower()
        assert lowered not in forbidden_exact
        assert not any(token in lowered for token in forbidden_tokens)


def test_join_safety_and_defaults() -> None:
    ts = pd.date_range("2024-01-01 00:00", periods=5, freq="15min", tz="UTC")
    features = pd.DataFrame({"timestamp": ts, "close": [1, 2, 3, 4, 5]})
    context = pd.DataFrame(
        {
            "timestamp": ts[:2],
            "fp_def_version": [FP_DEF_VERSION, FP_DEF_VERSION],
            "fp_active": [1, 0],
            "fp_age_bars": [3, 0],
            "fp_event_id": ["a", None],
            "fp_enter_ts": [ts[0], pd.NaT],
            "fp_exit_ts": [pd.NaT, pd.NaT],
            "fp_severity": [0.1, 0.0],
            "fp_norm_due": [0, 0],
        }
    )

    joined = _join_context_features(features, context)
    assert len(joined) == len(features)
    assert int(joined.loc[joined["timestamp"] == ts[2], "fp_active"].iloc[0]) == 0
    assert int(joined.loc[joined["timestamp"] == ts[2], "fp_age_bars"].iloc[0]) == 0
    assert joined.loc[joined["timestamp"] == ts[2], "fp_def_version"].iloc[0] == FP_DEF_VERSION
