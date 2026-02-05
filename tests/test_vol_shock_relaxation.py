import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "project"))

from features.vol_shock_relaxation import DEFAULT_VSR_CONFIG, detect_vol_shock_relaxation_events


def _fixture() -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=900, freq="15min", tz="UTC")
    close = pd.Series(100.0, index=ts)
    # calm
    for i in range(1, len(ts)):
        close.iloc[i] = close.iloc[i - 1] * (1.0 + 0.0002)
    # shock episode 1
    for i in range(350, 362):
        close.iloc[i] = close.iloc[i - 1] * (1.0 + (0.01 if i % 2 == 0 else -0.012))
    # relax
    for i in range(362, 420):
        close.iloc[i] = close.iloc[i - 1] * (1.0 + 0.0003)
    # shock episode 2
    for i in range(650, 664):
        close.iloc[i] = close.iloc[i - 1] * (1.0 + (0.011 if i % 2 == 0 else -0.013))

    high = close * 1.002
    low = close * 0.998
    return pd.DataFrame({"timestamp": ts, "close": close.values, "high": high.values, "low": low.values})


def test_vsr_deterministic_and_deoverlapped() -> None:
    frame = _fixture()
    ev1, core1, meta1 = detect_vol_shock_relaxation_events(frame, symbol="BTCUSDT", config=DEFAULT_VSR_CONFIG)
    ev2, core2, meta2 = detect_vol_shock_relaxation_events(frame, symbol="BTCUSDT", config=DEFAULT_VSR_CONFIG)

    pd.testing.assert_frame_equal(ev1, ev2)
    pd.testing.assert_frame_equal(core1, core2)
    assert meta1 == meta2

    if not ev1.empty:
        for i in range(len(ev1) - 1):
            assert int(ev1.iloc[i + 1]["enter_idx"]) > int(ev1.iloc[i]["exit_idx"]) + DEFAULT_VSR_CONFIG.cooldown_bars


def test_vsr_outcomes_present_and_context_only() -> None:
    ev, _, _ = detect_vol_shock_relaxation_events(_fixture(), symbol="ETHUSDT", config=DEFAULT_VSR_CONFIG)
    expected = {
        "event_id",
        "time_to_relax",
        "rv_peak",
        "t_rv_peak",
        "rv_decay_half_life",
        "auc_excess_rv",
        "secondary_shock_within_h",
        "time_to_secondary_shock",
        "realized_vol_mean_96",
        "realized_vol_p90_96",
        "range_pct_96",
        "relaxed_within_96",
    }
    assert expected.issubset(set(ev.columns))

    bad_tokens = {"signal", "alpha", "side", "entry_rule"}
    for col in ev.columns:
        lowered = col.lower()
        assert not any(token in lowered for token in bad_tokens)
