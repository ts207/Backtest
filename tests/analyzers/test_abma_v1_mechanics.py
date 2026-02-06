import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "project"))

from analyzers.abma.controls import sample_matched_controls
from analyzers.abma.evaluate import evaluate_stabilization
from analyzers.abma.events import extract_auction_boundary_events
from analyzers.abma.metrics import compute_structural_metrics


def _synthetic_microstructure(symbol: str, start: str, end: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    ts = pd.date_range(start=start, end=end, freq="1s", tz="UTC")
    idx = np.arange(len(ts), dtype=float)
    base = 100.0 + 0.001 * idx
    vol_scale = 0.2 + idx / max(1.0, float(len(ts) - 1))
    wiggle = np.sin(idx / 30.0) * vol_scale * 0.01
    bid = base - 0.01 + wiggle
    ask = base + 0.01 + wiggle
    bid_sz = 10.0 + (idx % 20.0)
    ask_sz = 10.0 + ((idx + 7.0) % 20.0)
    quotes = pd.DataFrame(
        {
            "ts": ts,
            "symbol": symbol,
            "bid_px": bid,
            "ask_px": ask,
            "bid_sz": bid_sz,
            "ask_sz": ask_sz,
        }
    )
    size = 100.0 - (idx / max(1.0, float(len(ts) - 1))) * 95.0
    trades = pd.DataFrame(
        {
            "ts": ts,
            "symbol": symbol,
            "price": (bid + ask) / 2.0,
            "size": np.maximum(size, 0.1),
        }
    )
    return trades, quotes


def test_events_open_ts_timezone_and_hash_stability() -> None:
    symbol = "BTCUSDT"
    trades, quotes = _synthetic_microstructure(symbol, "2026-01-05T14:00:00Z", "2026-01-05T16:00:00Z")
    cal = pd.DataFrame(
        {
            "session_date": ["2026-01-05"],
            "open_ts": [pd.Timestamp("2026-01-05 09:30:00", tz="America/New_York")],
            "close_ts": [pd.Timestamp("2026-01-05 16:00:00", tz="America/New_York")],
        }
    )
    first = extract_auction_boundary_events(trades=trades, quotes=quotes, session_calendar=cal)
    second = extract_auction_boundary_events(trades=trades, quotes=quotes, session_calendar=cal)
    assert not first.empty
    assert first["event_id"].tolist() == second["event_id"].tolist()
    assert str(first["t0"].dt.tz) == "UTC"
    # 2026-01-05 09:30 America/New_York is 14:30 UTC (standard time).
    assert first["t0"].iloc[0] == pd.Timestamp("2026-01-05T14:30:00Z")


def test_events_dst_boundary_conversion_is_correct() -> None:
    symbol = "BTCUSDT"
    trades, quotes = _synthetic_microstructure(symbol, "2026-03-09T12:00:00Z", "2026-03-09T16:00:00Z")
    cal = pd.DataFrame(
        {
            "session_date": ["2026-03-09"],
            "open_ts": [pd.Timestamp("2026-03-09 09:30:00", tz="America/New_York")],
            "close_ts": [pd.Timestamp("2026-03-09 16:00:00", tz="America/New_York")],
        }
    )
    events = extract_auction_boundary_events(trades=trades, quotes=quotes, session_calendar=cal)
    # After DST starts, 09:30 ET maps to 13:30 UTC.
    assert events["t0"].iloc[0] == pd.Timestamp("2026-03-09T13:30:00Z")


def test_controls_time_offsets_respect_session_bounds_and_fallback() -> None:
    symbol = "BTCUSDT"
    trades, quotes = _synthetic_microstructure(symbol, "2026-01-05T09:00:00Z", "2026-01-05T13:00:00Z")
    cal = pd.DataFrame(
        {
            "session_date": ["2026-01-05"],
            "open_ts": [pd.Timestamp("2026-01-05T09:30:00Z")],
            "close_ts": [pd.Timestamp("2026-01-05T13:00:00Z")],
        }
    )
    # Use an explicit non-open anchor so baseline [t-15m, t) stays in-session.
    t0 = pd.Timestamp("2026-01-05T10:30:00Z")
    events = pd.DataFrame(
        [
            {
                "event_id": "evt_mid_session",
                "symbol": symbol,
                "session_date": pd.Timestamp("2026-01-05").date(),
                "t0": t0,
                "window_end": t0 + pd.Timedelta(minutes=30),
                "abma_def_version": "v1",
                "open_ts_utc": pd.Timestamp("2026-01-05T09:30:00Z"),
                "close_ts_utc": pd.Timestamp("2026-01-05T13:00:00Z"),
            }
        ]
    )
    controls = sample_matched_controls(events=events, trades=trades, quotes=quotes, session_calendar=cal)
    assert not controls.empty
    open_bound = events["open_ts_utc"].iloc[0] + pd.Timedelta(minutes=15)
    close_bound = events["close_ts_utc"].iloc[0]
    assert ((controls["tc"] >= open_bound) & (controls["tc"] < close_bound)).all()
    assert (controls["tc"] != t0).all()
    assert int((controls["event_id"] == events["event_id"].iloc[0]).sum()) <= 5
    assert set(controls["match_tier"].unique()).issubset({"exact", "rv_only"})


def test_controls_cross_session_baseline_is_invalidated_for_open_anchor() -> None:
    symbol = "BTCUSDT"
    trades, quotes = _synthetic_microstructure(symbol, "2026-01-05T08:00:00Z", "2026-01-05T11:00:00Z")
    cal = pd.DataFrame(
        {
            "session_date": ["2026-01-05"],
            "open_ts": [pd.Timestamp("2026-01-05T09:30:00Z")],
            "close_ts": [pd.Timestamp("2026-01-05T11:00:00Z")],
        }
    )
    events = extract_auction_boundary_events(trades=trades, quotes=quotes, session_calendar=cal)
    controls = sample_matched_controls(events=events, trades=trades, quotes=quotes, session_calendar=cal)
    assert controls.empty
    reasons = pd.DataFrame(controls.attrs.get("no_controls", []))
    assert not reasons.empty
    assert "baseline_crosses_session_start_or_insufficient_midpoints" in set(reasons["reason"])


def test_controls_deterministic_with_same_inputs() -> None:
    symbol = "BTCUSDT"
    trades, quotes = _synthetic_microstructure(symbol, "2026-01-05T09:00:00Z", "2026-01-05T13:00:00Z")
    cal = pd.DataFrame(
        {
            "session_date": ["2026-01-05"],
            "open_ts": [pd.Timestamp("2026-01-05T09:30:00Z")],
            "close_ts": [pd.Timestamp("2026-01-05T13:00:00Z")],
        }
    )
    t0 = pd.Timestamp("2026-01-05T10:45:00Z")
    events = pd.DataFrame(
        [
            {
                "event_id": "evt_deterministic",
                "symbol": symbol,
                "session_date": pd.Timestamp("2026-01-05").date(),
                "t0": t0,
                "window_end": t0 + pd.Timedelta(minutes=30),
                "abma_def_version": "v1",
                "open_ts_utc": pd.Timestamp("2026-01-05T09:30:00Z"),
                "close_ts_utc": pd.Timestamp("2026-01-05T13:00:00Z"),
            }
        ]
    )
    first = sample_matched_controls(events=events, trades=trades, quotes=quotes, session_calendar=cal)
    second = sample_matched_controls(events=events, trades=trades, quotes=quotes, session_calendar=cal)
    assert first["control_id"].tolist() == second["control_id"].tolist()
    assert first["tc"].tolist() == second["tc"].tolist()


def test_controls_sort_out_of_order_timestamps() -> None:
    symbol = "BTCUSDT"
    trades, quotes = _synthetic_microstructure(symbol, "2026-01-05T09:00:00Z", "2026-01-05T11:00:00Z")
    trades = trades.sample(frac=1.0, random_state=11).reset_index(drop=True)
    quotes = quotes.sample(frac=1.0, random_state=17).reset_index(drop=True)
    cal = pd.DataFrame(
        {
            "session_date": ["2026-01-05"],
            "open_ts": [pd.Timestamp("2026-01-05T09:30:00Z")],
            "close_ts": [pd.Timestamp("2026-01-05T11:00:00Z")],
        }
    )
    t0 = pd.Timestamp("2026-01-05T10:30:00Z")
    events = pd.DataFrame(
        [
            {
                "event_id": "evt_order",
                "symbol": symbol,
                "session_date": pd.Timestamp("2026-01-05").date(),
                "t0": t0,
                "window_end": t0 + pd.Timedelta(minutes=30),
                "abma_def_version": "v1",
                "open_ts_utc": pd.Timestamp("2026-01-05T09:30:00Z"),
                "close_ts_utc": pd.Timestamp("2026-01-05T11:00:00Z"),
            }
        ]
    )
    controls = sample_matched_controls(events=events, trades=trades, quotes=quotes, session_calendar=cal)
    assert not controls.empty


def test_controls_fallback_and_no_controls_reason_are_explicit() -> None:
    symbol = "BTCUSDT"
    trades, quotes = _synthetic_microstructure(symbol, "2026-01-05T09:00:00Z", "2026-01-05T13:00:00Z")
    cal = pd.DataFrame(
        {
            "session_date": ["2026-01-05"],
            "open_ts": [pd.Timestamp("2026-01-05T09:30:00Z")],
            "close_ts": [pd.Timestamp("2026-01-05T13:00:00Z")],
        }
    )
    # Force all candidate volume quartiles to one bucket so exact rv+vol matching is sparse.
    trades["size"] = 1.0
    events = pd.DataFrame(
        [
            {
                "event_id": "evt_fallback",
                "symbol": symbol,
                "session_date": pd.Timestamp("2026-01-05").date(),
                "t0": pd.Timestamp("2026-01-05T10:45:00Z"),
                "window_end": pd.Timestamp("2026-01-05T11:15:00Z"),
                "abma_def_version": "v1",
                "open_ts_utc": pd.Timestamp("2026-01-05T09:30:00Z"),
                "close_ts_utc": pd.Timestamp("2026-01-05T13:00:00Z"),
            },
            {
                "event_id": "evt_no_controls",
                "symbol": symbol,
                "session_date": pd.Timestamp("2026-01-05").date(),
                "t0": pd.Timestamp("2026-01-05T09:35:00Z"),
                "window_end": pd.Timestamp("2026-01-05T10:05:00Z"),
                "abma_def_version": "v1",
                "open_ts_utc": pd.Timestamp("2026-01-05T09:30:00Z"),
                "close_ts_utc": pd.Timestamp("2026-01-05T13:00:00Z"),
            },
        ]
    )
    controls = sample_matched_controls(events=events, trades=trades, quotes=quotes, session_calendar=cal)
    fallback_rows = controls[controls["event_id"] == "evt_fallback"]
    assert not fallback_rows.empty
    assert set(fallback_rows["match_tier"]).issubset({"exact", "rv_only"})
    reasons = pd.DataFrame(controls.attrs.get("no_controls", []))
    assert "evt_no_controls" in set(reasons["event_id"])
    assert "baseline_crosses_session_start_or_insufficient_midpoints" in set(reasons["reason"])


def test_metrics_asof_join_behavior() -> None:
    symbol = "BTCUSDT"
    t0 = pd.Timestamp("2026-01-05T09:30:00Z")
    events = pd.DataFrame(
        [
            {
                "event_id": "e1",
                "symbol": symbol,
                "session_date": pd.Timestamp("2026-01-05").date(),
                "t0": t0,
                "window_end": t0 + pd.Timedelta(minutes=30),
                "abma_def_version": "v1",
            }
        ]
    )
    controls = pd.DataFrame(
        [
            {
                "control_id": "c1",
                "event_id": "e1",
                "symbol": symbol,
                "session_date": pd.Timestamp("2026-01-05").date(),
                "tc": t0 + pd.Timedelta(minutes=5),
            }
        ]
    )
    quotes = pd.DataFrame(
        {
            "ts": [t0, t0 + pd.Timedelta(seconds=5), t0 + pd.Timedelta(seconds=10)],
            "symbol": [symbol, symbol, symbol],
            "bid_px": [99.0, 99.2, 99.4],
            "ask_px": [101.0, 101.2, 101.4],
            "bid_sz": [10.0, 10.0, 10.0],
            "ask_sz": [12.0, 12.0, 12.0],
        }
    )
    trades = pd.DataFrame(
        {
            "ts": [t0 + pd.Timedelta(seconds=i) for i in range(70)],
            "symbol": [symbol] * 70,
            "price": [100.0 + 0.01 * i for i in range(70)],
            "size": [1.0] * 70,
        }
    )

    out = compute_structural_metrics(events=events, controls=controls, trades=trades, quotes=quotes)
    event_curve = out["curves"][(out["curves"]["event_id"] == "e1") & (out["curves"]["anchor_kind"] == "event")].copy()
    tau1 = event_curve.loc[event_curve["tau_s"] == 1, "eff_spread"].iloc[0]
    tau4 = event_curve.loc[event_curve["tau_s"] == 4, "eff_spread"].iloc[0]
    assert np.isfinite(tau1)
    assert tau1 == tau4


def test_metrics_deterministic_curve_hash() -> None:
    symbol = "BTCUSDT"
    trades, quotes = _synthetic_microstructure(symbol, "2026-01-05T09:00:00Z", "2026-01-05T12:00:00Z")
    t0 = pd.Timestamp("2026-01-05T10:30:00Z")
    events = pd.DataFrame(
        [
            {
                "event_id": "evt_hash",
                "symbol": symbol,
                "session_date": pd.Timestamp("2026-01-05").date(),
                "t0": t0,
                "window_end": t0 + pd.Timedelta(minutes=30),
                "abma_def_version": "v1",
            }
        ]
    )
    controls = pd.DataFrame(
        [
            {
                "control_id": "evt_hash_c01",
                "event_id": "evt_hash",
                "symbol": symbol,
                "session_date": pd.Timestamp("2026-01-05").date(),
                "tc": pd.Timestamp("2026-01-05T10:45:00Z"),
            }
        ]
    )
    first = compute_structural_metrics(events=events, controls=controls, trades=trades, quotes=quotes)["curves"].copy()
    second = compute_structural_metrics(events=events, controls=controls, trades=trades, quotes=quotes)["curves"].copy()
    first_hash = int(pd.util.hash_pandas_object(first.fillna(-1.0), index=False).sum())
    second_hash = int(pd.util.hash_pandas_object(second.fillna(-1.0), index=False).sum())
    assert first_hash == second_hash


def test_gate_sign_consistency_and_flip_count() -> None:
    metrics = pd.DataFrame(
        {
            "session_date": ["2024-01-03", "2025-01-03", "2026-01-03"],
            "delta_spread_half_life_sec": [1.0, 1.2, 1.1],
            "delta_microprice_var_decay_rate": [0.1, 0.2, 0.1],
            "delta_trade_sign_entropy_mean": [0.05, 0.04, 0.03],
        }
    )
    curves = pd.DataFrame(
        {
            "anchor_kind": ["event", "event", "event", "control", "control", "control"],
            "tau_s": [0, 1, 2, 0, 1, 2],
            "eff_spread": [0.03, 0.02, 0.01, 0.03, 0.025, 0.02],
            "micro_dev": [0.01, 0.009, 0.008, 0.01, 0.01, 0.009],
        }
    )
    result = evaluate_stabilization(event_metrics=metrics, curves=curves)
    assert not result["stability"].empty
    assert bool(result["stability"]["pass"].iloc[0])

    bad_metrics = pd.DataFrame(
        {
            "session_date": ["2024-01-03", "2025-01-03", "2026-01-03"],
            "delta_spread_half_life_sec": [1.0, -1.0, 1.0],
            "delta_microprice_var_decay_rate": [0.0, 0.0, 0.0],
            "delta_trade_sign_entropy_mean": [0.0, 0.0, 0.0],
        }
    )
    bad_curves = pd.DataFrame(
        {
            "anchor_kind": ["event", "event", "event"],
            "tau_s": [0, 1, 2],
            "eff_spread": [0.01, 0.02, 0.015],
            "micro_dev": [0.01, 0.01, 0.01],
        }
    )
    bad = evaluate_stabilization(event_metrics=bad_metrics, curves=bad_curves)
    assert not bool(bad["stability"]["pass"].iloc[0])
