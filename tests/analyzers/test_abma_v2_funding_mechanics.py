import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "project"))

from analyzers.abma_v2_funding.config_v2 import ABMAFundingConfig
from analyzers.abma_v2_funding.controls import sample_matched_controls
from analyzers.abma_v2_funding.events import extract_funding_boundary_events
from analyzers.abma_v2_funding.metrics import compute_structural_metrics


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


def test_funding_events_stable_ids_and_session_assignment() -> None:
    symbol = "BTCUSDT"
    funding_events = pd.DataFrame(
        {
            "ts": [pd.Timestamp("2026-01-05T08:00:00Z"), pd.Timestamp("2026-01-05T16:00:00Z")],
            "symbol": [symbol, symbol],
            "event_type": ["FUNDING", "FUNDING"],
            "source": ["archive", "archive"],
        }
    )
    cal = pd.DataFrame(
        {
            "session_date": ["2026-01-05"],
            "open_ts": [pd.Timestamp("2026-01-05T00:00:00Z")],
            "close_ts": [pd.Timestamp("2026-01-05T23:59:59Z")],
        }
    )
    first = extract_funding_boundary_events(funding_events=funding_events, session_calendar=cal)
    second = extract_funding_boundary_events(funding_events=funding_events, session_calendar=cal)
    assert not first.empty
    assert first["event_id"].tolist() == second["event_id"].tolist()
    assert set(first["session_date"]) == {pd.Timestamp("2026-01-05").date()}


def test_controls_exclude_funding_windows() -> None:
    symbol = "BTCUSDT"
    trades, quotes = _synthetic_microstructure(symbol, "2026-01-05T09:00:00Z", "2026-01-05T13:00:00Z")
    cal = pd.DataFrame(
        {
            "session_date": ["2026-01-05"],
            "open_ts": [pd.Timestamp("2026-01-05T09:00:00Z")],
            "close_ts": [pd.Timestamp("2026-01-05T13:00:00Z")],
        }
    )
    t0 = pd.Timestamp("2026-01-05T10:30:00Z")
    events = pd.DataFrame(
        [
            {
                "event_id": "evt_funding",
                "symbol": symbol,
                "session_date": pd.Timestamp("2026-01-05").date(),
                "t0": t0,
                "window_end": t0 + pd.Timedelta(minutes=30),
                "abma_def_version": "v2_funding",
                "open_ts_utc": pd.Timestamp("2026-01-05T09:00:00Z"),
                "close_ts_utc": pd.Timestamp("2026-01-05T13:00:00Z"),
            }
        ]
    )
    config = ABMAFundingConfig(exclusion_pre_minutes=2, exclusion_post_minutes=2)
    controls = sample_matched_controls(events=events, trades=trades, quotes=quotes, session_calendar=cal, config=config)
    assert not controls.empty
    lower = t0 - pd.Timedelta(minutes=config.exclusion_pre_minutes)
    upper = t0 + pd.Timedelta(minutes=config.exclusion_post_minutes)
    assert (~controls["tc"].between(lower, upper, inclusive="both")).all()


def test_controls_fallback_and_no_controls_reason_are_explicit() -> None:
    symbol = "BTCUSDT"
    trades, quotes = _synthetic_microstructure(symbol, "2026-01-05T09:00:00Z", "2026-01-05T13:00:00Z")
    cal = pd.DataFrame(
        {
            "session_date": ["2026-01-05"],
            "open_ts": [pd.Timestamp("2026-01-05T09:00:00Z")],
            "close_ts": [pd.Timestamp("2026-01-05T13:00:00Z")],
        }
    )
    trades["size"] = 1.0
    events = pd.DataFrame(
        [
            {
                "event_id": "evt_fallback",
                "symbol": symbol,
                "session_date": pd.Timestamp("2026-01-05").date(),
                "t0": pd.Timestamp("2026-01-05T10:45:00Z"),
                "window_end": pd.Timestamp("2026-01-05T11:15:00Z"),
                "abma_def_version": "v2_funding",
                "open_ts_utc": pd.Timestamp("2026-01-05T09:00:00Z"),
                "close_ts_utc": pd.Timestamp("2026-01-05T13:00:00Z"),
            },
            {
                "event_id": "evt_no_controls",
                "symbol": symbol,
                "session_date": pd.Timestamp("2026-01-05").date(),
                "t0": pd.Timestamp("2026-01-05T09:05:00Z"),
                "window_end": pd.Timestamp("2026-01-05T09:35:00Z"),
                "abma_def_version": "v2_funding",
                "open_ts_utc": pd.Timestamp("2026-01-05T09:00:00Z"),
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


def test_metrics_asof_join_behavior_for_funding() -> None:
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
                "abma_def_version": "v2_funding",
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
