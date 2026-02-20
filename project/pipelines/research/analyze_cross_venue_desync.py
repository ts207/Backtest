from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import choose_partition_dir, ensure_dir, list_parquet_files, read_parquet, run_scoped_lake_path

WINDOW_END = 32
ANCHOR_MODE = "basis_shock"
ANCHOR_QUANTILE = 0.99
REVERT_Z = 0.5
BASIS_LOOKBACK = 96


def _table_text(df: pd.DataFrame) -> str:
    try:
        return df.to_markdown(index=False)
    except ImportError:
        return df.to_string(index=False)


def _load_feature_frame(run_id: str, symbol: str, market: str, timeframe: str = "5m") -> pd.DataFrame:
    symbol_candidates = [symbol]
    if market == "spot":
        for suffix in ("USDT", "BUSD", "USDC", "FDUSD"):
            if symbol.endswith(suffix) and len(symbol) > len(suffix):
                symbol_candidates.append(symbol[: -len(suffix)])
                break

    frame = pd.DataFrame()
    for symbol_candidate in symbol_candidates:
        candidates = [
            run_scoped_lake_path(DATA_ROOT, run_id, "features", market, symbol_candidate, timeframe, "features_v1"),
            DATA_ROOT / "lake" / "features" / market / symbol_candidate / timeframe / "features_v1",
        ]
        features_dir = choose_partition_dir(candidates)
        if features_dir is None:
            continue
        loaded = read_parquet(list_parquet_files(features_dir))
        if loaded.empty:
            continue
        frame = loaded
        break

    if frame.empty:
        return pd.DataFrame()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame = frame.sort_values("timestamp").reset_index(drop=True)

    close = frame["close"].astype(float)
    if "ret_1" not in frame.columns:
        frame["ret_1"] = close.pct_change()
    if "logret_1" not in frame.columns:
        frame["logret_1"] = np.log(close).diff()
    if "rv" not in frame.columns:
        if "rv_96" in frame.columns:
            frame["rv"] = frame["rv_96"].astype(float)
        else:
            frame["rv"] = frame["logret_1"].rolling(96, min_periods=32).std()
    frame["range_frac"] = (frame["high"].astype(float) - frame["low"].astype(float)) / close.replace(0.0, np.nan)
    frame["range_base"] = frame["range_frac"].rolling(96, min_periods=32).median()
    frame["tail_move"] = frame["ret_1"].abs()
    return frame


def _hazard(times: pd.Series, horizon: int) -> pd.DataFrame:
    t = times.dropna().astype(int)
    rows = []
    for age in range(1, horizon + 1):
        at_risk = int((times.isna() | (times >= age)).sum())
        hits = int((t == age).sum())
        rows.append({"age": age, "at_risk": at_risk, "hits": hits, "hazard": float(hits / at_risk) if at_risk else np.nan})
    return pd.DataFrame(rows)


def _event_metrics(core: pd.DataFrame, start_idx: int, end_idx: int, tail_threshold: float) -> Dict[str, float]:
    win = core.iloc[start_idx : end_idx + 1]
    if win.empty:
        return {}

    close0 = float(core["close"].iat[start_idx]) if pd.notna(core["close"].iat[start_idx]) else np.nan
    range_expansion = np.nan
    if np.isfinite(close0) and close0 > 0:
        range_expansion = float((win["high"].max() - win["low"].min()) / close0)

    t_tail = np.nan
    if np.isfinite(tail_threshold):
        for k in range(start_idx, end_idx + 1):
            if pd.notna(core["tail_move"].iat[k]) and float(core["tail_move"].iat[k]) >= tail_threshold:
                t_tail = float(k - start_idx + 1)
                break

    return {
        "realized_vol_mean": float(win["rv"].mean()) if win["rv"].notna().any() else np.nan,
        "range_expansion": range_expansion,
        "tail_move_probability": float((win["tail_move"] >= tail_threshold).mean()) if np.isfinite(tail_threshold) else np.nan,
        "tail_move_within": int(np.isfinite(t_tail)),
        "time_to_tail_move": t_tail,
    }


def _build_event_and_control_rows(
    symbol: str,
    perp: pd.DataFrame,
    spot: pd.DataFrame,
    window_end: int,
    anchor_mode: str,
    anchor_quantile: float,
    revert_z: float,
    basis_lookback: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if perp.empty or spot.empty:
        return pd.DataFrame(), pd.DataFrame()

    merged = perp.merge(spot, on="timestamp", suffixes=("_perp", "_spot"))
    if merged.empty:
        return pd.DataFrame(), pd.DataFrame()

    merged["basis"] = merged["close_perp"].astype(float) / merged["close_spot"].astype(float) - 1.0
    basis_mean = merged["basis"].rolling(basis_lookback, min_periods=max(4, basis_lookback // 4)).mean()
    basis_std = merged["basis"].rolling(basis_lookback, min_periods=max(4, basis_lookback // 4)).std()
    merged["basis_z"] = (merged["basis"] - basis_mean) / basis_std.replace(0.0, np.nan)
    merged["lead_lag"] = merged["ret_1_perp"] - merged["ret_1_spot"]

    if anchor_mode == "basis_shock":
        anchor_score = merged["basis_z"].abs()
        threshold = anchor_score.quantile(anchor_quantile) if anchor_score.notna().any() else np.nan
        anchors = merged[(anchor_score >= threshold) & anchor_score.notna()].copy() if np.isfinite(threshold) else pd.DataFrame()
        anchors["direction"] = np.sign(merged.loc[anchors.index, "basis_z"].fillna(0.0))
    else:
        anchor_score = merged["lead_lag"].abs()
        threshold = anchor_score.quantile(anchor_quantile) if anchor_score.notna().any() else np.nan
        anchors = merged[(anchor_score >= threshold) & anchor_score.notna()].copy() if np.isfinite(threshold) else pd.DataFrame()
        anchors["direction"] = np.sign(merged.loc[anchors.index, "lead_lag"].fillna(0.0))

    if anchors.empty:
        return pd.DataFrame(), pd.DataFrame()

    core = merged.rename(
        columns={
            "open_perp": "open",
            "high_perp": "high",
            "low_perp": "low",
            "close_perp": "close",
            "ret_1_perp": "ret_1",
            "logret_1_perp": "logret_1",
            "rv_perp": "rv",
            "range_frac_perp": "range_frac",
            "range_base_perp": "range_base",
            "tail_move_perp": "tail_move",
        }
    )

    tail_threshold = float(core["tail_move"].quantile(0.95)) if core["tail_move"].notna().any() else np.nan

    rows_event: List[Dict[str, object]] = []
    rows_ctrl: List[Dict[str, object]] = []

    non_event_pool = core.copy()
    non_event_pool = non_event_pool.assign(rv_rank=non_event_pool["rv"].rank(pct=True)) if not non_event_pool.empty else non_event_pool

    seen_until = -1
    for idx, row in anchors.iterrows():
        if idx <= seen_until:
            continue
        end_idx = min(len(core) - 1, idx + window_end)
        seen_until = end_idx

        basis_z = core["basis_z"].iat[idx]
        if pd.isna(basis_z):
            continue

        time_to_revert = float(window_end + 1)
        for k in range(idx + 1, end_idx + 1):
            if pd.notna(core["basis_z"].iat[k]) and float(abs(core["basis_z"].iat[k])) <= revert_z:
                time_to_revert = float(k - idx)
                break
        failed_mean_revert = time_to_revert > window_end

        if not failed_mean_revert:
            continue

        metrics = _event_metrics(core, idx, end_idx, tail_threshold)
        if not metrics:
            continue
        event_id = f"cvd_{symbol}_{idx:06d}"
        rv_rank = float(core["rv"].rank(pct=True).iat[idx]) if pd.notna(core["rv"].iat[idx]) else np.nan

        rows_event.append(
            {
                "event_type": "cross_venue_desync",
                "event_id": event_id,
                "symbol": symbol,
                "anchor_ts": core["timestamp"].iat[idx],
                "start_idx": idx,
                "end_idx": end_idx,
                "year": int(pd.Timestamp(core["timestamp"].iat[idx]).year),
                "anchor_mode": anchor_mode,
                "anchor_quantile": anchor_quantile,
                "basis_z": float(basis_z),
                "basis": float(core["basis"].iat[idx]),
                "lead_lag": float(core["lead_lag"].iat[idx]),
                "revert_z": revert_z,
                "time_to_revert": time_to_revert,
                "rv_rank": rv_rank,
                **metrics,
            }
        )

        pool = non_event_pool
        if np.isfinite(rv_rank):
            pool = pool.assign(match_dist=(pool["rv_rank"] - rv_rank).abs()).nsmallest(200, "match_dist")
        if pool.empty:
            continue
        pick = pool.sort_values("match_dist", kind="mergesort").iloc[0]
        c_idx = int(pick.name)
        c_end = min(len(core) - 1, c_idx + window_end)
        c_metrics = _event_metrics(core, c_idx, c_end, tail_threshold)
        if not c_metrics:
            continue
        control_time_to_revert = float(window_end + 1)
        for k in range(c_idx + 1, c_end + 1):
            if pd.notna(core["basis_z"].iat[k]) and float(abs(core["basis_z"].iat[k])) <= revert_z:
                control_time_to_revert = float(k - c_idx)
                break
        rows_ctrl.append(
            {
                "event_id": event_id,
                "symbol": symbol,
                "start_idx": c_idx,
                "end_idx": c_end,
                "rv_rank": float(pick["rv_rank"]) if pd.notna(pick["rv_rank"]) else np.nan,
                "time_to_revert": control_time_to_revert,
                **c_metrics,
            }
        )

    return pd.DataFrame(rows_event), pd.DataFrame(rows_ctrl)


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase-1 analyzer for cross-venue lead-lag / desynchronization (structure only)")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "cross_venue_desync" / args.run_id
    ensure_dir(out_dir)

    event_parts: List[pd.DataFrame] = []
    ctrl_parts: List[pd.DataFrame] = []
    missing_perp_symbols: List[str] = []
    missing_spot_symbols: List[str] = []
    for symbol in [s.strip() for s in args.symbols.split(",") if s.strip()]:
        perp = _load_feature_frame(args.run_id, symbol, "perp")
        spot = _load_feature_frame(args.run_id, symbol, "spot")
        if perp.empty:
            missing_perp_symbols.append(symbol)
        if spot.empty:
            missing_spot_symbols.append(symbol)
        if perp.empty or spot.empty:
            continue
        ev, ct = _build_event_and_control_rows(
            symbol=symbol,
            perp=perp,
            spot=spot,
            window_end=WINDOW_END,
            anchor_mode=ANCHOR_MODE,
            anchor_quantile=ANCHOR_QUANTILE,
            revert_z=REVERT_Z,
            basis_lookback=BASIS_LOOKBACK,
        )
        event_parts.append(ev)
        ctrl_parts.append(ct)

    events = pd.concat(event_parts, ignore_index=True) if event_parts else pd.DataFrame()
    controls = pd.concat(ctrl_parts, ignore_index=True) if ctrl_parts else pd.DataFrame()

    deltas = pd.DataFrame()
    hazards = pd.DataFrame()
    phase = pd.DataFrame()
    sign = pd.DataFrame()

    if not events.empty:
        merged = events.merge(controls, on=["event_id", "symbol"], how="left", suffixes=("", "_ctrl"))
        deltas = pd.DataFrame(
            {
                "event_id": merged["event_id"],
                "symbol": merged["symbol"],
                "year": merged["year"],
                "delta_realized_vol_mean": merged["realized_vol_mean"] - merged["realized_vol_mean_ctrl"],
                "delta_range_expansion": merged["range_expansion"] - merged["range_expansion_ctrl"],
                "delta_tail_move_probability": merged["tail_move_probability"] - merged["tail_move_probability_ctrl"],
                "delta_tail_move_within": merged["tail_move_within"] - merged["tail_move_within_ctrl"],
            }
        )
        hazards = _hazard(events["time_to_revert"], WINDOW_END).assign(cohort="events")
        if not controls.empty:
            hazards = pd.concat([hazards, _hazard(controls["time_to_revert"], WINDOW_END).assign(cohort="controls")], ignore_index=True)
        phase = (
            deltas.groupby("symbol", as_index=False)
            .agg(
                n=("event_id", "size"),
                delta_realized_vol_mean=("delta_realized_vol_mean", "mean"),
                delta_range_expansion=("delta_range_expansion", "mean"),
                delta_tail_move_probability=("delta_tail_move_probability", "mean"),
            )
            .sort_values("symbol")
        )
        sign = (
            deltas.groupby(["symbol", "year"], as_index=False)
            .agg(
                n=("event_id", "size"),
                sign_tail_prob=("delta_tail_move_probability", lambda s: float(np.sign(np.nanmean(s)))),
                sign_vol=("delta_realized_vol_mean", lambda s: float(np.sign(np.nanmean(s)))),
            )
            .sort_values(["symbol", "year"])
        )

    events_path = out_dir / "cross_venue_desync_events.csv"
    controls_path = out_dir / "cross_venue_desync_controls.csv"
    deltas_path = out_dir / "cross_venue_desync_matched_deltas.csv"
    hazards_path = out_dir / "cross_venue_desync_hazards.csv"
    phase_path = out_dir / "cross_venue_desync_phase_stability.csv"
    sign_path = out_dir / "cross_venue_desync_sign_stability.csv"
    summary_md_path = out_dir / "cross_venue_desync_summary.md"
    summary_json_path = out_dir / "cross_venue_desync_summary.json"

    events.to_csv(events_path, index=False)
    controls.to_csv(controls_path, index=False)
    deltas.to_csv(deltas_path, index=False)
    hazards.to_csv(hazards_path, index=False)
    phase.to_csv(phase_path, index=False)
    sign.to_csv(sign_path, index=False)

    summary = {
        "event_type": "cross_venue_desync",
        "phase": 1,
        "window": {"x": 0, "y": WINDOW_END},
        "anchor_mode": ANCHOR_MODE,
        "anchor_quantile": ANCHOR_QUANTILE,
        "revert_z": REVERT_Z,
        "basis_lookback": BASIS_LOOKBACK,
        "actions_generated": 0,
        "events": int(len(events)),
        "controls": int(len(controls)),
        "missing_perp_symbols": sorted(set(missing_perp_symbols)),
        "missing_spot_symbols": sorted(set(missing_spot_symbols)),
        "outputs": {
            "events": str(events_path),
            "controls": str(controls_path),
            "deltas": str(deltas_path),
            "hazards": str(hazards_path),
            "phase_stability": str(phase_path),
            "sign_stability": str(sign_path),
        },
    }
    summary_json_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    lines = [
        "# Cross-Venue Lead-Lag / Desynchronization (Phase 1)",
        "",
        f"Run ID: `{args.run_id}`",
        f"Window: [0, {WINDOW_END}]",
        f"Anchor mode: {ANCHOR_MODE}",
        f"Anchor quantile: {ANCHOR_QUANTILE}",
        f"Revert z-threshold: {REVERT_Z}",
        f"Basis lookback: {BASIS_LOOKBACK}",
        "",
        f"- Events: {len(events)}",
        f"- Controls: {len(controls)}",
        f"- Missing perp features symbols: {sorted(set(missing_perp_symbols)) if missing_perp_symbols else 'none'}",
        f"- Missing spot features symbols: {sorted(set(missing_spot_symbols)) if missing_spot_symbols else 'none'}",
        "- Actions generated: 0 (Phase 1 structure only)",
        "",
        "## Matched deltas (head)",
        _table_text(deltas.head(12)) if not deltas.empty else "No matched deltas",
        "",
        "## Hazards (head)",
        _table_text(hazards.head(20)) if not hazards.empty else "No hazards",
        "",
        "## Phase stability",
        _table_text(phase) if not phase.empty else "No phase stability rows",
        "",
        "## Sign stability",
        _table_text(sign) if not sign.empty else "No sign stability rows",
    ]
    summary_md_path.write_text("\n".join(lines), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
