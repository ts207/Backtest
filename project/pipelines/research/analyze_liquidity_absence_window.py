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


ANCHOR_HOURS = {0, 8, 16}


def _table_text(df: pd.DataFrame) -> str:
    try:
        return df.to_markdown(index=False)
    except ImportError:
        return df.to_string(index=False)


def _load_feature_frame(run_id: str, symbol: str, timeframe: str = "15m") -> pd.DataFrame:
    candidates = [
        run_scoped_lake_path(DATA_ROOT, run_id, "features", "perp", symbol, timeframe, "features_v1"),
        DATA_ROOT / "lake" / "features" / "perp" / symbol / timeframe / "features_v1",
    ]
    features_dir = choose_partition_dir(candidates)
    if features_dir is None:
        return pd.DataFrame()
    frame = read_parquet(list_parquet_files(features_dir))
    if frame.empty:
        return frame
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame = frame.sort_values("timestamp").reset_index(drop=True)
    close = frame["close"].astype(float)
    frame["ret_1"] = close.pct_change()
    frame["logret_1"] = np.log(close).diff()
    if "rv_96" in frame.columns:
        frame["rv"] = frame["rv_96"].astype(float)
    else:
        frame["rv"] = frame["logret_1"].rolling(96, min_periods=32).std()
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


def _build_event_rows(symbol: str, core: pd.DataFrame, quantile: float, horizon: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    anchors = core[(core["timestamp"].dt.minute == 0) & (core["timestamp"].dt.hour.isin(ANCHOR_HOURS))].copy()
    if anchors.empty:
        return pd.DataFrame(), pd.DataFrame()

    anchors["anchor_hour"] = anchors["timestamp"].dt.hour
    metric_col = "volume" if "volume" in anchors.columns else "rv"
    anchors["activity_metric"] = anchors[metric_col].astype(float)
    anchors["activity_rank"] = anchors.groupby("anchor_hour")["activity_metric"].rank(pct=True)
    anchors["absence_flag"] = anchors["activity_rank"] <= quantile
    anchors["rv_rank"] = anchors["rv"].rank(pct=True)
    tail_threshold = float(core["tail_move"].quantile(0.95)) if core["tail_move"].notna().any() else np.nan

    events: List[Dict[str, object]] = []
    controls: List[Dict[str, object]] = []

    last_end = -1
    pool_controls = anchors[~anchors["absence_flag"]].copy()

    for _, a in anchors[anchors["absence_flag"]].iterrows():
        idx = int(a.name)
        if idx <= last_end:
            continue
        end_idx = min(len(core) - 1, idx + horizon)
        last_end = end_idx
        win = core.iloc[idx : end_idx + 1]
        if win.empty:
            continue
        close0 = float(core["close"].iat[idx]) if pd.notna(core["close"].iat[idx]) else np.nan
        range_expansion = np.nan
        if np.isfinite(close0) and close0 > 0:
            range_expansion = float((win["high"].max() - win["low"].min()) / close0)

        t_tail = np.nan
        if np.isfinite(tail_threshold):
            for k in range(idx, end_idx + 1):
                if pd.notna(core["tail_move"].iat[k]) and float(core["tail_move"].iat[k]) >= tail_threshold:
                    t_tail = float(k - idx)
                    break

        event_id = f"law_{symbol}_{idx:06d}"
        row = {
            "event_type": "liquidity_absence_window",
            "event_id": event_id,
            "symbol": symbol,
            "anchor_ts": core["timestamp"].iat[idx],
            "anchor_hour": int(a["anchor_hour"]),
            "start_idx": idx,
            "end_idx": end_idx,
            "year": int(pd.Timestamp(core["timestamp"].iat[idx]).year),
            "activity_metric": float(a["activity_metric"]) if pd.notna(a["activity_metric"]) else np.nan,
            "activity_rank": float(a["activity_rank"]),
            "rv_rank": float(a["rv_rank"]) if pd.notna(a["rv_rank"]) else np.nan,
            "tail_threshold": tail_threshold,
            "realized_vol_mean": float(win["rv"].mean()) if win["rv"].notna().any() else np.nan,
            "range_expansion": range_expansion,
            "tail_move_probability": float((win["tail_move"] >= tail_threshold).mean()) if np.isfinite(tail_threshold) else np.nan,
            "tail_move_within": int(np.isfinite(t_tail)),
            "time_to_tail_move": t_tail,
        }
        events.append(row)

        same_hour = pool_controls[pool_controls["anchor_hour"] == int(a["anchor_hour"])]
        if same_hour.empty:
            same_hour = pool_controls
        if not same_hour.empty:
            if np.isfinite(row["rv_rank"]):
                same_hour = same_hour.assign(match_dist=(same_hour["rv_rank"] - row["rv_rank"]).abs())
                pick = same_hour.nsmallest(1, "match_dist").iloc[0]
            else:
                pick = same_hour.sample(n=1, random_state=idx).iloc[0]
            c_idx = int(pick.name)
            c_end_idx = min(len(core) - 1, c_idx + horizon)
            c_win = core.iloc[c_idx : c_end_idx + 1]
            c_close0 = float(core["close"].iat[c_idx]) if pd.notna(core["close"].iat[c_idx]) else np.nan
            c_range = np.nan
            if np.isfinite(c_close0) and c_close0 > 0:
                c_range = float((c_win["high"].max() - c_win["low"].min()) / c_close0)
            c_t_tail = np.nan
            if np.isfinite(tail_threshold):
                for k in range(c_idx, c_end_idx + 1):
                    if pd.notna(core["tail_move"].iat[k]) and float(core["tail_move"].iat[k]) >= tail_threshold:
                        c_t_tail = float(k - c_idx)
                        break
            controls.append(
                {
                    "event_id": event_id,
                    "symbol": symbol,
                    "anchor_hour": int(pick["anchor_hour"]),
                    "start_idx": c_idx,
                    "end_idx": c_end_idx,
                    "activity_metric": float(pick["activity_metric"]) if pd.notna(pick["activity_metric"]) else np.nan,
                    "activity_rank": float(pick["activity_rank"]),
                    "rv_rank": float(pick["rv_rank"]) if pd.notna(pick["rv_rank"]) else np.nan,
                    "realized_vol_mean": float(c_win["rv"].mean()) if c_win["rv"].notna().any() else np.nan,
                    "range_expansion": c_range,
                    "tail_move_probability": float((c_win["tail_move"] >= tail_threshold).mean()) if np.isfinite(tail_threshold) else np.nan,
                    "tail_move_within": int(np.isfinite(c_t_tail)),
                    "time_to_tail_move": c_t_tail,
                }
            )

    return pd.DataFrame(events), pd.DataFrame(controls)


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase-1 analyzer for liquidity absence windows")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--window_end", type=int, default=96)
    parser.add_argument("--absence_quantile", type=float, default=0.2)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "liquidity_absence_window" / args.run_id
    ensure_dir(out_dir)

    all_events: List[pd.DataFrame] = []
    all_controls: List[pd.DataFrame] = []
    for symbol in [s.strip() for s in args.symbols.split(",") if s.strip()]:
        core = _load_feature_frame(args.run_id, symbol)
        if core.empty:
            continue
        events, controls = _build_event_rows(symbol, core, quantile=args.absence_quantile, horizon=args.window_end)
        all_events.append(events)
        all_controls.append(controls)

    events_df = pd.concat(all_events, ignore_index=True) if all_events else pd.DataFrame()
    controls_df = pd.concat(all_controls, ignore_index=True) if all_controls else pd.DataFrame()

    deltas = pd.DataFrame()
    hazards = pd.DataFrame()
    phase = pd.DataFrame()
    sign = pd.DataFrame()
    if not events_df.empty:
        merged = events_df.merge(
            controls_df,
            on=["event_id", "symbol"],
            how="left",
            suffixes=("", "_ctrl"),
        )
        deltas = pd.DataFrame(
            {
                "event_id": merged["event_id"],
                "symbol": merged["symbol"],
                "year": merged["year"],
                "anchor_hour": merged["anchor_hour"],
                "delta_realized_vol_mean": merged["realized_vol_mean"] - merged["realized_vol_mean_ctrl"],
                "delta_range_expansion": merged["range_expansion"] - merged["range_expansion_ctrl"],
                "delta_tail_move_probability": merged["tail_move_probability"] - merged["tail_move_probability_ctrl"],
                "delta_tail_move_within": merged["tail_move_within"] - merged["tail_move_within_ctrl"],
            }
        )
        hazards = pd.concat(
            [
                _hazard(events_df["time_to_tail_move"], args.window_end).assign(cohort="events"),
                _hazard(controls_df["time_to_tail_move"], args.window_end).assign(cohort="controls") if not controls_df.empty else pd.DataFrame(),
            ],
            ignore_index=True,
        )
        phase = (
            deltas.groupby(["anchor_hour"], as_index=False)
            .agg(
                n=("event_id", "size"),
                delta_realized_vol_mean=("delta_realized_vol_mean", "mean"),
                delta_range_expansion=("delta_range_expansion", "mean"),
                delta_tail_move_probability=("delta_tail_move_probability", "mean"),
                delta_tail_move_within=("delta_tail_move_within", "mean"),
            )
            .sort_values("anchor_hour")
        )
        sign = (
            deltas.groupby(["year", "anchor_hour"], as_index=False)
            .agg(
                n=("event_id", "size"),
                sign_tail_prob=("delta_tail_move_probability", lambda s: float(np.sign(np.nanmean(s)))),
                sign_vol=("delta_realized_vol_mean", lambda s: float(np.sign(np.nanmean(s)))),
            )
            .sort_values(["year", "anchor_hour"])
        )

    events_path = out_dir / "liquidity_absence_window_events.csv"
    controls_path = out_dir / "liquidity_absence_window_controls.csv"
    deltas_path = out_dir / "liquidity_absence_window_matched_deltas.csv"
    hazards_path = out_dir / "liquidity_absence_window_hazards.csv"
    phase_path = out_dir / "liquidity_absence_window_phase_stability.csv"
    sign_path = out_dir / "liquidity_absence_window_sign_stability.csv"
    summary_md_path = out_dir / "liquidity_absence_window_summary.md"
    summary_json_path = out_dir / "liquidity_absence_window_summary.json"
    note_path = out_dir / "liquidity_absence_window_interpretation.md"

    events_df.to_csv(events_path, index=False)
    controls_df.to_csv(controls_path, index=False)
    deltas.to_csv(deltas_path, index=False)
    hazards.to_csv(hazards_path, index=False)
    phase.to_csv(phase_path, index=False)
    sign.to_csv(sign_path, index=False)

    event_tail = float(events_df["tail_move_within"].mean()) if not events_df.empty else np.nan
    ctrl_tail = float(controls_df["tail_move_within"].mean()) if not controls_df.empty else np.nan
    interp = (
        "Preliminary Phase-1 read: liquidity-absence windows show lower tail-move incidence than matched controls."
        if np.isfinite(event_tail) and np.isfinite(ctrl_tail) and event_tail < ctrl_tail
        else "Preliminary Phase-1 read: no clear suppression edge yet versus matched controls."
    )
    note_path.write_text("# Liquidity Absence Phase-1 Interpretation\n\n" + interp + "\n", encoding="utf-8")

    summary = {
        "event_type": "liquidity_absence_window",
        "phase": 1,
        "window": {"x": 0, "y": args.window_end},
        "absence_quantile": args.absence_quantile,
        "actions_generated": 0,
        "events": int(len(events_df)),
        "controls": int(len(controls_df)),
        "outputs": {
            "events": str(events_path),
            "controls": str(controls_path),
            "deltas": str(deltas_path),
            "hazards": str(hazards_path),
            "phase_stability": str(phase_path),
            "sign_stability": str(sign_path),
            "interpretation_note": str(note_path),
        },
    }
    summary_json_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    lines = [
        "# Liquidity Absence Window (Phase 1)",
        "",
        f"Run ID: `{args.run_id}`",
        f"Window: [0, {args.window_end}]",
        f"Absence quantile: {args.absence_quantile}",
        "",
        f"- Events: {len(events_df)}",
        f"- Controls: {len(controls_df)}",
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
        "",
        "## Interpretation",
        interp,
    ]
    summary_md_path.write_text("\n".join(lines), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
