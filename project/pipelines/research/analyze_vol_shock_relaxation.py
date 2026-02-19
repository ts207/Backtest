from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from features.vol_shock_relaxation import DEFAULT_VSR_CONFIG, VolShockRelaxationConfig, detect_vol_shock_relaxation_events
from pipelines._lib.io_utils import ensure_dir, list_parquet_files, read_parquet


KEY_METRICS = [
    "relaxed_within_96",
    "auc_excess_rv",
    "rv_decay_half_life",
    "secondary_shock_within_h",
    "range_pct_96",
]


SEED_OFFSETS = {
    "relaxed_within_96": 11,
    "auc_excess_rv": 23,
    "rv_decay_half_life": 37,
    "secondary_shock_within_h": 41,
    "range_pct_96": 53,
}


def _table_text(df: pd.DataFrame) -> str:
    try:
        return df.to_markdown(index=False)
    except ImportError:
        return df.to_string(index=False)


def _bootstrap_mean_ci(values: np.ndarray, n_boot: int, seed: int, alpha: float = 0.05) -> Tuple[float, float, float]:
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    if len(arr) == 0:
        return np.nan, np.nan, np.nan
    rng = np.random.default_rng(seed)
    boots = []
    for _ in range(n_boot):
        sample = rng.choice(arr, size=len(arr), replace=True)
        boots.append(float(np.mean(sample)))
    return float(np.mean(arr)), float(np.quantile(boots, alpha / 2.0)), float(np.quantile(boots, 1.0 - alpha / 2.0))


def _tag_regimes(core: pd.DataFrame) -> pd.DataFrame:
    out = core.copy()
    out["tod_bucket"] = out["timestamp"].dt.hour.astype(int)
    out["vol_regime_q"] = pd.qcut(out["rv_base"].rank(method="first"), 4, labels=False, duplicates="drop")
    out["vol_regime"] = np.where(out["rv_base"] >= out["rv_base"].median(), "high", "low")
    trend = out["close"].astype(float) / out["close"].astype(float).rolling(96, min_periods=96).mean() - 1.0
    out["bull_bear"] = np.where(trend >= 0, "bull", "bear")
    return out


def _occupied_with_cooldown(events: pd.DataFrame, n_rows: int, cooldown: int) -> np.ndarray:
    occ = np.zeros(n_rows, dtype=bool)
    if events.empty:
        return occ
    for _, ev in events.iterrows():
        s = int(ev["enter_idx"])
        e = min(n_rows - 1, int(ev["exit_idx"]) + cooldown)
        occ[s : e + 1] = True
    return occ


def _match_controls(core: pd.DataFrame, events: pd.DataFrame, t_shock: float, m_controls: int, seed: int, cooldown: int) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()
    tagged = _tag_regimes(core)
    pool = tagged[(~_occupied_with_cooldown(events, len(tagged), cooldown)) & (tagged["shock_ratio"] < t_shock)].copy()
    if pool.empty:
        return pd.DataFrame()

    rng = np.random.default_rng(seed)
    rows: List[Dict[str, object]] = []
    for _, ev in events.iterrows():
        row = tagged.iloc[int(ev["enter_idx"])]
        subset = pool[(pool["tod_bucket"] == row["tod_bucket"]) & (pool["vol_regime_q"] == row["vol_regime_q"])]
        if subset.empty:
            subset = pool
        picks = subset.sample(n=min(m_controls, len(subset)), replace=len(subset) < m_controls, random_state=int(rng.integers(0, 2**31 - 1)))
        for _, c in picks.iterrows():
            rows.append(
                {
                    "event_id": ev["event_id"],
                    "control_idx": int(c.name),
                    "duration_bars": int(ev["duration_bars"]),
                }
            )
    return pd.DataFrame(rows)


def _time_to_secondary(core: pd.DataFrame, start: int, end: int, t_shock: float) -> float:
    w = core.iloc[start : end + 1]
    if w.empty or not w["rv"].notna().any():
        return np.nan
    peak_idx = int(w["rv"].idxmax())
    for k in range(peak_idx + 1, end + 1):
        sr = float(core["shock_ratio"].iat[k]) if pd.notna(core["shock_ratio"].iat[k]) else np.nan
        if np.isfinite(sr) and sr >= t_shock:
            return float(k - start)
    return np.nan


def _time_to_range_expansion(core: pd.DataFrame, start: int, end: int, threshold: float) -> float:
    close_enter = float(core["close"].iat[start])
    if close_enter <= 0:
        return np.nan
    for k in range(start + 1, end + 1):
        sub = core.iloc[start : k + 1]
        pct = float((sub["high"].max() - sub["low"].min()) / close_enter)
        if pct >= threshold:
            return float(k - start)
    return np.nan


def _control_metrics(core: pd.DataFrame, controls: pd.DataFrame, t_shock: float, cfg: VolShockRelaxationConfig) -> pd.DataFrame:
    if controls.empty:
        return pd.DataFrame()
    rows = []
    n = len(core)
    for _, c in controls.iterrows():
        start = int(c["control_idx"])
        dur = int(c["duration_bars"])
        exit_idx = min(n - 1, start + max(0, dur - 1))
        post_end = min(n - 1, start + cfg.post_horizon_bars)

        event_win = core.iloc[start : exit_idx + 1]
        post = core.iloc[start : post_end + 1]
        rv_base = float(core["rv_base"].iat[start]) if pd.notna(core["rv_base"].iat[start]) else np.nan
        rv_peak = float(event_win["rv"].max()) if not event_win.empty else np.nan

        half_target = rv_base + 0.5 * (rv_peak - rv_base) if np.isfinite(rv_base) and np.isfinite(rv_peak) else np.nan
        half_life = np.nan
        if np.isfinite(half_target):
            for h in range(0, min(cfg.post_horizon_bars, n - start)):
                rv_h = float(core["rv"].iat[start + h]) if pd.notna(core["rv"].iat[start + h]) else np.nan
                if np.isfinite(rv_h) and rv_h <= half_target:
                    half_life = float(h)
                    break

        h_end = min(n - 1, start + cfg.auc_horizon_bars)
        h_win = core.iloc[start : h_end + 1]
        excess = (h_win["rv"] - rv_base).clip(lower=0.0) if np.isfinite(rv_base) else pd.Series(dtype=float)

        t_sec = _time_to_secondary(core, start, exit_idx, t_shock=t_shock)
        t_range = _time_to_range_expansion(core, start, post_end, cfg.range_expansion_threshold)
        close_enter = float(core["close"].iat[start])
        range_pct_96 = float((post["high"].max() - post["low"].min()) / close_enter) if close_enter > 0 and not post.empty else np.nan

        rows.append(
            {
                "event_id": c["event_id"],
                "control_idx": start,
                "relaxed_within_96": int((exit_idx - start) <= cfg.post_horizon_bars),
                "auc_excess_rv": float(excess.fillna(0.0).sum()) if not excess.empty else np.nan,
                "rv_decay_half_life": half_life,
                "secondary_shock_within_h": int(np.isfinite(t_sec)),
                "time_to_secondary_shock": t_sec,
                "range_pct_96": range_pct_96,
                "time_to_range_expansion": t_range,
            }
        )
    return pd.DataFrame(rows)


def _hazard_curve(times: pd.Series, horizon: int) -> pd.DataFrame:
    rows = []
    t = times.dropna().astype(int)
    for k in range(1, horizon + 1):
        at_risk = int((times.isna() | (times >= k)).sum())
        hits = int((t == k).sum())
        rows.append({"age": k, "at_risk": at_risk, "hits": hits, "hazard": float(hits / at_risk) if at_risk else np.nan})
    return pd.DataFrame(rows)


def _sanity_table(events: pd.DataFrame, cfg: VolShockRelaxationConfig) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()
    rows = []
    for symbol, g in events.groupby("symbol", sort=True):
        gg = g.sort_values("enter_idx").reset_index(drop=True)
        overlap_viol = 0
        for i in range(len(gg) - 1):
            if int(gg.loc[i + 1, "enter_idx"]) <= int(gg.loc[i, "exit_idx"]) + cfg.cooldown_bars:
                overlap_viol += 1
        rows.append(
            {
                "symbol": symbol,
                "event_count": int(len(gg)),
                "median_time_to_relax": float(gg["time_to_relax"].median()),
                "median_duration": float(gg["duration_bars"].median()),
                "min_duration": int(gg["duration_bars"].min()),
                "max_duration": int(gg["duration_bars"].max()),
                "cooldown_overlap_violations": int(overlap_viol),
                "non_degenerate_count": bool((len(gg) >= 5) and (len(gg) < 100000)),
                "time_to_relax_in_bounds": bool((gg["time_to_relax"].median() > 0) and (gg["time_to_relax"].median() < cfg.post_horizon_bars)),
            }
        )
    return pd.DataFrame(rows)


def _delta_by_split(events: pd.DataFrame, controls: pd.DataFrame, n_boot: int, seed: int) -> pd.DataFrame:
    if events.empty or controls.empty:
        return pd.DataFrame()
    ctrl_mean = controls.groupby("event_id", as_index=False).mean(numeric_only=True)
    merged = events.merge(ctrl_mean, on="event_id", how="left", suffixes=("", "_ctrl"))

    split_defs = [("all", "ALL")]
    for col in ["symbol", "bull_bear", "vol_regime"]:
        for val in sorted(merged[col].dropna().unique().tolist()):
            split_defs.append((col, str(val)))

    rows = []
    for split_name, split_value in split_defs:
        if split_name == "all":
            sub = merged
        else:
            sub = merged[merged[split_name].astype(str) == split_value]
        if sub.empty:
            continue
        for metric in KEY_METRICS:
            delta = (sub[metric] - sub[f"{metric}_ctrl"]).to_numpy(dtype=float)
            mean, lo, hi = _bootstrap_mean_ci(delta, n_boot=n_boot, seed=seed + SEED_OFFSETS[metric])
            status = "positive" if np.isfinite(lo) and lo > 0 else "negative" if np.isfinite(hi) and hi < 0 else "includes_zero"
            rows.append(
                {
                    "split_name": split_name,
                    "split_value": split_value,
                    "metric": metric,
                    "n": int(np.isfinite(delta).sum()),
                    "delta_mean": mean,
                    "delta_ci_low": lo,
                    "delta_ci_high": hi,
                    "status": status,
                }
            )
    return pd.DataFrame(rows)


def _phase_stability(events: pd.DataFrame, tolerance: int) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()
    rows = []
    for split in ["symbol", "bull_bear", "vol_regime"]:
        grouped = events.groupby(split, dropna=False)
        t_peak = grouped["t_rv_peak"].median().dropna()
        t_half = grouped["rv_decay_half_life"].median().dropna()
        t_relax = grouped["time_to_relax"].median().dropna()
        dur = grouped["duration_bars"].median().dropna()
        if t_peak.empty or t_relax.empty:
            rows.append({"split": split, "status": "insufficient"})
            continue

        spread_peak = float(t_peak.max() - t_peak.min())
        spread_half = float(t_half.max() - t_half.min()) if not t_half.empty else np.nan
        spread_relax = float(t_relax.max() - t_relax.min())

        frac_peak = (t_peak / dur.reindex(t_peak.index)).dropna()
        frac_relax = (t_relax / dur.reindex(t_relax.index)).dropna()
        frac_stable = bool(
            (not frac_peak.empty and float(frac_peak.max() - frac_peak.min()) <= 0.15)
            and (not frac_relax.empty and float(frac_relax.max() - frac_relax.min()) <= 0.15)
        )
        abs_stable = spread_peak <= tolerance and spread_relax <= tolerance and (np.isnan(spread_half) or spread_half <= tolerance)
        rows.append(
            {
                "split": split,
                "status": "pass" if (abs_stable or frac_stable) else "fail",
                "spread_t_rv_peak": spread_peak,
                "spread_t_half_life": spread_half,
                "spread_t_relax": spread_relax,
                "fractional_stable": frac_stable,
            }
        )
    return pd.DataFrame(rows)


def _year_sign_stability(events: pd.DataFrame, controls: pd.DataFrame) -> pd.DataFrame:
    if events.empty or controls.empty:
        return pd.DataFrame()
    ctrl_mean = controls.groupby("event_id", as_index=False).mean(numeric_only=True)
    merged = events.merge(ctrl_mean, on="event_id", how="left", suffixes=("", "_ctrl"))
    rows = []
    for metric in KEY_METRICS:
        signs = []
        for _, g in merged.groupby("year", sort=True):
            d = (g[metric] - g[f"{metric}_ctrl"]).mean()
            if pd.isna(d) or abs(d) < 1e-12:
                signs.append(0)
            elif d > 0:
                signs.append(1)
            else:
                signs.append(-1)
        nz = [s for s in signs if s != 0]
        rows.append({"metric": metric, "year_signs": ",".join(str(x) for x in signs), "sign_flip": len(set(nz)) > 1 if nz else False})
    return pd.DataFrame(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze Vol Shock -> Relaxation events (event-level, de-overlapped)")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--m_controls", type=int, default=5)
    parser.add_argument("--bootstrap_iters", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--phase_tolerance_bars", type=int, default=16)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "vol_shock_relaxation" / args.run_id
    ensure_dir(out_dir)

    cfg = DEFAULT_VSR_CONFIG
    all_events = []
    all_controls = []
    all_hazards = []
    thresholds = []

    for symbol in symbols:
        features_dir = DATA_ROOT / "lake" / "features" / "perp" / symbol / args.timeframe / "features_v1"
        feats = read_parquet(list_parquet_files(features_dir))
        if feats.empty:
            raise ValueError(f"No features found for {symbol} in {features_dir}")

        events, core, meta = detect_vol_shock_relaxation_events(feats, symbol=symbol, config=cfg)
        if core.empty:
            continue
        core = _tag_regimes(core)
        thresholds.append({"symbol": symbol, **meta})
        if events.empty:
            continue

        events = events.reset_index(drop=True)
        enter_core = core.loc[events["enter_idx"].astype(int)].reset_index(drop=True)
        events["tod_bucket"] = enter_core["tod_bucket"].values
        events["vol_regime_q"] = enter_core["vol_regime_q"].values
        events["vol_regime"] = enter_core["vol_regime"].values
        events["bull_bear"] = enter_core["bull_bear"].values
        events["year"] = pd.to_datetime(events["enter_ts"], utc=True).dt.year.astype(int)
        events["time_to_range_expansion"] = [
            _time_to_range_expansion(core, int(r.enter_idx), min(len(core)-1, int(r.enter_idx)+cfg.post_horizon_bars), cfg.range_expansion_threshold)
            for r in events.itertuples(index=False)
        ]

        controls_map = _match_controls(core, events, t_shock=meta["t_shock"], m_controls=args.m_controls, seed=args.seed, cooldown=cfg.cooldown_bars)
        controls = _control_metrics(core, controls_map, t_shock=meta["t_shock"], cfg=cfg)

        if not controls.empty:
            all_controls.append(controls)
            # hazards absolute + delta
            ev_hz_sec = _hazard_curve(events["time_to_secondary_shock"], cfg.post_horizon_bars)
            ct_hz_sec = _hazard_curve(controls["time_to_secondary_shock"], cfg.post_horizon_bars)
            hz = ev_hz_sec.merge(ct_hz_sec[["age", "hazard"]].rename(columns={"hazard": "hazard_control"}), on="age", how="left")
            hz["hazard_control"] = hz["hazard_control"].fillna(0.0)
            hz["hazard_delta"] = hz["hazard"].fillna(0.0) - hz["hazard_control"]
            hz["symbol"] = symbol
            hz["outcome"] = "secondary_shock"
            all_hazards.append(hz)

            ev_hz_rng = _hazard_curve(events["time_to_range_expansion"], cfg.post_horizon_bars)
            ct_hz_rng = _hazard_curve(controls["time_to_range_expansion"], cfg.post_horizon_bars)
            hz2 = ev_hz_rng.merge(ct_hz_rng[["age", "hazard"]].rename(columns={"hazard": "hazard_control"}), on="age", how="left")
            hz2["hazard_control"] = hz2["hazard_control"].fillna(0.0)
            hz2["hazard_delta"] = hz2["hazard"].fillna(0.0) - hz2["hazard_control"]
            hz2["symbol"] = symbol
            hz2["outcome"] = "range_expansion"
            all_hazards.append(hz2)

        all_events.append(events)

    events_df = pd.concat(all_events, ignore_index=True) if all_events else pd.DataFrame()
    controls_df = pd.concat(all_controls, ignore_index=True) if all_controls else pd.DataFrame()
    hazards_df = pd.concat(all_hazards, ignore_index=True) if all_hazards else pd.DataFrame()
    thresholds_df = pd.DataFrame(thresholds)

    sanity_df = _sanity_table(events_df, cfg)
    delta_df = _delta_by_split(events_df, controls_df, n_boot=args.bootstrap_iters, seed=args.seed)
    phase_df = _phase_stability(events_df, tolerance=args.phase_tolerance_bars)
    sign_df = _year_sign_stability(events_df, controls_df)

    auc_rows = []
    if not hazards_df.empty:
        for (symbol, outcome), g in hazards_df.groupby(["symbol", "outcome"], sort=True):
            auc_rows.append(
                {
                    "symbol": symbol,
                    "outcome": outcome,
                    "auc_hazard": float(g["hazard"].fillna(0.0).sum()),
                    "auc_hazard_control": float(g["hazard_control"].fillna(0.0).sum()),
                    "auc_hazard_delta": float(g["hazard_delta"].fillna(0.0).sum()),
                }
            )
    auc_df = pd.DataFrame(auc_rows)

    # Decision gates
    required_splits = delta_df[delta_df["split_name"].isin(["symbol", "bull_bear", "vol_regime"])].copy() if not delta_df.empty else pd.DataFrame()
    stable_ci = False
    if not required_splits.empty:
        stable_ci = True
        for metric in KEY_METRICS:
            m = required_splits[required_splits["metric"] == metric]
            if m.empty or (m["status"] == "includes_zero").any() or (m["status"].nunique() != 1):
                stable_ci = False
                break
    phase_pass = (not phase_df.empty) and bool((phase_df["status"] == "pass").all())
    sign_pass = sign_df.empty or bool((~sign_df["sign_flip"]).all())
    decision = "promote" if (stable_ci and phase_pass and sign_pass) else "freeze"

    # Outputs
    files = {
        "events": out_dir / "vol_shock_relaxation_events.csv",
        "controls": out_dir / "vol_shock_relaxation_controls.csv",
        "hazards": out_dir / "vol_shock_relaxation_hazards.csv",
        "hazard_auc": out_dir / "vol_shock_relaxation_hazard_auc.csv",
        "deltas": out_dir / "vol_shock_relaxation_matched_deltas.csv",
        "phase": out_dir / "vol_shock_relaxation_phase_stability.csv",
        "sanity": out_dir / "vol_shock_relaxation_sanity.csv",
        "sign": out_dir / "vol_shock_relaxation_sign_stability.csv",
        "thresholds": out_dir / "vol_shock_relaxation_thresholds.csv",
    }
    events_df.to_csv(files["events"], index=False)
    controls_df.to_csv(files["controls"], index=False)
    hazards_df.to_csv(files["hazards"], index=False)
    auc_df.to_csv(files["hazard_auc"], index=False)
    delta_df.to_csv(files["deltas"], index=False)
    phase_df.to_csv(files["phase"], index=False)
    sanity_df.to_csv(files["sanity"], index=False)
    sign_df.to_csv(files["sign"], index=False)
    thresholds_df.to_csv(files["thresholds"], index=False)

    summary = {
        "run_id": args.run_id,
        "timeframe": args.timeframe,
        "vsr_def_version": cfg.def_version,
        "decision": decision,
        "gates": {"stable_ci": stable_ci, "phase_pass": phase_pass, "sign_pass": sign_pass},
        "event_count": int(len(events_df)),
        "files": {k: str(v) for k, v in files.items()},
    }
    json_path = out_dir / "vol_shock_relaxation_summary.json"
    json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    md_lines = [
        "# Vol Shock -> Relaxation Verification",
        "",
        f"Run ID: `{args.run_id}`",
        f"Decision: **{decision.upper()}**",
        "",
        "## 1) Sanity & Coverage",
        _table_text(sanity_df) if not sanity_df.empty else "No rows",
        "",
        "## 2) Matched-baseline deltas + CIs",
        _table_text(delta_df) if not delta_df.empty else "No rows",
        "",
        "## 3) Hazard AUC + Phase stability",
        _table_text(auc_df) if not auc_df.empty else "No rows",
        "",
        _table_text(phase_df) if not phase_df.empty else "No rows",
        "",
        "## 4) Year sign stability",
        _table_text(sign_df) if not sign_df.empty else "No rows",
    ]
    md_path = out_dir / "vol_shock_relaxation_summary.md"
    md_path.write_text("\n".join(md_lines), encoding="utf-8")

    for f in [*files.values(), json_path, md_path]:
        logging.info("Wrote %s", f)
    return 0


if __name__ == "__main__":
    sys.exit(main())
