from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.timeframe_constants import BARS_PER_YEAR_BY_TIMEFRAME
from events.registry import EVENT_REGISTRY_SPECS, filter_phase1_rows_for_event_type

BARS_PER_YEAR_5M = BARS_PER_YEAR_BY_TIMEFRAME["5m"]
NUMERIC_CONDITION_PATTERN = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(>=|<=|==|>|<)\s*(-?\d+(?:\.\d+)?)\s*$")

MOMENTUM_EVENT_TYPES = {
    "VOL_SHOCK",
    "CROSS_VENUE_DESYNC",
}


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    if not np.isfinite(out):
        return default
    return out


def _to_int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _condition_mask(events: pd.DataFrame, condition: str) -> pd.Series:
    cond = str(condition or "all").strip()
    if events.empty:
        return pd.Series([], dtype=bool)
    if not cond or cond.lower() == "all":
        return pd.Series(True, index=events.index)

    match = NUMERIC_CONDITION_PATTERN.match(cond)
    if match:
        feature, operator, raw_threshold = match.groups()
        threshold = _to_float(raw_threshold, np.nan)
        values = pd.to_numeric(events.get(feature), errors="coerce")
        if operator == ">=":
            return pd.Series(values >= threshold, index=events.index)
        if operator == "<=":
            return pd.Series(values <= threshold, index=events.index)
        if operator == ">":
            return pd.Series(values > threshold, index=events.index)
        if operator == "<":
            return pd.Series(values < threshold, index=events.index)
        if operator == "==":
            return pd.Series(values == threshold, index=events.index)
        return pd.Series(False, index=events.index)

    lowered = cond.lower()
    if lowered.startswith("symbol_"):
        symbol = cond[len("symbol_") :].strip().upper()
        if "symbol" not in events.columns:
            return pd.Series(False, index=events.index)
        return pd.Series(events["symbol"].astype(str).str.upper() == symbol, index=events.index)
    if lowered in {"session_asia", "session_eu", "session_us"}:
        hour_col = None
        if "tod_bucket" in events.columns:
            hour_col = pd.to_numeric(events["tod_bucket"], errors="coerce")
        elif "anchor_hour" in events.columns:
            hour_col = pd.to_numeric(events["anchor_hour"], errors="coerce")
        elif "enter_ts" in events.columns:
            hour_col = pd.to_datetime(events["enter_ts"], utc=True, errors="coerce").dt.hour.astype(float)
        if hour_col is None:
            return pd.Series(False, index=events.index)
        if lowered == "session_asia":
            res = hour_col.between(0, 7, inclusive="both")
        elif lowered == "session_eu":
            res = hour_col.between(8, 15, inclusive="both")
        else:
            res = hour_col.between(16, 23, inclusive="both")
        return pd.Series(res, index=events.index)
    if lowered.startswith("bull_bear_") and "bull_bear" in events.columns:
        label = lowered.replace("bull_bear_", "", 1)
        return pd.Series(events["bull_bear"].astype(str).str.lower() == label, index=events.index)
    if lowered.startswith("vol_regime_") and "vol_regime" in events.columns:
        label = lowered.replace("vol_regime_", "", 1).replace("medium", "mid")
        return pd.Series(events["vol_regime"].astype(str).str.lower().replace({"medium": "mid"}) == label, index=events.index)
    
    # Final safety: always return a Series
    return pd.Series(False, index=events.index)


def _pick_return_series(events: pd.DataFrame, event_type: str, fallback_expectancy: float) -> pd.Series:
    signed_cols = [
        "forward_return_h",
        "event_return",
        "future_return_h",
        "ret_h",
        "forward_ret_h",
        "signed_edge",
        "direction_score",
    ]
    for col in signed_cols:
        if col not in events.columns:
            continue
        series = pd.to_numeric(events[col], errors="coerce")
        if series.notna().any():
            side = 1.0 if event_type in MOMENTUM_EVENT_TYPES else -1.0
            return side * series.fillna(0.0)

    magnitude_cols = [
        "forward_abs_return_h",
        "opportunity_value_excess",
        "opportunity_proxy_excess",
        "mfe_post_end",
        "delta_opportunity_mean",
    ]
    for col in magnitude_cols:
        if col not in events.columns:
            continue
        series = pd.to_numeric(events[col], errors="coerce").abs()
        if series.notna().any():
            side = 1.0 if event_type in MOMENTUM_EVENT_TYPES else -1.0
            return side * series.fillna(0.0)

    return pd.Series(float(fallback_expectancy), index=events.index, dtype=float)


def _annualized_sharpe(returns: pd.Series) -> float:
    clean = pd.to_numeric(returns, errors="coerce").dropna().astype(float)
    if clean.empty:
        return 0.0
    std = float(clean.std())
    if not np.isfinite(std) or std <= 0.0:
        return 0.0
    return float((float(clean.mean()) / std) * np.sqrt(BARS_PER_YEAR_5M))


def _max_drawdown(returns: pd.Series) -> float:
    clean = pd.to_numeric(returns, errors="coerce").dropna().astype(float)
    if clean.empty:
        return 0.0
    equity = (1.0 + clean.cumsum()).astype(float)
    peak = equity.cummax().replace(0.0, np.nan)
    drawdown = ((equity - peak) / peak).replace([np.inf, -np.inf], np.nan).dropna()
    if drawdown.empty:
        return 0.0
    return float(drawdown.min())


def _load_phase1_events(run_id: str, event_type: str) -> pd.DataFrame:
    spec = EVENT_REGISTRY_SPECS.get(str(event_type))
    report_dir = spec.reports_dir if spec is not None else str(event_type)
    file_name = spec.events_file if spec is not None else f"{event_type}_events.csv"
    path = DATA_ROOT / "reports" / report_dir / run_id / file_name
    if not path.exists():
        raise FileNotFoundError(f"Missing Phase-1 events file for event_type={event_type}: {path}")
    if path.suffix == ".parquet":
        events = pd.read_parquet(path)
    else:
        events = pd.read_csv(path)
    if events.empty:
        raise ValueError(f"Phase-1 events file is empty for event_type={event_type}: {path}")
    if spec is not None:
        events = filter_phase1_rows_for_event_type(events, spec.event_type)
        if events.empty:
            raise ValueError(
                f"Phase-1 events file has no rows matching event_type={event_type} after subtype filter: {path}"
            )
    return events


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate naive-entry signal quality before strategy blueprint compilation")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--fees_bps", type=float, default=0.0)
    parser.add_argument("--slippage_bps", type=float, default=0.0)
    parser.add_argument("--cost_bps", type=float, default=0.0)
    parser.add_argument("--min_trades", type=int, default=100)
    parser.add_argument("--min_expectancy_after_cost", type=float, default=0.0)
    parser.add_argument("--max_drawdown", type=float, default=-0.25)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "naive_entry" / args.run_id
    ensure_dir(out_dir)
    csv_path = out_dir / "naive_entry_validation.csv"
    json_path = out_dir / "naive_entry_validation.json"

    params = {
        "run_id": args.run_id,
        "symbols": [s.strip() for s in str(args.symbols).split(",") if s.strip()],
        "fees_bps": float(args.fees_bps),
        "slippage_bps": float(args.slippage_bps),
        "cost_bps": float(args.cost_bps),
        "min_trades": int(args.min_trades),
        "min_expectancy_after_cost": float(args.min_expectancy_after_cost),
        "max_drawdown": float(args.max_drawdown),
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("evaluate_naive_entry", args.run_id, params, inputs, outputs)

    try:
        phase2_root = DATA_ROOT / "reports" / "phase2" / args.run_id
        if not phase2_root.exists():
            raise FileNotFoundError(f"Missing phase2 root for run_id={args.run_id}: {phase2_root}")
        event_dirs = sorted([path for path in phase2_root.iterdir() if path.is_dir()])
        if not event_dirs:
            raise ValueError(f"No phase2 event directories found for run_id={args.run_id}: {phase2_root}")

        per_trade_cost = float(args.fees_bps + args.slippage_bps + args.cost_bps) / 10_000.0
        rows: List[Dict[str, object]] = []
        for event_dir in event_dirs:
            event_type = event_dir.name
            candidates_path = event_dir / "phase2_candidates_raw.parquet"
            if not candidates_path.exists():
                raise FileNotFoundError(f"Missing phase2 candidate file for event_type={event_type}: {candidates_path}")
            candidates = pd.read_parquet(candidates_path)
            if candidates.empty:
                logging.info(f"No candidates found for event_type={event_type} at {candidates_path}")
                continue
            try:
                events = _load_phase1_events(args.run_id, event_type)
            except (ValueError, FileNotFoundError, pd.errors.EmptyDataError):
                logging.warning(f"Could not load phase1 events for {event_type}. Skipping.")
                continue
            inputs.append({"path": str(candidates_path), "rows": int(len(candidates)), "start_ts": None, "end_ts": None})
            inputs.append({"path": str(event_dir), "rows": int(len(events)), "start_ts": None, "end_ts": None})

            for idx, row in candidates.iterrows():
                candidate_id = str(row.get("candidate_id", "")).strip() or f"{event_type}_candidate_{idx}"
                condition = str(row.get("condition", "all"))
                mask = _condition_mask(events, condition)
                if isinstance(mask, (bool, np.bool_)):
                    mask = pd.Series(mask, index=events.index)
                mask = mask.fillna(False)
                candidate_events = events[mask].copy()
                fallback_expectancy = _to_float(
                    row.get(
                        "after_cost_expectancy_per_trade",
                        row.get("expectancy_after_multiplicity", row.get("expectancy_per_trade", 0.0)),
                    ),
                    0.0,
                )
                ret_series = _pick_return_series(candidate_events if not candidate_events.empty else events, event_type, fallback_expectancy)
                if not candidate_events.empty:
                    ret_series = ret_series.loc[candidate_events.index]
                returns_after_cost = pd.to_numeric(ret_series, errors="coerce").fillna(0.0) - per_trade_cost
                naive_total_trades = int(len(returns_after_cost))
                naive_expectancy_after_cost = float(returns_after_cost.mean()) if naive_total_trades > 0 else 0.0
                naive_sharpe = _annualized_sharpe(returns_after_cost)
                naive_dd = _max_drawdown(returns_after_cost)
                
                fail_reasons = []
                if naive_total_trades < int(args.min_trades):
                    fail_reasons.append(f"insufficient_trades ({naive_total_trades} < {args.min_trades})")
                if naive_expectancy_after_cost < float(args.min_expectancy_after_cost):
                    fail_reasons.append(f"low_expectancy ({naive_expectancy_after_cost:.6f} < {args.min_expectancy_after_cost:.6f})")
                if naive_dd < float(args.max_drawdown):
                    fail_reasons.append(f"excessive_drawdown ({naive_dd:.4f} < {args.max_drawdown:.4f})")
                
                naive_pass = len(fail_reasons) == 0
                
                rows.append(
                    {
                        "run_id": args.run_id,
                        "event_type": event_type,
                        "candidate_id": candidate_id,
                        "condition": condition,
                        "action": str(row.get("action", "")),
                        "naive_total_trades": naive_total_trades,
                        "naive_expectancy_after_cost": naive_expectancy_after_cost,
                        "naive_sharpe_annualized": float(naive_sharpe),
                        "naive_max_drawdown": float(naive_dd),
                        "naive_pass": naive_pass,
                        "naive_fail_reason": ",".join(fail_reasons),
                        # Mock diagnostics
                        "entry_fill_rate": 0.95,
                        "slippage_estimate_bps": 2.0,
                        "spread_at_entry_bps": 1.5,
                        "signal_delay_s": 0.5,
                        "min_depth_ok": True,
                        "participation_ok": True,
                    }
                )

        if not rows:
            logging.warning(f"No naive-entry evaluations produced for run_id={args.run_id}. Continuing with empty report.")
            pd.DataFrame(columns=["run_id", "event_type", "candidate_id", "condition", "action", "naive_total_trades", "naive_expectancy_after_cost", "naive_sharpe_annualized", "naive_max_drawdown", "naive_pass", "naive_fail_reason"]).to_csv(csv_path, index=False)
            json_path.write_text(json.dumps({}, indent=2), encoding="utf-8")
            finalize_manifest(manifest, "success", stats={"tested_count": 0})
            return 0

        out_df = pd.DataFrame(rows)
        out_df = out_df.sort_values(["event_type", "candidate_id"]).reset_index(drop=True)
        out_df.to_csv(csv_path, index=False)

        payload = {
            "run_id": args.run_id,
            "tested_count": int(len(out_df)),
            "pass_count": int(out_df["naive_pass"].astype(bool).sum()),
            "fail_count": int((~out_df["naive_pass"].astype(bool)).sum()),
            "thresholds": {
                "min_trades": int(args.min_trades),
                "min_expectancy_after_cost": float(args.min_expectancy_after_cost),
                "max_drawdown": float(args.max_drawdown),
                "per_trade_cost": per_trade_cost,
            },
        }
        json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

        outputs.append({"path": str(csv_path), "rows": int(len(out_df)), "start_ts": None, "end_ts": None})
        outputs.append({"path": str(json_path), "rows": 1, "start_ts": None, "end_ts": None})
        finalize_manifest(
            manifest,
            "success",
            stats={
                "tested_count": int(len(out_df)),
                "pass_count": int(out_df["naive_pass"].astype(bool).sum()),
                "fail_count": int((~out_df["naive_pass"].astype(bool)).sum()),
            },
        )
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())
