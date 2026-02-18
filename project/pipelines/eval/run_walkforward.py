from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from eval.splits import build_time_splits
from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.run_manifest import finalize_manifest, start_manifest

INITIAL_EQUITY = 1_000_000.0
BARS_PER_YEAR_15M = 365 * 24 * 4
REQUIRED_SPLIT_METRIC_KEYS = ["total_trades", "ending_equity", "sharpe_annualized", "max_drawdown"]
REQUIRED_STRATEGY_RETURN_COLUMNS = [
    "timestamp",
    "pos",
    "pnl",
    "gross_pnl",
    "trading_cost",
    "funding_pnl",
    "borrow_cost",
]


def _to_float_strict(value: object, *, label: str) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be numeric, got `{value}`") from exc
    if not np.isfinite(out):
        raise ValueError(f"{label} must be finite, got `{value}`")
    return out


def _load_split_metrics_strict(metrics_path: Path, *, split_label: str, split_run_id: str) -> Dict[str, object]:
    if not metrics_path.exists():
        raise ValueError(f"Missing metrics.json for split={split_label} run_id={split_run_id}: {metrics_path}")
    try:
        payload = json.loads(metrics_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"Invalid metrics.json for split={split_label} run_id={split_run_id}: {metrics_path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"metrics.json must be an object for split={split_label} run_id={split_run_id}: {metrics_path}")

    for key in REQUIRED_SPLIT_METRIC_KEYS:
        if key not in payload:
            raise ValueError(
                f"metrics.json missing required key `{key}` for split={split_label} run_id={split_run_id}: {metrics_path}"
            )
        _to_float_strict(payload.get(key), label=f"{split_label}.{key}")

    cost_decomposition = payload.get("cost_decomposition", {})
    if not isinstance(cost_decomposition, dict):
        raise ValueError(
            f"metrics.json cost_decomposition must be object for split={split_label} run_id={split_run_id}: {metrics_path}"
        )
    if "net_alpha" not in cost_decomposition:
        raise ValueError(
            f"metrics.json missing cost_decomposition.net_alpha for split={split_label} run_id={split_run_id}: {metrics_path}"
        )
    _to_float_strict(cost_decomposition.get("net_alpha"), label=f"{split_label}.cost_decomposition.net_alpha")
    return payload


def _run_split_backtest(cmd: List[str]) -> int:
    return subprocess.run(cmd).returncode


def _strategy_id_from_path(path: Path) -> str:
    name = path.name
    prefix = "strategy_returns_"
    suffix = ".csv"
    if not name.startswith(prefix) or not name.endswith(suffix):
        return ""
    strategy_id = name[len(prefix) : -len(suffix)].strip()
    return strategy_id


def _annualized_sharpe(pnl_series: pd.Series) -> float:
    if pnl_series.empty:
        return 0.0
    std = float(pnl_series.std())
    if not np.isfinite(std) or std <= 0.0:
        return 0.0
    mean = float(pnl_series.mean())
    return float((mean / std) * np.sqrt(BARS_PER_YEAR_15M))


def _compute_drawdown(equity_series: pd.Series) -> float:
    if equity_series.empty:
        return 0.0
    peak = equity_series.cummax().replace(0.0, np.nan)
    drawdown = ((equity_series - peak) / peak).replace([np.inf, -np.inf], np.nan).dropna()
    if drawdown.empty:
        return 0.0
    return float(drawdown.min())


def _entry_count(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    ordered = frame.copy()
    if "symbol" not in ordered.columns:
        ordered["symbol"] = "ALL"
    ordered = ordered.sort_values(["symbol", "timestamp"]).reset_index(drop=True)

    entries = 0
    for _, group in ordered.groupby("symbol", sort=True):
        pos = pd.to_numeric(group.get("pos"), errors="coerce").fillna(0.0)
        if "position_scale" in group.columns:
            scale = pd.to_numeric(group.get("position_scale"), errors="coerce").fillna(1.0)
        elif "allocated_position_scale" in group.columns:
            scale = pd.to_numeric(group.get("allocated_position_scale"), errors="coerce").fillna(1.0)
        elif "requested_position_scale" in group.columns:
            scale = pd.to_numeric(group.get("requested_position_scale"), errors="coerce").fillna(1.0)
        else:
            scale = pd.Series(1.0, index=group.index, dtype=float)
        effective_pos = pos * scale
        prior = effective_pos.shift(1).fillna(0.0)
        entries += int(((prior.abs() <= 1e-12) & (effective_pos.abs() > 1e-12)).sum())
    return int(entries)


def _strategy_metrics_from_frame(frame: pd.DataFrame) -> Dict[str, object]:
    if frame.empty:
        return {
            "total_trades": 0,
            "net_pnl": 0.0,
            "stressed_net_pnl": 0.0,
            "ending_equity": INITIAL_EQUITY,
            "sharpe_annualized": 0.0,
            "max_drawdown": 0.0,
            "gate_precheck": {"has_trades": False, "stressed_non_negative": True},
        }

    out = frame.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    out = out.dropna(subset=["timestamp"]).copy()
    if out.empty:
        return {
            "total_trades": 0,
            "net_pnl": 0.0,
            "stressed_net_pnl": 0.0,
            "ending_equity": INITIAL_EQUITY,
            "sharpe_annualized": 0.0,
            "max_drawdown": 0.0,
            "gate_precheck": {"has_trades": False, "stressed_non_negative": True},
        }

    if "symbol" not in out.columns:
        out["symbol"] = "ALL"

    out["pnl"] = pd.to_numeric(out.get("pnl"), errors="coerce").fillna(0.0)
    out["gross_pnl"] = pd.to_numeric(out.get("gross_pnl"), errors="coerce").fillna(out["pnl"])
    out["trading_cost"] = pd.to_numeric(out.get("trading_cost"), errors="coerce").fillna(0.0)
    out["funding_pnl"] = pd.to_numeric(out.get("funding_pnl"), errors="coerce").fillna(0.0)
    out["borrow_cost"] = pd.to_numeric(out.get("borrow_cost"), errors="coerce").fillna(0.0)

    entries = _entry_count(out)
    pnl_ts = out.groupby("timestamp", sort=True)["pnl"].sum()
    stressed = out["gross_pnl"] - (2.0 * out["trading_cost"]) + out["funding_pnl"] - out["borrow_cost"]
    stressed_ts = out.assign(stressed_pnl=stressed).groupby("timestamp", sort=True)["stressed_pnl"].sum()
    equity = INITIAL_EQUITY * (1.0 + pnl_ts.cumsum())

    net_pnl = float(pnl_ts.sum())
    stressed_net_pnl = float(stressed_ts.sum())
    ending_equity = float(equity.iloc[-1]) if not equity.empty else INITIAL_EQUITY

    return {
        "total_trades": int(entries),
        "net_pnl": net_pnl,
        "stressed_net_pnl": stressed_net_pnl,
        "ending_equity": ending_equity,
        "sharpe_annualized": _annualized_sharpe(pnl_ts),
        "max_drawdown": _compute_drawdown(equity),
        "gate_precheck": {
            "has_trades": bool(entries > 0),
            "stressed_non_negative": bool(stressed_net_pnl >= 0.0),
        },
    }


def _load_per_strategy_split_metrics_strict(
    split_run_id: str,
    *,
    split_label: str,
    expected_strategy_ids: List[str] | None = None,
) -> Dict[str, Dict[str, object]]:
    engine_dir = DATA_ROOT / "runs" / split_run_id / "engine"
    if not engine_dir.exists():
        raise ValueError(f"Missing engine directory for split={split_label} run_id={split_run_id}: {engine_dir}")
    strategy_files = sorted(engine_dir.glob("strategy_returns_*.csv"))
    if not strategy_files:
        raise ValueError(
            f"No strategy return artifacts found for split={split_label} run_id={split_run_id} in {engine_dir}"
        )

    out: Dict[str, Dict[str, object]] = {}
    for path in strategy_files:
        strategy_id = _strategy_id_from_path(path)
        if not strategy_id:
            raise ValueError(f"Invalid strategy return filename for split={split_label} run_id={split_run_id}: {path.name}")
        try:
            frame = pd.read_csv(path)
        except Exception as exc:
            raise ValueError(f"Failed reading strategy return file for split={split_label} run_id={split_run_id}: {path}") from exc
        missing_cols = [col for col in REQUIRED_STRATEGY_RETURN_COLUMNS if col not in frame.columns]
        if missing_cols:
            raise ValueError(
                f"Strategy return file missing required columns for split={split_label} run_id={split_run_id}: "
                f"{path.name} missing {missing_cols}"
            )
        out[strategy_id] = _strategy_metrics_from_frame(frame)
    if expected_strategy_ids is not None:
        expected = {str(x).strip() for x in expected_strategy_ids if str(x).strip()}
        missing = sorted(expected - set(out.keys()))
        if missing:
            raise ValueError(
                f"Missing expected strategy returns for split={split_label} run_id={split_run_id}: {missing}"
            )
    return out


def _build_backtest_cmd(
    *,
    split_run_id: str,
    symbols: str,
    start: str,
    end: str,
    force: int,
    fees_bps: float | None,
    slippage_bps: float | None,
    cost_bps: float | None,
    strategies: str | None,
    overlays: str,
    blueprints_path: str | None,
    blueprints_top_k: int,
    blueprints_filter_event_type: str,
    config_paths: List[str],
) -> List[str]:
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "pipelines" / "backtest" / "backtest_strategies.py"),
        "--run_id",
        split_run_id,
        "--symbols",
        symbols,
        "--start",
        start,
        "--end",
        end,
        "--force",
        str(int(force)),
    ]
    if fees_bps is not None:
        cmd.extend(["--fees_bps", str(float(fees_bps))])
    if slippage_bps is not None:
        cmd.extend(["--slippage_bps", str(float(slippage_bps))])
    if cost_bps is not None:
        cmd.extend(["--cost_bps", str(float(cost_bps))])
    if strategies and str(strategies).strip():
        cmd.extend(["--strategies", str(strategies)])
    if blueprints_path and str(blueprints_path).strip():
        cmd.extend(
            [
                "--blueprints_path",
                str(blueprints_path),
                "--blueprints_top_k",
                str(int(blueprints_top_k)),
                "--blueprints_filter_event_type",
                str(blueprints_filter_event_type),
            ]
        )
    if overlays:
        cmd.extend(["--overlays", str(overlays)])
    for config_path in config_paths:
        cmd.extend(["--config", str(config_path)])
    return cmd


def main() -> int:
    parser = argparse.ArgumentParser(description="Run deterministic walk-forward backtest evaluation")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--embargo_days", type=int, default=0)
    parser.add_argument("--train_frac", type=float, default=0.6)
    parser.add_argument("--validation_frac", type=float, default=0.2)
    parser.add_argument("--force", type=int, default=1)
    parser.add_argument("--fees_bps", type=float, default=None)
    parser.add_argument("--slippage_bps", type=float, default=None)
    parser.add_argument("--cost_bps", type=float, default=None)
    parser.add_argument("--strategies", default=None)
    parser.add_argument("--overlays", default="")
    parser.add_argument("--blueprints_path", default=None)
    parser.add_argument("--blueprints_top_k", type=int, default=10)
    parser.add_argument("--blueprints_filter_event_type", default="all")
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument("--out_dir", default=None)
    args = parser.parse_args()

    strategy_mode = bool(args.strategies and str(args.strategies).strip())
    blueprint_mode = bool(args.blueprints_path and str(args.blueprints_path).strip())
    if strategy_mode and blueprint_mode:
        print("run_walkforward: --strategies and --blueprints_path are mutually exclusive.", file=sys.stderr)
        return 1
    if not strategy_mode and not blueprint_mode:
        print("run_walkforward: provide either --strategies or --blueprints_path.", file=sys.stderr)
        return 1

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "eval" / args.run_id
    ensure_dir(out_dir)
    summary_path = out_dir / "walkforward_summary.json"

    params = {
        "run_id": args.run_id,
        "symbols": args.symbols,
        "start": args.start,
        "end": args.end,
        "embargo_days": int(args.embargo_days),
        "train_frac": float(args.train_frac),
        "validation_frac": float(args.validation_frac),
        "strategies": str(args.strategies or ""),
        "blueprints_path": str(args.blueprints_path or ""),
        "config": [str(path) for path in args.config],
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("run_walkforward", args.run_id, params, inputs, outputs)

    try:
        windows = build_time_splits(
            start=args.start,
            end=args.end,
            train_frac=float(args.train_frac),
            validation_frac=float(args.validation_frac),
            embargo_days=int(args.embargo_days),
        )
        split_labels = [str(window.label) for window in windows]
        if "test" not in split_labels:
            raise ValueError(
                "Walkforward split plan must include a `test` window. "
                f"labels={split_labels}, start={args.start}, end={args.end}, "
                f"train_frac={float(args.train_frac)}, validation_frac={float(args.validation_frac)}, "
                f"embargo_days={int(args.embargo_days)}"
            )

        expected_strategy_ids: List[str] | None
        if strategy_mode:
            expected_strategy_ids = [s.strip() for s in str(args.strategies).split(",") if s.strip()]
        else:
            expected_strategy_ids = None

        split_rows: List[Dict[str, object]] = []
        for split in windows:
            split_run_id = f"{args.run_id}__wf_{split.label}"
            cmd = _build_backtest_cmd(
                split_run_id=split_run_id,
                symbols=str(args.symbols),
                start=split.start.isoformat(),
                end=split.end.isoformat(),
                force=int(args.force),
                fees_bps=args.fees_bps,
                slippage_bps=args.slippage_bps,
                cost_bps=args.cost_bps,
                strategies=args.strategies,
                overlays=str(args.overlays or ""),
                blueprints_path=args.blueprints_path,
                blueprints_top_k=int(args.blueprints_top_k),
                blueprints_filter_event_type=str(args.blueprints_filter_event_type),
                config_paths=[str(path) for path in args.config],
            )
            rc = _run_split_backtest(cmd)
            if rc != 0:
                raise RuntimeError(f"Backtest failed for split={split.label} run_id={split_run_id}")

            metrics_path = DATA_ROOT / "lake" / "trades" / "backtests" / "vol_compression_expansion_v1" / split_run_id / "metrics.json"
            metrics = _load_split_metrics_strict(metrics_path, split_label=split.label, split_run_id=split_run_id)
            per_strategy_metrics = _load_per_strategy_split_metrics_strict(
                split_run_id=split_run_id,
                split_label=split.label,
                expected_strategy_ids=expected_strategy_ids,
            )
            stressed_net_pnl = _to_float_strict(
                metrics.get("cost_decomposition", {}).get("net_alpha"),
                label=f"{split.label}.cost_decomposition.net_alpha",
            )
            split_rows.append(
                {
                    "label": split.label,
                    "run_id": split_run_id,
                    "start": split.start.isoformat(),
                    "end": split.end.isoformat(),
                    "metrics_path": str(metrics_path),
                    "metrics": metrics,
                    "per_strategy_metrics": per_strategy_metrics,
                    "stressed_net_pnl": stressed_net_pnl,
                }
            )
            outputs.append({"path": str(metrics_path), "rows": 1, "start_ts": split.start.isoformat(), "end_ts": split.end.isoformat()})

        per_split_metrics = {
            row["label"]: {
                "run_id": row["run_id"],
                "start": row["start"],
                "end": row["end"],
                "total_trades": int(_to_float_strict(row["metrics"].get("total_trades"), label=f"{row['label']}.total_trades")),
                "ending_equity": _to_float_strict(row["metrics"].get("ending_equity"), label=f"{row['label']}.ending_equity"),
                "sharpe_annualized": _to_float_strict(
                    row["metrics"].get("sharpe_annualized"),
                    label=f"{row['label']}.sharpe_annualized",
                ),
                "max_drawdown": _to_float_strict(row["metrics"].get("max_drawdown"), label=f"{row['label']}.max_drawdown"),
                "stressed_net_pnl": _to_float_strict(row.get("stressed_net_pnl"), label=f"{row['label']}.stressed_net_pnl"),
                "gate_precheck": {
                    "has_trades": bool(int(_to_float_strict(row["metrics"].get("total_trades"), label=f"{row['label']}.total_trades")) > 0),
                    "stressed_non_negative": bool(
                        _to_float_strict(row.get("stressed_net_pnl"), label=f"{row['label']}.stressed_net_pnl") >= 0.0
                    ),
                },
            }
            for row in split_rows
        }
        per_strategy_split_metrics: Dict[str, Dict[str, Dict[str, object]]] = {}
        for row in split_rows:
            split_label = str(row["label"])
            split_run_id = str(row["run_id"])
            split_start = str(row["start"])
            split_end = str(row["end"])
            strategy_payload = row.get("per_strategy_metrics", {})
            if not isinstance(strategy_payload, dict):
                continue
            for strategy_id, strategy_metrics in strategy_payload.items():
                if not isinstance(strategy_metrics, dict):
                    continue
                metric_row = dict(strategy_metrics)
                metric_row["run_id"] = split_run_id
                metric_row["start"] = split_start
                metric_row["end"] = split_end
                per_strategy_split_metrics.setdefault(str(strategy_id), {})[split_label] = metric_row

        per_strategy_split_metrics = {
            strategy_id: {
                split_label: split_metrics[split_label]
                for split_label in sorted(split_metrics.keys())
            }
            for strategy_id, split_metrics in sorted(per_strategy_split_metrics.items())
        }
        test_row = next((row for row in split_rows if row["label"] == "test"), None)
        if test_row is None:
            raise ValueError("Walkforward execution did not produce a `test` split row.")
        final_test_metrics = test_row["metrics"]
        if not isinstance(final_test_metrics, dict) or not final_test_metrics:
            raise ValueError("Walkforward final_test_metrics is missing or invalid.")
        integrity_checks = {
            "artifacts_validated": True,
            "required_test_present": True,
            "config_passthrough_count": int(len(args.config)),
        }
        summary = {
            "run_id": args.run_id,
            "splits": [w.to_dict() for w in windows],
            "per_split_metrics": per_split_metrics,
            "per_strategy_split_metrics": per_strategy_split_metrics,
            "final_test_metrics": final_test_metrics,
            "tested_splits": len(split_rows),
            "integrity_checks": integrity_checks,
        }
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        outputs.append({"path": str(summary_path), "rows": int(len(split_rows)), "start_ts": None, "end_ts": None})
        finalize_manifest(
            manifest,
            "success",
            stats={
                "tested_splits": int(len(split_rows)),
                "summary_path": str(summary_path),
                "artifacts_validated": True,
                "required_test_present": True,
            },
        )
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())
