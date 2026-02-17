from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from eval.splits import build_time_splits
from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.run_manifest import finalize_manifest, start_manifest


def _read_json(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _run_split_backtest(cmd: List[str]) -> int:
    return subprocess.run(cmd).returncode


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
            )
            rc = _run_split_backtest(cmd)
            if rc != 0:
                raise RuntimeError(f"Backtest failed for split={split.label} run_id={split_run_id}")

            metrics_path = DATA_ROOT / "lake" / "trades" / "backtests" / "vol_compression_expansion_v1" / split_run_id / "metrics.json"
            metrics = _read_json(metrics_path)
            split_rows.append(
                {
                    "label": split.label,
                    "run_id": split_run_id,
                    "start": split.start.isoformat(),
                    "end": split.end.isoformat(),
                    "metrics_path": str(metrics_path),
                    "metrics": metrics,
                }
            )
            outputs.append({"path": str(metrics_path), "rows": 1 if metrics else 0, "start_ts": split.start.isoformat(), "end_ts": split.end.isoformat()})

        per_split_metrics = {
            row["label"]: {
                "run_id": row["run_id"],
                "start": row["start"],
                "end": row["end"],
                "total_trades": int(row["metrics"].get("total_trades", 0)) if isinstance(row["metrics"], dict) else 0,
                "ending_equity": float(row["metrics"].get("ending_equity", 0.0)) if isinstance(row["metrics"], dict) else 0.0,
                "sharpe_annualized": float(row["metrics"].get("sharpe_annualized", 0.0)) if isinstance(row["metrics"], dict) else 0.0,
                "max_drawdown": float(row["metrics"].get("max_drawdown", 0.0)) if isinstance(row["metrics"], dict) else 0.0,
                "stressed_net_pnl": float(
                    (
                        row["metrics"].get("cost_decomposition", {}).get("net_alpha", 0.0)
                        if isinstance(row["metrics"].get("cost_decomposition", {}), dict)
                        else 0.0
                    )
                    if isinstance(row["metrics"], dict)
                    else 0.0
                ),
                "gate_precheck": {
                    "has_trades": bool(int(row["metrics"].get("total_trades", 0)) > 0) if isinstance(row["metrics"], dict) else False,
                    "stressed_non_negative": bool(
                        float(
                            (
                                row["metrics"].get("cost_decomposition", {}).get("net_alpha", 0.0)
                                if isinstance(row["metrics"].get("cost_decomposition", {}), dict)
                                else 0.0
                            )
                            if isinstance(row["metrics"], dict)
                            else 0.0
                        )
                        >= 0.0
                    ),
                },
            }
            for row in split_rows
        }
        test_row = next((row for row in split_rows if row["label"] == "test"), None)
        final_test_metrics = test_row["metrics"] if test_row is not None else {}
        summary = {
            "run_id": args.run_id,
            "splits": [w.to_dict() for w in windows],
            "per_split_metrics": per_split_metrics,
            "final_test_metrics": final_test_metrics,
            "tested_splits": len(split_rows),
        }
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        outputs.append({"path": str(summary_path), "rows": int(len(split_rows)), "start_ts": None, "end_ts": None})
        finalize_manifest(manifest, "success", stats={"tested_splits": int(len(split_rows)), "summary_path": str(summary_path)})
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())
