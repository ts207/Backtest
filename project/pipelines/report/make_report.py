from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.config import load_configs
from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.run_manifest import finalize_manifest, start_manifest


def _read_json(path: Path) -> Dict[str, object]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _load_trades(trades_dir: Path) -> pd.DataFrame:
    trade_files = sorted(trades_dir.glob("trades_*.csv"))
    if not trade_files:
        return pd.DataFrame()
    frames = [pd.read_csv(path) for path in trade_files]
    return pd.concat(frames, ignore_index=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate report")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    run_id = args.run_id

    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    config_paths = [str(PROJECT_ROOT / "configs" / "pipeline.yaml")]
    config_paths.extend(args.config)
    config = load_configs(config_paths)

    params = {
        "trade_day_timezone": config.get("trade_day_timezone", "UTC"),
        "run_id": run_id,
        "out_dir": args.out_dir,
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("make_report", run_id, params, inputs, outputs)
    stats: Dict[str, object] = {}

    try:
        trades_dir = PROJECT_ROOT / "lake" / "trades" / "backtests" / "vol_compression_expansion_v1" / run_id
        metrics_path = trades_dir / "metrics.json"
        equity_curve_path = trades_dir / "equity_curve.csv"
        fee_path = trades_dir / "fee_sensitivity.json"
        metrics = _read_json(metrics_path)
        fee_sensitivity = _read_json(fee_path) if fee_path.exists() else {}
        trades = _load_trades(trades_dir)
        equity_curve = pd.read_csv(equity_curve_path) if equity_curve_path.exists() else pd.DataFrame()

        cleaned_manifest_path = PROJECT_ROOT / "runs" / run_id / "build_cleaned_15m.json"
        cleaned_stats = _read_json(cleaned_manifest_path).get("stats", {}) if cleaned_manifest_path.exists() else {}

        inputs.append({"path": str(metrics_path), "rows": 1, "start_ts": None, "end_ts": None})
        inputs.append({"path": str(trades_dir), "rows": len(trades), "start_ts": None, "end_ts": None})

        report_dir = Path(args.out_dir) if args.out_dir else PROJECT_ROOT / "reports" / "vol_compression_expansion_v1" / run_id
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / "summary.md"

        per_symbol_trades = trades.groupby("symbol").agg(total_trades=("symbol", "count"), avg_r=("r_multiple", "mean")) if not trades.empty else pd.DataFrame()
        total_trades = int(len(trades))
        avg_r_total = float(trades["r_multiple"].mean()) if not trades.empty else 0.0
        max_drawdown = 0.0
        if not equity_curve.empty:
            equity_series = equity_curve["equity"]
            peak = equity_series.cummax()
            drawdown = (equity_series - peak) / peak
            max_drawdown = float(drawdown.min())

        fee_table = []
        for key, value in fee_sensitivity.items():
            fee_table.append(
                {
                    "fee_bps_per_side": value.get("fee_bps_per_side", key),
                    "net_return": value.get("net_return"),
                    "avg_r": value.get("avg_r"),
                    "max_drawdown": value.get("max_drawdown"),
                    "trades": value.get("trades"),
                }
            )
        fee_df = pd.DataFrame(fee_table).sort_values("fee_bps_per_side") if fee_table else pd.DataFrame()

        lines = [
            "# Volatility Compression → Expansion Report",
            "",
            f"Run ID: `{run_id}`",
            "",
            "## Summary Metrics",
            f"- Total trades (combined): {total_trades}",
            f"- Avg R (combined): {avg_r_total:.2f}",
            f"- Max drawdown (combined): {max_drawdown:.2%}",
            "",
            "## Trades by Symbol",
            "",
        ]

        if per_symbol_trades.empty:
            lines.append("No trades for this run.")
        else:
            lines.append(per_symbol_trades.reset_index().to_markdown(index=False))

        lines.extend(["", "## Fee Sensitivity", ""])
        if fee_df.empty:
            lines.append("Fee sensitivity data unavailable.")
        else:
            lines.append(fee_df.to_markdown(index=False))

        lines.extend(["", "## Data Quality", ""])
        if not cleaned_stats:
            lines.append("No cleaned data quality stats available.")
        else:
            lines.append("### Missing OHLCV (%) by month")
            for symbol, details in cleaned_stats.get("symbols", {}).items():
                lines.append(f"- **{symbol}**")
                for month, values in details.get("pct_missing_ohlcv", {}).items():
                    lines.append(f"  - {month}: {values.get('pct_missing_ohlcv', 0.0):.2%}")
            lines.append("")
            lines.append("### Funding fill (%) by month")
            for symbol, details in cleaned_stats.get("symbols", {}).items():
                lines.append(f"- **{symbol}**")
                for month, values in details.get("pct_missing_funding_filled", {}).items():
                    lines.append(f"  - {month}: {values.get('pct_missing_funding_filled', 0.0):.2%}")

        report_path.write_text("\n".join(lines), encoding="utf-8")
        outputs.append({"path": str(report_path), "rows": len(lines), "start_ts": None, "end_ts": None})

        summary_json = {
            "run_id": run_id,
            "total_trades": total_trades,
            "avg_r": avg_r_total,
            "max_drawdown": max_drawdown,
            "per_symbol": per_symbol_trades.reset_index().to_dict(orient="records") if not per_symbol_trades.empty else [],
            "fee_sensitivity": fee_table,
            "data_quality": cleaned_stats,
        }
        summary_path = report_dir / "summary.json"
        summary_path.write_text(json.dumps(summary_json, indent=2, sort_keys=True), encoding="utf-8")
        outputs.append({"path": str(summary_path), "rows": 1, "start_ts": None, "end_ts": None})

        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("Report generation failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
