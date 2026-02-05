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


def _load_engine_entries(engine_dir: Path) -> pd.DataFrame:
    strategy_files = sorted(engine_dir.glob("strategy_returns_*.csv"))
    if not strategy_files:
        return pd.DataFrame()

    per_symbol: Dict[str, int] = {}
    for path in strategy_files:
        df = pd.read_csv(path, usecols=["timestamp", "symbol", "pos"])
        if df.empty:
            continue
        df = df.sort_values(["symbol", "timestamp"])
        for symbol, group in df.groupby("symbol", sort=True):
            pos = group["pos"].fillna(0)
            prior = pos.shift(1).fillna(0)
            entries = int(((prior == 0) & (pos != 0)).sum())
            per_symbol[symbol] = per_symbol.get(symbol, 0) + entries

    if not per_symbol:
        return pd.DataFrame()
    return pd.DataFrame(
        [{"symbol": symbol, "total_trades": count, "avg_r": 0.0} for symbol, count in per_symbol.items()]
    )


def _format_funding_section(cleaned_stats: Dict[str, object]) -> List[str]:
    lines: List[str] = []
    lines.append("### Funding coverage (%) by month")
    for symbol, details in cleaned_stats.get("symbols", {}).items():
        lines.append(f"- **{symbol}**")
        for month, values in details.get("pct_missing_funding_event", {}).items():
            coverage = float(values.get("pct_funding_event_coverage", 0.0))
            lines.append(f"  - {month}: {coverage:.2%}")
    lines.append("")
    lines.append("### Funding missing (%) by month")
    for symbol, details in cleaned_stats.get("symbols", {}).items():
        lines.append(f"- **{symbol}**")
        for month, values in details.get("pct_missing_funding_event", {}).items():
            missing = float(values.get("pct_missing_funding_event", 0.0))
            lines.append(f"  - {month}: {missing:.2%}")
    lines.append("")
    lines.append("### Funding diagnostics")
    for symbol, details in cleaned_stats.get("symbols", {}).items():
        lines.append(f"- **{symbol}**")
        pct_bars_with_event = float(details.get("pct_bars_with_funding_event", 0.0))
        funding_min = details.get("funding_rate_scaled_min")
        funding_max = details.get("funding_rate_scaled_max")
        funding_std = details.get("funding_rate_scaled_std")
        funding_min_display = f"{funding_min:.6f}" if isinstance(funding_min, (float, int)) else "n/a"
        funding_max_display = f"{funding_max:.6f}" if isinstance(funding_max, (float, int)) else "n/a"
        funding_std_display = f"{funding_std:.6f}" if isinstance(funding_std, (float, int)) else "n/a"
        lines.append(f"  - % bars with funding_event_ts: {pct_bars_with_event:.2%}")
        lines.append(f"  - funding_rate_scaled min/max/std: {funding_min_display} / {funding_max_display} / {funding_std_display}")
    return lines


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
        engine_dir = PROJECT_ROOT / "runs" / run_id / "engine"
        fallback_per_symbol = _load_engine_entries(engine_dir) if trades.empty else pd.DataFrame()
        equity_curve = pd.read_csv(equity_curve_path) if equity_curve_path.exists() else pd.DataFrame()

        cleaned_manifest_path = PROJECT_ROOT / "runs" / run_id / "build_cleaned_15m.json"
        cleaned_stats = _read_json(cleaned_manifest_path).get("stats", {}) if cleaned_manifest_path.exists() else {}
        features_manifest_path = PROJECT_ROOT / "runs" / run_id / "build_features_v1.json"
        features_stats = _read_json(features_manifest_path).get("stats", {}) if features_manifest_path.exists() else {}
        engine_metrics_path = PROJECT_ROOT / "runs" / run_id / "engine" / "metrics.json"
        engine_metrics = _read_json(engine_metrics_path) if engine_metrics_path.exists() else {}
        engine_diagnostics = engine_metrics.get("diagnostics", {})

        inputs.append({"path": str(metrics_path), "rows": 1, "start_ts": None, "end_ts": None})
        inputs.append({"path": str(trades_dir), "rows": len(trades), "start_ts": None, "end_ts": None})

        report_dir = Path(args.out_dir) if args.out_dir else PROJECT_ROOT / "reports" / "vol_compression_expansion_v1" / run_id
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / "summary.md"

        metrics_total_trades = int(metrics.get("total_trades", 0) or 0)
        metrics_avg_r = float(metrics.get("avg_r", 0.0) or 0.0)
        metrics_win_rate = float(metrics.get("win_rate", 0.0) or 0.0)
        ending_equity = float(metrics.get("ending_equity", 0.0) or 0.0)

        if trades.empty and not fallback_per_symbol.empty:
            per_symbol_trades = fallback_per_symbol
            total_trades = int(fallback_per_symbol["total_trades"].sum())
            avg_r_total = metrics_avg_r
        else:
            per_symbol_trades = (
                trades.groupby("symbol").agg(total_trades=("symbol", "count"), avg_r=("r_multiple", "mean"))
                if not trades.empty
                else pd.DataFrame()
            )
            total_trades = metrics_total_trades if metrics_total_trades else int(len(trades))
            avg_r_total = metrics_avg_r if metrics_total_trades else (float(trades["r_multiple"].mean()) if not trades.empty else 0.0)
        max_drawdown = 0.0
        if not equity_curve.empty:
            equity_series = equity_curve["equity"]
            peak = equity_series.cummax()
            drawdown = (equity_series - peak) / peak
            max_drawdown = float(drawdown.min())
        drawdown_display = f"{max_drawdown:.2%}"
        if abs(max_drawdown) < 0.0001 and max_drawdown != 0.0:
            drawdown_display = f"{max_drawdown:.6%}"

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
            f"- Win rate (combined): {metrics_win_rate:.2%}",
            f"- Avg R (combined): {avg_r_total:.2f}",
            f"- Ending equity (combined): {ending_equity:,.2f}",
            f"- Max drawdown (combined): {drawdown_display}",
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
            lines.append("### Bad bars by month")
            for symbol, details in cleaned_stats.get("symbols", {}).items():
                lines.append(f"- **{symbol}**")
                for month, values in details.get("bad_bar_count", {}).items():
                    lines.append(f"  - {month}: {values.get('bad_bar_count', 0)}")
            lines.append("")
            lines.extend(_format_funding_section(cleaned_stats))

        lines.extend(["", "## Feature Diagnostics", ""])
        if not features_stats:
            lines.append("No feature diagnostics available.")
        else:
            for symbol, details in features_stats.get("symbols", {}).items():
                lines.append(f"- **{symbol}**")
                nan_rates = details.get("nan_rates", {})
                if nan_rates:
                    lines.append(f"  - pct NaN ret_1: {nan_rates.get('ret_1', 0.0):.2%}")
                    lines.append(f"  - pct NaN logret_1: {nan_rates.get('logret_1', 0.0):.2%}")
                    lines.append(f"  - pct NaN rv_96: {nan_rates.get('rv_96', 0.0):.2%}")
                    lines.append(f"  - pct NaN rv_pct_17280: {nan_rates.get('rv_pct_17280', 0.0):.2%}")
                    lines.append(f"  - pct NaN range_med_2880: {nan_rates.get('range_med_2880', 0.0):.2%}")
                segment_stats = details.get("segment_stats", {})
                if segment_stats:
                    lines.append(
                        "  - segments count/min/median/max: "
                        f"{segment_stats.get('count', 0)} / "
                        f"{segment_stats.get('min', 0)} / "
                        f"{segment_stats.get('median', 0)} / "
                        f"{segment_stats.get('max', 0)}"
                    )
                lines.append(f"  - pct rows dropped: {details.get('pct_rows_dropped', 0.0):.2%}")

        lines.extend(["", "## Engine Diagnostics", ""])
        if not engine_diagnostics:
            lines.append("No engine diagnostics available.")
        else:
            for strategy_name, diag in engine_diagnostics.get("strategies", {}).items():
                lines.append(f"- **{strategy_name}**")
                lines.append(
                    f"  - nan_return_bars: {diag.get('nan_return_bars', 0)} "
                    f"({diag.get('nan_return_pct', 0.0):.2%})"
                )
                lines.append(
                    f"  - forced_flat_bars: {diag.get('forced_flat_bars', 0)} "
                    f"({diag.get('forced_flat_pct', 0.0):.2%})"
                )
                lines.append(
                    f"  - missing_feature_bars: {diag.get('missing_feature_bars', 0)} "
                    f"({diag.get('missing_feature_pct', 0.0):.2%})"
                )

        report_path.write_text("\n".join(lines), encoding="utf-8")
        outputs.append({"path": str(report_path), "rows": len(lines), "start_ts": None, "end_ts": None})

        summary_json = {
            "run_id": run_id,
            "total_trades": total_trades,
            "win_rate": metrics_win_rate,
            "avg_r": avg_r_total,
            "ending_equity": ending_equity,
            "max_drawdown": max_drawdown,
            "per_symbol": per_symbol_trades.reset_index().to_dict(orient="records") if not per_symbol_trades.empty else [],
            "fee_sensitivity": fee_table,
            "data_quality": cleaned_stats,
            "feature_quality": features_stats,
            "engine_diagnostics": engine_diagnostics,
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
