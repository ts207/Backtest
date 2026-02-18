from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.config import load_configs
from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines.report.capital_allocation import AllocationConfig, allocation_metadata, build_allocation_weights

REQUIRED_METRIC_FIELDS = (
    "total_trades",
    "avg_r",
    "win_rate",
    "ending_equity",
    "sharpe_annualized",
)
REQUIRED_TRADE_COLUMNS = ("symbol", "r_multiple")
REQUIRED_ENGINE_FALLBACK_COLUMNS = ("timestamp", "symbol", "pos")



def _table_text(df: pd.DataFrame) -> str:
    try:
        return df.to_markdown(index=False)
    except ImportError:
        return df.to_string(index=False)

def _read_json(path: Path) -> Dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required JSON artifact: {path}")
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid JSON payload (expected object): {path}")
    return payload


def _to_float_strict(value: object, *, field_name: str) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid numeric field '{field_name}': {value!r}") from exc
    if pd.isna(out):
        raise ValueError(f"Invalid numeric field '{field_name}': {value!r}")
    return out


def _load_trades(trades_dir: Path) -> tuple[pd.DataFrame, List[Path]]:
    trade_files = sorted(trades_dir.glob("trades_*.csv"))
    if not trade_files:
        return pd.DataFrame(), []
    frames = [pd.read_csv(path) for path in trade_files]
    missing_cols = [col for col in REQUIRED_TRADE_COLUMNS if any(col not in frame.columns for frame in frames)]
    if missing_cols:
        raise ValueError(f"Trade files missing required columns {missing_cols} under {trades_dir}")
    return pd.concat(frames, ignore_index=True), trade_files


def _load_engine_entries(engine_dir: Path, expected_strategy_ids: List[str]) -> tuple[pd.DataFrame, Dict[str, object]]:
    expected = [str(x).strip() for x in expected_strategy_ids if str(x).strip()]
    if not expected:
        raise ValueError("Engine fallback requires non-empty metrics.metadata.strategy_ids")
    expected_set = set(expected)
    strategy_files = sorted(engine_dir.glob("strategy_returns_*.csv"))
    strategy_by_id: Dict[str, Path] = {}
    for path in strategy_files:
        name = path.name
        if not name.startswith("strategy_returns_") or not name.endswith(".csv"):
            continue
        strategy_id = name[len("strategy_returns_") : -len(".csv")].strip()
        if strategy_id:
            strategy_by_id[strategy_id] = path

    missing_expected = sorted(expected_set - set(strategy_by_id.keys()))
    if missing_expected:
        raise ValueError(
            f"Engine fallback missing required strategy return files for ids={missing_expected} under {engine_dir}"
        )
    unexpected = sorted(set(strategy_by_id.keys()) - expected_set)
    considered_paths = [strategy_by_id[strategy_id] for strategy_id in expected if strategy_id in strategy_by_id]

    per_symbol: Dict[str, int] = {}
    for path in considered_paths:
        try:
            df = pd.read_csv(path, usecols=list(REQUIRED_ENGINE_FALLBACK_COLUMNS))
        except ValueError as exc:
            raise ValueError(
                f"Engine fallback file missing required columns {list(REQUIRED_ENGINE_FALLBACK_COLUMNS)}: {path}"
            ) from exc
        if df.empty:
            continue
        df = df.sort_values(["symbol", "timestamp"])
        for symbol, group in df.groupby("symbol", sort=True):
            pos = group["pos"].fillna(0)
            prior = pos.shift(1).fillna(0)
            entries = int(((prior == 0) & (pos != 0)).sum())
            per_symbol[symbol] = per_symbol.get(symbol, 0) + entries

    if not per_symbol:
        return pd.DataFrame(), {
            "strategy_files_considered": [p.name for p in considered_paths],
            "unexpected_strategy_files_detected": bool(unexpected),
            "unexpected_strategy_ids": unexpected,
        }
    return (
        pd.DataFrame(
            [{"symbol": symbol, "total_trades": count, "avg_r": 0.0} for symbol, count in per_symbol.items()]
        ),
        {
            "strategy_files_considered": [p.name for p in considered_paths],
            "unexpected_strategy_files_detected": bool(unexpected),
            "unexpected_strategy_ids": unexpected,
        },
    )




def _load_context_segments(engine_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    strategy_files = sorted(engine_dir.glob("strategy_returns_*.csv"))
    if not strategy_files:
        return pd.DataFrame(), pd.DataFrame()

    frames = []
    for path in strategy_files:
        usecols = ["timestamp", "symbol", "pnl", "fp_active", "fp_age_bars"]
        try:
            frame = pd.read_csv(path, usecols=usecols)
        except ValueError:
            continue
        if frame.empty:
            continue
        frame["fp_active"] = frame["fp_active"].fillna(0).astype(int)
        frame["fp_age_bars"] = frame["fp_age_bars"].fillna(0).astype(int)
        frame["pnl"] = frame["pnl"].fillna(0.0).astype(float)
        frames.append(frame)

    if not frames:
        return pd.DataFrame(), pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    by_active = (
        combined.groupby("fp_active", sort=True)
        .agg(bars=("pnl", "size"), total_pnl=("pnl", "sum"), mean_pnl=("pnl", "mean"))
        .reset_index()
    )

    def _age_bucket(row: pd.Series) -> str:
        if int(row["fp_active"]) == 0:
            return "inactive"
        age = int(row["fp_age_bars"])
        if age <= 8:
            return "0-8"
        if age <= 30:
            return "9-30"
        if age <= 96:
            return "31-96"
        return ">96"

    combined["fp_age_bucket"] = combined.apply(_age_bucket, axis=1)
    by_age = (
        combined.groupby("fp_age_bucket", sort=False)
        .agg(bars=("pnl", "size"), total_pnl=("pnl", "sum"), mean_pnl=("pnl", "mean"))
        .reset_index()
    )
    return by_active, by_age

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
    parser.add_argument("--allocation_max_weight", type=float, default=0.45)
    parser.add_argument("--allocation_min_weight", type=float, default=0.02)
    parser.add_argument("--allocation_volatility_target", type=float, default=0.0)
    parser.add_argument("--allocation_volatility_adjustment_cap", type=float, default=3.0)
    parser.add_argument("--allow_backtest_artifact_fallback", type=int, default=0)
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
        "allow_backtest_artifact_fallback": int(args.allow_backtest_artifact_fallback),
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("make_report", run_id, params, inputs, outputs)
    stats: Dict[str, object] = {}

    try:
        backtests_root = DATA_ROOT / "lake" / "trades" / "backtests"
        preferred_strategy = "vol_compression_expansion_v1"
        trades_dir = backtests_root / preferred_strategy / run_id
        used_backtest_fallback = False
        if not (trades_dir / "metrics.json").exists():
            if not bool(int(args.allow_backtest_artifact_fallback)):
                raise FileNotFoundError(
                    f"Missing preferred backtest metrics at {trades_dir / 'metrics.json'} "
                    "(set --allow_backtest_artifact_fallback 1 to scan alternate backtest dirs)"
                )
            fallback_dirs = sorted(
                path for path in backtests_root.glob(f"*/{run_id}") if (path / "metrics.json").exists()
            )
            if fallback_dirs:
                trades_dir = fallback_dirs[0]
                used_backtest_fallback = True
            else:
                raise FileNotFoundError(
                    f"No backtest metrics found under {backtests_root} for run_id={run_id}"
                )
        metrics_path = trades_dir / "metrics.json"
        equity_curve_path = trades_dir / "equity_curve.csv"
        fee_path = trades_dir / "fee_sensitivity.json"
        metrics = _read_json(metrics_path)
        for key in REQUIRED_METRIC_FIELDS:
            _to_float_strict(metrics.get(key), field_name=f"metrics.{key}")
        cost_decomposition_raw = metrics.get("cost_decomposition")
        if not isinstance(cost_decomposition_raw, dict):
            raise ValueError(f"Missing/invalid metrics.cost_decomposition in {metrics_path}")
        _to_float_strict(cost_decomposition_raw.get("net_alpha"), field_name="metrics.cost_decomposition.net_alpha")
        metrics_total_trades = int(_to_float_strict(metrics.get("total_trades"), field_name="metrics.total_trades"))
        metrics_avg_r = _to_float_strict(metrics.get("avg_r"), field_name="metrics.avg_r")
        metrics_win_rate = _to_float_strict(metrics.get("win_rate"), field_name="metrics.win_rate")
        ending_equity = _to_float_strict(metrics.get("ending_equity"), field_name="metrics.ending_equity")
        metrics_sharpe = _to_float_strict(metrics.get("sharpe_annualized"), field_name="metrics.sharpe_annualized")
        cost_decomposition = cost_decomposition_raw
        reproducibility_meta = metrics.get("reproducibility", {}) if isinstance(metrics, dict) else {}
        logging.info("Using backtest artifacts from %s", trades_dir)
        fee_sensitivity = _read_json(fee_path) if fee_path.exists() else {}
        trades, trade_files = _load_trades(trades_dir)
        engine_dir = DATA_ROOT / "runs" / run_id / "engine"
        trade_evidence_source = "trades_csv"
        fallback_per_symbol = pd.DataFrame()
        strategy_files_considered: List[str] = []
        unexpected_strategy_files_detected = False
        unexpected_strategy_ids: List[str] = []
        if trade_files:
            if int(len(trades)) != int(metrics_total_trades):
                raise ValueError(
                    "Trade evidence mismatch: trades_*.csv row count must equal metrics.total_trades "
                    f"(trade_rows={len(trades)}, metrics.total_trades={metrics_total_trades}, trades_dir={trades_dir})"
                )
        else:
            trade_evidence_source = "engine_fallback"
            metadata = metrics.get("metadata", {}) if isinstance(metrics, dict) else {}
            if not isinstance(metadata, dict):
                raise ValueError("metrics.metadata must be an object when using engine fallback trade evidence.")
            strategy_ids = metadata.get("strategy_ids", [])
            if not isinstance(strategy_ids, list):
                raise ValueError("metrics.metadata.strategy_ids must be a list when using engine fallback trade evidence.")
            fallback_per_symbol, fallback_diag = _load_engine_entries(
                engine_dir=engine_dir,
                expected_strategy_ids=[str(x) for x in strategy_ids],
            )
            strategy_files_considered = [str(x) for x in fallback_diag.get("strategy_files_considered", [])]
            unexpected_strategy_files_detected = bool(fallback_diag.get("unexpected_strategy_files_detected", False))
            unexpected_strategy_ids = [str(x) for x in fallback_diag.get("unexpected_strategy_ids", [])]
            fallback_total_trades = int(fallback_per_symbol["total_trades"].sum()) if not fallback_per_symbol.empty else 0
            if int(fallback_total_trades) != int(metrics_total_trades):
                raise ValueError(
                    "Engine fallback trade mismatch: fallback-derived trades must equal metrics.total_trades "
                    f"(fallback_total_trades={fallback_total_trades}, metrics.total_trades={metrics_total_trades}, run_id={run_id})"
                )
            if fallback_per_symbol.empty and metrics_total_trades > 0:
                raise ValueError(
                    "No engine fallback trade entries found while metrics.total_trades > 0 "
                    f"(run_id={run_id}, engine_dir={engine_dir})"
                )
        context_by_active, context_by_age = _load_context_segments(engine_dir)
        equity_curve = pd.read_csv(equity_curve_path) if equity_curve_path.exists() else pd.DataFrame()
        if equity_curve.empty:
            raise FileNotFoundError(f"Missing required equity curve: {equity_curve_path}")
        if "equity" not in equity_curve.columns:
            raise ValueError(f"equity_curve.csv missing required column 'equity': {equity_curve_path}")

        cleaned_manifest_path = DATA_ROOT / "runs" / run_id / "build_cleaned_15m.json"
        cleaned_stats = _read_json(cleaned_manifest_path).get("stats", {}) if cleaned_manifest_path.exists() else {}
        features_manifest_path = DATA_ROOT / "runs" / run_id / "build_features_v1.json"
        features_stats = _read_json(features_manifest_path).get("stats", {}) if features_manifest_path.exists() else {}
        universe_manifest_path = DATA_ROOT / "runs" / run_id / "build_universe_snapshots.json"
        universe_stats = _read_json(universe_manifest_path).get("stats", {}) if universe_manifest_path.exists() else {}
        universe_report_path = DATA_ROOT / "reports" / "universe" / run_id / "universe_membership.json"
        universe_report = _read_json(universe_report_path) if universe_report_path.exists() else {}
        engine_metrics_path = DATA_ROOT / "runs" / run_id / "engine" / "metrics.json"
        engine_metrics = _read_json(engine_metrics_path) if engine_metrics_path.exists() else {}
        engine_diagnostics = engine_metrics.get("diagnostics", {})

        inputs.append({"path": str(metrics_path), "rows": 1, "start_ts": None, "end_ts": None})
        inputs.append({"path": str(trades_dir), "rows": len(trades), "start_ts": None, "end_ts": None})

        report_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "vol_compression_expansion_v1" / run_id
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / "summary.md"

        allocation_input_path = DATA_ROOT / "reports" / "edge_candidates" / run_id / "edge_candidates_normalized.csv"
        allocation_df = pd.DataFrame()
        allocation_meta = {}
        if allocation_input_path.exists():
            candidate_df = pd.read_csv(allocation_input_path)
            allocation_config = AllocationConfig(
                max_weight=float(args.allocation_max_weight),
                min_weight=float(args.allocation_min_weight),
                volatility_target=float(args.allocation_volatility_target),
                volatility_adjustment_cap=float(args.allocation_volatility_adjustment_cap),
            )
            allocation_df = build_allocation_weights(candidate_df, allocation_config)
            allocation_meta = allocation_metadata(allocation_config, allocation_df)
            inputs.append({"path": str(allocation_input_path), "rows": int(len(candidate_df)), "start_ts": None, "end_ts": None})

        if trade_evidence_source == "engine_fallback":
            if fallback_per_symbol.empty:
                per_symbol_trades = pd.DataFrame()
                total_trades = int(metrics_total_trades)
            else:
                per_symbol_trades = fallback_per_symbol
                total_trades = int(metrics_total_trades)
            avg_r_total = metrics_avg_r
        else:
            per_symbol_trades = (
                trades.groupby("symbol").agg(total_trades=("symbol", "count"), avg_r=("r_multiple", "mean"))
                if not trades.empty
                else pd.DataFrame()
            )
            total_trades = int(metrics_total_trades)
            avg_r_total = metrics_avg_r
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
            f"- Sharpe (annualized): {metrics_sharpe:.2f}",
            "",
            "## Trades by Symbol",
            "",
        ]

        if per_symbol_trades.empty:
            lines.append("No trades for this run.")
        else:
            lines.append(_table_text(per_symbol_trades.reset_index()))

        lines.extend(["", "## Fee Sensitivity", ""])
        if fee_df.empty:
            lines.append("Fee sensitivity data unavailable.")
        else:
            lines.append(_table_text(fee_df))

        lines.extend(["", "## Cost Decomposition", ""])
        if not cost_decomposition:
            lines.append("Cost decomposition unavailable.")
        else:
            cost_rows = [
                {
                    "gross_alpha": float(cost_decomposition.get("gross_alpha", 0.0) or 0.0),
                    "fees": float(cost_decomposition.get("fees", 0.0) or 0.0),
                    "slippage": float(cost_decomposition.get("slippage", 0.0) or 0.0),
                    "impact": float(cost_decomposition.get("impact", 0.0) or 0.0),
                    "net_alpha": float(cost_decomposition.get("net_alpha", 0.0) or 0.0),
                    "turnover_units": float(cost_decomposition.get("turnover_units", 0.0) or 0.0),
                }
            ]
            lines.append(_table_text(pd.DataFrame(cost_rows)))

        lines.extend(["", "## Reproducibility Metadata", ""])
        if not reproducibility_meta:
            lines.append("Reproducibility metadata unavailable.")
        else:
            lines.append(f"- code_revision: `{reproducibility_meta.get('code_revision', 'unknown')}`")
            lines.append(f"- config_digest: `{reproducibility_meta.get('config_digest', '')}`")
            snapshot_ids = reproducibility_meta.get("data_snapshot_ids", {})
            if isinstance(snapshot_ids, dict) and snapshot_ids:
                for key, value in sorted(snapshot_ids.items()):
                    lines.append(f"- data_snapshot_ids.{key}: `{value}`")
            else:
                lines.append("- data_snapshot_ids: none")

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
                    rv_pct_keys = sorted(key for key in nan_rates.keys() if key.startswith("rv_pct_"))
                    range_med_keys = sorted(key for key in nan_rates.keys() if key.startswith("range_med_"))
                    for key in rv_pct_keys:
                        lines.append(f"  - pct NaN {key}: {nan_rates.get(key, 0.0):.2%}")
                    for key in range_med_keys:
                        lines.append(f"  - pct NaN {key}: {nan_rates.get(key, 0.0):.2%}")
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

        lines.extend(["", "## Context Segmentation (Funding Persistence)", ""])
        if context_by_active.empty:
            lines.append("No funding persistence context columns found in engine outputs.")
        else:
            lines.append("### By fp_active")
            lines.append(_table_text(context_by_active))
            lines.append("")
            lines.append("### By fp_age_bars bucket")
            lines.append(_table_text(context_by_age) if not context_by_age.empty else "No rows")

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

        lines.extend(["", "## Universe Membership", ""])
        if not universe_report:
            lines.append("No universe snapshot report available.")
        else:
            lines.append(f"- Symbols with history: {int(universe_report.get('symbols_with_history', 0))}")
            monthly = universe_report.get("monthly_membership", {}) if isinstance(universe_report, dict) else {}
            if monthly:
                month_rows = []
                for month, members in sorted(monthly.items()):
                    month_rows.append({"month": month, "members": ",".join(members), "count": len(members)})
                lines.append(_table_text(pd.DataFrame(month_rows)))

        lines.extend(["", "## Overlay Bindings", ""])
        overlay_bindings = engine_metrics.get("overlay_bindings", {}) if isinstance(engine_metrics, dict) else {}
        if not overlay_bindings:
            lines.append("No overlays applied.")
        else:
            for strategy_name, payload in overlay_bindings.items():
                applied = payload.get("applied_overlays", []) if isinstance(payload, dict) else []
                lines.append(f"- **{strategy_name}** overlays: {', '.join(applied) if applied else 'none'}")
                sym_rows = []
                for sym_payload in payload.get("symbols", []) if isinstance(payload, dict) else []:
                    symbol = sym_payload.get("symbol", "unknown")
                    for stat in sym_payload.get("binding_stats", []):
                        sym_rows.append(
                            {
                                "symbol": symbol,
                                "overlay": stat.get("overlay", ""),
                                "blocked_entries": int(stat.get("blocked_entries", 0)),
                                "delayed_entries": int(stat.get("delayed_entries", 0)),
                                "changed_bars": int(stat.get("changed_bars", 0)),
                            }
                        )
                lines.append(_table_text(pd.DataFrame(sym_rows)) if sym_rows else "No binding stats")

        lines.extend(["", "## Capital Allocation Weights", ""])
        if allocation_df.empty:
            lines.append("No promoted symbol-level edge metrics available for allocation sizing.")
        else:
            lines.append(_table_text(allocation_df[["symbol", "ev", "variance", "symbol_vol", "final_weight", "allocation_rank"]]))
            lines.append("")
            lines.append("Allocation constraints:")
            lines.append(f"- max_weight: {allocation_meta.get('max_weight', 0.0):.2%}")
            lines.append(f"- min_weight: {allocation_meta.get('min_weight', 0.0):.2%}")
            lines.append(f"- volatility_target: {allocation_meta.get('volatility_target', 0.0):.6f}")

        report_path.write_text("\n".join(lines), encoding="utf-8")
        outputs.append({"path": str(report_path), "rows": len(lines), "start_ts": None, "end_ts": None})

        summary_json = {
            "run_id": run_id,
            "total_trades": total_trades,
            "win_rate": metrics_win_rate,
            "avg_r": avg_r_total,
            "ending_equity": ending_equity,
            "max_drawdown": max_drawdown,
            "trade_evidence_source": trade_evidence_source,
            "strategy_files_considered": strategy_files_considered,
            "unexpected_strategy_files_detected": bool(unexpected_strategy_files_detected),
            "per_symbol": per_symbol_trades.reset_index().to_dict(orient="records") if not per_symbol_trades.empty else [],
            "fee_sensitivity": fee_table,
            "cost_decomposition": cost_decomposition,
            "reproducibility": reproducibility_meta,
            "data_quality": cleaned_stats,
            "feature_quality": features_stats,
            "universe_quality": universe_stats,
            "universe_membership": universe_report.get("monthly_membership", {}) if isinstance(universe_report, dict) else {},
            "engine_diagnostics": engine_diagnostics,
            "overlay_bindings": engine_metrics.get("overlay_bindings", {}) if isinstance(engine_metrics, dict) else {},
            "capital_allocation": allocation_df.to_dict(orient="records") if not allocation_df.empty else [],
            "capital_allocation_metadata": allocation_meta,
            "context_segmentation": {
                "by_active": context_by_active.to_dict(orient="records") if not context_by_active.empty else [],
                "by_age_bucket": context_by_age.to_dict(orient="records") if not context_by_age.empty else [],
            },
            "integrity_checks": {
                "artifacts_validated": True,
                "used_backtest_dir_fallback": bool(used_backtest_fallback),
                "trade_evidence_source": trade_evidence_source,
                "used_engine_trade_fallback": bool(trade_evidence_source == "engine_fallback"),
                "trade_files_found": bool(len(trade_files) > 0),
                "strategy_files_considered": strategy_files_considered,
                "unexpected_strategy_files_detected": bool(unexpected_strategy_files_detected),
                "unexpected_strategy_ids": unexpected_strategy_ids,
                "equity_curve_validated": True,
            },
        }
        summary_path = report_dir / "summary.json"
        summary_path.write_text(json.dumps(summary_json, indent=2, sort_keys=True), encoding="utf-8")
        outputs.append({"path": str(summary_path), "rows": 1, "start_ts": None, "end_ts": None})

        allocation_csv_path = report_dir / "allocation_weights.csv"
        allocation_json_path = report_dir / "allocation_weights.json"
        allocation_df.to_csv(allocation_csv_path, index=False)
        allocation_payload = {
            "run_id": run_id,
            "allocation": allocation_df.to_dict(orient="records"),
            "metadata": allocation_meta,
            "source": str(allocation_input_path),
        }
        allocation_json_path.write_text(json.dumps(allocation_payload, indent=2, sort_keys=True), encoding="utf-8")
        outputs.append({"path": str(allocation_csv_path), "rows": int(len(allocation_df)), "start_ts": None, "end_ts": None})
        outputs.append({"path": str(allocation_json_path), "rows": int(len(allocation_df)), "start_ts": None, "end_ts": None})

        logging.info("Wrote report markdown: %s", report_path)
        logging.info("Wrote report summary json: %s", summary_path)
        if not allocation_df.empty:
            logging.info("Wrote allocation weights: %s", allocation_csv_path)
            logging.info("Wrote allocation weights metadata: %s", allocation_json_path)

        stats.update(
            {
                "artifacts_validated": True,
                "used_backtest_dir_fallback": bool(used_backtest_fallback),
                "trade_evidence_source": trade_evidence_source,
                "used_engine_trade_fallback": bool(trade_evidence_source == "engine_fallback"),
                "trade_rows": int(len(trades)),
                "fallback_trade_rows": int(len(fallback_per_symbol)),
                "strategy_files_considered": strategy_files_considered,
                "unexpected_strategy_files_detected": bool(unexpected_strategy_files_detected),
                "unexpected_strategy_ids": unexpected_strategy_ids,
            }
        )
        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("Report generation failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
