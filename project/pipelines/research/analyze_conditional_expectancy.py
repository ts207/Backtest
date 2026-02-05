from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir, list_parquet_files, read_parquet


def _parse_horizons(value: str) -> List[int]:
    parts = [x.strip() for x in value.split(",") if x.strip()]
    horizons = sorted({int(x) for x in parts if int(x) > 0})
    if not horizons:
        raise ValueError("At least one positive horizon is required")
    return horizons


def _pick_window_column(columns: Iterable[str], prefix: str) -> str:
    candidates: List[Tuple[int, str]] = []
    for col in columns:
        if not col.startswith(prefix):
            continue
        try:
            window = int(col.split("_")[-1])
        except ValueError:
            continue
        candidates.append((window, col))
    if not candidates:
        raise ValueError(f"Missing required feature prefix: {prefix}")
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def _rolling_percentile(series: pd.Series, window: int) -> pd.Series:
    def pct_rank(values: np.ndarray) -> float:
        valid = values[~np.isnan(values)]
        if len(valid) == 0:
            return np.nan
        last = values[-1]
        if np.isnan(last):
            return np.nan
        return float(np.sum(valid <= last) / len(valid) * 100.0)

    return series.rolling(window=window, min_periods=window).apply(pct_rank, raw=True)


def _distribution_stats(returns: pd.Series) -> Dict[str, float]:
    clean = returns.dropna().astype(float)
    n = int(len(clean))
    if n == 0:
        return {
            "samples": 0,
            "mean_return": 0.0,
            "median_return": 0.0,
            "std_return": 0.0,
            "win_rate": 0.0,
            "p05": 0.0,
            "p25": 0.0,
            "p75": 0.0,
            "p95": 0.0,
            "t_stat": 0.0,
            "signal_to_noise": 0.0,
        }

    mean_val = float(clean.mean())
    median_val = float(clean.median())
    std_val = float(clean.std())
    win_rate = float((clean > 0).mean())
    p05 = float(clean.quantile(0.05))
    p25 = float(clean.quantile(0.25))
    p75 = float(clean.quantile(0.75))
    p95 = float(clean.quantile(0.95))

    if std_val > 0.0 and n > 1:
        t_stat = float(mean_val / (std_val / np.sqrt(n)))
        signal_to_noise = float(mean_val / std_val)
    else:
        t_stat = 0.0
        signal_to_noise = 0.0

    return {
        "samples": n,
        "mean_return": mean_val,
        "median_return": median_val,
        "std_return": std_val,
        "win_rate": win_rate,
        "p05": p05,
        "p25": p25,
        "p75": p75,
        "p95": p95,
        "t_stat": t_stat,
        "signal_to_noise": signal_to_noise,
    }


def _build_markdown_table(rows: List[Dict[str, object]], columns: List[str]) -> List[str]:
    if not rows:
        return ["No rows."]

    def _fmt(col: str, val: object) -> str:
        if isinstance(val, float):
            if col in {"win_rate"}:
                return f"{val:.2%}"
            return f"{val:.6f}"
        return str(val)

    header = "| " + " | ".join(columns) + " |"
    sep = "| " + " | ".join(["---"] * len(columns)) + " |"
    lines = [header, sep]
    for row in rows:
        lines.append("| " + " | ".join(_fmt(col, row.get(col, "")) for col in columns) + " |")
    return lines


def _load_features(symbol: str) -> pd.DataFrame:
    features_dir = DATA_ROOT / "lake" / "features" / "perp" / symbol / "15m" / "features_v1"
    files = list_parquet_files(features_dir)
    if not files:
        raise ValueError(f"No features found for {symbol}: {features_dir}")
    df = read_parquet(files)
    if df.empty:
        raise ValueError(f"Features are empty for {symbol}")
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def _analyze_symbol(
    symbol: str,
    df: pd.DataFrame,
    horizons: List[int],
    htf_window: int,
    htf_lookback: int,
    funding_pct_window: int,
) -> Tuple[List[Dict[str, object]], Dict[Tuple[str, int], pd.Series], Dict[str, str]]:
    required_cols = {"timestamp", "close", "range_96", "funding_rate_scaled"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"{symbol}: missing required columns: {sorted(missing)}")

    rv_pct_col = _pick_window_column(df.columns, "rv_pct_")
    range_med_col = _pick_window_column(df.columns, "range_med_")

    close = df["close"].astype(float)
    htf_base = close.rolling(window=htf_window, min_periods=htf_window).mean()
    htf_delta = htf_base - htf_base.shift(htf_lookback)
    trend_state = pd.Series(np.where(htf_delta > 0, 1, np.where(htf_delta < 0, -1, 0)), index=df.index)

    funding_pct = _rolling_percentile(df["funding_rate_scaled"].astype(float), funding_pct_window)
    funding_bucket = pd.Series(
        np.select(
            [funding_pct <= 20, funding_pct >= 80],
            ["low", "high"],
            default="mid",
        ),
        index=df.index,
    )
    funding_bucket = funding_bucket.where(funding_pct.notna())

    compression = (df[rv_pct_col] <= 10.0) & (df["range_96"] <= 0.8 * df[range_med_col])
    compression = compression.fillna(False)

    condition_returns: Dict[Tuple[str, int], pd.Series] = {}
    rows: List[Dict[str, object]] = []

    for horizon in horizons:
        fwd_ret = close.shift(-horizon) / close - 1.0
        directional_fwd = fwd_ret * trend_state.astype(float)

        masks = {
            "compression": compression,
            "compression_plus_htf_trend_directional": compression & (trend_state != 0),
            "compression_plus_funding_low": compression & (funding_bucket == "low"),
            "compression_plus_funding_mid": compression & (funding_bucket == "mid"),
            "compression_plus_funding_high": compression & (funding_bucket == "high"),
        }

        for condition_name, mask in masks.items():
            if condition_name == "compression_plus_htf_trend_directional":
                cond_returns = directional_fwd.where(mask)
            else:
                cond_returns = fwd_ret.where(mask)

            stats = _distribution_stats(cond_returns)
            rows.append(
                {
                    "scope": symbol,
                    "condition": condition_name,
                    "horizon_bars": horizon,
                    "horizon_hours": horizon / 4.0,
                    **stats,
                }
            )
            condition_returns[(condition_name, horizon)] = cond_returns

    return rows, condition_returns, {"rv_pct_col": rv_pct_col, "range_med_col": range_med_col}


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze conditional forward return expectancy (no execution)")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--horizons", default="4,16,96", help="Forward horizons in bars, comma-separated")
    parser.add_argument("--htf_window", type=int, default=384)
    parser.add_argument("--htf_lookback", type=int, default=96)
    parser.add_argument("--funding_pct_window", type=int, default=2880)
    parser.add_argument("--min_samples", type=int, default=100)
    parser.add_argument("--tstat_threshold", type=float, default=2.0)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    horizons = _parse_horizons(args.horizons)

    log_handlers: List[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "expectancy" / args.run_id
    ensure_dir(out_dir)

    all_rows: List[Dict[str, object]] = []
    combined_store: Dict[Tuple[str, int], List[pd.Series]] = {}
    chosen_columns: Dict[str, Dict[str, str]] = {}

    for symbol in symbols:
        df = _load_features(symbol)
        rows, condition_returns, cols = _analyze_symbol(
            symbol=symbol,
            df=df,
            horizons=horizons,
            htf_window=args.htf_window,
            htf_lookback=args.htf_lookback,
            funding_pct_window=args.funding_pct_window,
        )
        chosen_columns[symbol] = cols
        all_rows.extend(rows)

        for key, series in condition_returns.items():
            combined_store.setdefault(key, []).append(series)

    for (condition_name, horizon), series_list in combined_store.items():
        combined = pd.concat(series_list, axis=0, ignore_index=True)
        stats = _distribution_stats(combined)
        all_rows.append(
            {
                "scope": "combined",
                "condition": condition_name,
                "horizon_bars": horizon,
                "horizon_hours": horizon / 4.0,
                **stats,
            }
        )

    results_df = pd.DataFrame(all_rows)
    combined_df = results_df[results_df["scope"] == "combined"].copy()
    combined_df = combined_df.sort_values(["condition", "horizon_bars"], ignore_index=True)

    expectancy_candidates = combined_df[
        (combined_df["samples"] >= args.min_samples)
        & (combined_df["mean_return"] > 0.0)
        & (combined_df["t_stat"] >= args.tstat_threshold)
    ]

    expectancy_exists = not expectancy_candidates.empty
    if expectancy_exists:
        verdict = "Conditional expectancy detected under configured thresholds. Continue research before execution logic."
    else:
        verdict = "No robust conditional expectancy detected under configured thresholds. Abandon this idea now."

    payload = {
        "run_id": args.run_id,
        "symbols": symbols,
        "config": {
            "horizons": horizons,
            "htf_window": args.htf_window,
            "htf_lookback": args.htf_lookback,
            "funding_pct_window": args.funding_pct_window,
            "min_samples": args.min_samples,
            "tstat_threshold": args.tstat_threshold,
        },
        "feature_columns": chosen_columns,
        "expectancy_exists": expectancy_exists,
        "verdict": verdict,
        "expectancy_evidence": expectancy_candidates.to_dict(orient="records"),
        "all_results": results_df.to_dict(orient="records"),
    }

    json_path = out_dir / "conditional_expectancy.json"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    top_columns = [
        "condition",
        "horizon_bars",
        "samples",
        "mean_return",
        "win_rate",
        "t_stat",
        "signal_to_noise",
    ]

    md_lines: List[str] = [
        "# Conditional Expectancy Report",
        "",
        f"Run ID: `{args.run_id}`",
        f"Symbols: `{','.join(symbols)}`",
        "",
        "## Verdict",
        f"- Expectancy exists: `{expectancy_exists}`",
        f"- Rule: mean return > 0, t-stat >= {args.tstat_threshold}, samples >= {args.min_samples}",
        f"- Decision: {verdict}",
        "",
        "## Combined Results",
        "",
    ]

    md_lines.extend(_build_markdown_table(combined_df.to_dict(orient="records"), top_columns))
    md_lines.extend(["", "## Evidence Rows", ""])
    evidence_rows = expectancy_candidates.sort_values(["t_stat", "mean_return"], ascending=False).to_dict(orient="records")
    md_lines.extend(_build_markdown_table(evidence_rows, top_columns))

    md_path = out_dir / "conditional_expectancy.md"
    md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    logging.info("Wrote %s", json_path)
    logging.info("Wrote %s", md_path)
    logging.info("Expectancy exists=%s", expectancy_exists)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
