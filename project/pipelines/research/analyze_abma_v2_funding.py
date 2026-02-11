from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from analyzers.abma_v2_funding import (
    DEFAULT_ABMA_FUNDING_CONFIG,
    build_report_artifacts,
    compute_structural_metrics,
    evaluate_stabilization,
    extract_funding_boundary_events,
    sample_matched_controls,
)
from pipelines._lib.io_utils import ensure_dir, list_parquet_files, read_parquet, run_scoped_lake_path, write_parquet
from strategies.abma_funding_overlay_candidate import build_overlay_candidate


def _read_frame(path_str: str) -> pd.DataFrame:
    path = Path(path_str)
    if not path.exists():
        raise FileNotFoundError(f"Input path does not exist: {path}")
    if path.is_dir():
        files = list_parquet_files(path)
        if not files:
            raise FileNotFoundError(f"No parquet/csv partition files in: {path}")
        return read_parquet(files)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() in {".parquet", ".pq"}:
        return read_parquet([path])
    raise ValueError(f"Unsupported input file type for {path}")


def _resolve_input_path(explicit: str | None, run_id: str, kind: str) -> Path:
    if explicit:
        return Path(explicit)
    candidates = [
        run_scoped_lake_path(DATA_ROOT, run_id, "microstructure", kind),
        DATA_ROOT / "lake" / "microstructure" / kind,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"Missing required {kind} input. Pass --{kind}_path explicitly.")


def _parse_symbols(raw: str) -> list[str]:
    return [s.strip() for s in raw.split(",") if s.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase-1 ABMA v2 funding analyzer pipeline")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--trades_path", default=None)
    parser.add_argument("--quotes_path", default=None)
    parser.add_argument("--session_calendar_path", default=None)
    parser.add_argument("--funding_path", default=None)
    parser.add_argument("--out_dir", default=None)
    args = parser.parse_args()

    symbols = _parse_symbols(args.symbols)
    if not symbols:
        raise ValueError("No symbols provided")

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "abma_v2_funding" / args.run_id
    ensure_dir(out_dir)

    trades_path = _resolve_input_path(args.trades_path, args.run_id, "trades")
    quotes_path = _resolve_input_path(args.quotes_path, args.run_id, "quotes")
    cal_path = _resolve_input_path(args.session_calendar_path, args.run_id, "session_calendar")
    funding_path = _resolve_input_path(args.funding_path, args.run_id, "funding")

    trades = _read_frame(str(trades_path))
    quotes = _read_frame(str(quotes_path))
    session_calendar = _read_frame(str(cal_path))
    funding_events = _read_frame(str(funding_path))
    trades = trades[trades["symbol"].astype(str).isin(symbols)].copy()
    quotes = quotes[quotes["symbol"].astype(str).isin(symbols)].copy()
    funding_events = funding_events[funding_events["symbol"].astype(str).isin(symbols)].copy()

    if trades.empty or quotes.empty or session_calendar.empty or funding_events.empty:
        raise ValueError("Required inputs are empty after symbol/date filtering")

    events = extract_funding_boundary_events(funding_events=funding_events, session_calendar=session_calendar, config=DEFAULT_ABMA_FUNDING_CONFIG)
    if events.empty:
        raise ValueError("No ABMA funding events extracted")
    events = events[events["symbol"].astype(str).isin(symbols)].reset_index(drop=True)

    controls = sample_matched_controls(
        events=events,
        trades=trades,
        quotes=quotes,
        session_calendar=session_calendar,
        config=DEFAULT_ABMA_FUNDING_CONFIG,
    )
    no_controls_rows = list(controls.attrs.get("no_controls", []))
    metrics = compute_structural_metrics(
        events=events,
        controls=controls,
        trades=trades,
        quotes=quotes,
        config=DEFAULT_ABMA_FUNDING_CONFIG,
    )

    merged_event_metrics = pd.concat([metrics["event"], metrics["control_summary"], metrics["delta"]], axis=1)
    eval_outputs = evaluate_stabilization(event_metrics=merged_event_metrics, curves=metrics["curves"], config=DEFAULT_ABMA_FUNDING_CONFIG)
    report_payload = build_report_artifacts(eval_outputs, config=DEFAULT_ABMA_FUNDING_CONFIG)

    summary = report_payload["summary"]
    summary["run_id"] = args.run_id
    summary["n_events"] = int(len(metrics["event"]))
    summary["n_controls"] = int(len(controls))
    summary["control_sampler"] = {
        "mode": "deterministic_sorted_head",
        "seed": None,
        "no_controls_events": int(len(no_controls_rows)),
    }
    if no_controls_rows:
        no_controls_df = pd.DataFrame(no_controls_rows)
        summary["control_sampler"]["no_controls_reasons"] = (
            no_controls_df.groupby("reason", as_index=False).size().rename(columns={"size": "count"}).to_dict(orient="records")
        )

    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    curves_path, _ = write_parquet(metrics["curves"], out_dir / "curves.parquet")
    (out_dir / "report.md").write_text(str(report_payload["report_md"]), encoding="utf-8")

    metrics["event"].to_csv(out_dir / "event_metrics.csv", index=False)
    metrics["control_summary"].to_csv(out_dir / "control_summary.csv", index=False)
    metrics["delta"].to_csv(out_dir / "delta_metrics.csv", index=False)
    controls.to_csv(out_dir / "controls.csv", index=False)
    events.to_csv(out_dir / "events.csv", index=False)

    if bool(summary.get("phase1_accept")):
        overlay = build_overlay_candidate(run_id=args.run_id, config=DEFAULT_ABMA_FUNDING_CONFIG)
        (out_dir / "overlay_candidate.json").write_text(json.dumps(overlay, indent=2, sort_keys=True), encoding="utf-8")

    print(f"ABMA v2 funding completed: {out_dir}")
    print(f"curves artifact: {curves_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
