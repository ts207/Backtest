from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.run_manifest import finalize_manifest, start_manifest


def _run_research_chain(run_id: str, symbols: str) -> None:
    research_scripts = [
        ("analyze_vol_shock_relaxation.py", ["--timeframe", "15m"]),
        ("analyze_liquidity_refill_lag_window.py", []),
        ("analyze_vol_aftershock_window.py", []),
        ("analyze_liquidity_absence_window.py", []),
        ("analyze_cross_venue_desync.py", []),
        ("analyze_directional_exhaustion_after_forced_flow.py", []),
    ]

    for script, extra_args in research_scripts:
        script_path = PROJECT_ROOT / "pipelines" / "research" / script
        if not script_path.exists():
            continue
        cmd = [sys.executable, str(script_path), "--run_id", run_id, "--symbols", symbols, *extra_args]
        result = subprocess.run(cmd)
        if result.returncode != 0:
            logging.warning("Research stage failed (non-blocking): %s", script)


def _collect_phase2_candidates(run_id: str) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    phase2_root = DATA_ROOT / "reports" / "phase2" / run_id
    if not phase2_root.exists():
        return rows

    for event_dir in sorted([p for p in phase2_root.iterdir() if p.is_dir()]):
        candidate_csv = event_dir / "phase2_candidates.csv"
        if not candidate_csv.exists():
            continue
        df = pd.read_csv(candidate_csv)
        if df.empty:
            continue
        for idx, row in df.iterrows():
            rows.append(
                {
                    "run_id": run_id,
                    "event": event_dir.name,
                    "candidate_id": str(row.get("candidate_id", f"{event_dir.name}_{idx}")),
                    "status": str(row.get("status", "DRAFT")),
                    "edge_score": float(row.get("edge_score", row.get("score", 0.0)) or 0.0),
                    "expected_return_proxy": float(row.get("expected_return", row.get("net_return", 0.0)) or 0.0),
                    "stability_proxy": float(row.get("stability", row.get("stability_score", 0.0)) or 0.0),
                    "n_events": int(row.get("n_events", row.get("count", 0)) or 0),
                    "source_path": str(candidate_csv),
                }
            )
    return rows


def _collect_summary_candidates(run_id: str) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    reports_root = DATA_ROOT / "reports"
    if not reports_root.exists():
        return rows

    for event_root in sorted([p for p in reports_root.iterdir() if p.is_dir()]):
        if event_root.name in {"phase2", "by_run", "overlay_promotion", "edge_candidates"}:
            continue
        summary_json = event_root / run_id / "summary.json"
        if not summary_json.exists():
            continue

        payload = json.loads(summary_json.read_text(encoding="utf-8"))
        rows.append(
            {
                "run_id": run_id,
                "event": event_root.name,
                "candidate_id": f"{event_root.name}_summary_candidate",
                "status": "DRAFT",
                "edge_score": float(payload.get("net_total_return", 0.0) or 0.0),
                "expected_return_proxy": float(payload.get("net_total_return", 0.0) or 0.0),
                "stability_proxy": float(payload.get("sharpe_annualized", 0.0) or 0.0),
                "n_events": int(payload.get("total_trades", 0) or 0),
                "source_path": str(summary_json),
            }
        )

    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Expand and normalize edge candidate universe")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--execute", type=int, default=0)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    params = {"run_id": args.run_id, "symbols": args.symbols, "execute": int(args.execute)}
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("export_edge_candidates", args.run_id, params, inputs, outputs)

    try:
        if int(args.execute):
            _run_research_chain(run_id=args.run_id, symbols=args.symbols)

        rows = _collect_phase2_candidates(args.run_id)
        rows.extend(_collect_summary_candidates(args.run_id))

        out_dir = DATA_ROOT / "reports" / "edge_candidates" / args.run_id
        ensure_dir(out_dir)
        out_csv = out_dir / "edge_candidates_normalized.csv"
        out_json = out_dir / "edge_candidates_normalized.json"

        df = pd.DataFrame(
            rows,
            columns=[
                "run_id",
                "event",
                "candidate_id",
                "status",
                "edge_score",
                "expected_return_proxy",
                "stability_proxy",
                "n_events",
                "source_path",
            ],
        )
        if not df.empty:
            df = df.sort_values(["edge_score", "stability_proxy"], ascending=[False, False]).reset_index(drop=True)
        df.to_csv(out_csv, index=False)
        out_json.write_text(df.to_json(orient="records", indent=2), encoding="utf-8")

        outputs.append({"path": str(out_csv), "rows": int(len(df)), "start_ts": None, "end_ts": None})
        outputs.append({"path": str(out_json), "rows": int(len(df)), "start_ts": None, "end_ts": None})
        finalize_manifest(manifest, "success", stats={"candidate_count": int(len(df))})
        return 0
    except Exception as exc:  # pragma: no cover
        logging.exception("Edge candidate export failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())
