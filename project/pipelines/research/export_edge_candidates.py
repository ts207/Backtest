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

PHASE2_EVENT_CHAIN = [
    ("vol_shock_relaxation", "analyze_vol_shock_relaxation.py", ["--timeframe", "15m"]),
    ("liquidity_refill_lag_window", "analyze_liquidity_refill_lag_window.py", []),
    ("liquidity_absence_window", "analyze_liquidity_absence_window.py", []),
    ("vol_aftershock_window", "analyze_vol_aftershock_window.py", []),
    ("directional_exhaustion_after_forced_flow", "analyze_directional_exhaustion_after_forced_flow.py", []),
    ("cross_venue_desync", "analyze_cross_venue_desync.py", []),
    ("liquidity_vacuum", "analyze_liquidity_vacuum.py", ["--timeframe", "15m"]),
    ("funding_extreme_reversal_window", "analyze_funding_extreme_reversal_window.py", []),
    ("range_compression_breakout_window", "analyze_range_compression_breakout_window.py", []),
]


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        out = float(value)
        if pd.isna(out):
            return default
        return out
    except (TypeError, ValueError):
        return default


def _safe_int(value: object, default: int = 0) -> int:
    try:
        if value is None:
            return default
        out = int(float(value))
        return out
    except (TypeError, ValueError):
        return default


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    return text in {"1", "true", "t", "yes", "y"}


def _phase2_row_to_candidate(
    run_id: str,
    event: str,
    row: Dict[str, object],
    idx: int,
    source_path: Path,
    default_status: str,
) -> Dict[str, object]:
    risk_reduction = max(0.0, -_safe_float(row.get("delta_adverse_mean"), 0.0))
    opp_delta = _safe_float(row.get("delta_opportunity_mean"), 0.0)
    edge_score = _safe_float(row.get("edge_score"), risk_reduction + max(0.0, opp_delta))
    expected_return_proxy = _safe_float(row.get("expected_return_proxy"), opp_delta)

    gate_cols = [
        "gate_a_ci_separated",
        "gate_b_time_stable",
        "gate_c_regime_stable",
        "gate_d_friction_floor",
        "gate_f_exposure_guard",
        "gate_e_simplicity",
    ]
    gates_present = [g for g in gate_cols if g in row]
    if gates_present:
        stability_proxy = float(sum(1 for g in gates_present if _as_bool(row.get(g))) / len(gates_present))
    else:
        stability_proxy = _safe_float(row.get("stability_proxy"), 0.0)

    return {
        "run_id": run_id,
        "event": event,
        "candidate_id": str(row.get("candidate_id", f"{event}_{idx}")),
        "status": str(row.get("status", default_status)),
        "edge_score": edge_score,
        "expected_return_proxy": expected_return_proxy,
        "stability_proxy": stability_proxy,
        "n_events": _safe_int(row.get("sample_size", row.get("n_events", row.get("count", 0))), 0),
        "source_path": str(source_path),
    }


def _run_research_chain(
    run_id: str,
    symbols: str,
    run_hypothesis_generator: bool,
    hypothesis_datasets: str,
    hypothesis_max_fused: int,
) -> None:
    if run_hypothesis_generator:
        hypothesis_script = PROJECT_ROOT / "pipelines" / "research" / "generate_hypothesis_queue.py"
        if hypothesis_script.exists():
            hypothesis_cmd = [
                sys.executable,
                str(hypothesis_script),
                "--run_id",
                run_id,
                "--symbols",
                symbols,
                "--datasets",
                str(hypothesis_datasets),
                "--max_fused",
                str(int(hypothesis_max_fused)),
            ]
            hypothesis_result = subprocess.run(hypothesis_cmd)
            if hypothesis_result.returncode != 0:
                logging.warning("Hypothesis generator failed (non-blocking)")
        else:
            logging.warning("Missing hypothesis generator script (skipping): %s", hypothesis_script)

    phase2_script_path = PROJECT_ROOT / "pipelines" / "research" / "phase2_conditional_hypotheses.py"
    for event_type, script, extra_args in PHASE2_EVENT_CHAIN:
        script_path = PROJECT_ROOT / "pipelines" / "research" / script
        if not script_path.exists():
            logging.warning("Missing phase1 script (skipping): %s", script_path)
            continue

        cmd = [sys.executable, str(script_path), "--run_id", run_id, "--symbols", symbols, *extra_args]
        result = subprocess.run(cmd)
        if result.returncode != 0:
            logging.warning("Phase1 stage failed (non-blocking): %s", script)
            continue

        if not phase2_script_path.exists():
            logging.warning("Missing phase2 script (skipping): %s", phase2_script_path)
            continue

        phase2_cmd = [
            sys.executable,
            str(phase2_script_path),
            "--run_id",
            run_id,
            "--event_type",
            event_type,
            "--symbols",
            symbols,
        ]
        phase2_result = subprocess.run(phase2_cmd)
        if phase2_result.returncode != 0:
            logging.warning("Phase2 stage failed (non-blocking): %s", event_type)


def _collect_phase2_candidates(run_id: str) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    phase2_root = DATA_ROOT / "reports" / "phase2" / run_id
    if not phase2_root.exists():
        return rows

    for event_dir in sorted([p for p in phase2_root.iterdir() if p.is_dir()]):
        promoted_json = event_dir / "promoted_candidates.json"
        candidate_csv = event_dir / "phase2_candidates.csv"
        event_rows: List[Dict[str, object]] = []

        if promoted_json.exists():
            payload = json.loads(promoted_json.read_text(encoding="utf-8"))
            promoted = payload.get("candidates", []) if isinstance(payload, dict) else []
            for idx, candidate in enumerate(promoted):
                if not isinstance(candidate, dict):
                    continue
                event_rows.append(
                    _phase2_row_to_candidate(
                        run_id=run_id,
                        event=event_dir.name,
                        row=candidate,
                        idx=idx,
                        source_path=promoted_json,
                        default_status="PROMOTED",
                    )
                )

        if not event_rows and candidate_csv.exists():
            df = pd.read_csv(candidate_csv)
            if not df.empty:
                if "gate_all" in df.columns:
                    df = df[df["gate_all"].map(_as_bool)].copy()
                if not df.empty:
                    for idx, row in df.iterrows():
                        event_rows.append(
                            _phase2_row_to_candidate(
                                run_id=run_id,
                                event=event_dir.name,
                                row=row.to_dict(),
                                idx=idx,
                                source_path=candidate_csv,
                                default_status="DRAFT",
                            )
                        )

        rows.extend(event_rows)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Expand and normalize edge candidate universe")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--execute", type=int, default=0)
    parser.add_argument("--run_hypothesis_generator", type=int, default=1)
    parser.add_argument("--hypothesis_datasets", default="auto")
    parser.add_argument("--hypothesis_max_fused", type=int, default=24)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    params = {
        "run_id": args.run_id,
        "symbols": args.symbols,
        "execute": int(args.execute),
        "run_hypothesis_generator": int(args.run_hypothesis_generator),
        "hypothesis_datasets": str(args.hypothesis_datasets),
        "hypothesis_max_fused": int(args.hypothesis_max_fused),
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("export_edge_candidates", args.run_id, params, inputs, outputs)

    try:
        if int(args.execute):
            _run_research_chain(
                run_id=args.run_id,
                symbols=args.symbols,
                run_hypothesis_generator=bool(int(args.run_hypothesis_generator)),
                hypothesis_datasets=str(args.hypothesis_datasets),
                hypothesis_max_fused=int(args.hypothesis_max_fused),
            )

        rows = _collect_phase2_candidates(args.run_id)

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
