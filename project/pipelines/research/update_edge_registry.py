from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.ontology_contract import load_run_manifest_hashes
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from research.edge_identity import edge_id_from_row, structural_edge_components


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    if not np.isfinite(out):
        return float(default)
    return float(out)


def _normalize_event_type(row: Dict[str, Any]) -> str:
    token = str(
        row.get("canonical_event_type", row.get("event_type", row.get("event", "")))
    ).strip()
    return token or "UNKNOWN_EVENT"


def _load_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        if path.suffix == ".parquet":
            return pd.read_parquet(path)
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _load_history(path: Path) -> pd.DataFrame:
    df = _load_table(path)
    if df.empty:
        return pd.DataFrame()
    return df


def _effect_value(row: Dict[str, Any]) -> float:
    for key in (
        "effect_shrunk_state",
        "effect_shrunk_event",
        "effect_raw",
        "expectancy",
        "after_cost_expectancy_per_trade",
    ):
        if key in row:
            return _safe_float(row.get(key), 0.0)
    return 0.0


def _template_id(row: Dict[str, Any]) -> str:
    token = str(
        row.get(
            "template_id",
            row.get("template_verb", row.get("rule_template", row.get("template_family", ""))),
        )
    ).strip()
    return token or "UNKNOWN_TEMPLATE"


def _run_sort_value(value: str) -> Tuple[int, str]:
    token = str(value or "").strip()
    digits = "".join(ch for ch in token if ch.isdigit())
    if digits:
        try:
            return (int(digits), token)
        except ValueError:
            pass
    return (0, token)


def _effect_decay(values: Iterable[float]) -> float:
    y = np.asarray([_safe_float(v, np.nan) for v in values], dtype=float)
    y = y[np.isfinite(y)]
    if y.size < 2:
        return 0.0
    x = np.arange(float(y.size), dtype=float)
    try:
        slope = np.polyfit(x, y, 1)[0]
    except Exception:
        return 0.0
    if not np.isfinite(slope):
        return 0.0
    return float(slope)


def _build_observations(
    *,
    run_id: str,
    promoted_df: pd.DataFrame,
    audit_df: pd.DataFrame,
    ontology_spec_hash: str,
) -> pd.DataFrame:
    source_df = audit_df.copy() if not audit_df.empty else promoted_df.copy()
    if source_df.empty:
        return pd.DataFrame()

    rows: List[Dict[str, Any]] = []
    observed_at = datetime.now(timezone.utc).isoformat()
    for row in source_df.to_dict(orient="records"):
        record = dict(row)
        record["run_id"] = run_id
        record["candidate_id"] = str(record.get("candidate_id", "")).strip()
        if not record["candidate_id"]:
            continue
        record["event_type"] = _normalize_event_type(record)
        comps = structural_edge_components(record)
        record["edge_id"] = edge_id_from_row(record)
        record["template_id"] = _template_id(record)
        record["template_family"] = comps.template_family
        record["direction_rule"] = comps.direction_rule
        record["signal_polarity_logic"] = comps.signal_polarity_logic
        record["promotion_decision"] = str(
            record.get("promotion_decision", record.get("status", "rejected"))
        ).strip().lower()
        if record["promotion_decision"] not in {"promoted", "rejected"}:
            record["promotion_decision"] = (
                "promoted" if str(record.get("status", "")).strip().upper() == "PROMOTED" else "rejected"
            )
        record["promotion_score"] = _safe_float(record.get("promotion_score"), 0.0)
        record["effect_value"] = _effect_value(record)
        record["stability_score"] = _safe_float(record.get("stability_score"), 0.0)
        record["observed_at_utc"] = observed_at
        record["ontology_spec_hash"] = str(record.get("ontology_spec_hash", ontology_spec_hash)).strip()
        rows.append(record)
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    # Drop complex object columns that cause Parquet serialization errors
    cols_to_drop = [c for c in out.columns if "schema" in c or "trace" in c or c == "audit_statuses" or out[c].dtype == object and isinstance(out[c].dropna().iloc[0] if not out[c].dropna().empty else None, (list, dict, np.ndarray))]
    out = out.drop(columns=cols_to_drop, errors="ignore")
    out = out.drop_duplicates(subset=["run_id", "candidate_id", "event_type"], keep="last")
    return out


def _aggregate_registry(observations: pd.DataFrame) -> pd.DataFrame:
    if observations.empty:
        return pd.DataFrame()

    work = observations.copy()
    work["is_promoted"] = work["promotion_decision"].astype(str).str.lower() == "promoted"
    work["run_sort"] = work["run_id"].map(_run_sort_value)

    rows: List[Dict[str, Any]] = []
    for edge_id, sub in work.groupby("edge_id", sort=False):
        ordered = sub.sort_values(by=["run_sort", "observed_at_utc", "candidate_id"], kind="stable")
        first = ordered.iloc[0]
        last = ordered.iloc[-1]
        effect_series = pd.to_numeric(ordered["effect_value"], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        stability_series = pd.to_numeric(ordered["stability_score"], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        rows.append(
            {
                "edge_id": str(edge_id),
                "candidate_id": str(last.get("candidate_id", "")).strip(),
                "promotion_score": _safe_float(last.get("promotion_score"), 0.0),
                "promotion_decision": str(last.get("promotion_decision", "rejected")).strip().lower(),
                "event_type": str(last.get("event_type", "UNKNOWN_EVENT")).strip(),
                "template_id": str(last.get("template_id", "UNKNOWN_TEMPLATE")).strip(),
                "template_family": str(last.get("template_family", "UNKNOWN_TEMPLATE")).strip(),
                "direction_rule": str(last.get("direction_rule", "UNKNOWN_DIRECTION")).strip(),
                "signal_polarity_logic": str(last.get("signal_polarity_logic", "UNKNOWN_POLARITY")).strip(),
                "first_seen_run": str(first.get("run_id", "")).strip(),
                "last_seen_run": str(last.get("run_id", "")).strip(),
                "times_promoted": int(ordered["is_promoted"].sum()),
                "times_tested": int(len(ordered)),
                "median_effect": float(effect_series.median()) if not effect_series.empty else 0.0,
                "effect_decay_rate": _effect_decay(effect_series.tolist()),
                "stability_median": float(stability_series.median()) if not stability_series.empty else 0.0,
            }
        )
    out = pd.DataFrame(rows)
    out = out.sort_values(by=["event_type", "template_id", "edge_id"], kind="stable").reset_index(drop=True)
    return out


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Append promoted-candidate lineage and aggregate edge registry.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--promoted_candidates_path", default=None)
    parser.add_argument("--promotion_audit_path", default=None)
    parser.add_argument("--history_dir", default=None)
    parser.add_argument("--out_path", default=None)
    args = parser.parse_args(argv)

    promoted_path = (
        Path(args.promoted_candidates_path)
        if args.promoted_candidates_path
        else DATA_ROOT / "reports" / "promotions" / args.run_id / "promoted_candidates.parquet"
    )
    audit_path = (
        Path(args.promotion_audit_path)
        if args.promotion_audit_path
        else DATA_ROOT / "reports" / "promotions" / args.run_id / "promotion_audit.parquet"
    )
    history_dir = Path(args.history_dir) if args.history_dir else DATA_ROOT / "runs" / "history"
    out_path = (
        Path(args.out_path)
        if args.out_path
        else DATA_ROOT / "runs" / args.run_id / "research" / "edge_registry.parquet"
    )
    history_observations_path = history_dir / "edge_observations.parquet"
    history_registry_path = history_dir / "edge_registry.parquet"

    ensure_dir(history_dir)
    ensure_dir(out_path.parent)

    manifest = start_manifest("update_edge_registry", args.run_id, vars(args), [], [])
    try:
        run_manifest_hashes = load_run_manifest_hashes(DATA_ROOT, args.run_id)
        ontology_spec_hash = str(run_manifest_hashes.get("ontology_spec_hash", "") or "").strip()

        promoted_df = _load_table(promoted_path)
        audit_df = _load_table(audit_path)
        if promoted_df.empty and audit_df.empty:
            raise ValueError(
                f"No promotion artifacts found for run_id={args.run_id}: {promoted_path} and {audit_path}"
            )

        observations_new = _build_observations(
            run_id=args.run_id,
            promoted_df=promoted_df,
            audit_df=audit_df,
            ontology_spec_hash=ontology_spec_hash,
        )
        if observations_new.empty:
            raise ValueError(f"No valid promotion observations for run_id={args.run_id}")

        if ontology_spec_hash and "ontology_spec_hash" in observations_new.columns:
            mismatch = observations_new[
                observations_new["ontology_spec_hash"].astype(str).str.strip() != ontology_spec_hash
            ]
            if not mismatch.empty:
                raise ValueError("Ontology hash mismatch inside promotion artifacts for edge registry update.")

        history_existing = _load_history(history_observations_path)
        if history_existing.empty:
            history_all = observations_new
        else:
            history_all = pd.concat([history_existing, observations_new], ignore_index=True)
            history_all = history_all.drop_duplicates(
                subset=["run_id", "candidate_id", "event_type"],
                keep="last",
            )

        registry_df = _aggregate_registry(history_all)
        if registry_df.empty:
            raise ValueError("Edge registry aggregation produced no rows.")

        history_all.to_parquet(history_observations_path, index=False)
        registry_df.to_parquet(history_registry_path, index=False)
        registry_df.to_parquet(out_path, index=False)

        summary = {
            "run_id": args.run_id,
            "new_observations": int(len(observations_new)),
            "history_observations_total": int(len(history_all)),
            "edge_count_total": int(len(registry_df)),
            "paths": {
                "promoted_candidates_path": str(promoted_path),
                "promotion_audit_path": str(audit_path),
                "history_observations_path": str(history_observations_path),
                "history_registry_path": str(history_registry_path),
                "run_snapshot_path": str(out_path),
            },
        }
        (out_path.parent / "edge_registry_summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True),
            encoding="utf-8",
        )

        finalize_manifest(
            manifest,
            "success",
            stats={
                "new_observations": int(len(observations_new)),
                "history_observations_total": int(len(history_all)),
                "edge_count_total": int(len(registry_df)),
                "run_snapshot_path": (
                    str(out_path.relative_to(PROJECT_ROOT.parent))
                    if str(out_path).startswith(str(PROJECT_ROOT.parent))
                    else str(out_path)
                ),
            },
        )
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
