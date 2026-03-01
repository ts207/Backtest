"""
Lambda state persistence: load previous run's shrinkage lambdas, build snapshot.

Extracted from phase2_candidate_discovery.py — pure functions.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

def _load_previous_lambda_maps(
    *,
    data_root: Path,
    event_type: str,
    current_run_id: str,
) -> Tuple[Dict[str, Dict[Tuple[Any, ...], float]], Optional[Path]]:
    phase2_root = data_root / "reports" / "phase2"
    pattern = f"*/{event_type}/phase2_lambda_snapshot.parquet"
    candidates = []
    for path in phase2_root.glob(pattern):
        try:
            run_id = path.parts[-3]
        except Exception:
            continue
        if str(run_id) == str(current_run_id):
            continue
        candidates.append(path)
    if not candidates:
        return {}, None

    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    source = candidates[0]
    try:
        df = pd.read_parquet(source)
    except Exception:
        return {}, None

    out: Dict[str, Dict[Tuple[Any, ...], float]] = {"family": {}, "event": {}, "state": {}}
    if df.empty or "level" not in df.columns or "lambda_value" not in df.columns:
        return out, source

    def _safe_float(x: Any) -> Optional[float]:
        try:
            v = float(x)
        except (TypeError, ValueError):
            return None
        if not np.isfinite(v) or v <= 0.0:
            return None
        return v

    # Vectorized: process each level as a filtered DataFrame slice
    for _level, _key_cols in [
        ("family", ["template_verb", "horizon"]),
        ("event",  ["template_verb", "horizon", "canonical_family"]),
        ("state",  ["template_verb", "horizon", "canonical_family", "canonical_event_type"]),
    ]:
        _sub = df[df["level"].astype(str).str.strip().str.lower() == _level].copy()
        if _sub.empty:
            continue
        _sub["lam_"] = pd.to_numeric(_sub["lambda_value"], errors="coerce")
        _sub = _sub.dropna(subset=["lam_"])
        _sub = _sub[_sub["lam_"] > 0.0]
        if _sub.empty:
            continue
        _present = [c for c in _key_cols if c in _sub.columns]
        if not _present:
            continue
        # Normalise string columns
        for _c in _present:
            _sub[_c] = _sub[_c].fillna("").astype(str).str.strip()
            if _c in ("canonical_family", "canonical_event_type"):
                _sub[_c] = _sub[_c].str.upper()
        for _rec in _sub[[*_present, "lam_"]].itertuples(index=False):
            _key = tuple(getattr(_rec, c) for c in _present)
            out[_level][_key] = float(_rec.lam_)

    total_keys = sum(len(v) for v in out.values())
    log.info(
        "Loaded previous lambda state from %s: family=%d event=%d state=%d (total=%d)",
        source, len(out["family"]), len(out["event"]), len(out["state"]), total_keys,
    )
    return out, source


def _build_lambda_snapshot(fdr_df: pd.DataFrame) -> pd.DataFrame:
    if fdr_df.empty:
        return pd.DataFrame(
            columns=[
                "level",
                "template_verb",
                "horizon",
                "canonical_family",
                "canonical_event_type",
                "lambda_value",
                "lambda_status",
            ]
        )

    # Vectorized: build each level as a DataFrame slice then pd.concat
    level_frames: List[pd.DataFrame] = []

    fam_cols = ["template_verb", "horizon", "lambda_family", "lambda_family_status"]
    _fam = fdr_df[[c for c in fam_cols if c in fdr_df.columns]].drop_duplicates().copy()
    if not _fam.empty:
        _fam_out = pd.DataFrame({
            "level": "family",
            "template_verb": _fam.get("template_verb", "").fillna("").astype(str),
            "horizon": _fam.get("horizon", "").fillna("").astype(str),
            "canonical_family": "",
            "canonical_event_type": "",
            "lambda_value": pd.to_numeric(_fam.get("lambda_family", 0.0), errors="coerce").fillna(0.0),
            "lambda_status": _fam.get("lambda_family_status", "").fillna("").astype(str),
        })
        level_frames.append(_fam_out)

    evt_cols = ["template_verb", "horizon", "canonical_family", "lambda_event", "lambda_event_status"]
    _evt = fdr_df[[c for c in evt_cols if c in fdr_df.columns]].drop_duplicates().copy()
    if not _evt.empty:
        _evt_out = pd.DataFrame({
            "level": "event",
            "template_verb": _evt.get("template_verb", "").fillna("").astype(str),
            "horizon": _evt.get("horizon", "").fillna("").astype(str),
            "canonical_family": _evt.get("canonical_family", "").fillna("").astype(str).str.upper(),
            "canonical_event_type": "",
            "lambda_value": pd.to_numeric(_evt.get("lambda_event", 0.0), errors="coerce").fillna(0.0),
            "lambda_status": _evt.get("lambda_event_status", "").fillna("").astype(str),
        })
        level_frames.append(_evt_out)
    st_cols = ["template_verb", "horizon", "canonical_family", "canonical_event_type", "lambda_state", "lambda_state_status"]
    _st = fdr_df[[c for c in st_cols if c in fdr_df.columns]].drop_duplicates().copy()
    if not _st.empty:
        _st_out = pd.DataFrame({
            "level": "state",
            "template_verb": _st.get("template_verb", "").fillna("").astype(str),
            "horizon": _st.get("horizon", "").fillna("").astype(str),
            "canonical_family": _st.get("canonical_family", "").fillna("").astype(str).str.upper(),
            "canonical_event_type": _st.get("canonical_event_type", "").fillna("").astype(str).str.upper(),
            "lambda_value": pd.to_numeric(_st.get("lambda_state", 0.0), errors="coerce").fillna(0.0),
            "lambda_status": _st.get("lambda_state_status", "").fillna("").astype(str),
        })
        level_frames.append(_st_out)

    if not level_frames:
        return pd.DataFrame(columns=["level", "template_verb", "horizon",
                                     "canonical_family", "canonical_event_type",
                                     "lambda_value", "lambda_status"])
    return pd.concat(level_frames, ignore_index=True)


def save_lambda_state_json(
    fdr_df: pd.DataFrame,
    out_path: Path,
    *,
    run_id: str = "",
    event_type: str = "",
) -> None:
    """Write a canonical JSON representation of the lambda snapshot.

    The JSON mirrors the parquet snapshot columns but uses a nested
    ``level → [{key_cols…, lambda_value, lambda_status}]`` structure
    for easier human inspection and cross-format auditing.
    """
    snapshot_df = _build_lambda_snapshot(fdr_df)
    state: Dict[str, Any] = {
        "_meta": {
            "run_id": str(run_id),
            "event_type": str(event_type),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_rows": int(len(snapshot_df)),
        },
        "family": [],
        "event": [],
        "state": [],
    }

    for level in ("family", "event", "state"):
        sub = snapshot_df[snapshot_df["level"] == level]
        if sub.empty:
            continue
        for _, row in sub.iterrows():
            entry: Dict[str, Any] = {
                "template_verb": str(row.get("template_verb", "")),
                "horizon": str(row.get("horizon", "")),
                "lambda_value": float(row.get("lambda_value", 0.0)),
                "lambda_status": str(row.get("lambda_status", "")),
            }
            if level in ("event", "state"):
                entry["canonical_family"] = str(row.get("canonical_family", ""))
            if level == "state":
                entry["canonical_event_type"] = str(row.get("canonical_event_type", ""))
            state[level].append(entry)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    log.info("Wrote lambda state JSON: %s (%d entries)", out_path, len(snapshot_df))
