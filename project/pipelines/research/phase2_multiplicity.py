"""
Benjamini-Hochberg FDR controls and family key construction.

Extracted from phase2_candidate_discovery.py — pure functions, no side effects.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from pipelines._lib.bh_fdr_grouping import canonical_bh_group_key
from pipelines._lib.ontology_contract import state_id_to_context_column
from pipelines.research.analyze_conditional_expectancy import _bh_adjust

def _make_family_id(
    symbol: str,
    event_type: str,
    rule: str,
    horizon: str,
    cond_label: str,
    *,
    canonical_family: Optional[str] = None,
    state_id: Optional[str] = None,
) -> str:
    """BH family key based on ontology axes, stratified by symbol.

    Conditioning buckets are intentionally excluded to avoid multiplicity leakage.
    """
    base = canonical_bh_group_key(
        canonical_family=str(canonical_family or event_type),
        canonical_event_type=str(event_type),
        template_verb=str(rule),
        horizon=str(horizon),
        state_id=(str(state_id).strip() if state_id else None),
        symbol=None,
        include_symbol=False,
        direction_bucket=None,
    )
    return f"{str(symbol).strip().upper()}_{base}"


def _resolved_sample_size(joined_event_count: int, symbol_event_count: int) -> int:
    """Sample size for a candidate must reflect joined observations, not symbol totals."""
    try:
        joined = int(joined_event_count)
    except (TypeError, ValueError):
        joined = 0
    try:
        symbol_total = int(symbol_event_count)
    except (TypeError, ValueError):
        symbol_total = 0
    return max(0, min(joined, symbol_total if symbol_total > 0 else joined))


def _resolve_state_context_column(columns: pd.Index, state_id: Optional[str]) -> Optional[str]:
    state = str(state_id or "").strip()
    if not state:
        return None
    by_id = state_id_to_context_column(state)
    candidates = [
        by_id,
        state,
        state.upper(),
        state.lower(),
    ]
    for candidate in candidates:
        if candidate and candidate in columns:
            return str(candidate)
    return None


def _simes_p_value(p_vals: pd.Series) -> float:
    p = p_vals.dropna().sort_values()
    m = len(p)
    if m == 0:
        return 1.0
    return float((p * m / np.arange(1, m + 1)).min())


def _bool_mask_from_series(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series.fillna(False)
    values = pd.to_numeric(series, errors="coerce")
    if values.notna().any():
        return values.fillna(0.0) > 0.0
    text = series.astype(str).str.strip().str.lower()
    return text.isin({"1", "true", "t", "yes", "y", "on", "active"})


def _apply_multiplicity_controls(
    raw_df: pd.DataFrame,
    max_q: float,
    *,
    mode: str = "production",
    min_sample_size: int = 0,
) -> pd.DataFrame:
    """Apply BH correction per-family, then a global BH over family-adjusted q-values.

    In research mode, rows with sample_size below min_sample_size are retained for
    diagnostics but excluded from the multiplicity pool.
    """
    if raw_df.empty:
        out = raw_df.copy()
        out["q_value_family"] = pd.Series(dtype=float)
        out["is_discovery_family"] = pd.Series(dtype=bool)
        out["q_value"] = pd.Series(dtype=float)
        out["is_discovery"] = pd.Series(dtype=bool)
        return out

    out = raw_df.copy()
    out["q_value_family"] = 1.0
    out["is_discovery_family"] = False
    out["q_value"] = 1.0
    out["is_discovery"] = False

    eligible_mask = pd.Series(True, index=out.index)
    if str(mode) == "research" and int(min_sample_size) > 0 and "sample_size" in out.columns:
        sample = pd.to_numeric(out["sample_size"], errors="coerce").fillna(0.0)
        eligible_mask = sample >= float(int(min_sample_size))

    eligible = out[eligible_mask].copy()
    if eligible.empty:
        return out

    p_col = "p_value_for_fdr" if "p_value_for_fdr" in eligible.columns else "p_value"
    
    # 1. Compute Simes p-value per family
    family_simes = eligible.groupby("family_id")[p_col].apply(_simes_p_value).rename("p_value_family").reset_index()
    
    # 2. BH across families
    family_simes["q_value_family"] = _bh_adjust(family_simes["p_value_family"])
    family_simes["is_discovery_family"] = family_simes["q_value_family"] <= float(max_q)
    
    family_q_map = dict(zip(family_simes["family_id"], family_simes["q_value_family"]))
    family_disc_map = dict(zip(family_simes["family_id"], family_simes["is_discovery_family"]))
    
    eligible["q_value_family"] = eligible["family_id"].map(family_q_map).fillna(1.0)
    eligible["is_discovery_family"] = eligible["family_id"].map(family_disc_map).fillna(False)

    family_frames: List[pd.DataFrame] = []
    for _, family_df in eligible.groupby("family_id"):
        fam = family_df.copy()
        is_sel = fam["is_discovery_family"].iloc[0] if len(fam) > 0 else False
        if is_sel:
            fam["q_value"] = _bh_adjust(fam[p_col])
        else:
            fam["q_value"] = 1.0
        fam["is_discovery"] = fam["q_value"] <= float(max_q)
        family_frames.append(fam)

    eligible_scored = pd.concat(family_frames, axis=0)

    for col in ("q_value_family", "is_discovery_family", "q_value", "is_discovery"):
        out.loc[eligible_scored.index, col] = eligible_scored[col]
    return out
