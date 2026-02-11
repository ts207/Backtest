from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


@dataclass
class AuditConfig:
    duplicate_score_tol: float = 1e-12
    saturated_stability_value: float = 1.0
    high_corr_threshold: float = 0.6
    cofire_window_minutes: int = 15


def _read_input(path: Path) -> pd.DataFrame:
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, dict):
        payload = payload.get("candidates", [])
    if not isinstance(payload, list):
        raise ValueError("Input JSON must be a list (or a dict with key 'candidates').")
    return pd.DataFrame(payload)


def _table_text(df: pd.DataFrame) -> str:
    try:
        return df.to_markdown(index=False)
    except ImportError:
        return df.to_string(index=False)


def _family_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "event",
                "candidate_count",
                "n_events_median",
                "edge_score_median",
                "expected_return_proxy_median",
                "typical_sign",
            ]
        )
    grouped = (
        df.groupby("event", dropna=False)
        .agg(
            candidate_count=("candidate_id", "count"),
            n_events_median=("n_events", "median"),
            edge_score_median=("edge_score", "median"),
            expected_return_proxy_median=("expected_return_proxy", "median"),
        )
        .reset_index()
    )
    grouped["typical_sign"] = np.where(
        grouped["expected_return_proxy_median"] > 0,
        "positive",
        np.where(grouped["expected_return_proxy_median"] < 0, "negative", "flat"),
    )
    return grouped.sort_values(["edge_score_median", "n_events_median"], ascending=[False, False])


def _mechanism_from_candidate(candidate_id: str) -> str:
    # collapse ..._0, ..._1 style parameter variants into one mechanism key
    return re.sub(r"_\d+$", "", str(candidate_id))


def _collapse_near_duplicates(df: pd.DataFrame, tol: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        empty = pd.DataFrame(columns=["event", "mechanism", "kept_candidate", "dropped_candidates", "count"])
        return df.copy(), empty

    work = df.copy()
    work["mechanism"] = work.get("candidate_id", "").astype(str).map(_mechanism_from_candidate)
    if "edge_score" in work.columns:
        digits = max(0, int(abs(np.log10(tol))))
        work["edge_score_rounded"] = pd.to_numeric(work["edge_score"], errors="coerce").round(digits)
    else:
        work["edge_score_rounded"] = np.nan

    kept_rows: List[pd.Series] = []
    collapse_rows: List[Dict[str, object]] = []

    group_cols = ["event", "mechanism"]
    for (event, mech), g in work.groupby(group_cols, dropna=False):
        if g.empty:
            continue
        ranked = g.sort_values(["edge_score", "n_events"], ascending=[False, False], na_position="last")
        kept = ranked.iloc[0]
        dropped = ranked.iloc[1:]
        kept_rows.append(kept)
        if len(ranked) > 1:
            collapse_rows.append(
                {
                    "event": event,
                    "mechanism": mech,
                    "kept_candidate": str(kept.get("candidate_id", "")),
                    "dropped_candidates": sorted(dropped.get("candidate_id", pd.Series(dtype=str)).astype(str).tolist()),
                    "count": int(len(ranked)),
                }
            )

    out = pd.DataFrame(kept_rows).drop(columns=["edge_score_rounded"], errors="ignore")
    collapse_df = pd.DataFrame(collapse_rows)
    return out.reset_index(drop=True), collapse_df


def _duplicate_edges(df: pd.DataFrame, tol: float) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["event", "edge_score", "candidate_count", "candidate_ids"])
    rows: List[Dict[str, object]] = []
    digits = max(0, int(abs(np.log10(tol))))
    for event, sub in df.groupby("event", dropna=False):
        sub = sub.copy()
        if "edge_score" not in sub.columns:
            continue
        sub["edge_score_rounded"] = pd.to_numeric(sub["edge_score"], errors="coerce").round(digits)
        for score, g in sub.groupby("edge_score_rounded", dropna=False):
            if len(g) < 2:
                continue
            rows.append(
                {
                    "event": event,
                    "edge_score": float(score),
                    "candidate_count": int(len(g)),
                    "candidate_ids": sorted(g.get("candidate_id", pd.Series(dtype=str)).astype(str).tolist()),
                }
            )
    return pd.DataFrame(rows)


def _stability_saturation(df: pd.DataFrame, saturated_value: float) -> Dict[str, object]:
    if "stability_proxy" not in df.columns or df.empty:
        return {"available": False, "saturated_ratio": np.nan, "count": 0}
    vals = pd.to_numeric(df["stability_proxy"], errors="coerce")
    finite = vals[np.isfinite(vals)]
    if finite.empty:
        return {"available": True, "saturated_ratio": np.nan, "count": 0}
    saturated = (finite == saturated_value).sum()
    return {
        "available": True,
        "saturated_ratio": float(saturated / len(finite)),
        "count": int(len(finite)),
        "min": float(finite.min()),
        "max": float(finite.max()),
    }


def _expected_return_unit_hint(df: pd.DataFrame) -> Dict[str, object]:
    if "expected_return_proxy" not in df.columns or df.empty:
        return {"available": False, "note": "missing expected_return_proxy"}
    vals = pd.to_numeric(df["expected_return_proxy"], errors="coerce")
    vals = vals[np.isfinite(vals)]
    if vals.empty:
        return {"available": True, "note": "all NaN"}
    med_abs = float(vals.abs().median())
    if med_abs >= 0.01:
        note = "magnitude appears large; confirm this is not already scaled in bps/percent units"
    elif med_abs >= 0.001:
        note = "magnitude in 1e-3 range; if per-event decimal return, impact is economically meaningful"
    else:
        note = "magnitude small; unit interpretation required before economic conclusions"
    return {"available": True, "median_abs": med_abs, "note": note}


def _compute_independence(df: pd.DataFrame, cofire_window_minutes: int) -> Dict[str, object]:
    required = {"event", "enter_ts"}
    if df.empty or not required.issubset(df.columns):
        return {"available": False, "reason": "requires event and enter_ts columns"}

    w = df.copy()
    w["enter_ts"] = pd.to_datetime(w["enter_ts"], utc=True, errors="coerce")
    w = w.dropna(subset=["enter_ts", "event"]).copy()
    if w.empty:
        return {"available": False, "reason": "no usable timestamps"}

    event_to_ts = {
        ev: np.sort(g["enter_ts"].drop_duplicates().astype("int64").to_numpy())
        for ev, g in w.groupby("event", dropna=False)
    }
    events = sorted(event_to_ts.keys())
    rows: List[Dict[str, object]] = []
    window_ns = int(cofire_window_minutes) * 60 * 1_000_000_000

    for i, a in enumerate(events):
        ta = event_to_ts[a]
        set_a = set(ta.tolist())
        for b in events[i + 1 :]:
            tb = event_to_ts[b]
            set_b = set(tb.tolist())
            inter = len(set_a & set_b)
            union = len(set_a | set_b) if (set_a or set_b) else 0
            jaccard = float(inter / union) if union else np.nan

            # co-firing: % of A timestamps having at least one B timestamp within ±window
            if len(tb) == 0 or len(ta) == 0:
                cofire = np.nan
            else:
                hits = 0
                for t in ta:
                    idx = np.searchsorted(tb, t)
                    near = []
                    if idx < len(tb):
                        near.append(abs(tb[idx] - t))
                    if idx > 0:
                        near.append(abs(tb[idx - 1] - t))
                    if near and min(near) <= window_ns:
                        hits += 1
                cofire = float(hits / len(ta))

            rows.append(
                {
                    "event_a": a,
                    "event_b": b,
                    "timestamp_jaccard": jaccard,
                    f"cofire_rate_{cofire_window_minutes}m": cofire,
                }
            )

    pairwise = pd.DataFrame(rows)

    # optional event-level proxy correlation if expected_return_proxy exists
    corr_df = pd.DataFrame()
    if "expected_return_proxy" in w.columns:
        x = w[["event", "enter_ts", "expected_return_proxy"]].copy()
        x["expected_return_proxy"] = pd.to_numeric(x["expected_return_proxy"], errors="coerce")
        x = x.dropna(subset=["expected_return_proxy"])  # keep only numeric
        if not x.empty:
            pivot = x.pivot_table(index="enter_ts", columns="event", values="expected_return_proxy", aggfunc="mean")
            corr = pivot.corr(min_periods=3)
            if not corr.empty:
                corr_df = corr.reset_index().rename(columns={"index": "event"})

    return {
        "available": True,
        "pairwise_overlap": pairwise.to_dict(orient="records"),
        "proxy_corr_matrix": corr_df.to_dict(orient="records") if not corr_df.empty else [],
    }


def _recommendations(
    df: pd.DataFrame,
    duplicates: pd.DataFrame,
    collapse_df: pd.DataFrame,
    saturation: Dict[str, object],
    unit_hint: Dict[str, object],
    independence: Dict[str, object],
    cfg: AuditConfig,
) -> List[str]:
    recs: List[str] = []
    if saturation.get("available") and saturation.get("count", 0) > 0:
        ratio = float(saturation.get("saturated_ratio", np.nan))
        if np.isfinite(ratio) and ratio >= 0.9:
            recs.append(
                "Stability proxy appears saturated near 1.0 for most candidates. Replace binary pass/fail proxy with split-level dispersion metrics (effect variance/CV + sign agreement + min split sample floor)."
            )
    if not duplicates.empty:
        recs.append("Duplicate/near-duplicate edge scores detected within event families. Collapse threshold variants to one canonical candidate per mechanism.")
    if not collapse_df.empty:
        recs.append("Mechanism collapse identified candidate variants that should be de-duplicated before ranking or portfolio construction.")
    if unit_hint.get("available"):
        recs.append(f"Expected-return proxy unit check: {unit_hint.get('note')}")
    if independence.get("available"):
        pairwise = pd.DataFrame(independence.get("pairwise_overlap", []))
        cofire_col = f"cofire_rate_{cfg.cofire_window_minutes}m"
        if not pairwise.empty and cofire_col in pairwise.columns:
            hi = pairwise[cofire_col] >= cfg.high_corr_threshold
            if bool(hi.any()):
                recs.append(
                    f"High co-firing detected (>= {cfg.high_corr_threshold:.2f}) for some family pairs; treat highly correlated edges as one structural edge."
                )
    else:
        recs.append("Independence audit unavailable: provide event-level timestamps (enter_ts) to compute overlap/co-firing.")
    recs.append("Do not run optimizer until de-duplication and independence checks are complete.")
    return recs


def _to_markdown_summary(
    df: pd.DataFrame,
    fam: pd.DataFrame,
    duplicates: pd.DataFrame,
    collapse_df: pd.DataFrame,
    saturation: Dict[str, object],
    unit_hint: Dict[str, object],
    independence: Dict[str, object],
    recs: List[str],
    cfg: AuditConfig,
) -> str:
    lines: List[str] = []
    lines.append("# Promoted Candidate Audit")
    lines.append("")
    lines.append(f"- candidates: {len(df)}")
    lines.append(f"- events: {df['event'].nunique() if 'event' in df.columns else 0}")
    if saturation.get("available"):
        ratio = saturation.get("saturated_ratio", np.nan)
        lines.append(
            f"- stability_proxy saturation ratio (@{cfg.saturated_stability_value}): {ratio:.2%}"
            if np.isfinite(ratio)
            else "- stability_proxy saturation ratio: n/a"
        )
    else:
        lines.append("- stability_proxy: not available")
    if unit_hint.get("available"):
        lines.append(f"- expected_return_proxy unit note: {unit_hint.get('note')}")
    lines.append("")

    lines.append("## Family summary")
    lines.append("")
    lines.append(_table_text(fam) if not fam.empty else "None")
    lines.append("")

    lines.append("## Duplicate edge-score groups")
    lines.append("")
    lines.append(_table_text(duplicates) if not duplicates.empty else "None")
    lines.append("")

    lines.append("## Mechanism-collapse candidates")
    lines.append("")
    lines.append(_table_text(collapse_df) if not collapse_df.empty else "None")
    lines.append("")

    lines.append("## Cross-family dependency snapshot")
    lines.append("")
    if independence.get("available"):
        pairwise = pd.DataFrame(independence.get("pairwise_overlap", []))
        lines.append(_table_text(pairwise) if not pairwise.empty else "None")
    else:
        lines.append(f"Unavailable: {independence.get('reason', 'unknown reason')}")
    lines.append("")

    lines.append("## Recommendations")
    lines.append("")
    for r in recs:
        lines.append(f"- {r}")
    lines.append("")
    return "\n".join(lines)


def run_audit(input_path: Path, out_dir: Path, cfg: AuditConfig) -> Dict[str, object]:
    df = _read_input(input_path)

    numeric_cols = ["edge_score", "expected_return_proxy", "stability_proxy", "n_events"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    fam = _family_summary(df)
    duplicates = _duplicate_edges(df, cfg.duplicate_score_tol)
    collapsed, collapse_df = _collapse_near_duplicates(df, cfg.duplicate_score_tol)
    saturation = _stability_saturation(df, cfg.saturated_stability_value)
    unit_hint = _expected_return_unit_hint(df)
    independence = _compute_independence(df, cfg.cofire_window_minutes)
    recs = _recommendations(df, duplicates, collapse_df, saturation, unit_hint, independence, cfg)

    out_dir.mkdir(parents=True, exist_ok=True)
    audit_json = out_dir / "promotion_audit.json"
    audit_md = out_dir / "promotion_audit.md"
    collapsed_json = out_dir / "collapsed_candidates.json"

    payload: Dict[str, object] = {
        "candidate_count": int(len(df)),
        "event_count": int(df["event"].nunique()) if "event" in df.columns else 0,
        "stability_saturation": saturation,
        "expected_return_unit_hint": unit_hint,
        "family_summary": fam.to_dict(orient="records"),
        "duplicate_edge_groups": duplicates.to_dict(orient="records"),
        "mechanism_collapse": collapse_df.to_dict(orient="records"),
        "independence": independence,
        "recommendations": recs,
    }

    audit_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    collapsed_json.write_text(json.dumps(collapsed.to_dict(orient="records"), indent=2), encoding="utf-8")
    audit_md.write_text(
        _to_markdown_summary(df, fam, duplicates, collapse_df, saturation, unit_hint, independence, recs, cfg),
        encoding="utf-8",
    )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit promoted Phase-2 candidates for red flags and consolidation steps")
    parser.add_argument("--input", required=True, help="Path to promoted_candidates JSON (list or {'candidates': [...]})")
    parser.add_argument("--out_dir", required=True, help="Output directory for audit artifacts")
    parser.add_argument("--duplicate_score_tol", type=float, default=1e-12)
    parser.add_argument("--cofire_window_minutes", type=int, default=15)
    parser.add_argument("--high_corr_threshold", type=float, default=0.6)
    args = parser.parse_args()

    payload = run_audit(
        Path(args.input),
        Path(args.out_dir),
        AuditConfig(
            duplicate_score_tol=float(args.duplicate_score_tol),
            cofire_window_minutes=int(args.cofire_window_minutes),
            high_corr_threshold=float(args.high_corr_threshold),
        ),
    )
    print(
        json.dumps(
            {
                "status": "ok",
                "candidate_count": payload["candidate_count"],
                "event_count": payload["event_count"],
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
