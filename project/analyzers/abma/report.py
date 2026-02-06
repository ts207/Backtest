from __future__ import annotations

import pandas as pd

from .config_v1 import ABMAConfig, DEFAULT_ABMA_CONFIG


def _table_text(df: pd.DataFrame) -> str:
    try:
        return df.to_markdown(index=False)
    except ImportError:
        return df.to_string(index=False)


def build_report_artifacts(
    evaluation_outputs: dict[str, pd.DataFrame],
    *,
    config: ABMAConfig = DEFAULT_ABMA_CONFIG,
) -> dict[str, object]:
    """Build report artifacts (plots/tables) for ABMA v1."""
    by_split = evaluation_outputs.get("by_split", pd.DataFrame())
    stability = evaluation_outputs.get("stability", pd.DataFrame())
    curve_summary = evaluation_outputs.get("curve_summary", pd.DataFrame())

    stability_row = stability.iloc[0].to_dict() if not stability.empty else {}
    summary = {
        "abma_def_version": config.def_version,
        "n_splits": int(len(by_split)),
        "stability_checks": {
            "sign_consistency": float(stability_row.get("sign_consistency", 0.0) or 0.0),
            "effect_ci_excludes_0": bool(stability_row.get("effect_ci_excludes_0", False)),
            "regime_flip_count": int(stability_row.get("regime_flip_count", 0) or 0),
            "monotonic_decay": bool(stability_row.get("monotonic_decay", False)),
        },
        "phase1_accept": bool(stability_row.get("pass", False)),
    }

    lines = [
        "# ABMA v1 Report",
        "",
        f"- Definition: `{config.def_version}`",
        f"- Phase-1 accept: `{summary['phase1_accept']}`",
        "",
        "## Stability",
    ]
    if stability.empty:
        lines.append("No stability rows produced.")
    else:
        lines.append(_table_text(stability))
    lines.append("")
    lines.append("## Split Summary")
    if by_split.empty:
        lines.append("No split rows produced.")
    else:
        lines.append(_table_text(by_split))
    lines.append("")
    lines.append("## Curve Summary")
    if curve_summary.empty:
        lines.append("No curve summary rows produced.")
    else:
        lines.append(_table_text(curve_summary.head(40)))

    return {
        "summary": summary,
        "report_md": "\n".join(lines) + "\n",
        "tables": {
            "by_split": by_split,
            "stability": stability,
            "curve_summary": curve_summary,
        },
    }
