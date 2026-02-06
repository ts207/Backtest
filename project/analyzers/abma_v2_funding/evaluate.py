from __future__ import annotations

import numpy as np
import pandas as pd

from .config_v2 import ABMAFundingConfig, DEFAULT_ABMA_FUNDING_CONFIG


def evaluate_stabilization(
    event_metrics: pd.DataFrame,
    curves: pd.DataFrame,
    *,
    config: ABMAFundingConfig = DEFAULT_ABMA_FUNDING_CONFIG,
) -> dict[str, pd.DataFrame]:
    """Compute stabilization curves and stability gates (v2 funding)."""
    metrics = event_metrics.copy()
    curves_local = curves.copy()
    if metrics.empty:
        empty = pd.DataFrame()
        return {"by_split": empty, "stability": empty, "curve_summary": empty}

    metrics["session_date"] = pd.to_datetime(metrics["session_date"], errors="coerce")
    metrics["year"] = metrics["session_date"].dt.year

    split_rows: list[dict[str, object]] = []
    for year, sub in metrics.groupby("year", sort=True):
        spread_sign = float(np.sign(sub["delta_spread_half_life_sec"].dropna().mean())) if sub["delta_spread_half_life_sec"].notna().any() else np.nan
        split_rows.append(
            {
                "split_name": "year",
                "split_value": int(year),
                "n_events": int(len(sub)),
                "delta_spread_half_life_mean": float(sub["delta_spread_half_life_sec"].mean()) if sub["delta_spread_half_life_sec"].notna().any() else np.nan,
                "delta_microprice_var_decay_mean": float(sub["delta_microprice_var_decay_rate"].mean()) if sub["delta_microprice_var_decay_rate"].notna().any() else np.nan,
                "delta_trade_sign_entropy_mean": float(sub["delta_trade_sign_entropy_mean"].mean()) if sub["delta_trade_sign_entropy_mean"].notna().any() else np.nan,
                "sign_delta_spread_half_life": spread_sign,
            }
        )
    by_split = pd.DataFrame(split_rows).sort_values(["split_name", "split_value"]).reset_index(drop=True)

    signs = by_split["sign_delta_spread_half_life"].dropna().to_numpy()
    if len(signs) == 0:
        sign_consistency = 0.0
        regime_flip_count = 0
    else:
        pos_ratio = float((signs > 0).mean())
        neg_ratio = float((signs < 0).mean())
        sign_consistency = max(pos_ratio, neg_ratio)
        regime_flip_count = int(np.sum(np.diff(signs) != 0)) if len(signs) > 1 else 0

    spread_delta = metrics["delta_spread_half_life_sec"].dropna().to_numpy(dtype=float)
    ci_low = np.nan
    ci_high = np.nan
    effect_ci_excludes_0 = False
    if len(spread_delta) > 1:
        rng = np.random.default_rng(7)
        samples = np.empty(config.bootstrap_samples, dtype=float)
        for i in range(config.bootstrap_samples):
            draw = rng.choice(spread_delta, size=len(spread_delta), replace=True)
            samples[i] = float(np.nanmean(draw))
        ci_low, ci_high = np.percentile(samples, [2.5, 97.5]).tolist()
        effect_ci_excludes_0 = bool(ci_low > 0.0 or ci_high < 0.0)

    monotonic_decay = False
    curve_summary = pd.DataFrame()
    if not curves_local.empty:
        g = (
            curves_local.groupby(["anchor_kind", "tau_s"], as_index=False)
            .agg(eff_spread_mean=("eff_spread", "mean"), micro_dev_mean=("micro_dev", "mean"))
            .sort_values(["anchor_kind", "tau_s"])
        )
        curve_summary = g
        event_curve = g[g["anchor_kind"] == "event"].sort_values("tau_s")
        if len(event_curve) > 2:
            diffs = event_curve["eff_spread_mean"].diff().dropna()
            monotonic_decay = bool((diffs <= 1e-9).mean() >= 0.9)

    stability = pd.DataFrame(
        [
            {
                "sign_consistency": float(sign_consistency),
                "regime_flip_count": int(regime_flip_count),
                "effect_ci_excludes_0": bool(effect_ci_excludes_0),
                "effect_ci_low": float(ci_low) if np.isfinite(ci_low) else np.nan,
                "effect_ci_high": float(ci_high) if np.isfinite(ci_high) else np.nan,
                "monotonic_decay": bool(monotonic_decay),
                "pass": bool(
                    sign_consistency >= config.sign_consistency_threshold
                    and regime_flip_count <= config.max_regime_flip_count
                    and effect_ci_excludes_0
                    and monotonic_decay
                ),
            }
        ]
    )
    return {"by_split": by_split, "stability": stability, "curve_summary": curve_summary}
