from __future__ import annotations

from typing import Any, Dict, Iterable

import pandas as pd


_ALLOWED_SIZING_CURVES = {"linear", "sqrt", "flat"}
_STRATEGY_FAMILY_KEYS = {
    "Carry": {
        "funding_percentile_entry_min",
        "funding_percentile_entry_max",
        "normalization_exit_percentile",
        "normalization_exit_consecutive_bars",
        "sizing_curve",
    },
    "MeanReversion": {
        "zscore_entry_abs",
        "extension_entry_abs",
        "reversion_target_zscore",
        "stop_zscore_abs",
    },
    "Spread": {
        "spread_zscore_entry_abs",
        "dislocation_threshold_bps",
        "convergence_target_zscore",
        "max_hold_bars",
    },
}


def ensure_utc_timestamp(series: pd.Series, name: str) -> pd.Series:
    """
    Validate that a pandas Series of timestamps is timezone-aware UTC.
    """
    if not isinstance(series.dtype, pd.DatetimeTZDtype):
        raise ValueError(f"{name} must be timezone-aware UTC")
    if str(series.dt.tz) != "UTC":
        raise ValueError(f"{name} must be UTC")
    return series


def validate_columns(df: pd.DataFrame, required: Iterable[str]) -> None:
    """
    Ensure that a DataFrame contains the required columns.
    """
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")


def strategy_family_allowed_keys(family: str) -> set[str]:
    return set(_STRATEGY_FAMILY_KEYS.get(family, set()))


def validate_strategy_family_params(config: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    raw = config.get("strategy_family_params", {})
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError("strategy_family_params must be a mapping of family -> params")

    out: Dict[str, Dict[str, Any]] = {}
    for family in ["Carry", "MeanReversion", "Spread"]:
        params = raw.get(family, {})
        if params is None:
            continue
        if not isinstance(params, dict):
            raise ValueError(f"strategy_family_params.{family} must be a mapping")
        unknown = sorted(set(params) - _STRATEGY_FAMILY_KEYS[family])
        if unknown:
            raise ValueError(f"strategy_family_params.{family} has unsupported keys: {unknown}")

        norm = dict(params)
        if family == "Carry":
            for key in [
                "funding_percentile_entry_min",
                "funding_percentile_entry_max",
                "normalization_exit_percentile",
            ]:
                if key in norm:
                    value = float(norm[key])
                    if value < 0.0 or value > 100.0:
                        raise ValueError(f"strategy_family_params.Carry.{key} must be in [0, 100]")
                    norm[key] = value
            if (
                "funding_percentile_entry_min" in norm
                and "funding_percentile_entry_max" in norm
                and norm["funding_percentile_entry_min"] > norm["funding_percentile_entry_max"]
            ):
                raise ValueError(
                    "strategy_family_params.Carry.funding_percentile_entry_min must be <= "
                    "funding_percentile_entry_max"
                )
            if "normalization_exit_consecutive_bars" in norm:
                bars = int(norm["normalization_exit_consecutive_bars"])
                if bars < 1:
                    raise ValueError(
                        "strategy_family_params.Carry.normalization_exit_consecutive_bars must be >= 1"
                    )
                norm["normalization_exit_consecutive_bars"] = bars
            if "sizing_curve" in norm:
                curve = str(norm["sizing_curve"]).strip().lower()
                if curve not in _ALLOWED_SIZING_CURVES:
                    allowed = ", ".join(sorted(_ALLOWED_SIZING_CURVES))
                    raise ValueError(f"strategy_family_params.Carry.sizing_curve must be one of: {allowed}")
                norm["sizing_curve"] = curve

        elif family == "MeanReversion":
            for key in ["zscore_entry_abs", "extension_entry_abs"]:
                if key in norm:
                    value = float(norm[key])
                    if value <= 0.0:
                        raise ValueError(f"strategy_family_params.MeanReversion.{key} must be > 0")
                    norm[key] = value
            if "reversion_target_zscore" in norm:
                norm["reversion_target_zscore"] = float(norm["reversion_target_zscore"])
            if "stop_zscore_abs" in norm and norm["stop_zscore_abs"] is not None:
                stop_z = float(norm["stop_zscore_abs"])
                if stop_z <= 0.0:
                    raise ValueError("strategy_family_params.MeanReversion.stop_zscore_abs must be > 0 when set")
                norm["stop_zscore_abs"] = stop_z

        elif family == "Spread":
            for key in ["spread_zscore_entry_abs", "convergence_target_zscore"]:
                if key in norm:
                    value = float(norm[key])
                    if key == "spread_zscore_entry_abs" and value <= 0.0:
                        raise ValueError("strategy_family_params.Spread.spread_zscore_entry_abs must be > 0")
                    norm[key] = value
            if "dislocation_threshold_bps" in norm:
                dislocation = float(norm["dislocation_threshold_bps"])
                if dislocation < 0.0:
                    raise ValueError("strategy_family_params.Spread.dislocation_threshold_bps must be >= 0")
                norm["dislocation_threshold_bps"] = dislocation
            if "max_hold_bars" in norm:
                hold = int(norm["max_hold_bars"])
                if hold < 1:
                    raise ValueError("strategy_family_params.Spread.max_hold_bars must be >= 1")
                norm["max_hold_bars"] = hold

        out[family] = norm

    return out
