from __future__ import annotations
from strategy_dsl.contract_v1 import validate_feature_references, resolve_trigger_column


import logging
from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

LOGGER = logging.getLogger(__name__)

from events.registry import REGISTRY_BACKED_SIGNALS
from strategy_dsl.schema import (
    Blueprint,
    ConditionNodeSpec,
    EntrySpec,
    EvaluationSpec,
    ExitSpec,
    LineageSpec,
    OverlaySpec,
    SizingSpec,
    SymbolScopeSpec,
)


from strategy_dsl.contract_v1 import BULL_BEAR_CONDITION_MAP, SESSION_CONDITION_MAP, VOL_REGIME_CONDITION_MAP

KNOWN_ENTRY_SIGNALS = {
    "event_detected",
    "vol_shock_relaxation_event",
    "liquidity_refill_lag_event",
    "liquidity_absence_event",
    "vol_aftershock_event",
    "forced_flow_exhaustion_event",
    "cross_venue_desync_event",
    "liquidity_vacuum_event",
    "funding_extreme_event",
    "range_compression_breakout_event",
    "regime_stability_pass",
    "refill_persistence_pass",
    "spread_guard_pass",
    "oos_validation_pass",
    "cross_venue_consensus_pass",
    "vacuum_refill_confirmation",
    "funding_normalization_pass",
    "breakout_confirmation",
}
def _active_signal_column(signal: str) -> str:
    return f"{signal.removesuffix('_event')}_active"

REGISTRY_SIGNAL_COLUMNS = set()
for signal in REGISTRY_BACKED_SIGNALS:
    REGISTRY_SIGNAL_COLUMNS.add(signal)
    REGISTRY_SIGNAL_COLUMNS.add(_active_signal_column(signal))

MOMENTUM_BIAS_EVENTS = {
    "vol_shock",
    "cross_venue_desync",
    "oi_spike_positive",
}
CONTRARIAN_BIAS_EVENTS = {
    "liquidity_vacuum",
    "forced_flow_exhaustion",
    "funding_extreme_onset",
    "funding_persistence_trigger",
    "funding_normalization_trigger",
    "oi_spike_negative",
    "oi_flush",
    "liquidation_cascade",
}


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        out = float(value)
        if np.isnan(out):
            return default
        return out
    except (TypeError, ValueError):
        return default


def _build_blueprint(raw: Dict[str, object]) -> Blueprint:
    validate_feature_references(raw)
    scope = raw.get("symbol_scope", {})
    entry = raw.get("entry", {})
    exit_spec = raw.get("exit", {})
    sizing = raw.get("sizing", {})
    eval_spec = raw.get("evaluation", {})
    lineage = raw.get("lineage", {})
    overlays = raw.get("overlays", [])
    if not isinstance(scope, dict) or not isinstance(entry, dict) or not isinstance(exit_spec, dict):
        raise ValueError("Invalid blueprint payload shape")

    overlay_rows = []
    for row in overlays if isinstance(overlays, list) else []:
        if not isinstance(row, dict):
            raise ValueError("overlay row must be an object")
        overlay_rows.append(OverlaySpec(name=str(row.get("name", "")), params=dict(row.get("params", {}))))

    condition_nodes: List[ConditionNodeSpec] = []
    raw_nodes = entry.get("condition_nodes", [])
    if isinstance(raw_nodes, list):
        for row in raw_nodes:
            if not isinstance(row, dict):
                raise ValueError("entry.condition_nodes[] must be an object")
            condition_nodes.append(
                ConditionNodeSpec(
                    feature=str(row.get("feature", "")),
                    operator=str(row.get("operator", "")),  # type: ignore[arg-type]
                    value=float(row.get("value", 0.0)),
                    value_high=(None if row.get("value_high") is None else float(row.get("value_high"))),
                    lookback_bars=int(row.get("lookback_bars", 0)),
                    window_bars=int(row.get("window_bars", 0)),
                )
            )

    bp = Blueprint(
        id=str(raw.get("id", "")),
        run_id=str(raw.get("run_id", "")),
        event_type=str(raw.get("event_type", "")),
        candidate_id=str(raw.get("candidate_id", "")),
        symbol_scope=SymbolScopeSpec(
            mode=str(scope.get("mode", "")),  # type: ignore[arg-type]
            symbols=[str(x) for x in scope.get("symbols", [])] if isinstance(scope.get("symbols", []), list) else [],
            candidate_symbol=str(scope.get("candidate_symbol", "")),
        ),
        direction=str(raw.get("direction", "")),  # type: ignore[arg-type]
        entry=EntrySpec(
            triggers=[str(x) for x in entry.get("triggers", [])] if isinstance(entry.get("triggers", []), list) else [],
            conditions=[str(x) for x in entry.get("conditions", [])] if isinstance(entry.get("conditions", []), list) else [],
            confirmations=[str(x) for x in entry.get("confirmations", [])] if isinstance(entry.get("confirmations", []), list) else [],
            delay_bars=int(entry.get("delay_bars", 0)),
            cooldown_bars=int(entry.get("cooldown_bars", 0)),
            condition_logic=str(entry.get("condition_logic", "all")),  # type: ignore[arg-type]
            condition_nodes=condition_nodes,
            arm_bars=int(entry.get("arm_bars", 0)),
            reentry_lockout_bars=int(entry.get("reentry_lockout_bars", 0)),
        ),
        exit=ExitSpec(
            time_stop_bars=int(exit_spec.get("time_stop_bars", 0)),
            invalidation=dict(exit_spec.get("invalidation", {})),
            stop_type=str(exit_spec.get("stop_type", "")),  # type: ignore[arg-type]
            stop_value=float(exit_spec.get("stop_value", 0.0)),
            target_type=str(exit_spec.get("target_type", "")),  # type: ignore[arg-type]
            target_value=float(exit_spec.get("target_value", 0.0)),
            trailing_stop_type=str(exit_spec.get("trailing_stop_type", "none")),  # type: ignore[arg-type]
            trailing_stop_value=float(exit_spec.get("trailing_stop_value", 0.0)),
            break_even_r=float(exit_spec.get("break_even_r", 0.0)),
        ),
        sizing=SizingSpec(
            mode=str(sizing.get("mode", "")),  # type: ignore[arg-type]
            risk_per_trade=(None if sizing.get("risk_per_trade") is None else float(sizing.get("risk_per_trade"))),
            target_vol=(None if sizing.get("target_vol") is None else float(sizing.get("target_vol"))),
            max_gross_leverage=float(sizing.get("max_gross_leverage", 0.0)),
            max_position_scale=float(sizing.get("max_position_scale", 1.0)),
            portfolio_risk_budget=float(sizing.get("portfolio_risk_budget", 1.0)),
            symbol_risk_budget=float(sizing.get("symbol_risk_budget", 1.0)),
        ),
        overlays=overlay_rows,
        evaluation=EvaluationSpec(
            min_trades=int(eval_spec.get("min_trades", 0)),
            cost_model=dict(eval_spec.get("cost_model", {})),
            robustness_flags=dict(eval_spec.get("robustness_flags", {})),
        ),
        lineage=LineageSpec(
            source_path=str(lineage.get("source_path", "")),
            compiler_version=str(lineage.get("compiler_version", "")),
            generated_at_utc=str(lineage.get("generated_at_utc", "")),
            bridge_embargo_days_used=(
                None
                if lineage.get("bridge_embargo_days_used") in (None, "")
                else int(lineage.get("bridge_embargo_days_used"))
            ),
        ),
    )
    bp.validate()
    return bp


def _condition_mask_legacy(merged: pd.DataFrame, expr: str) -> pd.Series:
    expression = str(expr).strip()
    if not expression or expression == "all":
        return pd.Series(True, index=merged.index, dtype=bool)

    lowered = expression.lower()
    # Support symbolic conditions emitted by the research compiler for transparency/promotion diagnostics.
    # Numeric expressions continue to be supported below.
    if lowered in SESSION_CONDITION_MAP:
        low, high = SESSION_CONDITION_MAP[lowered]
        hours = pd.to_numeric(merged.get("session_hour_utc"), errors="coerce")
        return ((hours >= float(low)) & (hours <= float(high))).fillna(False)

    if lowered in BULL_BEAR_CONDITION_MAP:
        target = float(BULL_BEAR_CONDITION_MAP[lowered])
        series = pd.to_numeric(merged.get("bull_bear_flag"), errors="coerce")
        return (series == target).fillna(False)

    if lowered in VOL_REGIME_CONDITION_MAP:
        target = float(VOL_REGIME_CONDITION_MAP[lowered])
        series = pd.to_numeric(merged.get("vol_regime_code"), errors="coerce")
        return (series == target).fillna(False)

    if lowered.startswith("symbol_"):
        # Symbol scoping is applied elsewhere (Blueprint.symbol_scope).
        return pd.Series(True, index=merged.index, dtype=bool)

    for operator in [">=", "<=", "==", ">", "<"]:
        if operator in expression:
            col, raw_value = expression.split(operator, 1)
            column = col.strip()
            if column not in merged.columns:
                raise ValueError(f"Unknown condition column: {column}")
            value = _to_float(raw_value.strip(), default=np.nan)
            if np.isnan(value):
                raise ValueError(f"Condition threshold is not numeric: {expression}")
            series = pd.to_numeric(merged[column], errors="coerce")
            if operator == ">=":
                return (series >= value).fillna(False)
            if operator == "<=":
                return (series <= value).fillna(False)
            if operator == "==":
                return (series == value).fillna(False)
            if operator == ">":
                return (series > value).fillna(False)
            return (series < value).fillna(False)
    raise ValueError(f"Unsupported condition syntax: {expression}")


def _condition_mask_node(merged: pd.DataFrame, node: ConditionNodeSpec) -> pd.Series:
    if node.feature not in merged.columns:
        raise ValueError(f"Unknown condition feature: {node.feature}")
    series = pd.to_numeric(merged[node.feature], errors="coerce")
    if int(node.lookback_bars) > 0:
        series = series.shift(int(node.lookback_bars))

    op = node.operator
    if op == ">":
        return (series > float(node.value)).fillna(False)
    if op == ">=":
        return (series >= float(node.value)).fillna(False)
    if op == "<":
        return (series < float(node.value)).fillna(False)
    if op == "<=":
        return (series <= float(node.value)).fillna(False)
    if op == "==":
        return (series == float(node.value)).fillna(False)
    if op == "crosses_above":
        prior = series.shift(1)
        return ((prior <= float(node.value)) & (series > float(node.value))).fillna(False)
    if op == "crosses_below":
        prior = series.shift(1)
        return ((prior >= float(node.value)) & (series < float(node.value))).fillna(False)
    if op == "in_range":
        high = float(node.value_high) if node.value_high is not None else float(node.value)
        return ((series >= float(node.value)) & (series <= high)).fillna(False)
    if op in {"zscore_gt", "zscore_lt"}:
        window = int(node.window_bars)
        mean = series.rolling(window, min_periods=window).mean()
        raw_std = series.rolling(window, min_periods=window).std()
        zero_std_count = int((raw_std == 0.0).sum())
        if zero_std_count:
            LOGGER.warning(
                "zscore condition on feature '%s': %d bars have zero std (constant window)"
                " — condition will evaluate False for those bars",
                node.feature,
                zero_std_count,
            )
        std = raw_std.replace(0.0, np.nan)
        z = (series - mean) / std
        if op == "zscore_gt":
            return (z > float(node.value)).fillna(False)
        return (z < float(node.value)).fillna(False)
    raise ValueError(f"Unsupported condition operator: {op}")


def _combined_entry_mask(merged: pd.DataFrame, entry: EntrySpec) -> pd.Series:
    masks: List[pd.Series] = []
    masks.extend(_condition_mask_legacy(merged, expr) for expr in entry.conditions)
    masks.extend(_condition_mask_node(merged, node) for node in entry.condition_nodes)
    if not masks:
        return pd.Series(True, index=merged.index, dtype=bool)
    out = masks[0]
    if entry.condition_logic == "any":
        for mask in masks[1:]:
            out = out | mask
    else:
        for mask in masks[1:]:
            out = out & mask
    return out.fillna(False)


def _rolling_quantile(series: pd.Series, window: int, q: float) -> pd.Series:
    return series.rolling(window, min_periods=1).quantile(q)


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denom = denominator.replace(0.0, np.nan)
    return (numerator / denom).replace([np.inf, -np.inf], np.nan)


def _first_overlay_param(blueprint: Blueprint, overlay_name: str, key: str, default: float) -> float:
    for overlay in blueprint.overlays:
        if overlay.name == overlay_name:
            return _to_float(overlay.params.get(key), default=default)
    return default


def _validate_signal_columns(merged: pd.DataFrame, signals: List[str], blueprint_id: str) -> None:
    cols = set(merged.columns)
    missing_by_signal: Dict[str, List[str]] = {}

    def _has_numeric_values(column: str) -> bool:
        if column not in cols:
            return False
        series = pd.to_numeric(merged[column], errors="coerce")
        return bool(series.notna().any())

    for signal in signals:
        missing: List[str] = []
        if signal in REGISTRY_SIGNAL_COLUMNS:
            if signal not in cols:
                missing.append(signal)
            if missing:
                missing_by_signal[signal] = sorted(set(missing))
            continue
        if signal in {"spread_guard_pass", "cross_venue_desync_event", "cross_venue_consensus_pass"} and not _has_numeric_values("spread_bps"):
            missing.append("spread_bps")
        if signal in {"funding_extreme_event", "funding_normalization_pass"} and not _has_numeric_values("funding_rate_scaled"):
            missing.append("funding_rate_scaled")
        if signal in {"liquidity_absence_event", "liquidity_refill_lag_event", "refill_persistence_pass", "liquidity_vacuum_event"} and not _has_numeric_values("quote_volume"):
            missing.append("quote_volume")
        if signal in {
            "forced_flow_exhaustion_event",
            "liquidity_vacuum_event",
            "breakout_confirmation",
            "range_compression_breakout_event",
        } and not _has_numeric_values("close"):
            missing.append("close")
        if signal in {"vol_aftershock_event", "vol_shock_relaxation_event", "regime_stability_pass", "range_compression_breakout_event"}:
            has_range = _has_numeric_values("range_96") or (_has_numeric_values("high_96") and _has_numeric_values("low_96")) or _has_numeric_values("range_ratio")
            if not has_range:
                missing.append("range_96 or (high_96+low_96)")
            if not _has_numeric_values("close") and "close" not in missing:
                missing.append("close")
        if missing:
            missing_by_signal[signal] = sorted(set(missing))

    if missing_by_signal:
        detail = "; ".join(f"{name}: {', '.join(cols)}" for name, cols in sorted(missing_by_signal.items()))
        raise ValueError(f"Blueprint `{blueprint_id}` missing required columns for entry signals -> {detail}")


def _build_signal_frame(merged: pd.DataFrame) -> pd.DataFrame:
    frame = merged.copy()
    timestamp = pd.to_datetime(frame.get("timestamp", pd.Series(pd.NaT, index=frame.index)), utc=True, errors="coerce")
    frame["timestamp"] = timestamp
    frame["session_hour_utc"] = timestamp.dt.hour.astype(float)

    close = pd.to_numeric(frame.get("close", pd.Series(np.nan, index=frame.index)), errors="coerce")
    frame["close"] = close

    volume = pd.to_numeric(frame.get("volume", pd.Series(np.nan, index=frame.index)), errors="coerce")
    volume_quote_fallback = (volume * close).replace([np.inf, -np.inf], np.nan)
    if "quote_volume" in frame.columns:
        quote_volume = pd.to_numeric(frame.get("quote_volume"), errors="coerce")
        quote_volume = quote_volume.where(quote_volume.notna(), volume_quote_fallback)
    else:
        quote_volume = volume_quote_fallback
    frame["quote_volume"] = quote_volume

    if "spread_bps" in frame.columns:
        spread_bps = pd.to_numeric(frame.get("spread_bps"), errors="coerce")
        if "basis_bps" in frame.columns:
            basis_spread = pd.to_numeric(frame.get("basis_bps"), errors="coerce")
            spread_bps = spread_bps.where(spread_bps.notna(), basis_spread)
    elif "basis_bps" in frame.columns:
        spread_bps = pd.to_numeric(frame.get("basis_bps"), errors="coerce")
    else:
        spread_bps = pd.Series(np.nan, index=frame.index, dtype=float)
    frame["spread_bps"] = spread_bps
    frame["spread_abs"] = spread_bps.abs()

    if "funding_rate_scaled" in frame.columns:
        funding_rate = pd.to_numeric(frame.get("funding_rate_scaled"), errors="coerce")
        if "funding_rate" in frame.columns:
            funding_fallback = pd.to_numeric(frame.get("funding_rate"), errors="coerce")
            funding_rate = funding_rate.where(funding_rate.notna(), funding_fallback)
    elif "funding_rate" in frame.columns:
        funding_rate = pd.to_numeric(frame.get("funding_rate"), errors="coerce")
    else:
        funding_rate = pd.Series(np.nan, index=frame.index, dtype=float)
    frame["funding_rate_scaled"] = funding_rate
    frame["funding_bps_abs"] = (funding_rate * 10_000.0).abs()

    ret_1 = close.pct_change().replace([np.inf, -np.inf], np.nan).fillna(0.0)
    ret_4 = close.pct_change(4).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    frame["ret_1"] = ret_1
    frame["ret_4"] = ret_4
    frame["abs_ret_1"] = ret_1.abs()
    frame["abs_ret_4"] = ret_4.abs()
    trend_96 = _safe_divide(close, close.shift(96)) - 1.0
    bull_bear_flag = np.sign(trend_96).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    frame["bull_bear_flag"] = bull_bear_flag.astype(float)

    volume_median = quote_volume.rolling(96, min_periods=1).median()
    frame["volume_ratio"] = _safe_divide(quote_volume, volume_median)

    if "range_96" in frame.columns:
        range_num = pd.to_numeric(frame.get("range_96"), errors="coerce").abs()
    else:
        high_96 = pd.to_numeric(frame.get("high_96", pd.Series(np.nan, index=frame.index)), errors="coerce")
        low_96 = pd.to_numeric(frame.get("low_96", pd.Series(np.nan, index=frame.index)), errors="coerce")
        range_num = (high_96 - low_96).abs()

    if "range_med_480" in frame.columns:
        range_den = pd.to_numeric(frame.get("range_med_480"), errors="coerce").abs()
    else:
        range_den = range_num.rolling(480, min_periods=1).median()

    range_ratio = _safe_divide(range_num, range_den)
    frame["range_ratio"] = range_ratio
    vol_mean = range_ratio.rolling(96, min_periods=96).mean()
    vol_std = range_ratio.rolling(96, min_periods=96).std().replace(0.0, np.nan)
    frame["vol_z"] = ((range_ratio - vol_mean) / vol_std).replace([np.inf, -np.inf], np.nan)
    realized_vol = ret_1.abs().rolling(96, min_periods=1).mean()
    vol_q33 = realized_vol.rolling(480, min_periods=1).quantile(1.0 / 3.0)
    vol_q66 = realized_vol.rolling(480, min_periods=1).quantile(2.0 / 3.0)
    vol_regime_code = pd.Series(1.0, index=frame.index, dtype=float)
    vol_regime_code = vol_regime_code.mask(realized_vol <= vol_q33, 0.0)
    vol_regime_code = vol_regime_code.mask(realized_vol >= vol_q66, 2.0)
    frame["vol_regime_code"] = vol_regime_code.fillna(1.0)

    frame["spread_q75"] = _rolling_quantile(frame["spread_abs"], 96, 0.75)
    frame["abs_ret_q75"] = _rolling_quantile(frame["abs_ret_1"], 96, 0.75)
    frame["abs_ret4_q90"] = _rolling_quantile(frame["abs_ret_4"], 96, 0.90)
    frame["range_ratio_q25"] = _rolling_quantile(frame["range_ratio"], 96, 0.25)

    for col in ["direction_score", "signed_edge", "forward_return_h"]:
        frame[col] = pd.to_numeric(frame.get(col, pd.Series(np.nan, index=frame.index)), errors="coerce")

    return frame


def _signal_mask(signal: str, frame: pd.DataFrame, blueprint: Blueprint) -> pd.Series:
    if signal == "event_detected":
        return pd.Series(True, index=frame.index, dtype=bool)
    if signal == "oos_validation_pass":
        return pd.Series(True, index=frame.index, dtype=bool)
    if signal in REGISTRY_SIGNAL_COLUMNS:
        if signal not in frame.columns:
            raise ValueError(f"Blueprint `{blueprint.id}` missing registry signal column: {signal}")
        return frame[signal].fillna(False).astype(bool)

    if signal == "spread_guard_pass":
        max_spread = _first_overlay_param(blueprint, "spread_guard", "max_spread_bps", default=12.0)
        return (frame["spread_abs"] <= max_spread).fillna(False)
    if signal == "cross_venue_consensus_pass":
        max_desync = _first_overlay_param(blueprint, "cross_venue_guard", "max_desync_bps", default=20.0)
        return (frame["spread_abs"] <= max_desync).fillna(False)

    if signal == "funding_normalization_pass":
        max_funding = _first_overlay_param(blueprint, "funding_guard", "max_abs_funding_bps", default=15.0)
        return (frame["funding_bps_abs"] <= max_funding).fillna(False)

    if signal == "refill_persistence_pass":
        return (frame["volume_ratio"] >= 1.0).fillna(False)
    if signal == "vacuum_refill_confirmation":
        return (frame["volume_ratio"] >= 1.0).fillna(False)

    if signal == "regime_stability_pass":
        return (frame["vol_z"].abs() <= 1.0).fillna(False)

    if signal == "breakout_confirmation":
        return (frame["abs_ret_1"] >= frame["abs_ret_q75"]).fillna(False)

    raise ValueError(f"Unknown entry signal `{signal}`")


def _signal_list_mask(frame: pd.DataFrame, signal_names: List[str], blueprint: Blueprint, signal_kind: str) -> pd.Series:
    if not signal_names:
        return pd.Series(True, index=frame.index, dtype=bool)

    unknown = sorted(
        signal
        for signal in set(signal_names)
        if signal not in KNOWN_ENTRY_SIGNALS and signal not in REGISTRY_SIGNAL_COLUMNS
    )
    if unknown:
        joined = ", ".join(unknown)
        raise ValueError(f"Blueprint `{blueprint.id}` has unknown {signal_kind} signals: {joined}")

    _validate_signal_columns(frame, signal_names, blueprint.id)

    out = pd.Series(True, index=frame.index, dtype=bool)
    for signal in signal_names:
        out = out & _signal_mask(signal=signal, frame=frame, blueprint=blueprint)
    return out.fillna(False)


def _entry_eligibility_mask(frame: pd.DataFrame, entry: EntrySpec, blueprint: Blueprint) -> pd.Series:
    condition_mask = _combined_entry_mask(frame, entry)
    trigger_mask = _signal_list_mask(frame, entry.triggers, blueprint, signal_kind="trigger")
    confirmation_mask = _signal_list_mask(frame, entry.confirmations, blueprint, signal_kind="confirmation")
    return (condition_mask & trigger_mask & confirmation_mask).fillna(False)


def _validate_overlay_columns(frame: pd.DataFrame, overlays: List[OverlaySpec], blueprint_id: str) -> None:
    cols = set(frame.columns)

    def _has_numeric_values(column: str) -> bool:
        if column not in cols:
            return False
        series = pd.to_numeric(frame[column], errors="coerce")
        return bool(series.notna().any())

    missing: Dict[str, List[str]] = {}
    for overlay in overlays:
        required: List[str] = []
        if overlay.name == "liquidity_guard":
            required = ["quote_volume"]
        elif overlay.name == "spread_guard":
            required = ["spread_bps"]
        elif overlay.name == "funding_guard":
            required = ["funding_rate_scaled"]
        elif overlay.name == "cross_venue_guard":
            required = ["spread_bps"]
        elif overlay.name in {"risk_throttle", "session_guard"}:
            required = []
        else:
            required = []
        missing_cols = [col for col in required if not _has_numeric_values(col)]
        if missing_cols:
            missing[overlay.name] = sorted(set(missing_cols))

    if missing:
        detail = "; ".join(f"{name}: {', '.join(cols_)}" for name, cols_ in sorted(missing.items()))
        raise ValueError(f"Blueprint `{blueprint_id}` missing required columns for overlays -> {detail}")


def _event_direction_bias(event_type: str) -> int:
    normalized = str(event_type).strip().lower()
    if normalized in CONTRARIAN_BIAS_EVENTS:
        return -1
    if normalized in MOMENTUM_BIAS_EVENTS:
        return 1
    return 1


def _direction_score(row: pd.Series) -> float:
    for col in ["direction_score", "signed_edge", "forward_return_h", "ret_4", "ret_1"]:
        value = _to_float(row.get(col), default=np.nan)
        if not np.isnan(value):
            return value
    return np.nan


def _entry_side(row: pd.Series, blueprint: Blueprint) -> int:
    if blueprint.direction == "long":
        return 1
    if blueprint.direction == "short":
        return -1

    score = _direction_score(row)
    if np.isnan(score) or abs(score) <= 1e-12:
        return 0

    base = 1 if score > 0.0 else -1
    return int(base * _event_direction_bias(blueprint.event_type))


def _range_proxy(row: pd.Series) -> float:
    high_96 = _to_float(row.get("high_96"), default=np.nan)
    low_96 = _to_float(row.get("low_96"), default=np.nan)
    close = _to_float(row.get("close"), default=np.nan)
    if not np.isnan(high_96) and not np.isnan(low_96) and close > 0:
        return abs(high_96 - low_96) / close
    return 0.0


def _offset(row: pd.Series, kind: str, value: float) -> float:
    close = _to_float(row.get("close"), default=np.nan)
    if close <= 0:
        return 0.0
    if kind == "percent":
        out = close * float(value)
    elif kind == "range_pct":
        out = close * _range_proxy(row) * float(value)
    else:  # atr
        atr_like = _to_float(row.get("atr_14"), default=np.nan)
        if np.isnan(atr_like) or atr_like <= 0:
            atr_like = close * _range_proxy(row)
        out = atr_like * float(value)
    return max(0.0, out)


def _stop_target_offsets(row: pd.Series, exit_spec: ExitSpec) -> Tuple[float, float]:
    stop = _offset(row, exit_spec.stop_type, float(exit_spec.stop_value))
    target = _offset(row, exit_spec.target_type, float(exit_spec.target_value))
    return stop, target


def _apply_overlay_entry_gate(overlay: OverlaySpec, row: pd.Series, side: int) -> int:
    name = overlay.name
    params = overlay.params
    if name == "risk_throttle":
        scale = _to_float(params.get("size_scale", 1.0), default=1.0)
        return 0 if scale <= 0.0 else side
    if name == "liquidity_guard":
        min_notional = _to_float(params.get("min_notional", 0.0), default=0.0)
        notional = _to_float(row.get("quote_volume"), default=0.0)
        return 0 if notional < min_notional else side
    if name == "spread_guard":
        max_spread_bps = _to_float(params.get("max_spread_bps", 12.0), default=12.0)
        spread = _to_float(row.get("spread_bps"), default=0.0)
        return 0 if spread > max_spread_bps else side
    if name == "session_guard":
        return side
    if name == "funding_guard":
        max_abs_funding_bps = _to_float(params.get("max_abs_funding_bps", 15.0), default=15.0)
        funding = abs(_to_float(row.get("funding_rate_scaled"), default=0.0) * 10_000.0)
        return 0 if funding > max_abs_funding_bps else side
    if name == "cross_venue_guard":
        max_desync_bps = _to_float(params.get("max_desync_bps", 20.0), default=20.0)
        desync = abs(_to_float(row.get("spread_bps"), default=0.0))
        return 0 if desync > max_desync_bps else side
    raise ValueError(f"Unknown overlay `{name}` in blueprint")


@dataclass
class DslInterpreterV1:
    name: str = "dsl_interpreter_v1"
    required_features: List[str] = None

    def __post_init__(self) -> None:
        if self.required_features is None:
            self.required_features = ["high_96", "low_96"]

    def generate_positions(self, bars: pd.DataFrame, features: pd.DataFrame, params: dict) -> pd.Series:
        if "dsl_blueprint" not in params or not isinstance(params.get("dsl_blueprint"), dict):
            raise ValueError("dsl_interpreter_v1 requires params.dsl_blueprint")
        blueprint = _build_blueprint(dict(params["dsl_blueprint"]))

        bars = bars.copy()
        features = features.copy()
        bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
        features["timestamp"] = pd.to_datetime(features["timestamp"], utc=True)
        feature_base = features.drop(columns=["open", "high", "low", "close"], errors="ignore")
        bar_cols = ["timestamp", "open", "high", "low", "close"]
        for col in ("volume", "quote_volume"):
            if col in bars.columns and col not in feature_base.columns:
                bar_cols.append(col)
        merged = feature_base.merge(
            bars[bar_cols],
            on="timestamp",
            how="left",
        )
        merged = merged.sort_values("timestamp").reset_index(drop=True)
        frame = _build_signal_frame(merged)
        _validate_overlay_columns(frame, blueprint.overlays, blueprint.id)
        trigger_coverage = _compute_trigger_coverage(frame, blueprint.entry.triggers)
        if int(params.get("fail_on_zero_trigger_coverage", 0)):
            if bool(trigger_coverage.get("all_zero", False)):
                raise ValueError(
                    f"Blueprint `{blueprint.id}` has all-zero trigger coverage "
                    f"for triggers={blueprint.entry.triggers}."
                )

        symbol = str(params.get("strategy_symbol", "")).strip().upper()
        allowed_symbols = {str(s).strip().upper() for s in blueprint.symbol_scope.symbols}
        if allowed_symbols and symbol and symbol not in allowed_symbols:
            out = pd.Series(0, index=frame["timestamp"], name="position", dtype=int)
            out.attrs["signal_events"] = []
            out.attrs["strategy_metadata"] = {
                "family": "dsl",
                "strategy_id": blueprint.id,
                "blueprint_id": blueprint.id,
                "event_type": blueprint.event_type,
                "trigger_coverage": trigger_coverage,
            }
            return out

        eligible_mask = _entry_eligibility_mask(frame, blueprint.entry, blueprint)
        positions: List[int] = []
        signal_events: List[Dict[str, object]] = []

        in_pos = 0
        state = "flat"
        arm_remaining = 0
        entry_idx = -1
        entry_price = np.nan
        risk_per_unit = np.nan
        best_price = np.nan
        stop_price = np.nan
        target_price = np.nan
        cooldown_until = -1

        # Conservative default: disable intrabar stop/target exits unless explicitly enabled.
        # The engine PnL model is close-to-close; intrabar exits require OHLC fill simulation to be realistic.
        allow_intrabar_exits = bool(params.get("allow_intrabar_exits", False)) if isinstance(params, dict) else False

        for idx, row in frame.iterrows():
            ts = row["timestamp"]
            close = _to_float(row.get("close"), default=np.nan)
            high = _to_float(row.get("high"), default=np.nan)
            low = _to_float(row.get("low"), default=np.nan)

            if in_pos != 0:
                held = idx - entry_idx
                inv = blueprint.exit.invalidation
                inv_col = str(inv.get("metric", "")).strip()
                inv_op = str(inv.get("operator", "")).strip()
                inv_val = _to_float(inv.get("value"), default=np.nan)
                invalidate = False
                if inv_col in frame.columns and not np.isnan(inv_val):
                    metric = _to_float(row.get(inv_col), default=np.nan)
                    if not np.isnan(metric):
                        invalidate = (
                            (inv_op == ">" and metric > inv_val)
                            or (inv_op == ">=" and metric >= inv_val)
                            or (inv_op == "<" and metric < inv_val)
                            or (inv_op == "<=" and metric <= inv_val)
                        )

                if in_pos > 0:
                    best_price = max(_to_float(best_price, close), high if not np.isnan(high) else close)
                else:
                    best_price = min(_to_float(best_price, close), low if not np.isnan(low) else close)

                if float(blueprint.exit.break_even_r) > 0 and not np.isnan(risk_per_unit) and risk_per_unit > 0:
                    if in_pos > 0:
                        reached_r = (best_price - entry_price) / risk_per_unit
                        if reached_r >= float(blueprint.exit.break_even_r):
                            stop_price = max(stop_price, entry_price)
                    else:
                        reached_r = (entry_price - best_price) / risk_per_unit
                        if reached_r >= float(blueprint.exit.break_even_r):
                            stop_price = min(stop_price, entry_price)

                if blueprint.exit.trailing_stop_type != "none" and float(blueprint.exit.trailing_stop_value) > 0:
                    trail = _offset(row, blueprint.exit.trailing_stop_type, float(blueprint.exit.trailing_stop_value))
                    if trail > 0:
                        if in_pos > 0:
                            stop_price = max(stop_price, best_price - trail)
                        else:
                            stop_price = min(stop_price, best_price + trail)

                should_exit = False
                reason = ""
                if held >= int(blueprint.exit.time_stop_bars):
                    should_exit = True
                    reason = "time_stop"
                elif invalidate:
                    should_exit = True
                    reason = "invalidation"
                elif allow_intrabar_exits and in_pos > 0 and not np.isnan(low) and low <= stop_price:
                    should_exit = True
                    reason = "stop"
                elif allow_intrabar_exits and in_pos < 0 and not np.isnan(high) and high >= stop_price:
                    should_exit = True
                    reason = "stop"
                elif allow_intrabar_exits and in_pos > 0 and not np.isnan(high) and high >= target_price:
                    should_exit = True
                    reason = "target"
                elif allow_intrabar_exits and in_pos < 0 and not np.isnan(low) and low <= target_price:
                    should_exit = True
                    reason = "target"

                if should_exit:
                    in_pos = 0
                    state = "cooldown"
                    cooldown_until = idx + max(int(blueprint.entry.cooldown_bars), int(blueprint.entry.reentry_lockout_bars))
                    signal_events.append({"timestamp": ts.isoformat(), "event": "exit", "reason": reason})

            if in_pos == 0:
                if state == "cooldown" and idx < cooldown_until:
                    positions.append(0)
                    continue
                if state == "cooldown" and idx >= cooldown_until:
                    state = "flat"
                is_eligible = bool(eligible_mask.iat[idx])
                if state == "flat":
                    if is_eligible:
                        state = "armed"
                        # Enforce at least one-bar decision lag: signals computed on bar t are tradable no earlier than t+1.
                        arm_remaining = max(1, int(blueprint.entry.delay_bars), int(blueprint.entry.arm_bars))
                    else:
                        positions.append(0)
                        continue
                if state == "armed":
                    if not is_eligible:
                        state = "flat"
                        positions.append(0)
                        continue
                    if arm_remaining > 0:
                        arm_remaining -= 1
                        positions.append(0)
                        continue

                    side = _entry_side(row, blueprint)
                    for overlay in blueprint.overlays:
                        side = _apply_overlay_entry_gate(overlay=overlay, row=row, side=side)
                        if side == 0:
                            break
                    if side == 0 or close <= 0:
                        state = "flat"
                        positions.append(0)
                        continue

                    stop_offset, target_offset = _stop_target_offsets(row=row, exit_spec=blueprint.exit)
                    if stop_offset <= 0 or target_offset <= 0:
                        state = "flat"
                        positions.append(0)
                        continue

                    in_pos = side
                    state = "in_position"
                    entry_idx = idx
                    entry_price = close
                    risk_per_unit = stop_offset
                    best_price = close
                    if side > 0:
                        stop_price = close - stop_offset
                        target_price = close + target_offset
                    else:
                        stop_price = close + stop_offset
                        target_price = close - target_offset
                    signal_events.append({"timestamp": ts.isoformat(), "event": "entry", "reason": "blueprint_entry"})
            positions.append(int(in_pos))

        out = pd.Series(positions, index=frame["timestamp"], name="position", dtype=int)
        out.attrs["signal_events"] = signal_events
        out.attrs["strategy_metadata"] = {
            "family": "dsl",
            "strategy_id": blueprint.id,
            "blueprint_id": blueprint.id,
            "event_type": blueprint.event_type,
            "candidate_id": blueprint.candidate_id,
            "trigger_coverage": trigger_coverage,
        }
        return out

def _compute_trigger_coverage(frame, triggers):
    cols = list(getattr(frame, "columns", []))
    out = {"triggers": {}, "missing": [], "resolved": {}}
    for trig in (triggers or []):
        resolved = resolve_trigger_column(str(trig), cols)
        out["resolved"][str(trig)] = resolved
        if resolved is None or resolved not in cols:
            out["missing"].append(str(trig))
            out["triggers"][str(trig)] = {"resolved": None, "true_count": 0, "true_rate": 0.0}
            continue
        s = frame[resolved]
        # tolerate 0/1 ints or bools
        true_count = int((s.astype(bool)).sum())
        true_rate = float((s.astype(bool)).mean()) if len(s) else 0.0
        out["triggers"][str(trig)] = {"resolved": resolved, "true_count": true_count, "true_rate": true_rate}
    out["all_zero"] = all(v.get("true_count", 0) == 0 for v in out["triggers"].values()) if out["triggers"] else True
    return out
