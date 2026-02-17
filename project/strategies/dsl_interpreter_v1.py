from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

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


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        out = float(value)
        if np.isnan(out):
            return default
        return out
    except (TypeError, ValueError):
        return default


def _build_blueprint(raw: Dict[str, object]) -> Blueprint:
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
        ),
    )
    bp.validate()
    return bp


def _condition_mask_legacy(merged: pd.DataFrame, expr: str) -> pd.Series:
    expression = str(expr).strip()
    if not expression or expression == "all":
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
        std = series.rolling(window, min_periods=window).std().replace(0.0, np.nan)
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
        merged = features.drop(columns=["open", "high", "low", "close"], errors="ignore").merge(
            bars[["timestamp", "open", "high", "low", "close"]],
            on="timestamp",
            how="left",
        )
        merged = merged.sort_values("timestamp").reset_index(drop=True)

        symbol = str(params.get("strategy_symbol", "")).strip().upper()
        allowed_symbols = {str(s).strip().upper() for s in blueprint.symbol_scope.symbols}
        if allowed_symbols and symbol and symbol not in allowed_symbols:
            out = pd.Series(0, index=merged["timestamp"], name="position", dtype=int)
            out.attrs["signal_events"] = []
            out.attrs["strategy_metadata"] = {"family": "dsl", "strategy_id": blueprint.id, "blueprint_id": blueprint.id, "event_type": blueprint.event_type}
            return out

        eligible_mask = _combined_entry_mask(merged, blueprint.entry)
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

        for idx, row in merged.iterrows():
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
                if inv_col in merged.columns and not np.isnan(inv_val):
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
                elif in_pos > 0 and not np.isnan(low) and low <= stop_price:
                    should_exit = True
                    reason = "stop"
                elif in_pos < 0 and not np.isnan(high) and high >= stop_price:
                    should_exit = True
                    reason = "stop"
                elif in_pos > 0 and not np.isnan(high) and high >= target_price:
                    should_exit = True
                    reason = "target"
                elif in_pos < 0 and not np.isnan(low) and low <= target_price:
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
                        arm_remaining = max(int(blueprint.entry.delay_bars), int(blueprint.entry.arm_bars))
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

                    direction = blueprint.direction
                    side = 1
                    if direction == "short":
                        side = -1
                    elif direction in {"both", "conditional"}:
                        hint = _to_float(row.get("forward_abs_return_h"), default=1.0)
                        side = -1 if hint < 0 else 1

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

        out = pd.Series(positions, index=merged["timestamp"], name="position", dtype=int)
        out.attrs["signal_events"] = signal_events
        out.attrs["strategy_metadata"] = {
            "family": "dsl",
            "strategy_id": blueprint.id,
            "blueprint_id": blueprint.id,
            "event_type": blueprint.event_type,
            "candidate_id": blueprint.candidate_id,
        }
        return out
