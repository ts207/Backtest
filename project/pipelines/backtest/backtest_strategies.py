from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.execution_costs import resolve_execution_costs
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.validation import strategy_family_allowed_keys, validate_strategy_family_params
from engine.pnl import compute_pnl
from engine.runner import BARS_PER_YEAR, run_engine
from strategies.overlay_registry import apply_overlay
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

INITIAL_EQUITY = 1_000_000.0
BARS_PER_YEAR_5M = 365 * 24 * 12

STRATEGY_EXECUTION_FAMILY = {
    "vol_compression_v1": "breakout",
    "liquidity_refill_lag_v1": "mean_reversion",
    "liquidity_absence_gate_v1": "mean_reversion",
    "forced_flow_exhaustion_v1": "mean_reversion",
    "funding_extreme_reversal_v1": "carry",
    "cross_venue_desync_v1": "spread",
    "liquidity_vacuum_v1": "mean_reversion",
    "onchain_flow_v1": "onchain",
    "dsl_interpreter_v1": "dsl",
}


def _execution_family_for_strategies(strategies: List[str]) -> str:
    families = set()
    for strategy in strategies:
        strategy_id = str(strategy).strip()
        if not strategy_id:
            continue
        if strategy_id.startswith("dsl_interpreter_v1__"):
            families.add("dsl")
            continue
        families.add(STRATEGY_EXECUTION_FAMILY.get(strategy_id, "unknown"))
    if len(families) == 1:
        return next(iter(families))
    if not families:
        return "unknown"
    return "hybrid"


DEFAULT_STRATEGY_FAMILIES: Dict[str, str] = {
    "funding_extreme_reversal_v1": "Carry",
    "liquidity_refill_lag_v1": "MeanReversion",
    "liquidity_absence_gate_v1": "MeanReversion",
    "forced_flow_exhaustion_v1": "MeanReversion",
    "liquidity_vacuum_v1": "MeanReversion",
    "cross_venue_desync_v1": "Spread",
}


def _strategy_family_for_name(strategy_name: str, config: Dict[str, object]) -> str | None:
    mapping = config.get("strategy_families", {}) if isinstance(config.get("strategy_families"), dict) else {}
    if strategy_name in mapping:
        family = str(mapping[strategy_name]).strip()
        return family or None
    return DEFAULT_STRATEGY_FAMILIES.get(strategy_name)


def _build_base_strategy_params(config: Dict[str, object], trade_day_timezone: str) -> Dict[str, object]:
    strategy_defaults = config.get("strategy_defaults", {}) if isinstance(config.get("strategy_defaults"), dict) else {}
    vc_defaults = (
        strategy_defaults.get("vol_compression_v1", {})
        if isinstance(strategy_defaults.get("vol_compression_v1"), dict)
        else {}
    )

    def _param(name: str, default: object) -> object:
        if name in config:
            return config[name]
        if name in vc_defaults:
            return vc_defaults[name]
        return default

    return {
        "trade_day_timezone": trade_day_timezone,
        "one_trade_per_day": False,
        "compression_rv_pct_max": float(_param("compression_rv_pct_max", 10.0)),
        "compression_range_ratio_max": float(_param("compression_range_ratio_max", 0.8)),
        "breakout_confirm_bars": int(_param("breakout_confirm_bars", 0)),
        "breakout_confirm_buffer_bps": float(_param("breakout_confirm_buffer_bps", 0.0)),
        "max_hold_bars": int(_param("max_hold_bars", 48)),
        "volatility_exit_rv_pct": float(_param("volatility_exit_rv_pct", 40.0)),
        "expansion_timeout_bars": int(_param("expansion_timeout_bars", 0)),
        "expansion_min_r": float(_param("expansion_min_r", 0.5)),
        "adaptive_exit_enabled": bool(_param("adaptive_exit_enabled", False)),
        "adaptive_activation_r": float(_param("adaptive_activation_r", 1.0)),
        "adaptive_trail_lookback_bars": int(_param("adaptive_trail_lookback_bars", 8)),
        "adaptive_trail_buffer_bps": float(_param("adaptive_trail_buffer_bps", 0.0)),
    }


def _build_strategy_params_by_name(
    strategies: List[str],
    config: Dict[str, object],
    trade_day_timezone: str,
    overlays: List[str],
) -> Tuple[Dict[str, object], Dict[str, Dict[str, object]]]:
    base_strategy_params = _build_base_strategy_params(config, trade_day_timezone)
    validated_family_params = validate_strategy_family_params(config)
    strategy_params_by_name: Dict[str, Dict[str, object]] = {}
    for strategy_name in strategies:
        strategy_params = dict(base_strategy_params)
        family = _strategy_family_for_name(strategy_name, config)
        if family:
            allowed_keys = strategy_family_allowed_keys(family)
            family_params = validated_family_params.get(family, {})
            strategy_params.update({key: value for key, value in family_params.items() if key in allowed_keys})
            strategy_params["strategy_family"] = family
        for overlay_name in overlays:
            strategy_params = apply_overlay(overlay_name, strategy_params)
        strategy_params_by_name[strategy_name] = strategy_params
    return base_strategy_params, strategy_params_by_name


def _sanitize_id(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", str(value).strip().lower()).strip("_")


def _blueprint_from_dict(raw: Dict[str, object]) -> Blueprint:
    scope = raw.get("symbol_scope", {})
    entry = raw.get("entry", {})
    exit_spec = raw.get("exit", {})
    sizing = raw.get("sizing", {})
    eval_spec = raw.get("evaluation", {})
    lineage = raw.get("lineage", {})
    overlays = raw.get("overlays", [])
    if not isinstance(scope, dict) or not isinstance(entry, dict) or not isinstance(exit_spec, dict):
        raise ValueError("Blueprint is missing required nested objects")
    overlay_rows: List[OverlaySpec] = []
    condition_nodes: List[ConditionNodeSpec] = []
    if isinstance(overlays, list):
        for row in overlays:
            if not isinstance(row, dict):
                raise ValueError("Blueprint overlay entry must be an object")
            overlay_rows.append(OverlaySpec(name=str(row.get("name", "")), params=dict(row.get("params", {}))))
    raw_nodes = entry.get("condition_nodes", [])
    if isinstance(raw_nodes, list):
        for row in raw_nodes:
            if not isinstance(row, dict):
                raise ValueError("Blueprint entry.condition_nodes[] entry must be an object")
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
            promotion_track=str(lineage.get("promotion_track", "standard")),
        ),
    )
    bp.validate()
    return bp


def _blueprint_position_scale(base_weight: float, bp: Blueprint) -> float:
    caps = [
        float(base_weight),
        float(bp.sizing.max_position_scale),
        float(bp.sizing.portfolio_risk_budget),
        float(bp.sizing.symbol_risk_budget),
    ]
    scale = min(caps)
    return float(max(0.0, scale))


def _load_blueprints(path: Path) -> List[Blueprint]:
    if not path.exists():
        raise ValueError(f"Blueprint file not found: {path}")
    text = path.read_text(encoding="utf-8")
    blueprints: List[Blueprint] = []
    if path.suffix.lower() == ".json":
        payload = json.loads(text)
        if not isinstance(payload, list):
            raise ValueError("Blueprint JSON must be an array")
        for idx, row in enumerate(payload, start=1):
            if not isinstance(row, dict):
                raise ValueError(f"Invalid blueprint at index {idx}: expected object")
            try:
                blueprints.append(_blueprint_from_dict(row))
            except Exception as exc:
                bp_id = row.get("id", "<unknown>")
                raise ValueError(f"Invalid blueprint at index {idx} (id={bp_id}): {exc}") from exc
    else:
        for line_no, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except Exception as exc:
                raise ValueError(f"Invalid blueprint at line {line_no}: invalid JSON ({exc})") from exc
            if not isinstance(row, dict):
                raise ValueError(f"Invalid blueprint at line {line_no}: expected object")
            try:
                blueprints.append(_blueprint_from_dict(row))
            except Exception as exc:
                bp_id = row.get("id", "<unknown>")
                raise ValueError(f"Invalid blueprint at line {line_no} (id={bp_id}): {exc}") from exc
    if not blueprints:
        raise ValueError(f"No blueprint rows found in {path}")
    return blueprints


def _filter_blueprints(
    blueprints: List[Blueprint],
    event_type: str,
    top_k: int,
    cli_symbols: List[str],
) -> List[Blueprint]:
    # B1: Hard fail if ANY input blueprint has promotion_track != 'standard'.
    # Checked before filtering so nothing slips through on event_type/top_k path.
    fallback_inputs = [bp for bp in blueprints if bp.lineage.promotion_track != "standard"]
    if fallback_inputs:
        bad_ids = ", ".join(bp.id for bp in fallback_inputs[:5])
        extra = f" (+{len(fallback_inputs) - 5} more)" if len(fallback_inputs) > 5 else ""
        raise ValueError(
            f"EVALUATION GUARD [INV_NO_FALLBACK_IN_MEASUREMENT]: "
            f"{len(fallback_inputs)} blueprint(s) have promotion_track != 'standard': {bad_ids}{extra}. "
            "Fallback-only blueprints bypass BH-FDR and cannot be used in evaluation. "
            "Remediation: set spec/gates.yaml gate_v1_fallback.promotion_eligible_regardless_of_fdr: false."
        )
    filtered = blueprints
    if event_type != "all":
        filtered = [bp for bp in filtered if bp.event_type == event_type]
    selected: List[Blueprint] = []
    cli_set = {s.upper() for s in cli_symbols}
    for bp in filtered:
        scope_symbols = [str(s).strip().upper() for s in bp.symbol_scope.symbols if str(s).strip()]
        if cli_set:
            scope_symbols = [s for s in scope_symbols if s in cli_set]
        if not scope_symbols:
            continue
        new_bp = Blueprint(
            id=bp.id,
            run_id=bp.run_id,
            event_type=bp.event_type,
            candidate_id=bp.candidate_id,
            symbol_scope=SymbolScopeSpec(
                mode=bp.symbol_scope.mode,
                symbols=scope_symbols,
                candidate_symbol=bp.symbol_scope.candidate_symbol,
            ),
            direction=bp.direction,
            entry=bp.entry,
            exit=bp.exit,
            sizing=bp.sizing,
            overlays=bp.overlays,
            evaluation=bp.evaluation,
            lineage=bp.lineage,
        )
        selected.append(new_bp)
        if len(selected) >= max(1, int(top_k)):
            break
    return selected



def _compute_drawdown(equity_curve: pd.Series) -> float:
    peak = equity_curve.cummax()
    drawdown = (equity_curve - peak) / peak
    return float(drawdown.min()) if not drawdown.empty else 0.0


def _annualized_sharpe(returns: pd.Series, periods_per_year: int = BARS_PER_YEAR_5M) -> float:
    returns = returns.dropna()
    if returns.empty:
        return 0.0
    std = float(returns.std())
    if std == 0.0:
        return 0.0
    return float((returns.mean() / std) * np.sqrt(periods_per_year))


def _empty_trades_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "symbol",
            "direction",
            "entry_reason",
            "entry_time",
            "exit_time",
            "entry_price",
            "exit_price",
            "qty",
            "pnl",
            "r_multiple",
            "exit_reason",
            "stop_price",
            "target_price",
        ]
    )


def _extract_trades(frame: pd.DataFrame, max_hold_bars: int = 48) -> pd.DataFrame:
    if frame.empty:
        return _empty_trades_frame()
    if "close" not in frame.columns:
        logging.warning("Strategy frame missing close prices; skipping trade extraction.")
        return _empty_trades_frame()

    frame = frame.sort_values("timestamp").reset_index(drop=True)
    pos = frame["pos"].fillna(0).astype(int)
    pnl = frame["pnl"].fillna(0.0).astype(float)
    close = frame["close"].astype(float)
    high = frame["high"].astype(float) if "high" in frame.columns else pd.Series([float("nan")] * len(frame))
    low = frame["low"].astype(float) if "low" in frame.columns else pd.Series([float("nan")] * len(frame))
    high_96 = frame["high_96"].astype(float) if "high_96" in frame.columns else pd.Series([float("nan")] * len(frame))
    low_96 = frame["low_96"].astype(float) if "low_96" in frame.columns else pd.Series([float("nan")] * len(frame))
    ts = frame["timestamp"]

    trades: List[Dict[str, object]] = []
    current_pos = 0
    entry_idx: int | None = None
    entry_time: object | None = None
    entry_price: float | None = None
    direction: str | None = None
    stop_price: float | None = None
    target_price: float | None = None
    risk_return: float | None = None
    entry_reason: str = ""

    for i in range(len(frame)):
        pos_i = int(pos.iat[i])
        if current_pos == 0:
            if pos_i != 0:
                current_pos = pos_i
                entry_idx = i
                entry_time = ts.iat[i]
                entry_price = float(close.iat[i]) if pd.notna(close.iat[i]) else float("nan")
                direction = "long" if pos_i > 0 else "short"
                stop_price = float(low_96.iat[i]) if direction == "long" else float(high_96.iat[i])
                entry_reason = str(frame["entry_reason"].iat[i]) if "entry_reason" in frame.columns else ""
                if pd.notna(entry_price) and pd.notna(stop_price) and entry_price > 0:
                    risk_per_unit = entry_price - stop_price if direction == "long" else stop_price - entry_price
                    if risk_per_unit > 0:
                        target_price = (
                            entry_price + 2 * risk_per_unit if direction == "long" else entry_price - 2 * risk_per_unit
                        )
                        risk_return = risk_per_unit / entry_price
                    else:
                        target_price = float("nan")
                        risk_return = None
                else:
                    stop_price = float("nan")
                    target_price = float("nan")
                    risk_return = None
            continue

        if pos_i == current_pos:
            continue

        exit_idx = i
        exit_time = ts.iat[i]
        exit_price = float(close.iat[i]) if pd.notna(close.iat[i]) else float("nan")
        trade_pnl = float(pnl.iloc[entry_idx : exit_idx + 1].sum()) if entry_idx is not None else 0.0
        r_multiple = trade_pnl / risk_return if risk_return and risk_return > 0 else 0.0
        exit_reason = "position_flip" if pos_i != 0 else "position_exit"
        signaled_exit_reason = str(frame["exit_signal_reason"].iat[i]) if "exit_signal_reason" in frame.columns else ""
        if signaled_exit_reason:
            exit_reason = signaled_exit_reason
        bars_held = int(exit_idx - entry_idx) if entry_idx is not None else 0
        if (not signaled_exit_reason) and pd.notna(stop_price) and pd.notna(target_price):
            high_exit = float(high.iat[exit_idx]) if pd.notna(high.iat[exit_idx]) else float("nan")
            low_exit = float(low.iat[exit_idx]) if pd.notna(low.iat[exit_idx]) else float("nan")
            if direction == "long":
                if pd.notna(low_exit) and low_exit <= stop_price:
                    exit_reason = "stop"
                elif pd.notna(high_exit) and high_exit >= target_price:
                    exit_reason = "target"
            elif direction == "short":
                if pd.notna(high_exit) and high_exit >= stop_price:
                    exit_reason = "stop"
                elif pd.notna(low_exit) and low_exit <= target_price:
                    exit_reason = "target"
        if exit_reason == "position_exit" and bars_held >= int(max_hold_bars):
            exit_reason = "time"
        trades.append(
            {
                "symbol": frame["symbol"].iat[i],
                "direction": direction,
                "entry_reason": entry_reason,
                "entry_time": entry_time,
                "exit_time": exit_time,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "qty": 1.0,
                "pnl": trade_pnl,
                "r_multiple": r_multiple,
                "exit_reason": exit_reason,
                "stop_price": stop_price,
                "target_price": target_price,
            }
        )

        if pos_i == 0:
            current_pos = 0
            entry_idx = None
            entry_time = None
            entry_price = None
            direction = None
        else:
            current_pos = pos_i
            entry_idx = i
            entry_time = ts.iat[i]
            entry_price = float(close.iat[i]) if pd.notna(close.iat[i]) else float("nan")
            direction = "long" if pos_i > 0 else "short"
            stop_price = float(low_96.iat[i]) if direction == "long" else float(high_96.iat[i])
            entry_reason = str(frame["entry_reason"].iat[i]) if "entry_reason" in frame.columns else ""
            if pd.notna(entry_price) and pd.notna(stop_price) and entry_price > 0:
                risk_per_unit = entry_price - stop_price if direction == "long" else stop_price - entry_price
                if risk_per_unit > 0:
                    target_price = (
                        entry_price + 2 * risk_per_unit if direction == "long" else entry_price - 2 * risk_per_unit
                    )
                    risk_return = risk_per_unit / entry_price
                else:
                    target_price = float("nan")
                    risk_return = None
            else:
                stop_price = float("nan")
                target_price = float("nan")
                risk_return = None

    if current_pos != 0 and entry_idx is not None:
        exit_idx = len(frame) - 1
        exit_time = ts.iat[exit_idx]
        exit_price = float(close.iat[exit_idx]) if pd.notna(close.iat[exit_idx]) else float("nan")
        trade_pnl = float(pnl.iloc[entry_idx : exit_idx + 1].sum())
        r_multiple = trade_pnl / risk_return if risk_return and risk_return > 0 else 0.0
        trades.append(
            {
                "symbol": frame["symbol"].iat[exit_idx],
                "direction": direction,
                "entry_reason": entry_reason,
                "entry_time": entry_time,
                "exit_time": exit_time,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "qty": 1.0,
                "pnl": trade_pnl,
                "r_multiple": r_multiple,
                "exit_reason": "eod",
                "stop_price": stop_price,
                "target_price": target_price,
            }
        )

    if not trades:
        return _empty_trades_frame()
    return pd.DataFrame(trades, columns=_empty_trades_frame().columns)


def _sha256_text(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_revision(project_root: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(project_root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def _build_reproducibility_metadata(
    run_id: str,
    config: Dict[str, object],
    params: Dict[str, object],
    data_root: Path = DATA_ROOT,
    project_root: Path = PROJECT_ROOT,
) -> Dict[str, object]:
    stage_files = [
        "build_cleaned_5m.json",
        "build_features_v1.json",
        "build_context_features.json",
        "build_market_context.json",
    ]
    snapshots: Dict[str, str] = {}
    for stage_file in stage_files:
        path = data_root / "runs" / run_id / stage_file
        if path.exists():
            snapshots[stage_file.removesuffix(".json")] = _file_sha256(path)

    config_payload = json.dumps(config, sort_keys=True, default=str)
    params_payload = json.dumps(params, sort_keys=True, default=str)
    config_digest = _sha256_text(f"{config_payload}\n{params_payload}")
    return {
        "config_digest": config_digest,
        "code_revision": _git_revision(project_root),
        "data_snapshot_ids": snapshots,
    }


def _cost_components_from_frame(
    frame: pd.DataFrame,
    fee_bps: float,
    slippage_bps: float,
    impact_bps: float = 0.0,
) -> Dict[str, float]:
    if frame.empty:
        return {
            "gross_alpha": 0.0,
            "fees": 0.0,
            "slippage": 0.0,
            "impact": 0.0,
            "dynamic_slippage": 0.0,
            "dynamic_impact": 0.0,
            "effective_avg_cost_bps": 0.0,
            "funding_pnl": 0.0,
            "borrow_cost": 0.0,
            "net_alpha": 0.0,
            "turnover_units": 0.0,
        }
    if "position_scale" in frame.columns:
        position_scale = pd.to_numeric(frame["position_scale"], errors="coerce").fillna(1.0)
    else:
        position_scale = pd.Series(1.0, index=frame.index, dtype=float)
    pos = pd.to_numeric(frame["pos"], errors="coerce").fillna(0.0) * position_scale
    scaled_ret = pd.to_numeric(frame["ret"], errors="coerce").fillna(0.0)
    # Engine frames already store scaled returns; recover raw returns so gross/cost decomposition
    # is computed once against effective (scaled) position.
    safe_scale = position_scale.where(position_scale.abs() > 1e-12, np.nan)
    ret = (scaled_ret / safe_scale).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    prior_pos = pos.shift(1).fillna(0.0)
    turnover = (pos - prior_pos).abs()
    gross = float((prior_pos * ret).sum())
    fee_cost = float(turnover.sum() * (float(fee_bps) / 10_000.0))
    slippage_cost = float(turnover.sum() * (float(slippage_bps) / 10_000.0))
    impact_cost = float(turnover.sum() * (float(impact_bps) / 10_000.0))
    if "dynamic_cost_bps" in frame.columns:
        dynamic_cost_bps = pd.to_numeric(frame["dynamic_cost_bps"], errors="coerce").fillna(0.0)
        dynamic_cost = float((turnover * (dynamic_cost_bps / 10_000.0)).sum())
        effective_avg_cost_bps = float(dynamic_cost_bps.mean()) if len(dynamic_cost_bps) else 0.0
    else:
        dynamic_cost = float(slippage_cost + impact_cost)
        effective_avg_cost_bps = float(fee_bps + slippage_bps + impact_bps)
    funding_series = frame["funding_pnl"] if "funding_pnl" in frame.columns else pd.Series(0.0, index=frame.index)
    borrow_series = frame["borrow_cost"] if "borrow_cost" in frame.columns else pd.Series(0.0, index=frame.index)
    funding_component = float(pd.to_numeric(funding_series, errors="coerce").fillna(0.0).sum())
    borrow_component = float(pd.to_numeric(borrow_series, errors="coerce").fillna(0.0).sum())
    net = gross - fee_cost - slippage_cost - impact_cost + funding_component - borrow_component
    return {
        "gross_alpha": gross,
        "fees": fee_cost,
        "slippage": slippage_cost,
        "impact": impact_cost,
        "dynamic_slippage": dynamic_cost,
        "dynamic_impact": max(0.0, dynamic_cost - slippage_cost),
        "effective_avg_cost_bps": effective_avg_cost_bps,
        "funding_pnl": funding_component,
        "borrow_cost": borrow_component,
        "net_alpha": net,
        "turnover_units": float(turnover.sum()),
    }


def _aggregate_cost_components(
    strategy_frames: Dict[str, pd.DataFrame],
    fee_bps: float,
    slippage_bps: float,
    impact_bps: float = 0.0,
) -> Dict[str, float]:
    totals = {
        "gross_alpha": 0.0,
        "fees": 0.0,
        "slippage": 0.0,
        "impact": 0.0,
        "dynamic_slippage": 0.0,
        "dynamic_impact": 0.0,
        "effective_avg_cost_bps": 0.0,
        "funding_pnl": 0.0,
        "borrow_cost": 0.0,
        "net_alpha": 0.0,
        "turnover_units": 0.0,
    }
    for frame in strategy_frames.values():
        comp = _cost_components_from_frame(frame, fee_bps=fee_bps, slippage_bps=slippage_bps, impact_bps=impact_bps)
        for key in totals:
            totals[key] += float(comp.get(key, 0.0))
    n = max(1, len(strategy_frames))
    totals["effective_avg_cost_bps"] = float(totals["effective_avg_cost_bps"] / n)
    return totals


def _purge_engine_artifacts(run_id: str, data_root: Path = DATA_ROOT) -> Dict[str, object]:
    engine_dir = data_root / "runs" / run_id / "engine"
    patterns = [
        "strategy_returns_*.csv",
        "strategy_trace_*.csv",
        "portfolio_returns.csv",
        "metrics.json",
    ]
    removed_files: List[str] = []
    if engine_dir.exists():
        for pattern in patterns:
            for path in sorted(engine_dir.glob(pattern)):
                if not path.is_file():
                    continue
                path.unlink()
                removed_files.append(path.name)
    return {
        "enabled": True,
        "engine_dir": str(engine_dir),
        "patterns": patterns,
        "removed_file_count": int(len(removed_files)),
        "removed_files": removed_files,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Backtest registered strategies")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", default=None, help="Optional inclusive UTC start timestamp/date for backtest window")
    parser.add_argument("--end", default=None, help="Optional inclusive UTC end timestamp/date for backtest window")
    parser.add_argument("--fees_bps", type=float, default=None)
    parser.add_argument("--slippage_bps", type=float, default=None)
    parser.add_argument("--cost_bps", type=float, default=None)
    parser.add_argument("--fail_on_zero_trigger_coverage", type=int, default=0)
    parser.add_argument("--write_trigger_coverage", type=int, default=1)
    parser.add_argument("--strategies", default=None)
    parser.add_argument("--blueprints_path", default=None)
    parser.add_argument("--blueprints_top_k", type=int, default=10)
    parser.add_argument("--blueprints_filter_event_type", default="all")
    parser.add_argument("--overlays", default="")
    parser.add_argument("--timeframe", default="5m", help="Bar timeframe for data loading (e.g. '5m', '15m')")
    parser.add_argument("--force", type=int, default=0)
    parser.add_argument("--clean_engine_artifacts", type=int, default=1)
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    run_id = args.run_id
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    resolved_costs = resolve_execution_costs(
        project_root=PROJECT_ROOT,
        config_paths=args.config,
        fees_bps=args.fees_bps,
        slippage_bps=args.slippage_bps,
        cost_bps=args.cost_bps,
    )
    config = resolved_costs.config
    fee_bps = float(resolved_costs.fee_bps_per_side)
    slippage_bps = float(resolved_costs.slippage_bps_per_fill)
    trade_day_timezone = str(config.get("trade_day_timezone", "UTC"))
    cost_bps = float(resolved_costs.cost_bps)
    strategy_mode = bool(args.strategies and str(args.strategies).strip())
    blueprint_mode = bool(args.blueprints_path and str(args.blueprints_path).strip())
    if strategy_mode and blueprint_mode:
        print("Invalid arguments: --strategies and --blueprints_path are mutually exclusive.", file=sys.stderr)
        return 1
    if not strategy_mode and not blueprint_mode:
        print("Provide either --strategies or --blueprints_path.", file=sys.stderr)
        return 1

    overlays = [o.strip() for o in str(args.overlays).split(",") if o.strip()]
    strategies: List[str] = []
    blueprint_ids: List[str] = []
    selected_blueprints: List[Blueprint] = []
    blueprint_source_path = str(args.blueprints_path or "")
    if strategy_mode:
        strategies = [s.strip() for s in str(args.strategies).split(",") if s.strip()]
        if not strategies:
            print("No strategies provided. Pass --strategies with at least one strategy id.", file=sys.stderr)
            return 1
    params = {
        "fee_bps_per_side": fee_bps,
        "slippage_bps_per_fill": slippage_bps,
        "trade_day_timezone": trade_day_timezone,
        "start": str(args.start) if args.start is not None else "",
        "end": str(args.end) if args.end is not None else "",
        "symbols": symbols,
        "force": int(args.force),
        "clean_engine_artifacts": int(args.clean_engine_artifacts),
        "cost_bps": cost_bps,
        "strategies": strategies,
        "overlays": overlays,
        "blueprints_path": blueprint_source_path if blueprint_mode else "",
        "blueprints_top_k": int(args.blueprints_top_k),
        "blueprints_filter_event_type": str(args.blueprints_filter_event_type),
        "execution_mode": "blueprint" if blueprint_mode else "strategy",
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("backtest_strategies", run_id, params, inputs, outputs)
    stats: Dict[str, object] = {"symbols": {}}

    try:
        start_ts = pd.to_datetime(args.start, utc=True) if args.start else None
        end_ts = pd.to_datetime(args.end, utc=True) if args.end else None
        if start_ts is not None and end_ts is not None and start_ts > end_ts:
            raise ValueError("--start must be <= --end")

        if blueprint_mode:
            raw_blueprints = _load_blueprints(Path(str(args.blueprints_path)))
            selected_blueprints = _filter_blueprints(
                blueprints=raw_blueprints,
                event_type=str(args.blueprints_filter_event_type or "all"),
                top_k=int(args.blueprints_top_k),
                cli_symbols=symbols,
            )
            if not selected_blueprints:
                raise ValueError("No blueprints selected after applying filters/top-k and symbol intersection.")
            # B1: Hard fail on fallback blueprints — they bypass BH-FDR and are banned from
            # evaluation artifacts. [INV_NO_FALLBACK_IN_MEASUREMENT]
            fallback_bps = [bp for bp in selected_blueprints if bp.lineage.promotion_track != "standard"]
            if fallback_bps:
                bad_ids = ", ".join(bp.id for bp in fallback_bps)
                raise ValueError(
                    f"EVALUATION GUARD [INV_NO_FALLBACK_IN_MEASUREMENT]: "
                    f"{len(fallback_bps)} blueprint(s) have promotion_track != 'standard': {bad_ids}. "
                    "Fallback-only blueprints bypass BH-FDR and cannot be used in evaluation. "
                    "Remediation: set spec/gates.yaml gate_v1_fallback.promotion_eligible_regardless_of_fdr: false "
                    "and re-run discovery to produce standard-track survivors only."
                )
            for bp in selected_blueprints:
                strategy_id = f"dsl_interpreter_v1__{_sanitize_id(bp.id)}"
                strategies.append(strategy_id)
                blueprint_ids.append(bp.id)
            params["strategies"] = list(strategies)

        execution_family = 'dsl' if blueprint_mode else _execution_family_for_strategies(strategies)
        trades_dir = DATA_ROOT / 'lake' / 'trades' / 'backtests' / str(execution_family) / run_id
        trades_dir.mkdir(parents=True, exist_ok=True)

        if not args.force and (trades_dir / "metrics.json").exists():
            finalize_manifest(manifest, "success", stats={"skipped": True})
            return 0

        if int(args.force) and bool(int(args.clean_engine_artifacts)):
            stats["engine_artifact_cleanup"] = _purge_engine_artifacts(run_id=run_id, data_root=DATA_ROOT)
        else:
            stats["engine_artifact_cleanup"] = {
                "enabled": False,
                "engine_dir": str(DATA_ROOT / "runs" / run_id / "engine"),
                "patterns": [
                    "strategy_returns_*.csv",
                    "strategy_trace_*.csv",
                    "portfolio_returns.csv",
                    "metrics.json",
                ],
                "removed_file_count": 0,
                "removed_files": [],
            }

        if strategy_mode:
            base_strategy_params, strategy_params_by_name = _build_strategy_params_by_name(
                strategies=strategies,
                config=config,
                trade_day_timezone=trade_day_timezone,
                overlays=overlays,
            )
            base_strategy_params["execution_model"] = dict(resolved_costs.execution_model)
            base_strategy_params["fail_on_zero_trigger_coverage"] = int(args.fail_on_zero_trigger_coverage)
            for strategy_name in list(strategy_params_by_name.keys()):
                strategy_params_by_name[strategy_name] = {
                    **strategy_params_by_name[strategy_name],
                    "execution_model": dict(resolved_costs.execution_model),
                }
        else:
            base_strategy_params = _build_base_strategy_params(config, trade_day_timezone)
            base_strategy_params["execution_model"] = dict(resolved_costs.execution_model)
            strategy_params_by_name = {}
            bp_map = {f"dsl_interpreter_v1__{_sanitize_id(bp.id)}": bp for bp in selected_blueprints}
            strategy_weight = 1.0 / float(len(selected_blueprints))
            for strategy_name in strategies:
                bp = bp_map[strategy_name]
                strategy_params = dict(base_strategy_params)
                strategy_params["position_scale"] = _blueprint_position_scale(base_weight=strategy_weight, bp=bp)
                strategy_params["dsl_blueprint"] = bp.to_dict()
                strategy_params["strategy_family"] = "DSL"
                strategy_params_by_name[strategy_name] = strategy_params

        engine_results = run_engine(
            run_id=run_id,
            symbols=symbols,
            strategies=strategies,
            params=base_strategy_params,
            cost_bps=cost_bps,
            data_root=DATA_ROOT,
            params_by_strategy=strategy_params_by_name,
            start_ts=start_ts,
            end_ts=end_ts,
            timeframe=args.timeframe,
        )

        for strategy_name in strategies:
            strategy_path = engine_results["engine_dir"] / f"strategy_returns_{strategy_name}.csv"
            strategy_rows = len(engine_results["strategy_frames"].get(strategy_name, pd.DataFrame()))
            outputs.append({"path": str(strategy_path), "rows": strategy_rows, "start_ts": None, "end_ts": None})
            trace_path = engine_results["engine_dir"] / f"strategy_trace_{strategy_name}.csv"
            if trace_path.exists():
                try:
                    trace_rows = len(pd.read_csv(trace_path))
                except Exception:
                    trace_rows = 0
                outputs.append({"path": str(trace_path), "rows": trace_rows, "start_ts": None, "end_ts": None})

        portfolio = engine_results["portfolio"]
        portfolio_path = engine_results["engine_dir"] / "portfolio_returns.csv"
        outputs.append({"path": str(portfolio_path), "rows": len(portfolio), "start_ts": None, "end_ts": None})

        if len(strategies) > 1:
            logging.warning("Multiple strategies detected; trades will be combined without strategy attribution.")

        trades_by_symbol: Dict[str, List[pd.DataFrame]] = {symbol: [] for symbol in symbols}
        for strategy_name, frame in engine_results["strategy_frames"].items():
            for symbol, symbol_frame in frame.groupby("symbol", sort=True):
                trades_by_symbol.setdefault(symbol, []).append(
                    _extract_trades(
                        symbol_frame,
                        max_hold_bars=int(strategy_params_by_name.get(strategy_name, {}).get("max_hold_bars", 48)),
                    )
                )

        all_symbol_trades: List[pd.DataFrame] = []
        symbol_entry_counts: Dict[str, int] = {}
        for symbol in symbols:
            trades_frames = [frame for frame in trades_by_symbol.get(symbol, []) if not frame.empty]
            symbol_trades = pd.concat(trades_frames, ignore_index=True) if trades_frames else _empty_trades_frame()
            symbol_entry_counts[symbol] = int(len(symbol_trades))
            trades_path = trades_dir / f"trades_{symbol}.csv"
            symbol_trades.to_csv(trades_path, index=False)
            if not symbol_trades.empty:
                all_symbol_trades.append(symbol_trades)
            outputs.append({"path": str(trades_path), "rows": len(symbol_trades), "start_ts": None, "end_ts": None})

        all_trades = pd.concat(all_symbol_trades, ignore_index=True) if all_symbol_trades else _empty_trades_frame()
        total_trades = int(len(all_trades))
        if total_trades:
            win_rate = float((all_trades["pnl"] > 0).mean())
            avg_r = float(all_trades["r_multiple"].mean())
        else:
            win_rate = 0.0
            avg_r = 0.0

        metrics_path = trades_dir / "metrics.json"
        if portfolio.empty:
            ending_equity = INITIAL_EQUITY
            max_drawdown = 0.0
            sharpe_annualized = 0.0
        else:
            cumulative_return = portfolio["portfolio_pnl"].cumsum()
            equity_series = INITIAL_EQUITY * (1.0 + cumulative_return)
            ending_equity = float(equity_series.iloc[-1])
            max_drawdown = _compute_drawdown(equity_series)
            sharpe_annualized = _annualized_sharpe(
                portfolio["portfolio_pnl"],
                periods_per_year=BARS_PER_YEAR.get(args.timeframe, BARS_PER_YEAR_5M),
            )
        metrics_payload = {
            "total_trades": total_trades,
            "win_rate": win_rate,
            "avg_r": avg_r,
            "max_drawdown": max_drawdown,
            "ending_equity": ending_equity,
            "sharpe_annualized": sharpe_annualized,
            "metadata": {
                "strategy_ids": strategies,
                "execution_family": _execution_family_for_strategies(strategies),
                "execution_mode": "blueprint" if blueprint_mode else "strategy",
                "blueprint_ids": blueprint_ids,
                "blueprint_source_path": blueprint_source_path if blueprint_mode else "",
                "blueprint_filter_event_type": str(args.blueprints_filter_event_type) if blueprint_mode else "",
                "blueprint_top_k": int(args.blueprints_top_k) if blueprint_mode else 0,
            },
            "cost_decomposition": _aggregate_cost_components(
                engine_results["strategy_frames"],
                fee_bps=fee_bps,
                slippage_bps=slippage_bps,
                impact_bps=0.0,
            ),
            "reproducibility": _build_reproducibility_metadata(
                run_id=run_id,
                config=config,
                params=params,
                data_root=DATA_ROOT,
                project_root=PROJECT_ROOT,
            ),
        }
        metrics_path.write_text(json.dumps(metrics_payload, indent=2, sort_keys=True), encoding="utf-8")
        outputs.append({"path": str(metrics_path), "rows": 1, "start_ts": None, "end_ts": None})

        if portfolio.empty:
            equity_curve = pd.DataFrame(columns=["timestamp", "equity"])
        else:
            equity_curve = pd.DataFrame(
                {
                    "timestamp": portfolio["timestamp"],
                    "equity": INITIAL_EQUITY * (1.0 + portfolio["portfolio_pnl"].cumsum()),
                }
            )
        equity_curve_path = trades_dir / "equity_curve.csv"
        equity_curve.to_csv(equity_curve_path, index=False)
        outputs.append({"path": str(equity_curve_path), "rows": len(equity_curve), "start_ts": None, "end_ts": None})

        fee_scenarios = [0, 2, 5, 10]
        if cost_bps not in fee_scenarios:
            fee_scenarios.insert(0, cost_bps)

        fee_sensitivity: Dict[str, Dict[str, object]] = {}
        for scenario_cost in fee_scenarios:
            scenario_pnl_frames: List[pd.Series] = []
            scenario_trade_frames: List[pd.DataFrame] = []
            entry_count = 0
            for _, frame in engine_results["strategy_frames"].items():
                for _, symbol_frame in frame.groupby("symbol", sort=True):
                    indexed = symbol_frame.set_index("timestamp")
                    if "position_scale" in indexed.columns:
                        scale_series = pd.to_numeric(indexed["position_scale"], errors="coerce").fillna(1.0)
                    else:
                        scale_series = pd.Series(1.0, index=indexed.index, dtype=float)
                    pos = indexed["pos"] * scale_series
                    scaled_ret = pd.to_numeric(indexed["ret"], errors="coerce").fillna(0.0)
                    safe_scale = scale_series.where(scale_series.abs() > 1e-12, np.nan)
                    raw_ret = (scaled_ret / safe_scale).replace([np.inf, -np.inf], np.nan).fillna(0.0)
                    pnl = compute_pnl(pos, raw_ret, scenario_cost)
                    scenario_pnl_frames.append(pnl)
                    prior = pos.shift(1).fillna(0)
                    entry_count += int(((prior == 0) & (pos != 0)).sum())
                    temp_frame = symbol_frame.copy()
                    temp_frame = temp_frame.set_index("timestamp")
                    temp_frame["pnl"] = pnl
                    temp_frame = temp_frame.reset_index()
                    scenario_trade_frames.append(_extract_trades(temp_frame))
            scenario_pnl = (
                pd.concat(scenario_pnl_frames, axis=1).sum(axis=1) if scenario_pnl_frames else pd.Series(dtype=float)
            )
            scenario_equity = INITIAL_EQUITY * (1.0 + scenario_pnl.cumsum())
            if scenario_trade_frames:
                scenario_trades = pd.concat(scenario_trade_frames, ignore_index=True)
                avg_r = float(scenario_trades["r_multiple"].mean()) if not scenario_trades.empty else 0.0
            else:
                avg_r = 0.0
            fee_sensitivity[str(scenario_cost)] = {
                "fee_bps_per_side": scenario_cost,
                "net_return": float(scenario_pnl.sum()),
                "avg_r": avg_r,
                "max_drawdown": _compute_drawdown(scenario_equity) if not scenario_equity.empty else 0.0,
                "trades": entry_count,
            }

        fee_path = trades_dir / "fee_sensitivity.json"
        fee_path.write_text(json.dumps(fee_sensitivity, indent=2, sort_keys=True), encoding="utf-8")
        outputs.append({"path": str(fee_path), "rows": len(fee_sensitivity), "start_ts": None, "end_ts": None})

        stats["symbols"] = {symbol: {"entries": int(symbol_entry_counts.get(symbol, 0))} for symbol in symbols}
        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("Backtest failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())


def _write_trigger_coverage(run_dir: Path, results: list, enabled: int = 1):
    """Persist DSL trigger coverage diagnostics (best-effort).

    Writes: <run_dir>/engine/trigger_coverage.json
    Structure:
      {
        "by_strategy": { "<strategy_name>": { "blueprint_id": ..., "coverage": {...} }, ... },
        "missing_any": [...],
        "all_zero_strategies": [...]
      }
    """
    if not int(enabled):
        return
    out = {"by_strategy": {}, "missing_any": [], "all_zero_strategies": []}
    missing_set = set()
    for r in results or []:
        # StrategyResult compatibility: tolerate dict-like and object-like
        name = getattr(r, "strategy_name", None) or (r.get("strategy_name") if isinstance(r, dict) else None) or "unknown"
        diag = getattr(r, "diagnostics", None) or (r.get("diagnostics") if isinstance(r, dict) else {}) or {}
        dsl = diag.get("dsl", {}) if isinstance(diag, dict) else {}
        cov = dsl.get("trigger_coverage")
        if cov is None:
            continue
        blueprint_id = None
        meta = getattr(r, "strategy_metadata", None) or (r.get("strategy_metadata") if isinstance(r, dict) else {}) or {}
        blueprint_id = meta.get("blueprint_id") if isinstance(meta, dict) else None
        out["by_strategy"][name] = {"blueprint_id": blueprint_id, "coverage": cov}
        for m in cov.get("missing", []) if isinstance(cov, dict) else []:
            missing_set.add(str(m))
        if isinstance(cov, dict) and cov.get("all_zero", False):
            out["all_zero_strategies"].append(name)

    out["missing_any"] = sorted(missing_set)
    run_dir = Path(run_dir)
    (run_dir / "engine").mkdir(parents=True, exist_ok=True)
    (run_dir / "engine" / "trigger_coverage.json").write_text(json.dumps(out, indent=2, sort_keys=True))
