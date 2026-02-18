from __future__ import annotations

import re
from typing import Iterable, List, Optional, Tuple

from strategy_dsl.schema import ConditionNodeSpec


class NonExecutableConditionError(ValueError):
    pass


class NonExecutableActionError(ValueError):
    pass


# Canonical, executable condition names supported by the Strategy DSL compiler.
# NOTE: "research-only" condition names (e.g., age buckets, half-life buckets)
# are intentionally excluded unless/until they are materialized as runtime features.
SESSION_CONDITION_MAP = {
    "session_asia": (0, 7),
    "session_eu": (8, 15),
    "session_us": (16, 23),
}

BULL_BEAR_CONDITION_MAP = {
    "bull_bear_bull": 1,
    "bull_bear_bear": -1,
}

VOL_REGIME_CONDITION_MAP = {
    "vol_regime_low": 0,
    "vol_regime_mid": 1,
    "vol_regime_medium": 1,
    "vol_regime_high": 2,
}

_NUMERIC_CONDITION_PATTERN = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(>=|<=|==|>|<)\s*(-?\d+(?:\.\d+)?)\s*$"
)


# Canonical, executable action names supported end-to-end.
# The compiler currently uses actions primarily to derive delay/cooldown semantics.
_ALLOWED_ACTIONS = {
    "no_action",
    "entry_gate_skip",
    "risk_throttle_0.5",
    "risk_throttle_0.0",
    "delay_0",
    "delay_8",
    "delay_30",
    "reenable_at_half_life",
    "risk_throttle_0",
}


def is_executable_action(action: str) -> bool:
    return str(action or "").strip() in _ALLOWED_ACTIONS


def validate_action(action: str, *, event_type: str, candidate_id: str) -> str:
    value = str(action or "").strip()
    if not value:
        raise NonExecutableActionError(f"Empty action for event={event_type}, candidate_id={candidate_id}")
    if value not in _ALLOWED_ACTIONS:
        raise NonExecutableActionError(
            f"Non-executable action for event={event_type}, candidate_id={candidate_id}: `{value}`"
        )
    return value


def normalize_entry_condition(
    condition: object,
    *,
    event_type: str,
    candidate_id: str,
    run_symbols: Optional[Iterable[str]] = None,
) -> Tuple[str, List[ConditionNodeSpec], Optional[str]]:
    """Return (canonical_condition_str, condition_nodes, symbol_override).

    canonical_condition_str is stored in blueprint.entry.conditions for downstream promotion diagnostics.
    condition_nodes are evaluated at runtime.
    symbol_override forces a single-symbol deployment when the condition is `symbol_XXX`.
    """

    raw = str(condition if condition is not None else "all")
    lowered = raw.strip().lower()

    if lowered in {"", "all"}:
        return "all", [], None

    if lowered in SESSION_CONDITION_MAP:
        start, end = SESSION_CONDITION_MAP[lowered]
        return (
            lowered,
            [ConditionNodeSpec(feature="session_hour_utc", operator="in_range", value=float(start), value_high=float(end))],
            None,
        )

    if lowered in BULL_BEAR_CONDITION_MAP:
        code = float(BULL_BEAR_CONDITION_MAP[lowered])
        return (
            lowered,
            [ConditionNodeSpec(feature="bull_bear_flag", operator="==", value=code)],
            None,
        )

    if lowered in VOL_REGIME_CONDITION_MAP:
        code = float(VOL_REGIME_CONDITION_MAP[lowered])
        return (
            lowered,
            [ConditionNodeSpec(feature="vol_regime_code", operator="==", value=code)],
            None,
        )

    match = _NUMERIC_CONDITION_PATTERN.match(raw)
    if match:
        feature, operator, value = match.groups()
        value_f = float(value)
        canonical = f"{feature}{operator}{value_f:g}"  # remove whitespace; normalize numeric
        return (
            canonical,
            [ConditionNodeSpec(feature=feature, operator=operator, value=value_f)],
            None,
        )

    if lowered.startswith("symbol_"):
        symbol = lowered[len("symbol_") :].strip().upper()
        if not symbol:
            raise NonExecutableConditionError(
                f"Non-executable condition for event={event_type}, candidate_id={candidate_id}: `{raw}` (empty symbol)"
            )
        if run_symbols is not None:
            symbols_set = {str(s).strip().upper() for s in run_symbols if str(s).strip()}
            if symbols_set and symbol not in symbols_set:
                raise NonExecutableConditionError(
                    f"Non-executable condition for event={event_type}, candidate_id={candidate_id}: `{raw}` "
                    f"(symbol not in run symbols: {sorted(symbols_set)})"
                )
        return f"symbol_{symbol}", [], symbol

    raise NonExecutableConditionError(
        f"Non-executable condition for event={event_type}, candidate_id={candidate_id}: `{raw}`"
    )


def is_executable_condition(
    condition: object,
    *,
    run_symbols: Optional[Iterable[str]] = None,
) -> bool:
    try:
        normalize_entry_condition(condition, event_type="_", candidate_id="_", run_symbols=run_symbols)
        return True
    except NonExecutableConditionError:
        return False
