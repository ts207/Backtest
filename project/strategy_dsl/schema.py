from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Literal


def _require_non_empty(value: str, field: str) -> None:
    if not str(value).strip():
        raise ValueError(f"{field} must be non-empty")


def _require_non_negative(value: float | int, field: str) -> None:
    if float(value) < 0.0:
        raise ValueError(f"{field} must be >= 0")


@dataclass(frozen=True)
class SymbolScopeSpec:
    mode: Literal["single_symbol", "multi_symbol", "all"]
    symbols: List[str]
    candidate_symbol: str

    def validate(self) -> None:
        if self.mode not in {"single_symbol", "multi_symbol", "all"}:
            raise ValueError(f"invalid symbol scope mode: {self.mode}")
        if not isinstance(self.symbols, list):
            raise ValueError("symbol_scope.symbols must be a list")
        for symbol in self.symbols:
            _require_non_empty(symbol, "symbol_scope.symbols[]")
        _require_non_empty(self.candidate_symbol, "symbol_scope.candidate_symbol")


@dataclass(frozen=True)
class ConditionNodeSpec:
    feature: str
    operator: Literal[
        ">",
        ">=",
        "<",
        "<=",
        "==",
        "crosses_above",
        "crosses_below",
        "in_range",
        "zscore_gt",
        "zscore_lt",
    ]
    value: float
    value_high: float | None = None
    lookback_bars: int = 0
    window_bars: int = 0

    def validate(self) -> None:
        _require_non_empty(self.feature, "entry.condition_nodes[].feature")
        if self.operator not in {">", ">=", "<", "<=", "==", "crosses_above", "crosses_below", "in_range", "zscore_gt", "zscore_lt"}:
            raise ValueError(f"invalid entry.condition_nodes[].operator: {self.operator}")
        _require_non_negative(self.lookback_bars, "entry.condition_nodes[].lookback_bars")
        _require_non_negative(self.window_bars, "entry.condition_nodes[].window_bars")
        if self.operator == "in_range":
            if self.value_high is None:
                raise ValueError("entry.condition_nodes[].value_high is required for in_range")
            if float(self.value_high) < float(self.value):
                raise ValueError("entry.condition_nodes[].value_high must be >= value")
        if self.operator in {"zscore_gt", "zscore_lt"} and int(self.window_bars) < 2:
            raise ValueError("entry.condition_nodes[].window_bars must be >= 2 for zscore operators")


@dataclass(frozen=True)
class EntrySpec:
    triggers: List[str]
    conditions: List[str]
    confirmations: List[str]
    delay_bars: int
    cooldown_bars: int
    condition_logic: Literal["all", "any"] = "all"
    condition_nodes: List[ConditionNodeSpec] = field(default_factory=list)
    arm_bars: int = 0
    reentry_lockout_bars: int = 0

    def validate(self) -> None:
        if not isinstance(self.triggers, list) or not self.triggers:
            raise ValueError("entry.triggers must be a non-empty list")
        if not isinstance(self.conditions, list):
            raise ValueError("entry.conditions must be a list")
        if not isinstance(self.confirmations, list):
            raise ValueError("entry.confirmations must be a list")
        _require_non_negative(self.delay_bars, "entry.delay_bars")
        _require_non_negative(self.cooldown_bars, "entry.cooldown_bars")
        if self.condition_logic not in {"all", "any"}:
            raise ValueError("entry.condition_logic must be one of {all, any}")
        if not isinstance(self.condition_nodes, list):
            raise ValueError("entry.condition_nodes must be a list")
        for node in self.condition_nodes:
            node.validate()
        _require_non_negative(self.arm_bars, "entry.arm_bars")
        _require_non_negative(self.reentry_lockout_bars, "entry.reentry_lockout_bars")


@dataclass(frozen=True)
class ExitSpec:
    time_stop_bars: int
    invalidation: Dict[str, Any]
    stop_type: Literal["atr", "range_pct", "percent"]
    stop_value: float
    target_type: Literal["atr", "range_pct", "percent"]
    target_value: float
    trailing_stop_type: Literal["none", "atr", "range_pct", "percent"] = "none"
    trailing_stop_value: float = 0.0
    break_even_r: float = 0.0

    def validate(self) -> None:
        _require_non_negative(self.time_stop_bars, "exit.time_stop_bars")
        if self.stop_type not in {"atr", "range_pct", "percent"}:
            raise ValueError(f"invalid exit.stop_type: {self.stop_type}")
        if self.target_type not in {"atr", "range_pct", "percent"}:
            raise ValueError(f"invalid exit.target_type: {self.target_type}")
        if self.trailing_stop_type not in {"none", "atr", "range_pct", "percent"}:
            raise ValueError(f"invalid exit.trailing_stop_type: {self.trailing_stop_type}")
        _require_non_negative(self.stop_value, "exit.stop_value")
        _require_non_negative(self.target_value, "exit.target_value")
        _require_non_negative(self.trailing_stop_value, "exit.trailing_stop_value")
        _require_non_negative(self.break_even_r, "exit.break_even_r")
        if not isinstance(self.invalidation, dict):
            raise ValueError("exit.invalidation must be a dict")
        for key in ("metric", "operator", "value"):
            if key not in self.invalidation:
                raise ValueError(f"exit.invalidation missing `{key}`")


@dataclass(frozen=True)
class SizingSpec:
    mode: Literal["fixed_risk", "vol_target"]
    risk_per_trade: float | None
    target_vol: float | None
    max_gross_leverage: float
    max_position_scale: float = 1.0
    portfolio_risk_budget: float = 1.0
    symbol_risk_budget: float = 1.0

    def validate(self) -> None:
        if self.mode not in {"fixed_risk", "vol_target"}:
            raise ValueError(f"invalid sizing.mode: {self.mode}")
        if self.mode == "fixed_risk" and self.risk_per_trade is None:
            raise ValueError("sizing.risk_per_trade is required in fixed_risk mode")
        if self.mode == "vol_target" and self.target_vol is None:
            raise ValueError("sizing.target_vol is required in vol_target mode")
        if self.risk_per_trade is not None:
            _require_non_negative(self.risk_per_trade, "sizing.risk_per_trade")
        if self.target_vol is not None:
            _require_non_negative(self.target_vol, "sizing.target_vol")
        _require_non_negative(self.max_gross_leverage, "sizing.max_gross_leverage")
        _require_non_negative(self.max_position_scale, "sizing.max_position_scale")
        _require_non_negative(self.portfolio_risk_budget, "sizing.portfolio_risk_budget")
        _require_non_negative(self.symbol_risk_budget, "sizing.symbol_risk_budget")


@dataclass(frozen=True)
class OverlaySpec:
    name: str
    params: Dict[str, Any]

    def validate(self) -> None:
        _require_non_empty(self.name, "overlay.name")
        if not isinstance(self.params, dict):
            raise ValueError("overlay.params must be a dict")


@dataclass(frozen=True)
class EvaluationSpec:
    min_trades: int
    cost_model: Dict[str, Any]
    robustness_flags: Dict[str, bool]

    def validate(self) -> None:
        _require_non_negative(self.min_trades, "evaluation.min_trades")
        if not isinstance(self.cost_model, dict):
            raise ValueError("evaluation.cost_model must be a dict")
        for key in ("fees_bps", "slippage_bps", "funding_included"):
            if key not in self.cost_model:
                raise ValueError(f"evaluation.cost_model missing `{key}`")
        if not isinstance(self.robustness_flags, dict):
            raise ValueError("evaluation.robustness_flags must be a dict")
        for key in ("oos_required", "multiplicity_required", "regime_stability_required"):
            if key not in self.robustness_flags:
                raise ValueError(f"evaluation.robustness_flags missing `{key}`")


@dataclass(frozen=True)
class LineageSpec:
    source_path: str
    compiler_version: str
    generated_at_utc: str

    def validate(self) -> None:
        _require_non_empty(self.source_path, "lineage.source_path")
        _require_non_empty(self.compiler_version, "lineage.compiler_version")
        _require_non_empty(self.generated_at_utc, "lineage.generated_at_utc")


@dataclass(frozen=True)
class Blueprint:
    id: str
    run_id: str
    event_type: str
    candidate_id: str
    symbol_scope: SymbolScopeSpec
    direction: Literal["long", "short", "both", "conditional"]
    entry: EntrySpec
    exit: ExitSpec
    sizing: SizingSpec
    overlays: List[OverlaySpec]
    evaluation: EvaluationSpec
    lineage: LineageSpec

    def validate(self) -> None:
        _require_non_empty(self.id, "id")
        _require_non_empty(self.run_id, "run_id")
        _require_non_empty(self.event_type, "event_type")
        _require_non_empty(self.candidate_id, "candidate_id")
        if self.direction not in {"long", "short", "both", "conditional"}:
            raise ValueError(f"invalid direction: {self.direction}")
        self.symbol_scope.validate()
        self.entry.validate()
        self.exit.validate()
        self.sizing.validate()
        for overlay in self.overlays:
            overlay.validate()
        self.evaluation.validate()
        self.lineage.validate()

    def to_dict(self) -> Dict[str, Any]:
        self.validate()
        return asdict(self)
