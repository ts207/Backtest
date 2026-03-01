from __future__ import annotations

from typing import Any, Dict, List, Literal
from pydantic import BaseModel, ConfigDict, Field, field_validator


class SymbolScopeSpec(BaseModel):
    model_config = ConfigDict(frozen=True)
    mode: Literal["single_symbol", "multi_symbol", "all"]
    symbols: List[str] = Field(min_length=1)
    candidate_symbol: str = Field(min_length=1)

    @field_validator("symbols")
    @classmethod
    def check_symbols(cls, v):
        for sym in v:
            if not str(sym).strip():
                raise ValueError("symbol_scope.symbols[] must be non-empty")
        return v

    @field_validator("candidate_symbol")
    @classmethod
    def check_candidate_symbol(cls, v):
        if not str(v).strip():
            raise ValueError("symbol_scope.candidate_symbol must be non-empty")
        return v


class ConditionNodeSpec(BaseModel):
    model_config = ConfigDict(frozen=True)
    feature: str = Field(min_length=1)
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
    lookback_bars: int = Field(default=0, ge=0)
    window_bars: int = Field(default=0, ge=0)

    @field_validator("feature")
    @classmethod
    def check_feature(cls, v):
        if not str(v).strip():
            raise ValueError("entry.condition_nodes[].feature must be non-empty")
        return v

    @field_validator("value_high")
    @classmethod
    def check_value_high(cls, v, info):
        if info.data.get("operator") == "in_range":
            if v is None:
                raise ValueError("entry.condition_nodes[].value_high is required for in_range")
            value = info.data.get("value")
            if value is not None and float(v) < float(value):
                raise ValueError("entry.condition_nodes[].value_high must be >= value")
        return v

    @field_validator("window_bars")
    @classmethod
    def check_window_bars(cls, v, info):
        op = info.data.get("operator")
        if op in {"zscore_gt", "zscore_lt"} and int(v) < 2:
            raise ValueError("entry.condition_nodes[].window_bars must be >= 2 for zscore operators")
        return v


class EntrySpec(BaseModel):
    model_config = ConfigDict(frozen=True)
    triggers: List[str] = Field(min_length=1)
    conditions: List[str]
    confirmations: List[str]
    delay_bars: int = Field(ge=0)
    cooldown_bars: int = Field(ge=0)
    condition_logic: Literal["all", "any"] = "all"
    condition_nodes: List[ConditionNodeSpec] = Field(default_factory=list)
    arm_bars: int = Field(default=0, ge=0)
    reentry_lockout_bars: int = Field(default=0, ge=0)


class ExitSpec(BaseModel):
    model_config = ConfigDict(frozen=True)
    time_stop_bars: int = Field(ge=0)
    invalidation: Dict[str, Any]
    stop_type: Literal["atr", "range_pct", "percent"]
    stop_value: float = Field(ge=0)
    target_type: Literal["atr", "range_pct", "percent"]
    target_value: float = Field(ge=0)
    trailing_stop_type: Literal["none", "atr", "range_pct", "percent"] = "none"
    trailing_stop_value: float = Field(default=0.0, ge=0)
    break_even_r: float = Field(default=0.0, ge=0)

    @field_validator("invalidation")
    @classmethod
    def check_invalidation(cls, v):
        for key in ("metric", "operator", "value"):
            if key not in v:
                raise ValueError(f"exit.invalidation missing `{key}`")
        return v


class SizingSpec(BaseModel):
    model_config = ConfigDict(frozen=True)
    mode: Literal["fixed_risk", "vol_target"]
    risk_per_trade: float | None = None
    target_vol: float | None = None
    max_gross_leverage: float = Field(ge=0)
    max_position_scale: float = Field(default=1.0, ge=0)
    portfolio_risk_budget: float = Field(default=1.0, ge=0)
    symbol_risk_budget: float = Field(default=1.0, ge=0)

    @field_validator("risk_per_trade")
    @classmethod
    def check_risk(cls, v, info):
        if info.data.get("mode") == "fixed_risk" and v is None:
            raise ValueError("sizing.risk_per_trade is required in fixed_risk mode")
        if v is not None and v < 0:
            raise ValueError("sizing.risk_per_trade must be >= 0")
        return v

    @field_validator("target_vol")
    @classmethod
    def check_vol(cls, v, info):
        if info.data.get("mode") == "vol_target" and v is None:
            raise ValueError("sizing.target_vol is required in vol_target mode")
        if v is not None and v < 0:
            raise ValueError("sizing.target_vol must be >= 0")
        return v


class OverlaySpec(BaseModel):
    model_config = ConfigDict(frozen=True)
    name: str = Field(min_length=1)
    params: Dict[str, Any]

    @field_validator("name")
    @classmethod
    def check_name(cls, v):
        if not str(v).strip():
            raise ValueError("overlay.name must be non-empty")
        return v


class EvaluationSpec(BaseModel):
    model_config = ConfigDict(frozen=True)
    min_trades: int = Field(ge=0)
    cost_model: Dict[str, Any]
    robustness_flags: Dict[str, bool]

    @field_validator("cost_model")
    @classmethod
    def check_cost_model(cls, v):
        for key in ("fees_bps", "slippage_bps", "funding_included"):
            if key not in v:
                raise ValueError(f"evaluation.cost_model missing `{key}`")
        return v

    @field_validator("robustness_flags")
    @classmethod
    def check_robustness_flags(cls, v):
        for key in ("oos_required", "multiplicity_required", "regime_stability_required"):
            if key not in v:
                raise ValueError(f"evaluation.robustness_flags missing `{key}`")
        return v


class LineageSpec(BaseModel):
    model_config = ConfigDict(frozen=True)
    source_path: str = Field(min_length=1)
    compiler_version: str = Field(min_length=1)
    generated_at_utc: str = Field(min_length=1)
    bridge_embargo_days_used: int | None = Field(default=None, ge=0)
    wf_evidence_hash: str = ""
    wf_status: Literal["pass", "trimmed_zero_trade", "trimmed_worst_negative", "pending"] = "pending"
    events_count_used_for_gate: int = Field(default=0, ge=0)
    min_events_threshold: int = Field(default=0, ge=0)
    cost_config_digest: str = ""
    promotion_track: Literal["standard", "fallback_only"] = "standard"
    ontology_spec_hash: str = ""
    canonical_event_type: str = ""
    canonical_family: str = ""
    state_id: str = ""
    template_verb: str = ""
    operator_version: str = ""
    discovery_start: str = ""
    discovery_end: str = ""
    constraints: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("source_path", "compiler_version", "generated_at_utc")
    @classmethod
    def check_non_empty(cls, v):
        if not str(v).strip():
            raise ValueError("field must be non-empty")
        return v

    @field_validator("ontology_spec_hash")
    @classmethod
    def check_hash(cls, v):
        if v and not str(v).startswith("sha256:"):
            raise ValueError("lineage.ontology_spec_hash must start with sha256: when provided")
        return v


class Blueprint(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    event_type: str = Field(min_length=1)
    candidate_id: str = Field(min_length=1)
    symbol_scope: SymbolScopeSpec
    direction: Literal["long", "short", "both", "conditional"]
    entry: EntrySpec
    exit: ExitSpec
    sizing: SizingSpec
    overlays: List[OverlaySpec]
    evaluation: EvaluationSpec
    lineage: LineageSpec

    @field_validator("id", "run_id", "event_type", "candidate_id")
    @classmethod
    def check_non_empty(cls, v):
        if not str(v).strip():
            raise ValueError("field must be non-empty")
        return v

    def validate(self) -> None:
        pass

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()
