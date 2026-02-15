from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List

import pandas as pd

from strategies.base import Strategy
from strategies.adapters import build_adapter_registry
from strategies.carry_funding_v1 import CarryFundingV1
from strategies.mean_reversion_exhaustion_v1 import MeanReversionExhaustionV1
from strategies.onchain_flow_v1 import OnchainFlowV1
from strategies.spread_desync_v1 import SpreadDesyncV1
from strategies.vol_compression_v1 import VolCompressionV1


_REGISTRY: Dict[str, Strategy] = {
    "vol_compression_v1": VolCompressionV1(),
    "carry_funding_v1": CarryFundingV1(),
    "mean_reversion_exhaustion_v1": MeanReversionExhaustionV1(),
    "spread_desync_v1": SpreadDesyncV1(),
    "onchain_flow_v1": OnchainFlowV1(),
}
_REGISTRY.update(build_adapter_registry())

_SYMBOL_SUFFIX_PATTERN = re.compile(r"^(?P<base>[a-z0-9_]+)_(?P<symbol>[A-Z0-9]+)$")


@dataclass
class _SymbolScopedStrategy:
    name: str
    symbol: str
    _base: Strategy

    @property
    def required_features(self) -> List[str]:
        return list(getattr(self._base, "required_features", []) or [])

    def generate_positions(self, bars: pd.DataFrame, features: pd.DataFrame, params: dict) -> pd.Series:
        merged_params = dict(params or {})
        merged_params.setdefault("strategy_symbol", self.symbol)
        out = self._base.generate_positions(bars, features, merged_params)
        if not hasattr(out, "attrs"):
            return out
        metadata = out.attrs.get("strategy_metadata", {}) if isinstance(out.attrs, dict) else {}
        if not isinstance(metadata, dict):
            metadata = {}
        metadata.setdefault("base_strategy_id", getattr(self._base, "name", ""))
        metadata["strategy_id"] = self.name
        metadata["strategy_symbol"] = self.symbol
        out.attrs["strategy_metadata"] = metadata
        return out


def get_strategy(name: str) -> Strategy:
    key = name.strip()
    if key not in _REGISTRY:
        match = _SYMBOL_SUFFIX_PATTERN.match(key)
        if match:
            base_name = match.group("base")
            symbol = match.group("symbol")
            if base_name in _REGISTRY:
                return _SymbolScopedStrategy(name=key, symbol=symbol, _base=_REGISTRY[base_name])
        available = ", ".join(sorted(_REGISTRY.keys()))
        raise ValueError(f"Unknown strategy '{name}'. Available strategies: {available}")
    return _REGISTRY[key]


def list_strategies() -> List[str]:
    return sorted(_REGISTRY.keys())
