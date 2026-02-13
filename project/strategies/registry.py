from __future__ import annotations

from typing import Dict, List

from strategies.base import Strategy
from strategies.adapters import build_adapter_registry
from strategies.carry_funding_v1 import CarryFundingV1
from strategies.mean_reversion_exhaustion_v1 import MeanReversionExhaustionV1
from strategies.spread_desync_v1 import SpreadDesyncV1
from strategies.vol_compression_v1 import VolCompressionV1


_REGISTRY: Dict[str, Strategy] = {
    "vol_compression_v1": VolCompressionV1(),
    "carry_funding_v1": CarryFundingV1(),
    "mean_reversion_exhaustion_v1": MeanReversionExhaustionV1(),
    "spread_desync_v1": SpreadDesyncV1(),
}
_REGISTRY.update(build_adapter_registry())


def get_strategy(name: str) -> Strategy:
    key = name.strip()
    if key not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY.keys()))
        raise ValueError(f"Unknown strategy '{name}'. Available strategies: {available}")
    return _REGISTRY[key]


def list_strategies() -> List[str]:
    return sorted(_REGISTRY.keys())
