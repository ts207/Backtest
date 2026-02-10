from __future__ import annotations

from typing import Dict, List

from strategies.base import Strategy
from strategies.vol_compression_momentum_v1 import VolCompressionMomentumV1
from strategies.funding_carry_v1 import FundingCarryV1
from strategies.intraday_reversion_v1 import IntradayReversionV1
from strategies.tsmom_v1 import TsmomV1
from strategies.vol_compression_reversion_v1 import VolCompressionReversionV1
from strategies.vol_compression_v1 import VolCompressionV1


_REGISTRY: Dict[str, Strategy] = {
    "vol_compression_v1": VolCompressionV1(),
    "vol_compression_momentum_v1": VolCompressionMomentumV1(),
    "vol_compression_reversion_v1": VolCompressionReversionV1(),
    "tsmom_v1": TsmomV1(),
    "funding_carry_v1": FundingCarryV1(),
    "intraday_reversion_v1": IntradayReversionV1(),
}


def get_strategy(name: str) -> Strategy:
    key = name.strip()
    if key not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY.keys()))
        raise ValueError(f"Unknown strategy '{name}'. Available strategies: {available}")
    return _REGISTRY[key]


def list_strategies() -> List[str]:
    return sorted(_REGISTRY.keys())
