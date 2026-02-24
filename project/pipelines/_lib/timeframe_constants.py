from __future__ import annotations

from typing import Dict, List

# Canonical annualization factors by timeframe.
BARS_PER_YEAR_BY_TIMEFRAME: Dict[str, int] = {
    "1m": 365 * 24 * 60,
    "5m": 365 * 24 * 12,
    "15m": 365 * 24 * 4,
    "30m": 365 * 24 * 2,
    "60m": 365 * 24,
    "1h": 365 * 24,
    "4h": 365 * 6,
    "24h": 365,
    "1d": 365,
}

# Canonical horizon label to 5-minute bars mapping used by research tooling.
HORIZON_BARS_BY_TIMEFRAME: Dict[str, int] = {
    "5m": 1,
    "15m": 3,
    "30m": 6,
    "60m": 12,
    "1h": 12,
    "4h": 48,
    "24h": 288,
    "1d": 288,
}

# Default horizon grid for event quality / conditional analysis at 5m base bars.
DEFAULT_EVENT_HORIZON_BARS: List[int] = [1, 3, 12]


def bars_per_year_for_timeframe(timeframe: str, default: int | None = None) -> int:
    key = str(timeframe or "").strip().lower()
    if default is None:
        default = BARS_PER_YEAR_BY_TIMEFRAME["5m"]
    return int(BARS_PER_YEAR_BY_TIMEFRAME.get(key, int(default)))


def horizon_bars_for_label(horizon: str, default: int = 12) -> int:
    key = str(horizon or "").strip().lower()
    return int(HORIZON_BARS_BY_TIMEFRAME.get(key, int(default)))
