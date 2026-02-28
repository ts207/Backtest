from __future__ import annotations
import math
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class SymbolConstraints:
    tick_size: Optional[float]   # minimum price increment
    step_size: Optional[float]   # minimum quantity increment (lot size)
    min_notional: Optional[float]  # minimum order value in quote currency

    def round_qty(self, qty: float) -> float:
        if self.step_size is None or self.step_size <= 0.0:
            return qty
        precision = max(0, -int(math.floor(math.log10(self.step_size))))
        return round(math.floor(qty / self.step_size) * self.step_size, precision)

    def enforce_min_notional(self, qty: float, price: float) -> float:
        if self.min_notional is None or self.min_notional <= 0.0:
            return qty
        notional = abs(qty) * abs(price)
        return 0.0 if notional < self.min_notional else qty


def apply_constraints(
    requested_qty: float,
    price: float,
    constraints: SymbolConstraints,
) -> float:
    """Round qty to step_size then zero if below min_notional."""
    sign = 1.0 if requested_qty >= 0.0 else -1.0
    qty = constraints.round_qty(abs(requested_qty))
    qty = constraints.enforce_min_notional(qty=qty, price=price)
    return sign * qty


def load_symbol_constraints(symbol: str, meta_dir) -> SymbolConstraints:
    """
    Load exchange filters from data/lake/raw/binance/meta/<symbol>.json.
    Returns unconstrained SymbolConstraints if file is absent.
    """
    import json
    from pathlib import Path
    path = Path(meta_dir) / f"{symbol}.json"
    if not path.exists():
        return SymbolConstraints(tick_size=None, step_size=None, min_notional=None)
    data = json.loads(path.read_text(encoding="utf-8"))
    filters = {f["filterType"]: f for f in data.get("filters", [])}
    tick = float(filters.get("PRICE_FILTER", {}).get("tickSize", 0)) or None
    step = float(filters.get("LOT_SIZE", {}).get("stepSize", 0)) or None
    notional = float(filters.get("MIN_NOTIONAL", {}).get("minNotional", 0)) or None
    return SymbolConstraints(tick_size=tick, step_size=step, min_notional=notional)
