from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from pipelines._lib.spec_loader import load_global_defaults


DEFAULT_UNIVERSE = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


def load_hypothesis_defaults(project_root: Path) -> Dict[str, object]:
    defaults = load_global_defaults(project_root=project_root)
    horizons = defaults.get("horizons", ["5m", "15m", "60m"])
    rule_templates = defaults.get("rule_templates", ["mean_reversion", "continuation"])
    conditioning = defaults.get(
        "conditioning",
        {
            "vol_regime": ["high"],
            "severity_bucket": ["top_10pct", "extreme_5pct"],
        },
    )
    return {
        "horizons": [str(x) for x in horizons],
        "rule_templates": [str(x) for x in rule_templates],
        "conditioning": dict(conditioning) if isinstance(conditioning, dict) else {},
    }


def parse_symbols_filter(assets_filter: str, universe: Sequence[str]) -> List[str]:
    normalized_universe = [str(s).strip().upper() for s in universe if str(s).strip()]
    if not normalized_universe:
        normalized_universe = list(DEFAULT_UNIVERSE)

    raw = str(assets_filter or "").strip()
    if not raw or raw == "*":
        return normalized_universe

    filters = [token.strip().upper() for token in raw.split("|") if token.strip()]
    if not filters:
        return normalized_universe

    selected: List[str] = []
    for symbol in normalized_universe:
        for token in filters:
            token_norm = token if token.endswith("USDT") else f"{token}USDT"
            if token_norm == symbol:
                selected.append(symbol)
                break
    return selected


def extract_event_type(statement: str) -> Optional[str]:
    text = str(statement or "")
    match = re.search(r'""event_type"": ""([A-Z0-9_]+)""', text)
    if match:
        return match.group(1)
    matches = re.findall(r"\b[A-Z][A-Z0-9_]{5,}\b", text)
    if matches:
        return matches[0]
    return None
