from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable

import yaml


def _deep_merge(base: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            base[key] = _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


def load_configs(paths: Iterable[str]) -> Dict[str, Any]:
    """
    Load and merge one or more YAML config files.
    Later configs override values from earlier ones.
    """
    merged: Dict[str, Any] = {}
    for path_str in paths:
        path = Path(path_str)
        if not path.exists():
            raise FileNotFoundError(f"Config not found: {path}")
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(data, dict):
            raise ValueError(f"Config must be a mapping: {path}")
        merged = _deep_merge(merged, data)
    return merged
