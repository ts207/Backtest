from __future__ import annotations

from typing import Mapping


def is_fp_active(row: Mapping[str, object]) -> bool:
    return bool(int(row.get("fp_active", 0) or 0))


def fp_age(row: Mapping[str, object]) -> int:
    return int(row.get("fp_age_bars", 0) or 0)


def fp_norm_due(row: Mapping[str, object]) -> bool:
    return bool(int(row.get("fp_norm_due", 0) or 0))
