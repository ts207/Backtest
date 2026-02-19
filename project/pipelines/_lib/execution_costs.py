from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Sequence

from pipelines._lib.config import load_configs


@dataclass(frozen=True)
class ResolvedExecutionCosts:
    config_paths: List[str]
    config: Dict[str, Any]
    fee_bps_per_side: float
    slippage_bps_per_fill: float
    cost_bps: float
    execution_model: Dict[str, float]
    config_digest: str


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _default_config_paths(project_root: Path) -> List[Path]:
    return [project_root / "configs" / "pipeline.yaml", project_root / "configs" / "fees.yaml"]


def _resolve_config_paths(project_root: Path, config_paths: Sequence[str] | None) -> List[Path]:
    paths = _default_config_paths(project_root)
    for raw in list(config_paths or []):
        path = Path(str(raw))
        if not path.is_absolute():
            path = project_root / path
        paths.append(path)
    return paths


def resolve_execution_costs(
    *,
    project_root: Path,
    config_paths: Sequence[str] | None,
    fees_bps: float | None,
    slippage_bps: float | None,
    cost_bps: float | None,
) -> ResolvedExecutionCosts:
    paths = _resolve_config_paths(project_root, config_paths)
    merged = load_configs([str(path) for path in paths])

    fee = float(fees_bps) if fees_bps is not None else float(merged.get("fee_bps_per_side", 4.0))
    slippage = float(slippage_bps) if slippage_bps is not None else float(merged.get("slippage_bps_per_fill", 2.0))
    cost = float(cost_bps) if cost_bps is not None else float(fee + slippage)

    execution_model_raw = merged.get("execution_model", {})
    execution_model = dict(execution_model_raw) if isinstance(execution_model_raw, dict) else {}
    execution_model.setdefault("base_fee_bps", float(fee))
    execution_model.setdefault("base_slippage_bps", float(slippage))

    payload = {
        "config_paths": [str(path) for path in paths],
        "fee_bps_per_side": float(fee),
        "slippage_bps_per_fill": float(slippage),
        "cost_bps": float(cost),
        "execution_model": execution_model,
    }
    digest = _sha256_text(json.dumps(payload, sort_keys=True, default=str))
    return ResolvedExecutionCosts(
        config_paths=[str(path) for path in paths],
        config=merged,
        fee_bps_per_side=float(fee),
        slippage_bps_per_fill=float(slippage),
        cost_bps=float(cost),
        execution_model={str(k): float(v) for k, v in execution_model.items() if _is_floatable(v)},
        config_digest=digest,
    )


def _is_floatable(value: object) -> bool:
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


