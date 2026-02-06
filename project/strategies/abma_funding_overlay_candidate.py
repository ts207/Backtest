from __future__ import annotations

from typing import Dict

from analyzers.abma_v2_funding.config_v2 import ABMAFundingConfig


def build_overlay_candidate(*, run_id: str, config: ABMAFundingConfig) -> Dict[str, object]:
    """Build a DRAFT overlay candidate spec for funding boundary stabilization."""
    return {
        "spec_version": "v1",
        "name": f"abma_funding_boundary_delay_{config.overlay_delay_seconds}s",
        "event": "funding_boundary",
        "condition": "abma_stabilization_incomplete == true",
        "action": f"delay_entry_by_seconds: {config.overlay_delay_seconds}",
        "status": "DRAFT",
        "tuning_allowed": False,
        "run_ids_evidence": [
            {
                "run_id": run_id,
                "split": "year",
                "date_range": "unknown",
                "universe": "binance_um",
                "config_hash": config.def_version,
            }
        ],
        "cost_bps_validated": 0.0,
        "notes": "Draft overlay candidate emitted only when ABMA v2 funding Phase-1 passes.",
        "objective": {
            "target_metric": "net_total_return",
            "baseline": "none",
        },
        "constraints": {
            "max_drawdown_pct": None,
            "tail_loss": None,
            "exposure_limits": None,
            "turnover_budget": None,
        },
        "stability": {
            "sign_consistency_min": config.sign_consistency_threshold,
            "effect_ci_excludes_0": True,
            "max_regime_flip_count": config.max_regime_flip_count,
        },
    }
