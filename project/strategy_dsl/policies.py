from __future__ import annotations

from typing import Dict, List


DEFAULT_POLICY = {
    "direction": "conditional",
    "triggers": ["event_detected"],
    "confirmations": ["oos_validation_pass"],
    "stop_type": "range_pct",
    "target_type": "range_pct",
    "overlays": ["liquidity_guard"],
}


EVENT_POLICIES: Dict[str, Dict[str, object]] = {
    "vol_shock_relaxation": {
        "direction": "conditional",
        "triggers": ["vol_shock_relaxation_event"],
        "confirmations": ["regime_stability_pass"],
        "stop_type": "range_pct",
        "target_type": "range_pct",
        "overlays": ["liquidity_guard", "session_guard"],
    },
    "liquidity_refill_lag_window": {
        "direction": "conditional",
        "triggers": ["liquidity_refill_lag_event"],
        "confirmations": ["refill_persistence_pass"],
        "stop_type": "percent",
        "target_type": "percent",
        "overlays": ["liquidity_guard"],
    },
    "liquidity_absence_window": {
        "direction": "conditional",
        "triggers": ["liquidity_absence_event"],
        "confirmations": ["spread_guard_pass"],
        "stop_type": "percent",
        "target_type": "percent",
        "overlays": ["liquidity_guard", "spread_guard"],
    },
    "vol_aftershock_window": {
        "direction": "conditional",
        "triggers": ["vol_aftershock_event"],
        "confirmations": ["regime_stability_pass"],
        "stop_type": "range_pct",
        "target_type": "range_pct",
        "overlays": ["session_guard"],
    },
    "directional_exhaustion_after_forced_flow": {
        "direction": "conditional",
        "triggers": ["forced_flow_exhaustion_event"],
        "confirmations": ["oos_validation_pass"],
        "stop_type": "percent",
        "target_type": "percent",
        "overlays": ["liquidity_guard"],
    },
    "cross_venue_desync": {
        "direction": "conditional",
        "triggers": ["cross_venue_desync_event"],
        "confirmations": ["cross_venue_consensus_pass"],
        "stop_type": "percent",
        "target_type": "percent",
        "overlays": ["cross_venue_guard"],
    },
    "liquidity_vacuum": {
        "direction": "conditional",
        "triggers": ["liquidity_vacuum_event"],
        "confirmations": ["vacuum_refill_confirmation"],
        "stop_type": "range_pct",
        "target_type": "range_pct",
        "overlays": ["liquidity_guard"],
    },
    "funding_extreme_reversal_window": {
        "direction": "conditional",
        "triggers": ["funding_extreme_event"],
        "confirmations": ["funding_normalization_pass"],
        "stop_type": "percent",
        "target_type": "percent",
        "overlays": ["funding_guard", "liquidity_guard"],
    },
    "range_compression_breakout_window": {
        "direction": "conditional",
        "triggers": ["range_compression_breakout_event"],
        "confirmations": ["breakout_confirmation"],
        "stop_type": "range_pct",
        "target_type": "range_pct",
        "overlays": ["session_guard"],
    },
}


def event_policy(event_type: str) -> Dict[str, object]:
    key = str(event_type).strip().lower()
    return EVENT_POLICIES.get(key, DEFAULT_POLICY)


def overlay_defaults(names: List[str], robustness_score: float) -> List[dict]:
    overlays = []
    for name in names:
        if name == "liquidity_guard":
            overlays.append({"name": name, "params": {"min_notional": 100_000.0}})
        elif name == "spread_guard":
            overlays.append({"name": name, "params": {"max_spread_bps": 8.0}})
        elif name == "session_guard":
            overlays.append({"name": name, "params": {"session": "all"}})
        elif name == "funding_guard":
            overlays.append({"name": name, "params": {"max_abs_funding_bps": 12.0}})
        elif name == "cross_venue_guard":
            overlays.append({"name": name, "params": {"max_desync_bps": 12.0}})
    if robustness_score < 0.6:
        overlays.append({"name": "risk_throttle", "params": {"size_scale": 0.25}})
    elif robustness_score < 0.8:
        overlays.append({"name": "risk_throttle", "params": {"size_scale": 0.5}})
    return overlays
