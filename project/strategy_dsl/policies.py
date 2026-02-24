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
    "vol_shock": {
        "direction": "conditional",
        "triggers": ["vol_shock_relaxation_event"],
        "confirmations": ["regime_stability_pass"],
        "stop_type": "range_pct",
        "target_type": "range_pct",
        "overlays": ["liquidity_guard", "session_guard"],
    },
    "forced_flow_exhaustion": {
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
    "funding_extreme_onset": {
        "direction": "conditional",
        "triggers": ["funding_extreme_onset_event"],
        "confirmations": ["funding_normalization_pass", "oos_validation_pass"],
        "stop_type": "percent",
        "target_type": "percent",
        "overlays": ["funding_guard", "liquidity_guard"],
    },
    "funding_persistence_trigger": {
        "direction": "conditional",
        "triggers": ["funding_persistence_event"],
        "confirmations": ["funding_normalization_pass", "oos_validation_pass"],
        "stop_type": "percent",
        "target_type": "percent",
        "overlays": ["funding_guard", "liquidity_guard"],
    },
    "funding_normalization_trigger": {
        "direction": "conditional",
        "triggers": ["funding_normalization_event"],
        "confirmations": ["oos_validation_pass"],
        "stop_type": "percent",
        "target_type": "percent",
        "overlays": ["funding_guard", "liquidity_guard"],
    },
    "oi_spike_positive": {
        "direction": "conditional",
        "triggers": ["oi_spike_pos_event"],
        "confirmations": ["spread_guard_pass", "oos_validation_pass"],
        "stop_type": "percent",
        "target_type": "percent",
        "overlays": ["liquidity_guard", "spread_guard"],
    },
    "oi_spike_negative": {
        "direction": "conditional",
        "triggers": ["oi_spike_neg_event"],
        "confirmations": ["spread_guard_pass", "oos_validation_pass"],
        "stop_type": "percent",
        "target_type": "percent",
        "overlays": ["liquidity_guard", "spread_guard"],
    },
    "oi_flush": {
        "direction": "conditional",
        "triggers": ["oi_flush_event"],
        "confirmations": ["spread_guard_pass", "oos_validation_pass"],
        "stop_type": "percent",
        "target_type": "percent",
        "overlays": ["liquidity_guard", "spread_guard"],
    },
    "liquidation_cascade": {
        "direction": "conditional",
        "triggers": ["liquidation_cascade_event"],
        "confirmations": ["spread_guard_pass", "oos_validation_pass"],
        "stop_type": "percent",
        "target_type": "percent",
        "overlays": ["liquidity_guard", "spread_guard", "session_guard"],
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
