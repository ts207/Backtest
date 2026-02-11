from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.run_manifest import finalize_manifest, start_manifest


NO_OUTCOME_TOKENS = {
    "return",
    "returns",
    "pnl",
    "profit",
    "sharpe",
    "sortino",
    "alpha",
    "drawdown",
}


@dataclass(frozen=True)
class DatasetSpec:
    dataset_id: str
    sensor: str
    roles: Tuple[str, ...]
    resolution: str
    required_paths: Tuple[str, ...]
    source: str = "binance"


@dataclass(frozen=True)
class TemplateSpec:
    template_id: str
    name: str
    required_sensors: Tuple[str, ...]
    negative_controls: Tuple[str, ...]


@dataclass(frozen=True)
class FusionOperator:
    operator_id: str
    description: str
    arity: int


DATASET_REGISTRY: Dict[str, DatasetSpec] = {
    "um_liquidation_snapshot": DatasetSpec(
        dataset_id="um_liquidation_snapshot",
        sensor="forced_flow",
        roles=("trigger", "confirm"),
        resolution="minute",
        required_paths=("lake/raw/binance/perp/{symbol}/liquidation_snapshot",),
    ),
    "um_open_interest_hist": DatasetSpec(
        dataset_id="um_open_interest_hist",
        sensor="crowding",
        roles=("state",),
        resolution="5m",
        required_paths=("lake/raw/binance/perp/{symbol}/open_interest",),
    ),
    "um_funding_rates": DatasetSpec(
        dataset_id="um_funding_rates",
        sensor="crowding",
        roles=("state",),
        resolution="8h",
        required_paths=("lake/raw/binance/perp/{symbol}/funding",),
    ),
    "um_abma_l1_quotes": DatasetSpec(
        dataset_id="um_abma_l1_quotes",
        sensor="liquidity_state",
        roles=("confirm", "invalidate"),
        resolution="sub_minute",
        required_paths=("lake/runs/{run_id}/microstructure/quotes",),
    ),
    "um_abma_l1_trades": DatasetSpec(
        dataset_id="um_abma_l1_trades",
        sensor="liquidity_state",
        roles=("confirm",),
        resolution="sub_minute",
        required_paths=("lake/runs/{run_id}/microstructure/trades",),
    ),
    "um_perp_ohlcv_15m": DatasetSpec(
        dataset_id="um_perp_ohlcv_15m",
        sensor="liquidity_state",
        roles=("state", "invalidate"),
        resolution="15m",
        required_paths=("lake/raw/binance/perp/{symbol}/ohlcv_15m",),
    ),
    "spot_ohlcv_15m": DatasetSpec(
        dataset_id="spot_ohlcv_15m",
        sensor="synchronization",
        roles=("trigger", "confirm"),
        resolution="15m",
        required_paths=("lake/raw/binance/spot/{symbol}/ohlcv_15m",),
    ),
    "event_calendar_placeholder": DatasetSpec(
        dataset_id="event_calendar_placeholder",
        sensor="expectation",
        roles=("trigger", "state"),
        resolution="scheduled",
        required_paths=("lake/external/event_calendar",),
        source="external",
    ),
}


TEMPLATE_REGISTRY: Dict[str, TemplateSpec] = {
    "T1_forced_participation_constraint": TemplateSpec(
        template_id="T1_forced_participation_constraint",
        name="Forced Participation Constraint",
        required_sensors=("forced_flow",),
        negative_controls=("time_shuffled_events", "direction_inverted_events", "randomized_clustering_windows"),
    ),
    "T2_latency_synchronization_failure": TemplateSpec(
        template_id="T2_latency_synchronization_failure",
        name="Latency / Synchronization Failure",
        required_sensors=("synchronization",),
        negative_controls=("time_permuted_counterpart", "lead_lag_inversion", "window_scramble"),
    ),
    "T3_capacity_saturation_liquidity_discontinuity": TemplateSpec(
        template_id="T3_capacity_saturation_liquidity_discontinuity",
        name="Capacity Saturation / Liquidity Discontinuity",
        required_sensors=("liquidity_state",),
        negative_controls=("pre_event_depth_shift", "randomized_depletion_groups", "state_label_shuffle"),
    ),
    "T4_crowding_constraint_unwind": TemplateSpec(
        template_id="T4_crowding_constraint_unwind",
        name="Crowding & Constraint Unwind",
        required_sensors=("crowding",),
        negative_controls=("crowding_state_shuffle", "side_inversion", "stress_trigger_permutation"),
    ),
    "T5_information_release_asymmetry": TemplateSpec(
        template_id="T5_information_release_asymmetry",
        name="Information Release Asymmetry",
        required_sensors=("expectation",),
        negative_controls=("event_time_jitter", "expectation_proxy_permutation", "pre_post_window_swap"),
    ),
}


FUSION_OPERATORS: Tuple[FusionOperator, ...] = (
    FusionOperator("F1_trigger_plus_context", "Trigger + Context", 2),
    FusionOperator("F2_confirmation", "Confirmation", 2),
    FusionOperator("F3_causal_chain", "Causal chain (pressure->release)", 2),
    FusionOperator("F4_cross_domain_sync", "Cross-domain synchronization", 2),
    FusionOperator("F5_triangulated_gating", "Triangulated gating", 3),
)


def _parse_symbols(symbols: str) -> List[str]:
    return [s.strip().upper() for s in str(symbols or "").split(",") if s.strip()]


def _parse_datasets(datasets: str) -> Set[str]:
    raw = str(datasets or "").strip().lower()
    if raw in {"", "auto", "all"}:
        return set(DATASET_REGISTRY.keys())
    requested = {x.strip() for x in raw.split(",") if x.strip()}
    unknown = requested - set(DATASET_REGISTRY.keys())
    if unknown:
        raise ValueError(f"Unknown dataset ids: {','.join(sorted(unknown))}")
    return requested


def _dataset_available(spec: DatasetSpec, run_id: str, symbols: Sequence[str], data_root: Path) -> bool:
    for symbol in symbols:
        for rel in spec.required_paths:
            candidate = data_root / Path(rel.format(run_id=run_id, symbol=symbol))
            if candidate.exists():
                return True
    return False


def _introspect_datasets(run_id: str, symbols: Sequence[str], dataset_ids: Iterable[str], data_root: Path) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for dataset_id in sorted(dataset_ids):
        spec = DATASET_REGISTRY[dataset_id]
        available = _dataset_available(spec, run_id=run_id, symbols=symbols, data_root=data_root)
        rows.append(
            {
                "dataset_id": spec.dataset_id,
                "sensor": spec.sensor,
                "roles": list(spec.roles),
                "resolution": spec.resolution,
                "source": spec.source,
                "available": bool(available),
            }
        )
    return rows


def _allowed_templates(dataset_rows: Sequence[Dict[str, object]]) -> List[TemplateSpec]:
    sensors = {str(row["sensor"]) for row in dataset_rows if bool(row.get("available"))}
    return [
        template
        for template in TEMPLATE_REGISTRY.values()
        if all(required_sensor in sensors for required_sensor in template.required_sensors)
    ]


def _score_resolution(resolution: str) -> float:
    mapping = {
        "sub_minute": 1.0,
        "minute": 0.9,
        "5m": 0.85,
        "15m": 0.7,
        "8h": 0.55,
        "scheduled": 0.65,
    }
    return mapping.get(resolution, 0.5)


def _build_template_hypotheses(
    run_id: str,
    templates: Sequence[TemplateSpec],
    dataset_rows: Sequence[Dict[str, object]],
) -> List[Dict[str, object]]:
    available_by_sensor: Dict[str, List[Dict[str, object]]] = {}
    for row in dataset_rows:
        if bool(row.get("available")):
            available_by_sensor.setdefault(str(row["sensor"]), []).append(row)

    out: List[Dict[str, object]] = []
    idx = 0
    for template in templates:
        if template.template_id == "T1_forced_participation_constraint":
            for side in ("forced_buy_flow", "forced_sell_flow"):
                for crowd_state, consequence, horizon in (
                    ("isolated", "short_term_volatility_expansion", "short"),
                    ("clustered", "temporary_price_dislocation", "short"),
                    ("clustered", "liquidity_vacuum", "medium"),
                    ("cascading", "liquidity_vacuum", "medium"),
                    ("cascading", "volatility_aftershock", "medium"),
                ):
                    idx += 1
                    out.append(
                        {
                            "hypothesis_id": f"{run_id}_H{idx:04d}",
                            "source_kind": "template",
                            "template_id": template.template_id,
                            "fusion_operator": None,
                            "mechanism_sentence": (
                                f"Non-economic {side} under {crowd_state} participation "
                                f"creates {consequence.replace('_', ' ')} over a {horizon} horizon."
                            ),
                            "event_family_spec": {
                                "event_type": "forced_flow_cluster",
                                "trigger": side,
                                "context": crowd_state,
                                "consequence_class": consequence,
                                "horizon_bucket": horizon,
                                "invalidation": "opposite_forced_flow_in_window",
                                "pre_event_conditioning_only": True,
                            },
                            "fixed_horizon_bucket": horizon,
                            "negative_controls": list(template.negative_controls),
                            "required_datasets": sorted(
                                {r["dataset_id"] for r in available_by_sensor.get("forced_flow", [])}
                            ),
                        }
                    )
        elif template.template_id == "T2_latency_synchronization_failure":
            for trigger, consequence in (
                ("desync_spike", "convergence_window"),
                ("lead_lag_break", "delayed_refill"),
                ("basis_jump", "temporary_dislocation"),
            ):
                idx += 1
                out.append(
                    {
                        "hypothesis_id": f"{run_id}_H{idx:04d}",
                        "source_kind": "template",
                        "template_id": template.template_id,
                        "fusion_operator": None,
                        "mechanism_sentence": (
                            f"Asynchronous propagation after {trigger.replace('_', ' ')} produces "
                            f"{consequence.replace('_', ' ')} over a short horizon."
                        ),
                        "event_family_spec": {
                            "event_type": "cross_venue_desync",
                            "trigger": trigger,
                            "context": "segmented_information_flow",
                            "consequence_class": consequence,
                            "horizon_bucket": "short",
                            "invalidation": "desync_resolved_before_window_end",
                            "pre_event_conditioning_only": True,
                        },
                        "fixed_horizon_bucket": "short",
                        "negative_controls": list(template.negative_controls),
                        "required_datasets": sorted(
                            {r["dataset_id"] for r in available_by_sensor.get("synchronization", [])}
                        ),
                    }
                )
        elif template.template_id == "T3_capacity_saturation_liquidity_discontinuity":
            for trigger, consequence, horizon in (
                ("depth_depletion_burst", "nonlinear_impact", "short"),
                ("aggressive_trade_burst", "refill_lag", "medium"),
                ("book_thinning_event", "liquidity_vacuum", "medium"),
            ):
                idx += 1
                out.append(
                    {
                        "hypothesis_id": f"{run_id}_H{idx:04d}",
                        "source_kind": "template",
                        "template_id": template.template_id,
                        "fusion_operator": None,
                        "mechanism_sentence": (
                            f"When {trigger.replace('_', ' ')} exceeds local capacity, "
                            f"the market shows {consequence.replace('_', ' ')} over a {horizon} horizon."
                        ),
                        "event_family_spec": {
                            "event_type": "liquidity_discontinuity",
                            "trigger": trigger,
                            "context": "finite_supply_of_immediacy",
                            "consequence_class": consequence,
                            "horizon_bucket": horizon,
                            "invalidation": "book_depth_normalized_early",
                            "pre_event_conditioning_only": True,
                        },
                        "fixed_horizon_bucket": horizon,
                        "negative_controls": list(template.negative_controls),
                        "required_datasets": sorted(
                            {r["dataset_id"] for r in available_by_sensor.get("liquidity_state", [])}
                        ),
                    }
                )
        elif template.template_id == "T4_crowding_constraint_unwind":
            for crowd_state, stress, consequence in (
                ("crowded_long", "forced_flow_shock", "asymmetric_unwind"),
                ("crowded_short", "forced_flow_shock", "asymmetric_unwind"),
                ("crowded_long", "volatility_spike", "delayed_aftershock"),
                ("crowded_short", "volatility_spike", "delayed_aftershock"),
            ):
                idx += 1
                out.append(
                    {
                        "hypothesis_id": f"{run_id}_H{idx:04d}",
                        "source_kind": "template",
                        "template_id": template.template_id,
                        "fusion_operator": None,
                        "mechanism_sentence": (
                            f"Under {crowd_state.replace('_', ' ')}, {stress.replace('_', ' ')} induces "
                            f"{consequence.replace('_', ' ')} over a medium horizon."
                        ),
                        "event_family_spec": {
                            "event_type": "crowding_unwind",
                            "trigger": stress,
                            "context": crowd_state,
                            "consequence_class": consequence,
                            "horizon_bucket": "medium",
                            "invalidation": "opposite_crowding_state_appears",
                            "pre_event_conditioning_only": True,
                        },
                        "fixed_horizon_bucket": "medium",
                        "negative_controls": list(template.negative_controls),
                        "required_datasets": sorted(
                            {r["dataset_id"] for r in available_by_sensor.get("crowding", [])}
                        ),
                    }
                )
        elif template.template_id == "T5_information_release_asymmetry":
            idx += 1
            out.append(
                {
                    "hypothesis_id": f"{run_id}_H{idx:04d}",
                    "source_kind": "template",
                    "template_id": template.template_id,
                    "fusion_operator": None,
                    "mechanism_sentence": (
                        "Scheduled information releases create expectation-realization asymmetry "
                        "that drives post-event repricing over a short horizon."
                    ),
                    "event_family_spec": {
                        "event_type": "post_event_repricing",
                        "trigger": "scheduled_release",
                        "context": "expectation_skew_state",
                        "consequence_class": "repricing_drift",
                        "horizon_bucket": "short",
                        "invalidation": "information_fully_absorbed_early",
                        "pre_event_conditioning_only": True,
                    },
                    "fixed_horizon_bucket": "short",
                    "negative_controls": list(template.negative_controls),
                    "required_datasets": sorted(
                        {r["dataset_id"] for r in available_by_sensor.get("expectation", [])}
                    ),
                }
            )
    return out


def _score_hypothesis(
    hypothesis: Dict[str, object],
    dataset_rows_by_id: Dict[str, Dict[str, object]],
) -> Dict[str, float]:
    required = [str(x) for x in hypothesis.get("required_datasets", [])]
    specs = [dataset_rows_by_id[x] for x in required if x in dataset_rows_by_id]
    roles: Set[str] = set()
    for spec in specs:
        roles.update(str(x) for x in spec.get("roles", []))

    mechanism_coherence = 0.0
    if "trigger" in roles:
        mechanism_coherence += 0.4
    if "state" in roles:
        mechanism_coherence += 0.2
    if "confirm" in roles:
        mechanism_coherence += 0.2
    if "invalidate" in roles:
        mechanism_coherence += 0.2
    mechanism_coherence = min(1.0, mechanism_coherence)

    if specs:
        observability = float(
            sum(_score_resolution(str(s.get("resolution", "15m"))) * (1.0 if bool(s.get("available")) else 0.4) for s in specs)
            / len(specs)
        )
    else:
        observability = 0.25

    testability = 0.5
    event_spec = hypothesis.get("event_family_spec", {}) if isinstance(hypothesis.get("event_family_spec"), dict) else {}
    if event_spec.get("horizon_bucket") in {"short", "medium"}:
        testability += 0.2
    if len(hypothesis.get("negative_controls", [])) >= 2:
        testability += 0.2
    if bool(event_spec.get("pre_event_conditioning_only")):
        testability += 0.1
    testability = min(1.0, testability)

    return {
        "mechanism_coherence_score": round(mechanism_coherence, 4),
        "observability_score": round(observability, 4),
        "testability_score": round(testability, 4),
        "priority_score": round((mechanism_coherence + observability + testability) / 3.0, 4),
    }


def _generate_fusion_candidates(
    run_id: str,
    dataset_rows: Sequence[Dict[str, object]],
    max_fused: int,
) -> List[Dict[str, object]]:
    available = [r for r in dataset_rows if bool(r.get("available"))]
    by_role: Dict[str, List[Dict[str, object]]] = {}
    by_sensor: Dict[str, List[Dict[str, object]]] = {}
    for row in available:
        by_sensor.setdefault(str(row["sensor"]), []).append(row)
        for role in row.get("roles", []):
            by_role.setdefault(str(role), []).append(row)

    out: List[Dict[str, object]] = []
    idx = 0

    def _append_candidate(
        operator: str,
        sentence: str,
        event_type: str,
        trigger_ds: str,
        context_ds: str | None,
        confirm_ds: str | None,
        invalidate_ds: str | None,
        consequence: str,
        horizon: str,
    ) -> None:
        nonlocal idx
        idx += 1
        required = [trigger_ds]
        if context_ds:
            required.append(context_ds)
        if confirm_ds:
            required.append(confirm_ds)
        if invalidate_ds:
            required.append(invalidate_ds)
        required = sorted(set(required))
        out.append(
            {
                "hypothesis_id": f"{run_id}_F{idx:04d}",
                "source_kind": "fusion",
                "template_id": None,
                "fusion_operator": operator,
                "mechanism_sentence": sentence,
                "event_family_spec": {
                    "event_type": event_type,
                    "trigger_dataset": trigger_ds,
                    "context_dataset": context_ds,
                    "confirm_dataset": confirm_ds,
                    "invalidate_dataset": invalidate_ds,
                    "consequence_class": consequence,
                    "horizon_bucket": horizon,
                    "pre_event_conditioning_only": True,
                },
                "fixed_horizon_bucket": horizon,
                "negative_controls": [
                    "time_shuffled_events",
                    "state_label_permutation",
                    "direction_inverted_events",
                ],
                "required_datasets": required,
            }
        )

    for trigger in by_role.get("trigger", []):
        for context in by_role.get("state", []):
            if trigger["dataset_id"] == context["dataset_id"]:
                continue
            if len(out) >= max_fused:
                return out
            _append_candidate(
                operator="F1_trigger_plus_context",
                sentence=(
                    f"When trigger state from {trigger['sensor']} occurs under "
                    f"context from {context['sensor']}, the market exhibits temporary dislocation over a short horizon."
                ),
                event_type="trigger_with_context",
                trigger_ds=str(trigger["dataset_id"]),
                context_ds=str(context["dataset_id"]),
                confirm_ds=None,
                invalidate_ds=None,
                consequence="temporary_dislocation",
                horizon="short",
            )

    for trigger in by_role.get("trigger", []):
        for confirm in by_role.get("confirm", []):
            if trigger["dataset_id"] == confirm["dataset_id"]:
                continue
            if trigger["sensor"] == confirm["sensor"]:
                continue
            if len(out) >= max_fused:
                return out
            _append_candidate(
                operator="F2_confirmation",
                sentence=(
                    f"When a trigger from {trigger['sensor']} is confirmed by an independent "
                    f"{confirm['sensor']} sensor, delayed refill is more likely over a medium horizon."
                ),
                event_type="confirmed_trigger_event",
                trigger_ds=str(trigger["dataset_id"]),
                context_ds=None,
                confirm_ds=str(confirm["dataset_id"]),
                invalidate_ds=None,
                consequence="delayed_refill",
                horizon="medium",
            )

    for state in by_role.get("state", []):
        for trigger in by_role.get("trigger", []):
            if state["dataset_id"] == trigger["dataset_id"]:
                continue
            if len(out) >= max_fused:
                return out
            _append_candidate(
                operator="F3_causal_chain",
                sentence=(
                    f"Pressure state from {state['sensor']} followed by a release event from "
                    f"{trigger['sensor']} creates asymmetric aftershock over a medium horizon."
                ),
                event_type="pressure_release_chain",
                trigger_ds=str(trigger["dataset_id"]),
                context_ds=str(state["dataset_id"]),
                confirm_ds=None,
                invalidate_ds=None,
                consequence="asymmetric_aftershock",
                horizon="medium",
            )

    for sync in by_sensor.get("synchronization", []):
        for trigger in by_sensor.get("forced_flow", []):
            if len(out) >= max_fused:
                return out
            _append_candidate(
                operator="F4_cross_domain_sync",
                sentence=(
                    f"Synchronization failure co-occurring with forced flow indicates segmentation "
                    f"and overshoot with convergence over a short horizon."
                ),
                event_type="desync_with_forced_flow",
                trigger_ds=str(sync["dataset_id"]),
                context_ds=None,
                confirm_ds=str(trigger["dataset_id"]),
                invalidate_ds=None,
                consequence="overshoot_then_convergence",
                horizon="short",
            )

    triggers = by_role.get("trigger", [])
    states = by_role.get("state", [])
    invalidators = by_role.get("invalidate", [])
    for trigger in triggers:
        for state in states:
            if state["dataset_id"] == trigger["dataset_id"]:
                continue
            for inv in invalidators:
                if inv["dataset_id"] in {trigger["dataset_id"], state["dataset_id"]}:
                    continue
                if len(out) >= max_fused:
                    return out
                _append_candidate(
                    operator="F5_triangulated_gating",
                    sentence=(
                        f"Trigger from {trigger['sensor']} gated by {state['sensor']} and bounded by "
                        f"{inv['sensor']} yields regime-conditional dislocation over a short horizon."
                    ),
                    event_type="triangulated_trigger_event",
                    trigger_ds=str(trigger["dataset_id"]),
                    context_ds=str(state["dataset_id"]),
                    confirm_ds=None,
                    invalidate_ds=str(inv["dataset_id"]),
                    consequence="regime_conditional_dislocation",
                    horizon="short",
                )
    return out


def _anti_leak_checks(candidate: Dict[str, object]) -> Dict[str, object]:
    sentence = str(candidate.get("mechanism_sentence", "")).lower()
    event_spec = candidate.get("event_family_spec", {}) if isinstance(candidate.get("event_family_spec"), dict) else {}
    horizon = str(candidate.get("fixed_horizon_bucket", "")).lower()
    controls = candidate.get("negative_controls", [])
    required = [str(x).lower() for x in candidate.get("required_datasets", [])]

    checks = {
        "mechanism_without_outcome_reference": not any(token in sentence for token in NO_OUTCOME_TOKENS),
        "mechanism_without_dataset_ids": not any(dataset_id in sentence for dataset_id in required),
        "event_defined_without_outcome_reference": not any(token in json.dumps(event_spec).lower() for token in NO_OUTCOME_TOKENS),
        "fixed_horizon_bucket": horizon in {"short", "medium"},
        "negative_controls_predefined": isinstance(controls, list) and len(controls) >= 2,
        "pre_event_conditioning_only": bool(event_spec.get("pre_event_conditioning_only")),
    }
    checks["passed"] = bool(all(checks.values()))
    return checks


def _format_summary_md(
    run_id: str,
    dataset_rows: Sequence[Dict[str, object]],
    template_hypotheses: Sequence[Dict[str, object]],
    fusion_hypotheses: Sequence[Dict[str, object]],
    all_candidates: Sequence[Dict[str, object]],
) -> str:
    available_count = sum(1 for r in dataset_rows if bool(r.get("available")))
    lines = [
        "# Dataset-To-Mechanism Hypothesis Generator",
        "",
        f"Run ID: `{run_id}`",
        "",
        "## Dataset Introspection",
        f"- Datasets registered: {len(dataset_rows)}",
        f"- Datasets available: {available_count}",
        "",
        "## Candidate Counts",
        f"- Template-bound hypotheses: {len(template_hypotheses)}",
        f"- Fusion hypotheses: {len(fusion_hypotheses)}",
        f"- Total queue candidates: {len(all_candidates)}",
        "",
        "## Enforcement",
        "- Generator uses no return/PnL/sharpe inputs.",
        "- Horizons are fixed buckets (`short`/`medium`).",
        "- Every candidate includes predefined negative controls.",
    ]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate non-leaking dataset->mechanism hypothesis queue")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--datasets", default="auto")
    parser.add_argument("--max_fused", type=int, default=24)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    symbols = _parse_symbols(args.symbols)
    dataset_ids = _parse_datasets(args.datasets)
    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "hypothesis_generator" / args.run_id
    ensure_dir(out_dir)

    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest(
        "generate_hypothesis_queue",
        args.run_id,
        params={
            "run_id": args.run_id,
            "symbols": symbols,
            "datasets": sorted(dataset_ids),
            "max_fused": int(args.max_fused),
        },
        inputs=inputs,
        outputs=outputs,
    )

    try:
        dataset_rows = _introspect_datasets(
            run_id=args.run_id,
            symbols=symbols,
            dataset_ids=dataset_ids,
            data_root=DATA_ROOT,
        )
        dataset_rows_by_id = {str(row["dataset_id"]): row for row in dataset_rows}
        templates = _allowed_templates(dataset_rows)
        template_hypotheses = _build_template_hypotheses(
            run_id=args.run_id,
            templates=templates,
            dataset_rows=dataset_rows,
        )
        fusion_hypotheses = _generate_fusion_candidates(
            run_id=args.run_id,
            dataset_rows=dataset_rows,
            max_fused=max(0, int(args.max_fused)),
        )
        candidates = template_hypotheses + fusion_hypotheses

        enriched: List[Dict[str, object]] = []
        for row in candidates:
            scored = dict(row)
            scored.update(_score_hypothesis(scored, dataset_rows_by_id=dataset_rows_by_id))
            scored["anti_leak_checks"] = _anti_leak_checks(scored)
            enriched.append(scored)

        enriched = sorted(enriched, key=lambda x: (-float(x.get("priority_score", 0.0)), str(x.get("hypothesis_id", ""))))
        queue_rows = [r for r in enriched if bool(r.get("anti_leak_checks", {}).get("passed"))]

        dataset_path = out_dir / "dataset_introspection.json"
        template_path = out_dir / "template_hypotheses.json"
        fusion_path = out_dir / "fusion_hypotheses.json"
        all_path = out_dir / "hypothesis_candidates.json"
        queue_jsonl_path = out_dir / "phase1_hypothesis_queue.jsonl"
        queue_csv_path = out_dir / "phase1_hypothesis_queue.csv"
        summary_md_path = out_dir / "summary.md"

        dataset_path.write_text(json.dumps(dataset_rows, indent=2), encoding="utf-8")
        template_path.write_text(json.dumps(template_hypotheses, indent=2), encoding="utf-8")
        fusion_path.write_text(json.dumps(fusion_hypotheses, indent=2), encoding="utf-8")
        all_path.write_text(json.dumps(enriched, indent=2), encoding="utf-8")
        with queue_jsonl_path.open("w", encoding="utf-8") as f:
            for row in queue_rows:
                f.write(json.dumps(row, sort_keys=True) + "\n")

        if queue_rows:
            queue_df = pd.DataFrame(queue_rows)
        else:
            queue_df = pd.DataFrame(
                columns=[
                    "hypothesis_id",
                    "source_kind",
                    "template_id",
                    "fusion_operator",
                    "mechanism_sentence",
                    "event_family_spec",
                    "fixed_horizon_bucket",
                    "negative_controls",
                    "required_datasets",
                    "mechanism_coherence_score",
                    "observability_score",
                    "testability_score",
                    "priority_score",
                ]
            )
        queue_df.to_csv(queue_csv_path, index=False)
        summary_md_path.write_text(
            _format_summary_md(
                run_id=args.run_id,
                dataset_rows=dataset_rows,
                template_hypotheses=template_hypotheses,
                fusion_hypotheses=fusion_hypotheses,
                all_candidates=enriched,
            ),
            encoding="utf-8",
        )

        outputs.extend(
            [
                {"path": str(dataset_path), "rows": int(len(dataset_rows)), "start_ts": None, "end_ts": None},
                {"path": str(template_path), "rows": int(len(template_hypotheses)), "start_ts": None, "end_ts": None},
                {"path": str(fusion_path), "rows": int(len(fusion_hypotheses)), "start_ts": None, "end_ts": None},
                {"path": str(all_path), "rows": int(len(enriched)), "start_ts": None, "end_ts": None},
                {"path": str(queue_jsonl_path), "rows": int(len(queue_rows)), "start_ts": None, "end_ts": None},
                {"path": str(queue_csv_path), "rows": int(len(queue_df)), "start_ts": None, "end_ts": None},
                {"path": str(summary_md_path), "rows": int(len(enriched)), "start_ts": None, "end_ts": None},
            ]
        )

        finalize_manifest(
            manifest,
            status="success",
            stats={
                "dataset_count": int(len(dataset_rows)),
                "template_count": int(len(template_hypotheses)),
                "fusion_count": int(len(fusion_hypotheses)),
                "candidate_count": int(len(enriched)),
                "queue_count": int(len(queue_rows)),
            },
        )
        return 0
    except Exception as exc:  # pragma: no cover
        logging.exception("Hypothesis generator failed")
        finalize_manifest(manifest, status="failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())
