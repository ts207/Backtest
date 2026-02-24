from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import (
    ensure_dir,
)
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.ontology_contract import (
    bool_field,
    choose_template_ontology_hash,
    compare_hash_fields,
    load_ontology_linkage_hash,
    load_run_manifest_hashes,
    normalize_state_registry_records,
    ontology_component_hash_fields,
    ontology_component_hashes,
    ontology_spec_paths,
    ontology_spec_hash,
    parse_list_field,
    validate_candidate_templates_schema,
)
from events.registry import EVENT_REGISTRY_SPECS
from pipelines.research.feasibility_guard import FeasibilityGuard
from pipelines.research.hypothesis_spec_translator import (
    load_active_hypothesis_specs,
    load_template_side_policy,
    translate_candidate_hypotheses,
)
from pipelines.research.condition_key_contract import (
    format_available_key_sample,
    load_symbol_joined_condition_contract,
    load_symbol_joined_condition_keys,
)

# Planner Budgets
MAX_TOTAL_CANDIDATES = 200
MAX_CANDIDATES_PER_CLAIM = 20
MAX_SYMBOLS_PER_TEMPLATE = 10
MAX_CONDITIONING_VARIANTS = 6
MAX_HORIZONS_PER_TEMPLATE = 3

def _check_spec_exists(path_str: str) -> bool:
    return (PROJECT_ROOT.parent / path_str).exists()

def _check_dataset_exists(symbol: str, run_id: str) -> bool:
    # Basic check for OHLCV 5m data
    path = DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / "bars_5m"
    run_scoped = DATA_ROOT / "lake" / "runs" / run_id / "cleaned" / "perp" / symbol / "bars_5m"
    return path.exists() or run_scoped.exists()

def _check_market_context_exists(symbol: str, run_id: str) -> bool:
    path = DATA_ROOT / "lake" / "context" / "market_state" / symbol / "5m.parquet"
    run_scoped = DATA_ROOT / "lake" / "runs" / run_id / "context" / "market_state" / symbol / "5m.parquet"
    return path.exists() or run_scoped.exists()


def _match_assets(filter_str: str, universe: List[str]) -> List[str]:
    if not filter_str or filter_str == "*":
        return universe
    filters = [f.strip().upper() for f in filter_str.split("|") if f.strip()]
    return [s for s in universe if any(f in s for f in filters)]


def _load_state_registry(repo_root: Path) -> Dict[str, object]:
    path = ontology_spec_paths(repo_root).get("state_registry")
    if not path or not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    return payload if isinstance(payload, dict) else {}


def _build_state_expansion_maps(state_registry: Dict[str, object]) -> tuple[Dict[str, List[Dict[str, object]]], Dict[str, List[Dict[str, object]]]]:
    source_map: Dict[str, List[Dict[str, object]]] = {}
    family_safe_map: Dict[str, List[Dict[str, object]]] = {}
    records = normalize_state_registry_records(state_registry)
    for row in sorted(records, key=lambda r: str(r.get("state_id", ""))):
        source_event = str(row.get("source_event_type", "")).strip().upper()
        family = str(row.get("family", "")).strip().upper()
        state_scope = str(row.get("state_scope", "source_only")).strip().lower()
        source_map.setdefault(source_event, []).append(row)
        if state_scope in {"family_safe", "global"} and family:
            family_safe_map.setdefault(family, []).append(row)
    return source_map, family_safe_map

def main() -> int:
    parser = argparse.ArgumentParser(description="Knowledge Atlas: Run-time Candidate Plan Enumerator")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True, help="Symbols to consider for this run")
    parser.add_argument("--atlas_dir", default="atlas")
    parser.add_argument("--out_dir", default=None)
    parser.add_argument(
        "--allow_ontology_hash_mismatch",
        type=int,
        default=0,
        help="If 1, allow ontology hash drift between templates/linkage/run-manifest/current specs.",
    )
    parser.add_argument("--max_source_states_per_event", type=int, default=3)
    parser.add_argument("--max_family_safe_states_per_event", type=int, default=2)
    parser.add_argument("--min_events_per_state", type=int, default=200)
    parser.add_argument(
        "--hypothesis_spec_strict",
        type=int,
        default=0,
        help="If 1, fail when active hypothesis spec keys are missing from joined condition columns.",
    )
    parser.add_argument(
        "--hypothesis_spec_enabled",
        type=int,
        default=1,
        help="If 1, bind candidate rows to active spec/hypotheses/*.yaml translator output.",
    )
    parser.add_argument(
        "--state_expansion_strict",
        type=int,
        default=0,
        help="If 1, fail when state expansion exceeds per-event caps; otherwise truncate deterministically.",
    )
    args = parser.parse_args()

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "hypothesis_generator" / args.run_id
    ensure_dir(out_dir)

    run_symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    
    params = vars(args)
    params["budgets"] = {
        "max_total": MAX_TOTAL_CANDIDATES,
        "max_per_claim": MAX_CANDIDATES_PER_CLAIM,
        "max_symbols": MAX_SYMBOLS_PER_TEMPLATE
    }
    
    manifest = start_manifest("generate_candidate_plan", args.run_id, params, [], [])
    guard = FeasibilityGuard(PROJECT_ROOT.parent, DATA_ROOT, args.run_id)

    try:
        atlas_dir = PROJECT_ROOT.parent / args.atlas_dir
        templates_path = atlas_dir / "candidate_templates.parquet"
        if not templates_path.exists():
            raise FileNotFoundError(f"Candidate templates not found: {templates_path}")
        
        templates_df = pd.read_parquet(templates_path)
        if not templates_df.empty:
            validate_candidate_templates_schema(templates_df)

        allow_hash_mismatch = bool(int(args.allow_ontology_hash_mismatch))
        state_expansion_strict = bool(int(args.state_expansion_strict))
        hypothesis_spec_enabled = bool(int(args.hypothesis_spec_enabled))
        hypothesis_spec_strict = bool(int(args.hypothesis_spec_strict))
        current_ontology_hash = ontology_spec_hash(PROJECT_ROOT.parent)
        current_component_hashes = ontology_component_hashes(PROJECT_ROOT.parent)
        current_component_fields = ontology_component_hash_fields(current_component_hashes)
        state_registry = _load_state_registry(PROJECT_ROOT.parent)
        source_state_map, family_safe_state_map = _build_state_expansion_maps(state_registry)
        hypothesis_specs = (
            load_active_hypothesis_specs(PROJECT_ROOT.parent)
            if hypothesis_spec_enabled
            else []
        )
        template_side_policy = load_template_side_policy(PROJECT_ROOT.parent)
        implemented_event_types = {
            str(event_type).strip().upper()
            for event_type in EVENT_REGISTRY_SPECS.keys()
            if str(event_type).strip()
        }
        template_ontology_hash = choose_template_ontology_hash(templates_df)
        if not template_ontology_hash:
            raise ValueError("candidate_templates missing or inconsistent ontology_spec_hash values")

        linkage_hash = load_ontology_linkage_hash(atlas_dir)
        run_manifest_hashes = load_run_manifest_hashes(DATA_ROOT, args.run_id)
        run_manifest_ontology_hash = run_manifest_hashes.get("ontology_spec_hash")

        hash_mismatches = compare_hash_fields(
            template_ontology_hash,
            [
                ("current_specs", current_ontology_hash),
                ("ontology_linkage", linkage_hash),
                ("run_manifest", run_manifest_ontology_hash),
            ],
        )
        if hash_mismatches and not allow_hash_mismatch:
            raise ValueError(
                "Ontology hash mismatch detected; rerun template generation/plan or pass "
                "--allow_ontology_hash_mismatch 1. Details: " + "; ".join(hash_mismatches)
            )
        
        plan_rows = []
        seen_plan_ids = set()
        plan_duplicates = 0
        feasibility_report = []
        hypothesis_audit_rows: List[Dict[str, object]] = []
        condition_contract_cache: Dict[str, Dict[str, Set[str]]] = {}
        total_count = 0
        # Load Atlas Exclusions
        exclusions_path = DATA_ROOT / "reports" / "atlas_verification" / args.run_id / "planner_excluded_claims.json"
        excluded_claims = set()
        
        exclusions_hash = None
        exclusions_count = 0
        
        if exclusions_path.exists():
            try:
                excl_bytes = exclusions_path.read_bytes()
                ex_data = json.loads(excl_bytes.decode("utf-8"))
                for e in ex_data:
                    if "claim_id" in e:
                        excluded_claims.add(e["claim_id"])
                
                exclusions_hash = "sha256:" + hashlib.sha256(excl_bytes).hexdigest()
                exclusions_count = len(excluded_claims)
            except Exception as e:
                logging.getLogger(__name__).warning(f"Failed to read exclusions file: {e}")
                # User requested: Fail closed if exclusions exist but cannot be parsed.
                raise ValueError(f"Failed to parse planner_excluded_claims.json: {e}")

        def _expand_hypothesis_rows(
            *,
            base_row: Dict[str, object],
            claim_id: str,
            template_id: str,
            available_condition_keys: Set[str],
        ) -> List[Dict[str, object]]:
            if (not hypothesis_spec_enabled) or (not hypothesis_specs):
                row = dict(base_row)
                row["hypothesis_id"] = "HYPOTHESIS_UNBOUND"
                row["hypothesis_version"] = 0
                row["hypothesis_spec_path"] = ""
                row["hypothesis_spec_hash"] = ""
                row["template_id"] = str(row.get("rule_template", "")).strip()
                row["horizon_bars"] = 12
                row["entry_lag_bars"] = int(row.get("entry_lag_bars", 0) or 0)
                row["direction_rule"] = "both"
                cond = row.get("conditioning", {}) if isinstance(row.get("conditioning", {}), dict) else {}
                if cond:
                    parts = [f"{k}={cond[k]}" for k in sorted(cond.keys())]
                    row["condition_signature"] = "&".join(parts)
                    row["condition"] = " AND ".join([f'{k} == "{cond[k]}"' for k in sorted(cond.keys())])
                else:
                    row["condition_signature"] = "all"
                    row["condition"] = "all"
                row["hypothesis_metric"] = "lift_bps"
                row["hypothesis_output_schema"] = [
                    "lift_bps",
                    "p_value",
                    "q_value",
                    "n",
                    "effect_ci",
                    "stability_score",
                    "net_after_cost",
                ]
                row["candidate_id"] = "cand_" + hashlib.sha256(
                    str(row.get("plan_row_id", "")).encode("utf-8")
                ).hexdigest()[:24]
                row["candidate_hash_inputs"] = ""
                return [row]

            translated_rows, translated_audit = translate_candidate_hypotheses(
                base_candidate=base_row,
                hypothesis_specs=hypothesis_specs,
                available_condition_keys=available_condition_keys,
                template_side_policy=template_side_policy,
                strict=hypothesis_spec_strict,
                implemented_event_types=implemented_event_types,
            )
            for a in translated_audit:
                audit_row = dict(a)
                audit_row["template_id"] = template_id
                audit_row["claim_id"] = claim_id
                audit_row["plan_row_id"] = str(base_row.get("plan_row_id", ""))
                audit_row["symbol"] = str(base_row.get("symbol", ""))
                hypothesis_audit_rows.append(audit_row)
            return translated_rows


        # Templates are already sorted by priority from Stage 1
        for _, row in templates_df.iterrows():
            claim_id = row['source_claim_id']
            template_id = row['template_id']
            
            # Exclusion Check
            if claim_id in excluded_claims:
                feasibility_report.append({
                    "template_id": template_id,
                    "claim_id": claim_id,
                    "status": "blocked_non_executable_condition",
                    "reason": "Atlas claim tagged as non-executable (rolling blocked limit)"
                })
                continue

            # Feasibility Checks
            status = "ready"
            reason = ""
            object_type = str(row.get("object_type", "event")).strip().lower()
            runtime_event_type = str(row.get("runtime_event_type", row.get("event_type", ""))).strip() or None
            canonical_event_type = str(row.get("canonical_event_type", row.get("event_type", ""))).strip() or None
            event_type = canonical_event_type or None
            canonical_family = str(row.get("canonical_family", "")).strip().upper() or None
            row_ontology_hash = str(row.get("ontology_spec_hash", "")).strip()

            # 0. Ontology hash alignment for all objects
            if status == "ready" and row_ontology_hash != template_ontology_hash:
                status = "blocked_ontology_drift"
                reason = (
                    f"row ontology_spec_hash {row_ontology_hash or '<missing>'} "
                    f"!= plan ontology_spec_hash {template_ontology_hash}"
                )

            # 0b. Ontology contract checks for event objects
            if status == "ready" and object_type == "event":
                in_canonical_registry = bool_field(row.get("ontology_in_canonical_registry", False))
                unknown_templates = parse_list_field(row.get("ontology_unknown_templates", []))
                stored_event_type = str(row.get("event_type", "")).strip()
                if not in_canonical_registry:
                    status = "blocked_ontology_invalid"
                    reason = "event candidate not present in canonical registry"
                elif unknown_templates:
                    status = "blocked_ontology_invalid"
                    reason = f"unknown templates for event candidate: {unknown_templates}"
                elif not canonical_event_type:
                    status = "blocked_ontology_invalid"
                    reason = "canonical_event_type missing"
                elif stored_event_type and stored_event_type != canonical_event_type and (
                    not runtime_event_type or stored_event_type != runtime_event_type
                ):
                    status = "blocked_ontology_invalid"
                    reason = (
                        "event_type is neither canonical_event_type nor runtime_event_type "
                        f"(event_type={stored_event_type}, canonical={canonical_event_type}, runtime={runtime_event_type})"
                    )
            
            # 1. Spec Check
            target_spec_path = str(row['target_spec_path'])
            if object_type == "event" and canonical_event_type:
                target_spec_path = target_spec_path.replace("{event_type}", canonical_event_type)
            if not _check_spec_exists(target_spec_path):
                status = "blocked_missing_spec"
                reason = f"spec missing at {target_spec_path}"
            
            # 2. Registry Check
            if status == "ready" and event_type and event_type not in EVENT_REGISTRY_SPECS:
                status = "blocked_missing_registry"
                reason = f"event_type {event_type} not in registry"
            
            # 3. Asset Filtering
            eligible_symbols = _match_assets(row['assets_filter'], run_symbols)
            if status == "ready" and not eligible_symbols:
                status = "blocked_asset_mismatch"
                reason = f"no symbols match asset filter {row['assets_filter']}"
            
            if status != "ready":
                feasibility_report.append({
                    "template_id": template_id,
                    "claim_id": claim_id,
                    "status": status,
                    "reason": reason
                })
                continue

            eligible_symbols = eligible_symbols[:MAX_SYMBOLS_PER_TEMPLATE]
            claim_candidates = 0

            state_variants: List[Dict[str, object]] = [{"state_id": None, "state_provenance": "none"}]
            if object_type == "event" and canonical_event_type:
                source_states = list(source_state_map.get(canonical_event_type.upper(), []))
                if len(source_states) > int(args.max_source_states_per_event):
                    if state_expansion_strict:
                        raise ValueError(
                            f"state expansion exceeds max_source_states_per_event for {canonical_event_type}: "
                            f"{len(source_states)} > {int(args.max_source_states_per_event)}"
                        )
                    source_states = source_states[: int(args.max_source_states_per_event)]
                source_ids = {str(s.get("state_id", "")).strip().upper() for s in source_states}
                family_states = []
                if canonical_family:
                    family_states = [
                        s for s in list(family_safe_state_map.get(canonical_family, []))
                        if str(s.get("state_id", "")).strip().upper() not in source_ids
                    ]
                if len(family_states) > int(args.max_family_safe_states_per_event):
                    if state_expansion_strict:
                        raise ValueError(
                            f"state expansion exceeds max_family_safe_states_per_event for {canonical_event_type}: "
                            f"{len(family_states)} > {int(args.max_family_safe_states_per_event)}"
                        )
                    family_states = family_states[: int(args.max_family_safe_states_per_event)]

                for state_row in source_states:
                    entry = dict(state_row)
                    entry["state_provenance"] = "source"
                    state_variants.append(entry)
                for state_row in family_states:
                    entry = dict(state_row)
                    entry["state_provenance"] = "family"
                    state_variants.append(entry)
            
            for symbol in eligible_symbols:
                if total_count >= MAX_TOTAL_CANDIDATES or claim_candidates >= MAX_CANDIDATES_PER_CLAIM:
                    break
                
                # 4. Data Feasibility Guard (Deep Check)
                is_feasible, fail_reason = guard.check_feasibility(target_spec_path, symbol)
                if not is_feasible:
                    feasibility_report.append({
                        "template_id": template_id,
                        "symbol": symbol,
                        "status": "blocked_missing_dataset",
                        "reason": fail_reason
                    })
                    continue
                if symbol not in condition_contract_cache:
                    condition_contract_cache[symbol] = load_symbol_joined_condition_contract(
                        data_root=DATA_ROOT,
                        run_id=args.run_id,
                        symbol=symbol,
                        timeframe="5m",
                    )
                available_condition_keys = load_symbol_joined_condition_keys(
                    data_root=DATA_ROOT,
                    run_id=args.run_id,
                    symbol=symbol,
                    timeframe="5m",
                    include_soft_defaults=not hypothesis_spec_strict,
                )
                if hypothesis_spec_strict and not available_condition_keys:
                    available_sample = format_available_key_sample(
                        condition_contract_cache[symbol].get("keys", set())
                    )
                    raise ValueError(
                        f"No joined condition keys available for symbol {symbol}. "
                        f"Strict hypothesis validation requires real joined keys. Available: {available_sample}"
                    )
                
                for state_row in state_variants:
                    raw_state_id = state_row.get("state_id")
                    state_id = (str(raw_state_id).strip() if raw_state_id is not None else "") or None
                    state_token = state_id or "NO_STATE"
                    state_provenance = str(state_row.get("state_provenance", "none")).strip() or "none"
                    state_activation = str(state_row.get("activation_rule", "")).strip() or None
                    state_scope = str(state_row.get("state_scope", "")).strip() or None
                    state_min_events = int(state_row.get("min_events", 0) or 0)
                    state_activation_hash = None
                    if state_activation:
                        state_activation_hash = "sha256:" + hashlib.sha256(
                            (
                                state_activation
                                + "|"
                                + str(state_row.get("decay_rule", "")).strip()
                            ).encode("utf-8")
                        ).hexdigest()
                    min_events_required = int(row['min_events'])
                    if state_id:
                        min_events_required = max(
                            min_events_required,
                            int(args.min_events_per_state),
                            state_min_events,
                        )

                    for rule in row['rule_templates']:
                        for horizon in row['horizons'][:MAX_HORIZONS_PER_TEMPLATE]:
                        # conditioning variants
                            cond_config = row['conditioning']
                        
                            # Add "all" (base) variant
                            plan_row_id = (
                                f"{claim_id}:{event_type or row.get('feature_name')}:{rule}:{horizon}:{state_token}:all:{symbol}"
                            )
                            plan_row = {
                                "plan_row_id": plan_row_id,
                                "source_claim_ids": [claim_id],
                                "source_concept_ids": [row['concept_id']],
                                "object_type": object_type,
                                "runtime_event_type": runtime_event_type,
                                "canonical_event_type": canonical_event_type,
                                "canonical_family": canonical_family,
                                "event_type": event_type or "microstructure_proxy",
                                "rule_template": rule,
                                "horizon": horizon,
                                "symbol": symbol,
                                "state_id": state_id,
                                "state_provenance": state_provenance,
                                "state_scope": state_scope,
                                "state_activation": state_activation,
                                "state_activation_hash": state_activation_hash,
                                "conditioning": {},
                                "min_events": min_events_required,
                                "state_min_events": state_min_events if state_id else 0,
                                "ontology_spec_hash": template_ontology_hash,
                                "ontology_in_canonical_registry": bool_field(row.get("ontology_in_canonical_registry", False)),
                                "ontology_unknown_templates": parse_list_field(row.get("ontology_unknown_templates", [])),
                            }
                            translated_rows = _expand_hypothesis_rows(
                                base_row=plan_row,
                                claim_id=claim_id,
                                template_id=template_id,
                                available_condition_keys=available_condition_keys,
                            )
                            for translated in translated_rows:
                                if total_count >= MAX_TOTAL_CANDIDATES or claim_candidates >= MAX_CANDIDATES_PER_CLAIM:
                                    break
                                translated_row_id = str(translated.get("plan_row_id", plan_row_id)).strip() or plan_row_id
                                hypothesis_id = str(translated.get("hypothesis_id", "")).strip()
                                if hypothesis_id:
                                    translated_row_id = f"{translated_row_id}:{hypothesis_id}"
                                    translated["plan_row_id"] = translated_row_id
                                if translated_row_id in seen_plan_ids:
                                    plan_duplicates += 1
                                    continue
                                seen_plan_ids.add(translated_row_id)
                                plan_rows.append(translated)
                                total_count += 1
                                claim_candidates += 1
                        
                            # Add buckets
                            for c_key, c_vals in cond_config.items():
                                # Feasibility checks for state variables
                                market_context_required = ["vol_regime", "funding_bps", "regime_vol_liquidity"]
                                if c_key in market_context_required and not _check_market_context_exists(symbol, args.run_id):
                                    feasibility_report.append({
                                        "template_id": template_id,
                                        "symbol": symbol,
                                        "status": "blocked_missing_dataset",
                                        "reason": f"market_context missing for {c_key} on {symbol}"
                                    })
                                    continue
                                
                                # VPIN check (assuming it's in features or market context)
                                if c_key == "vpin" and not _check_market_context_exists(symbol, args.run_id):
                                    feasibility_report.append({
                                        "template_id": template_id,
                                        "symbol": symbol,
                                        "status": "blocked_missing_dataset",
                                        "reason": f"market_context (or features) missing for vpin on {symbol}"
                                    })
                                    continue
                                    
                                for c_val in c_vals[:MAX_CONDITIONING_VARIANTS]:
                                    if total_count >= MAX_TOTAL_CANDIDATES or claim_candidates >= MAX_CANDIDATES_PER_CLAIM:
                                        break
                                    
                                    plan_row_id = (
                                        f"{claim_id}:{event_type or row.get('feature_name')}:{rule}:{horizon}:"
                                        f"{state_token}:{c_key}_{c_val}:{symbol}"
                                    )
                                    plan_row = {
                                        "plan_row_id": plan_row_id,
                                        "source_claim_ids": [claim_id],
                                        "source_concept_ids": [row['concept_id']],
                                        "object_type": object_type,
                                        "runtime_event_type": runtime_event_type,
                                        "canonical_event_type": canonical_event_type,
                                        "canonical_family": canonical_family,
                                        "event_type": event_type or "microstructure_proxy",
                                        "rule_template": rule,
                                        "horizon": horizon,
                                        "symbol": symbol,
                                        "state_id": state_id,
                                        "state_provenance": state_provenance,
                                        "state_scope": state_scope,
                                        "state_activation": state_activation,
                                        "state_activation_hash": state_activation_hash,
                                        "conditioning": {c_key: c_val},
                                        "min_events": min_events_required,
                                        "state_min_events": state_min_events if state_id else 0,
                                        "ontology_spec_hash": template_ontology_hash,
                                        "ontology_in_canonical_registry": bool_field(row.get("ontology_in_canonical_registry", False)),
                                        "ontology_unknown_templates": parse_list_field(row.get("ontology_unknown_templates", [])),
                                    }
                                    translated_rows = _expand_hypothesis_rows(
                                        base_row=plan_row,
                                        claim_id=claim_id,
                                        template_id=template_id,
                                        available_condition_keys=available_condition_keys,
                                    )
                                    for translated in translated_rows:
                                        if total_count >= MAX_TOTAL_CANDIDATES or claim_candidates >= MAX_CANDIDATES_PER_CLAIM:
                                            break
                                        translated_row_id = str(translated.get("plan_row_id", plan_row_id)).strip() or plan_row_id
                                        hypothesis_id = str(translated.get("hypothesis_id", "")).strip()
                                        if hypothesis_id:
                                            translated_row_id = f"{translated_row_id}:{hypothesis_id}"
                                            translated["plan_row_id"] = translated_row_id
                                        if translated_row_id in seen_plan_ids:
                                            plan_duplicates += 1
                                            continue
                                        seen_plan_ids.add(translated_row_id)
                                        plan_rows.append(translated)
                                        total_count += 1
                                        claim_candidates += 1
            
            feasibility_report.append({
                "template_id": template_id,
                "claim_id": claim_id,
                "status": "ready",
                "candidates_enumerated": claim_candidates
            })

        # Assert and abort if duplicates generated according to new rules
        if plan_duplicates > 0:
            raise ValueError(f"Plan duplicate count > 0: detected {plan_duplicates} duplicated candidate configurations.")

        # Write Plan
        plan_path = out_dir / "candidate_plan.jsonl"
        with plan_path.open("w", encoding="utf-8") as f:
            for row in plan_rows:
                f.write(json.dumps(row) + "\n")
        
        # Write Feasibility Report
        pd.DataFrame(feasibility_report).to_parquet(out_dir / "plan_feasibility_report.parquet", index=False)
        hypothesis_audit_df = pd.DataFrame(hypothesis_audit_rows)
        hypothesis_audit_path = out_dir / "hypothesis_execution_audit.parquet"
        hypothesis_audit_df.to_parquet(hypothesis_audit_path, index=False)
        
        # Generate Hash
        plan_bytes = plan_path.read_bytes()
        plan_hash = "sha256:" + hashlib.sha256(plan_bytes).hexdigest()
        
        # Populate inputs_hashes dictionary securely (Standardize "hash field names")
        
        inputs_hashes = {
            "candidate_plan": {
                "path": str(plan_path.relative_to(PROJECT_ROOT.parent)),
                "hash": plan_hash,
                "rows": len(plan_rows)
            },
            "ontology": {
                "ontology_spec_hash": template_ontology_hash,
                "taxonomy_hash": current_component_fields.get("taxonomy_hash"),
                "canonical_event_registry_hash": current_component_fields.get("canonical_event_registry_hash"),
                "state_registry_hash": current_component_fields.get("state_registry_hash"),
                "verb_lexicon_hash": current_component_fields.get("verb_lexicon_hash"),
                "hash_mismatch_allowed": bool(allow_hash_mismatch),
                "hash_mismatches": list(hash_mismatches),
            },
            "planner_exclusions": {
                "path": str(exclusions_path.relative_to(PROJECT_ROOT.parent)) if exclusions_path.exists() else None,
                "hash": exclusions_hash,
                "excluded_claims_count": exclusions_count,
                "mode": "file" if exclusions_path.exists() else "none"
            },
            "hypothesis_specs": {
                "enabled": bool(hypothesis_spec_enabled),
                "strict": bool(hypothesis_spec_strict),
                "active_specs": [str(spec.get("hypothesis_id", "")) for spec in hypothesis_specs],
                "audit_path": str(hypothesis_audit_path.relative_to(PROJECT_ROOT.parent)),
                "audit_rows": int(len(hypothesis_audit_df)),
            }
        }
        hypothesis_status_counts = {
            "executed": 0,
            "skipped_event_not_implemented": 0,
            "skipped_missing_condition_key": 0,
            "skipped_disabled": 0,
        }
        if not hypothesis_audit_df.empty and "status" in hypothesis_audit_df.columns:
            observed = {
                str(k): int(v)
                for k, v in hypothesis_audit_df["status"].value_counts(dropna=False).to_dict().items()
            }
            for key, value in observed.items():
                hypothesis_status_counts[key] = int(value)

        finalize_manifest(manifest, "success", stats={
            "total_candidates": len(plan_rows),
            "inputs_hashes": inputs_hashes,
            # Duplicate as convenience root keys as requested
            "candidate_plan_hash": plan_hash,
            "planner_exclusions_hash": exclusions_hash,
            "ontology_spec_hash": template_ontology_hash,
            "taxonomy_hash": current_component_fields.get("taxonomy_hash"),
            "canonical_event_registry_hash": current_component_fields.get("canonical_event_registry_hash"),
            "state_registry_hash": current_component_fields.get("state_registry_hash"),
            "verb_lexicon_hash": current_component_fields.get("verb_lexicon_hash"),
            "ontology_hash_mismatches": list(hash_mismatches),
            "hypothesis_spec_enabled": int(hypothesis_spec_enabled),
            "hypothesis_spec_strict": int(hypothesis_spec_strict),
            "hypothesis_specs_executed": int(sum(1 for r in hypothesis_audit_rows if str(r.get("status", "")).startswith("executed"))),
            "hypothesis_audit_status_counts": hypothesis_status_counts,
        })

        
        # Explicitly append to global run manifest
        global_manifest_path = DATA_ROOT / "runs" / args.run_id / "run_manifest.json"
        if global_manifest_path.exists():
            try:
                g_manifest = json.loads(global_manifest_path.read_text(encoding="utf-8"))
                g_manifest["inputs_hashes"] = inputs_hashes
                g_manifest["candidate_plan_hash"] = plan_hash
                g_manifest["planner_exclusions_hash"] = exclusions_hash
                g_manifest["ontology_spec_hash"] = template_ontology_hash
                g_manifest["taxonomy_hash"] = current_component_fields.get("taxonomy_hash")
                g_manifest["canonical_event_registry_hash"] = current_component_fields.get("canonical_event_registry_hash")
                g_manifest["state_registry_hash"] = current_component_fields.get("state_registry_hash")
                g_manifest["verb_lexicon_hash"] = current_component_fields.get("verb_lexicon_hash")
                global_manifest_path.write_text(json.dumps(g_manifest, indent=2, sort_keys=True), encoding="utf-8")
            except Exception as e:
                logging.getLogger(__name__).warning(f"Failed to update global run manifest: {e}")

        # Output hash to stdout for orchestrator
        print(f"CANDIDATE_PLAN_HASH={plan_hash}")
        return 0

    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc))
        return 1

if __name__ == "__main__":
    sys.exit(main())
