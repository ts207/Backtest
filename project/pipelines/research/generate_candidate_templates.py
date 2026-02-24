from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.spec_loader import load_global_defaults

def _load_global_defaults() -> Dict[str, Any]:
    return load_global_defaults(project_root=PROJECT_ROOT)

GLOBAL_DEFAULTS = _load_global_defaults()

# Default configuration for expansion
DEFAULT_HORIZONS = GLOBAL_DEFAULTS.get("horizons", ["5m", "15m", "60m"])
DEFAULT_RULE_TEMPLATES = GLOBAL_DEFAULTS.get("rule_templates", ["mean_reversion", "continuation", "trend_continuation", "liquidity_reversion_v2"])
DEFAULT_CONDITIONING = GLOBAL_DEFAULTS.get("conditioning", {
    "vol_regime": ["high", "low"],
    "carry_state": ["pos", "neg", "neutral"],
    "severity_bucket": ["top_10pct", "extreme_5pct"],
    "funding_bps": ["extreme_pos", "extreme_neg"],
    "vpin": ["high_toxic"],
    "regime_vol_liquidity": ["high_vol_low_liq", "low_vol_high_liq"]
})

def _load_taxonomy() -> Dict[str, Any]:
    """Load event taxonomy from spec."""
    path = PROJECT_ROOT.parent / "spec" / "multiplicity" / "taxonomy.yaml"
    if not path.exists():
        return {}
    with open(path, "r") as f:
        return yaml.safe_load(f)

def _get_templates_for_event(event_type: str, taxonomy: Optional[Dict[str, Any]] = None) -> List[str]:
    """Determine rule templates for a given event type based on taxonomy."""
    if taxonomy is None:
        taxonomy = _load_taxonomy()
    
    event_type_norm = str(event_type).strip().lower()
    families = taxonomy.get("families", {})
    
    for family_id, family_cfg in families.items():
        events = [str(e).strip().lower() for e in family_cfg.get("events", [])]
        if event_type_norm in events:
            return family_cfg.get("templates", DEFAULT_RULE_TEMPLATES)
            
    return DEFAULT_RULE_TEMPLATES

def _extract_event_type(statement: str) -> Optional[str]:
    """Heuristically extract event type from statement."""
    match = re.search(r'""event_type"": ""([A-Z0-9_]+)""', statement)
    if match:
        return match.group(1)
    matches = re.findall(r'\b[A-Z][A-Z0-9_]{5,}\b', statement)
    if matches:
        return matches[0]
    return None

def main() -> int:
    parser = argparse.ArgumentParser(description="Knowledge Atlas: Global Candidate Template Generator")
    parser.add_argument("--backlog", default="research_backlog.csv")
    parser.add_argument("--atlas_dir", default="atlas")
    parser.add_argument("--attribution_report", default=None, help="Path to regime performance attribution report (Parquet)")
    args = parser.parse_args()

    atlas_dir = PROJECT_ROOT.parent / args.atlas_dir
    ensure_dir(atlas_dir)

    # Use a dummy run_id for manifest, as this is a global task
    manifest = start_manifest("generate_candidate_templates", "global_atlas", vars(args), [], [])

    try:
        backlog_path = PROJECT_ROOT.parent / args.backlog
        if not backlog_path.exists():
            raise FileNotFoundError(f"Backlog not found: {backlog_path}")
        
        df = pd.read_csv(backlog_path)
        taxonomy = _load_taxonomy()
        
        # 1. Load Performance Attribution (if provided)
        attribution_map = {}
        if args.attribution_report:
            attr_path = Path(args.attribution_report)
            if attr_path.exists():
                attr_df = pd.read_parquet(attr_path)
                # Map concept_id -> score (e.g. sharpe_ratio)
                if "concept_id" in attr_df.columns and "sharpe_ratio" in attr_df.columns:
                    # If multiple rows per concept (different regimes), take the mean or max
                    attribution_map = attr_df.groupby("concept_id")["sharpe_ratio"].mean().to_dict()
        
        def _get_attribution_bonus(row):
            concept_id = str(row.get("concept_id"))
            return attribution_map.get(concept_id, 0.0)

        df["regime_attribution_score"] = df.apply(_get_attribution_bonus, axis=1)
        # Higher attribution bonus reduces priority_score (making it higher priority)
        # We clip bonus to avoid negative priority_scores
        df["adjusted_priority_score"] = (df["priority_score"] - df["regime_attribution_score"]).clip(lower=0.0)

        # 2. Deterministic Ordering
        # priority_score (lower is higher priority), tie-break with claim_id
        df = df.sort_values(by=["adjusted_priority_score", "claim_id"]).reset_index(drop=True)
        
        # 2. Filter for operationalizable and unverified
        active_claims = df[(df['operationalizable'] == 'Y') & (df['status'] != 'verified')].copy()
        
        candidate_templates = []
        spec_tasks = []
        
        print("Iterating over active claims...")
        for i, row in active_claims.iterrows():
            try:
                claim_id = str(row['claim_id'])
                c_type = str(row['candidate_type']).lower() # event or feature
                statement = str(row['statement_summary'])
                target_pattern = str(row['next_artifact'])
                # print(f"Processing claim: {claim_id}, type: {c_type}")
                
                if c_type == 'event':
                    event_type = _extract_event_type(statement)
                    if not event_type:
                        continue
                    
                    target_path = target_pattern.replace("{event_type}", event_type)
                    spec_exists = (PROJECT_ROOT.parent / target_path).exists()
                    
                    if not spec_exists:
                        spec_tasks.append({
                            "claim_id": claim_id,
                            "concept_id": row['concept_id'],
                            "object_type": "event",
                            "target_path": target_path,
                            "priority_score": row['priority_score'],
                            "statement": statement,
                            "assets_filter": row['assets']
                        })
                    
                    # Add template regardless of spec existence (Stage 2 will filter)
                    candidate_templates.append({
                        "template_id": f"{claim_id}@{event_type}",
                        "source_claim_id": claim_id,
                        "concept_id": row['concept_id'],
                        "object_type": "event",
                        "event_type": event_type,
                        "target_spec_path": target_path,
                        "rule_templates": _get_templates_for_event(event_type, taxonomy),
                        "horizons": DEFAULT_HORIZONS,
                        "conditioning": DEFAULT_CONDITIONING,
                        "assets_filter": row['assets'],
                        "min_events": 50,
                        "label_type": "returns",
                        "regime_attribution_score": row['regime_attribution_score']
                    })

                elif c_type == 'feature':
                    # Features like ROLL/VPIN
                    features_val = str(row.get('features', ''))
                    if not features_val or features_val == 'nan':
                        continue
                    features_list = features_val.split('|')
                    for feat in features_list:
                        target_path = target_pattern.replace("{feature_name}", feat)
                        spec_exists = (PROJECT_ROOT.parent / target_path).exists()
                        
                        if not spec_exists:
                            spec_tasks.append({
                                "claim_id": claim_id,
                                "concept_id": row['concept_id'],
                                "object_type": "feature",
                                "target_path": target_path,
                                "priority_score": row['priority_score'],
                                "statement": statement,
                                "assets_filter": row['assets']
                            })
                        
                        candidate_templates.append({
                            "template_id": f"{claim_id}@{feat}",
                            "source_claim_id": claim_id,
                            "concept_id": row['concept_id'],
                            "object_type": "feature",
                            "feature_name": feat,
                            "target_spec_path": target_path,
                            "rule_templates": ["feature_conditioned_prediction"],
                            "horizons": DEFAULT_HORIZONS,
                            "conditioning": {"vol_regime": ["high"]},
                            "assets_filter": row['assets'],
                            "min_events": 50,
                            "label_type": "RV" if "vol" in statement.lower() or "rv" in statement.lower() else "returns",
                            "regime_attribution_score": row['regime_attribution_score']
                        })
            except Exception as e:
                print(f"Error processing claim {row.get('claim_id')}: {e}")
                continue

        # Save Global Artifacts
        templates_df = pd.DataFrame(candidate_templates)
        templates_df.to_parquet(atlas_dir / "candidate_templates.parquet", index=False)
        
        tasks_df = pd.DataFrame(spec_tasks)
        tasks_df.to_parquet(atlas_dir / "spec_tasks.parquet", index=False)
        
        # Human-readable index
        with (atlas_dir / "template_index.md").open("w", encoding="utf-8") as f:
            f.write("# Knowledge Atlas: Candidate Templates\n\n")
            f.write(f"Generated from `{args.backlog}`\n\n")
            if not templates_df.empty:
                f.write(templates_df[["template_id", "object_type", "event_type", "regime_attribution_score", "assets_filter"]].to_markdown(index=False))
            else:
                f.write("No active templates found.\n")

        finalize_manifest(manifest, "success", stats={
            "templates_count": len(candidate_templates),
            "spec_tasks_count": len(spec_tasks)
        })
        return 0

    except Exception as exc:
        import traceback
        traceback.print_exc()
        finalize_manifest(manifest, "failed", error=str(exc))
        return 1

if __name__ == "__main__":
    sys.exit(main())
