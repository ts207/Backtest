#!/usr/bin/env python3
import argparse
import json
import os
import sys
import yaml
from pathlib import Path
from typing import Dict, List, Set

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SPEC_ROOT = PROJECT_ROOT / "spec"
PROJECT_DIR = PROJECT_ROOT / "project"

def get_yaml_specs(subdir: str) -> Dict[str, dict]:
    specs = {}
    spec_path = SPEC_ROOT / subdir
    if not spec_path.exists():
        return specs
    for yaml_file in spec_path.glob("*.yaml"):
        try:
            with open(yaml_file, "r") as f:
                data = yaml.safe_load(f)
                if data:
                    specs[yaml_file.stem] = data
        except Exception as e:
            print(f"Error reading {yaml_file}: {e}")
    return specs

def audit_features():
    print("--- Auditing Features ---")
    specs = get_yaml_specs("features")
    impl_dir = PROJECT_DIR / "features"
    
    # Simple check: see if the feature name exists as a function in the feature family file
    # Or if a file with the family name exists.
    families = set(spec.get("feature_family") for spec in specs.values() if spec.get("feature_family"))
    
    errors = 0
    for family in families:
        impl_file = impl_dir / f"{family}.py"
        if not impl_file.exists():
            print(f"MISSING: Implementation file for family '{family}' ({impl_file})")
            errors += 1
        else:
            with open(impl_file, "r") as f:
                content = f.read()
            for name, spec in specs.items():
                if spec.get("feature_family") == family:
                    # Look for 'def calculate_{name}' or 'def {name}'
                    if f"def calculate_{name}" not in content and f"def {name}" not in content:
                        print(f"MISSING: Function for feature '{name}' in {impl_file}")
                        errors += 1
    
    if errors == 0:
        print("SUCCESS: All features have corresponding implementations.")
    return errors

def audit_events():
    print("\n--- Auditing Events ---")
    specs = get_yaml_specs("events")
    # Events are often registered in registry.py or have specialized scripts
    # This is a bit more complex, so we'll check if they are active and have registry fields
    errors = 0
    for name, spec in specs.items():
        if spec.get("kind") == "canonical_event_registry":
            continue
        required = {"event_type", "reports_dir", "events_file", "signal_column"}
        missing = required - set(spec.keys())
        if missing:
            print(f"MALFORMED: Event spec '{name}' is missing fields: {missing}")
            errors += 1
    
    if errors == 0:
        print("SUCCESS: All event specs are well-formed.")
    return errors

def sync_schemas():
    print("\n--- Syncing Schemas ---")
    # Example: Sync spec features to a JSON schema for validation
    specs = get_yaml_specs("features")
    schema_path = PROJECT_DIR / "schemas" / "feature_catalog.json"
    
    catalog = {
        "version": "1.0.0",
        "features": {name: {"family": s.get("feature_family"), "params": s.get("params")} for name, s in specs.items()}
    }
    
    with open(schema_path, "w") as f:
        json.dump(catalog, f, indent=2)
    print(f"UPDATED: {schema_path}")

def main():
    parser = argparse.ArgumentParser(description="Pipeline Governance Tool")
    parser.add_argument("--audit", action="store_true", help="Run audit on specs and implementations")
    parser.add_argument("--sync", action="store_true", help="Sync schemas and registries")
    args = parser.parse_args()

    total_errors = 0
    if args.audit:
        total_errors += audit_features()
        total_errors += audit_events()
    
    if args.sync:
        sync_schemas()
    
    if total_errors > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()
