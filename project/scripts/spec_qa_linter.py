import os
import yaml
import sys
from pathlib import Path
from typing import Dict, List, Set

KNOWN_DATASETS = {
    "perp_ohlcv_1m",
    "spot_ohlcv_1m",
    "perp_ohlcv_15m",
    "tob_1s",
    "tob_1m_agg",
    "basis_1m",
    "um_funding_rates",
    "um_open_interest_hist",
    "events.parquet",
    "forward_labels",
    "run_manifest.json",
    "validation_results",
    "equity_curves.parquet",
    "universe_snapshots.parquet",
    "feature_vectors",
    "event_flags",
    "event_registry",
    "sources",
    "fragments.jsonl",
    "blueprints.jsonl",
    "cleaned_1m",
    "cleaned_bars",
}

def load_specs(spec_dir: Path) -> Dict[str, dict]:
    specs = {}
    for yaml_file in spec_dir.glob("*.yaml"):
        with open(yaml_file, "r") as f:
            spec = yaml.safe_load(f)
            specs[spec["concept_id"]] = spec
    return specs

def check_cycles(specs: Dict[str, dict]):
    graph = {cid: spec.get("dependencies", []) for cid, spec in specs.items()}
    visited = set()
    path = []

    def visit(node):
        if node in path:
            print(f"ERROR: Cycle detected: {' -> '.join(path + [node])}")
            return False
        if node in visited:
            return True
        
        visited.add(node)
        path.append(node)
        for neighbor in graph.get(node, []):
            if not visit(neighbor):
                return False
        path.pop()
        return True

    for node in graph:
        if node not in visited:
            if not visit(node):
                sys.exit(1)
    print("SUCCESS: No cycles in dependency DAG.")

def check_datasets(specs: Dict[str, dict]):
    errors = 0
    for cid, spec in specs.items():
        for req in spec.get("data_requirements", []):
            ds = req.get("dataset")
            if ds not in KNOWN_DATASETS:
                print(f"ERROR: Unknown dataset '{ds}' in concept {cid}")
                errors += 1
    if errors == 0:
        print("SUCCESS: All dataset contracts valid.")
    else:
        sys.exit(1)

def check_tests(specs: Dict[str, dict]):
    test_ids = set()
    errors = 0
    for cid, spec in specs.items():
        for test in spec.get("tests", []):
            tid = test.get("id")
            if tid in test_ids:
                print(f"ERROR: Duplicate test ID '{tid}' in concept {cid}")
                errors += 1
            test_ids.add(tid)
    if errors == 0:
        print(f"SUCCESS: Test catalog unique. Total tests: {len(test_ids)}")
    else:
        sys.exit(1)

def check_artifacts(specs: Dict[str, dict], project_root: Path):
    missing = []
    for cid, spec in specs.items():
        for art in spec.get("artifacts", []):
            path_str = art.get("path")
            if not path_str: continue
            
            # Handle {symbol} placeholders
            path_str = path_str.replace("{symbol}", "BTCUSDT")
            path = project_root / path_str
            if not path.exists():
                missing.append(f"{cid}: {path_str}")
    
    if missing:
        print("REPORT: Missing artifacts:")
        for m in missing:
            print(f"  - {m}")
    else:
        print("SUCCESS: All referenced artifacts exist.")

def main():
    project_root = Path(".").resolve()
    spec_dir = project_root / "spec" / "concepts"
    
    if not spec_dir.exists():
        print(f"ERROR: Spec directory {spec_dir} not found.")
        sys.exit(1)
        
    specs = load_specs(spec_dir)
    print(f"Loaded {len(specs)} concepts.")
    
    check_cycles(specs)
    check_datasets(specs)
    check_tests(specs)
    check_artifacts(specs, project_root)

if __name__ == "__main__":
    main()
