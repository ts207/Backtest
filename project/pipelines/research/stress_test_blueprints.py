from __future__ import annotations

import argparse
import json
import os
import re
import sys
import subprocess
from pathlib import Path
from typing import Dict, List, Any

import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.run_manifest import finalize_manifest, start_manifest

NUMERIC_CONDITION_PATTERN = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(>=|<=|==|>|<)\s*(-?\d+(?:\.\d+)?)\s*$")

def _generate_variants(blueprint: Dict[str, Any]) -> List[Dict[str, Any]]:
    variants = []
    # Base case (original)
    variants.append(blueprint)
    
    # Simple neighborhood: +/- 10% on numeric thresholds in entry conditions
    entry = blueprint.get("entry", {})
    conditions = entry.get("conditions", [])
    
    for i, cond in enumerate(conditions):
        match = NUMERIC_CONDITION_PATTERN.match(str(cond))
        if match:
            feature, op, val_str = match.groups()
            val = float(val_str)
            if abs(val) < 1e-9:
                # If 0, try small epsilon steps? Or skip.
                # For adverse_proxy > 0.0, we might want > 0.05, > -0.05
                steps = [0.05, -0.05]
            else:
                steps = [val * 1.1, val * 0.9]
            
            for step_val in steps:
                new_val = val if abs(val) < 1e-9 else step_val # Logic for 0 is tricky with mult.
                if abs(val) < 1e-9:
                    new_val = val + step_val
                
                new_cond = f"{feature} {op} {new_val:.6f}"
                variant = json.loads(json.dumps(blueprint)) # Deep copy
                variant["entry"]["conditions"][i] = new_cond
                variant["id"] = f"{blueprint['id']}_var_{new_val:.4f}"
                variants.append(variant)
                
    return variants

def _load_returns(engine_dir: Path, strategy_id: str) -> pd.DataFrame:
    path = engine_dir / f"strategy_returns_{strategy_id}.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)

def main() -> int:
    parser = argparse.ArgumentParser(description="Stress test strategy blueprints")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--blueprints_path", default=None)
    args = parser.parse_args()

    blueprints_path = (
        Path(args.blueprints_path)
        if args.blueprints_path
        else DATA_ROOT / "reports" / "strategy_blueprints" / args.run_id / "blueprints.jsonl"
    )
    
    if not blueprints_path.exists():
        print(f"Blueprints not found: {blueprints_path}", file=sys.stderr)
        return 1

    params = {"run_id": args.run_id}
    manifest = start_manifest("stress_test_blueprints", args.run_id, params, [], [])

    try:
        blueprints = []
        with open(blueprints_path) as f:
            for line in f:
                if line.strip():
                    blueprints.append(json.loads(line))
        
        if not blueprints:
            print("No blueprints to stress test.")
            finalize_manifest(manifest, "success", stats={"blueprints": 0})
            return 0

        blueprint_variants_map = {}
        expanded_blueprints = []
        for bp in blueprints:
            vars_for_bp = _generate_variants(bp)
            blueprint_variants_map[bp["id"]] = [v["id"] for v in vars_for_bp]
            expanded_blueprints.extend(vars_for_bp)
            
        # Write expanded blueprints
        expanded_path = blueprints_path.parent / "blueprints_expanded.jsonl"
        with open(expanded_path, "w") as f:
            for bp in expanded_blueprints:
                f.write(json.dumps(bp) + "\n")
                
        print(f"Generated {len(expanded_blueprints)} variants from {len(blueprints)} blueprints.")
        
        # Run Backtest on expanded
        stress_run_id = f"{args.run_id}_stress"
        cmd = [
            sys.executable,
            str(PROJECT_ROOT / "pipelines" / "backtest" / "backtest_strategies.py"),
            "--run_id", stress_run_id,
            "--blueprints_path", str(expanded_path),
            "--symbols", "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,ADAUSDT,DOGEUSDT,AVAXUSDT,LINKUSDT,MATICUSDT,DOTUSDT",
            "--force", "1" 
        ]
        
        print(f"Running backtest on variants...")
        subprocess.run(cmd, check=True)
        
        # Analyze Results
        engine_dir = DATA_ROOT / "runs" / stress_run_id / "engine"
        
        stress_results = []
        pass_count = 0
        
        for bp in blueprints:
            bp_id = bp["id"]
            variant_ids = blueprint_variants_map[bp_id]
            
            variant_metrics = []
            for v_id in variant_ids:
                strategy_id = f"dsl_interpreter_v1__{v_id}"
                returns = _load_returns(engine_dir, strategy_id)
                if not returns.empty:
                    exp = float(returns["pnl"].mean())
                    variant_metrics.append(exp)
                else:
                    variant_metrics.append(-np.inf)
            
            orig_exp = variant_metrics[0] if variant_metrics else -np.inf
            worst_case = float(min(variant_metrics)) if variant_metrics else -np.inf
            
            # Neighborhood robustness gate: worst case must be positive
            # and not a "collapse" (e.g. worst case > 50% of original)
            is_robust = (worst_case > 0)
            if orig_exp > 0:
                 is_robust = is_robust and (worst_case / orig_exp > 0.5)
            
            if is_robust:
                pass_count += 1
                
            stress_results.append({
                "blueprint_id": bp_id,
                "original_expectancy": orig_exp,
                "worst_case_expectancy": worst_case,
                "n_variants": len(variant_metrics),
                "is_robust": is_robust,
                "fail_reason": "" if is_robust else "performance_collapse"
            })
            
        stress_df = pd.DataFrame(stress_results)
        stress_df.to_parquet(blueprints_path.parent / "stress_summary.parquet")
        
        print(f"Stress test passed for {pass_count} / {len(blueprints)} blueprints.")
        
        finalize_manifest(manifest, "success", stats={
            "blueprints": len(blueprints),
            "variants": len(expanded_blueprints),
            "robust_blueprints": pass_count
        })
        return 0
        
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1

if __name__ == "__main__":
    sys.exit(main())
