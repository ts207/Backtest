import argparse
import subprocess
import sys
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))

def run_stage(script_name, args):
    cmd = [sys.executable, str(PROJECT_ROOT / "project" / "pipelines" / "research" / script_name)] + args
    print(f"Running {script_name}...")
    subprocess.run(cmd, check=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    args = parser.parse_args()

    # 1. Deduplicate Survivors
    run_stage("deduplicate_survivors.py", ["--run_id", args.run_id])

    # 2. Compile Blueprints
    # Input: survivors_deduped.parquet
    candidates_file = DATA_ROOT / "runs" / args.run_id / "survivors_deduped.parquet"
    # We use the same run_id for output, so blueprints go to reports/strategy_blueprints/<run_id>
    cmd = [sys.executable, str(PROJECT_ROOT / "project" / "pipelines" / "research" / "compile_strategy_blueprints.py"),
           "--run_id", args.run_id,
           "--symbols", args.symbols,
           "--candidates_file", str(candidates_file),
           "--allow_fallback_blueprints", "1", # Ensure we pick them up
           "--min_events_floor", "1", # Safety
           "--ignore_checklist", "1", # We are bypassing checklist
           "--max_per_event", "10",
           "--max_total_compiles_per_run", "30"]
    print("Running compile_strategy_blueprints.py...")
    subprocess.run(cmd, check=True)

    # 3. Stress Sweeps
    run_stage("stress_test_blueprints.py", ["--run_id", args.run_id])

    # 4. Walkforward
    # run_walkforward.py is in pipelines/eval
    blueprints_path = DATA_ROOT / "reports" / "strategy_blueprints" / args.run_id / "blueprints.jsonl"
    cmd = [sys.executable, str(PROJECT_ROOT / "project" / "pipelines" / "eval" / "run_walkforward.py"),
           "--run_id", args.run_id,
           "--symbols", args.symbols,
           "--start", "2024-01-01", # Should match expansion run
           "--end", "2024-03-01",
           "--blueprints_path", str(blueprints_path),
           "--allow_unexpected_strategy_files", "1"] # Since we generated new ones
    print("Running run_walkforward.py...")
    subprocess.run(cmd, check=True)

    print("Promotion Pipeline Complete.")

if __name__ == "__main__":
    main()
