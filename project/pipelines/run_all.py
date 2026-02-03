from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


STAGES = [
    ("ingest", "project/pipelines/ingest/ingest_binance_perp_15m.py"),
    ("clean", "project/pipelines/clean/build_cleaned_15m.py"),
    ("features", "project/pipelines/features/build_features_v1.py"),
    ("backtest", "project/pipelines/backtest/backtest_vol_compression_v1.py"),
    ("report", "project/pipelines/report/make_report.py"),
]


def _run_stage(cmd: list[str]) -> None:
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"Stage failed: {' '.join(cmd)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run full pipeline")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    run_id = args.run_id
    symbols = args.symbols
    start = args.start
    end = args.end

    try:
        for name, script in STAGES:
            cmd = [sys.executable, script, "--run_id", run_id]
            if name in {"ingest"}:
                cmd.extend(["--symbols", symbols, "--start", start, "--end", end])
            elif name in {"clean", "features", "backtest"}:
                cmd.extend(["--symbols", symbols])
            _run_stage(cmd)

        report_path = Path("project") / "reports" / "vol_compression_expansion_v1" / run_id / "summary.md"
        print(f"Report generated: {report_path}")
        return 0
    except Exception as exc:
        print(f"Pipeline failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
