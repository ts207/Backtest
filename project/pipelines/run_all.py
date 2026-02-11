from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _run_id_default() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _run_stage(
    stage: str,
    script_path: Path,
    base_args: List[str],
    run_id: str,
) -> bool:
    runs_dir = PROJECT_ROOT / "runs" / run_id
    runs_dir.mkdir(parents=True, exist_ok=True)
    log_path = runs_dir / f"{stage}.log"
    manifest_path = runs_dir / f"{stage}.json"

    cmd = [sys.executable, str(script_path)] + base_args + ["--log_path", str(log_path)]
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"Stage failed: {stage}", file=sys.stderr)
        print(f"Stage log: {log_path}", file=sys.stderr)
        print(f"Stage manifest: {manifest_path}", file=sys.stderr)
        return False
    return True


def main() -> int:
    """
    Parse command-line arguments and run all pipeline stages in sequence.
    """
    parser = argparse.ArgumentParser(description="Run full pipeline")
    parser.add_argument("--run_id", required=False)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--force", type=int, default=0)
    parser.add_argument("--skip_ingest_ohlcv", type=int, default=0)
    parser.add_argument("--skip_ingest_funding", type=int, default=0)
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument("--fees_bps", type=float, default=None)
    parser.add_argument("--slippage_bps", type=float, default=None)
    args = parser.parse_args()

    run_id = args.run_id or _run_id_default()
    symbols = args.symbols
    start = args.start
    end = args.end
    force_flag = str(int(args.force))

    stages: List[Tuple[str, Path, List[str]]] = []
    if not args.skip_ingest_ohlcv:
        stages.append(
            (
                "ingest_binance_um_ohlcv_15m",
                PROJECT_ROOT / "pipelines" / "ingest" / "ingest_binance_um_ohlcv_15m.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--start",
                    start,
                    "--end",
                    end,
                    "--force",
                    force_flag,
                ],
            )
        )
    if not args.skip_ingest_funding:
        stages.append(
            (
                "ingest_binance_um_funding",
                PROJECT_ROOT / "pipelines" / "ingest" / "ingest_binance_um_funding.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--start",
                    start,
                    "--end",
                    end,
                    "--force",
                    force_flag,
                ],
            )
        )

    stages.extend(
        [
            (
                "build_cleaned_15m",
                PROJECT_ROOT / "pipelines" / "clean" / "build_cleaned_15m.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--start",
                    start,
                    "--end",
                    end,
                    "--force",
                    force_flag,
                ],
            ),
            (
                "build_features_v1",
                PROJECT_ROOT / "pipelines" / "features" / "build_features_v1.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--force",
                    force_flag,
                ],
            ),
            (
                "backtest_vol_compression_v1",
                PROJECT_ROOT / "pipelines" / "backtest" / "backtest_vol_compression_v1.py",
                [
                    "--run_id",
                    run_id,
                    "--symbols",
                    symbols,
                    "--force",
                    force_flag,
                ],
            ),
            (
                "make_report",
                PROJECT_ROOT / "pipelines" / "report" / "make_report.py",
                ["--run_id", run_id],
            ),
        ]
    )

    stages_with_config = {
        "build_cleaned_15m",
        "build_features_v1",
        "backtest_vol_compression_v1",
        "make_report",
    }
    for stage_name, _, base_args in stages:
        if stage_name in stages_with_config:
            for config_path in args.config:
                base_args.extend(["--config", config_path])
        if stage_name == "backtest_vol_compression_v1":
            if args.fees_bps is not None:
                base_args.extend(["--fees_bps", str(args.fees_bps)])
            if args.slippage_bps is not None:
                base_args.extend(["--slippage_bps", str(args.slippage_bps)])

    for stage, script, base_args in stages:
        if not _run_stage(stage, script, base_args, run_id):
            return 1

    report_path = PROJECT_ROOT / "reports" / "vol_compression_expansion_v1" / run_id / "summary.md"
    print(f"Report generated: {report_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
