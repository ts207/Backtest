from pathlib import Path
from typing import List, Tuple

def build_core_stages(
    args,
    run_id: str,
    symbols: str,
    start: str,
    end: str,
    force_flag: str,
    allow_missing_funding_flag: str,
    run_spot_pipeline: bool,
    project_root: Path
) -> List[Tuple[str, Path, List[str]]]:
    stages: List[Tuple[str, Path, List[str]]] = []

    stages.extend([
        (
            "build_cleaned_5m",
            project_root / "pipelines" / "clean" / "build_cleaned_5m.py",
            [
                "--run_id", run_id,
                "--symbols", symbols,
                "--market", "perp",
                "--start", start,
                "--end", end,
                "--force", force_flag,
            ],
        ),
        (
            "build_features_v1",
            project_root / "pipelines" / "features" / "build_features_v1.py",
            [
                "--run_id", run_id,
                "--symbols", symbols,
                "--force", force_flag,
                "--allow_missing_funding", allow_missing_funding_flag,
            ],
        ),
        (
            "build_universe_snapshots",
            project_root / "pipelines" / "ingest" / "build_universe_snapshots.py",
            [
                "--run_id", run_id,
                "--symbols", symbols,
                "--market", "perp",
                "--force", force_flag,
            ],
        ),
        (
            "build_context_features",
            project_root / "pipelines" / "features" / "build_context_features.py",
            [
                "--run_id", run_id,
                "--symbols", symbols,
                "--timeframe", "5m",
                "--start", start,
                "--end", end,
                "--force", force_flag,
            ],
        ),
        (
            "build_market_context",
            project_root / "pipelines" / "features" / "build_market_context.py",
            [
                "--run_id", run_id,
                "--symbols", symbols,
                "--timeframe", "5m",
                "--start", start,
                "--end", end,
                "--force", force_flag,
            ],
        ),
    ])

    if run_spot_pipeline:
        stages.extend([
            (
                "build_cleaned_5m_spot",
                project_root / "pipelines" / "clean" / "build_cleaned_5m.py",
                [
                    "--run_id", run_id,
                    "--symbols", symbols,
                    "--market", "spot",
                    "--start", start,
                    "--end", end,
                    "--force", force_flag,
                ],
            ),
            (
                "build_features_v1_spot",
                project_root / "pipelines" / "features" / "build_features_v1.py",
                [
                    "--run_id", run_id,
                    "--symbols", symbols,
                    "--market", "spot",
                    "--force", force_flag,
                ],
            ),
        ])

    return stages
