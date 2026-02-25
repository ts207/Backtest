from pathlib import Path
from typing import List, Tuple

def build_ingest_stages(
    args,
    run_id: str,
    symbols: str,
    start: str,
    end: str,
    force_flag: str,
    run_spot_pipeline: bool,
    project_root: Path
) -> List[Tuple[str, Path, List[str]]]:
    stages: List[Tuple[str, Path, List[str]]] = []

    if not args.skip_ingest_ohlcv:
        stages.append(
            (
                "ingest_binance_um_ohlcv_5m",
                project_root / "pipelines" / "ingest" / "ingest_binance_um_ohlcv_5m.py",
                [
                    "--run_id", run_id,
                    "--symbols", symbols,
                    "--start", start,
                    "--end", end,
                    "--force", force_flag,
                ],
            )
        )

    if not args.skip_ingest_funding:
        stages.append(
            (
                "ingest_binance_um_funding",
                project_root / "pipelines" / "ingest" / "ingest_binance_um_funding.py",
                [
                    "--run_id", run_id,
                    "--symbols", symbols,
                    "--start", start,
                    "--end", end,
                    "--force", force_flag,
                ],
            )
        )

    if run_spot_pipeline and not args.skip_ingest_spot_ohlcv:
        stages.append(
            (
                "ingest_binance_spot_ohlcv_5m",
                project_root / "pipelines" / "ingest" / "ingest_binance_spot_ohlcv_5m.py",
                [
                    "--run_id", run_id,
                    "--symbols", symbols,
                    "--start", start,
                    "--end", end,
                    "--force", force_flag,
                ],
            )
        )

    if int(args.run_ingest_liquidation_snapshot):
        stages.append(
            (
                "ingest_binance_um_liquidation_snapshot",
                project_root / "pipelines" / "ingest" / "ingest_binance_um_liquidation_snapshot.py",
                [
                    "--run_id", run_id,
                    "--symbols", symbols,
                    "--start", start,
                    "--end", end,
                    "--force", force_flag,
                ],
            )
        )

    if int(args.run_ingest_open_interest_hist):
        stages.append(
            (
                "ingest_binance_um_open_interest_hist",
                project_root / "pipelines" / "ingest" / "ingest_binance_um_open_interest_hist.py",
                [
                    "--run_id", run_id,
                    "--symbols", symbols,
                    "--start", start,
                    "--end", end,
                    "--period", str(args.open_interest_period),
                    "--force", force_flag,
                ],
            )
        )

    return stages
