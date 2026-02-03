from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.run_manifest import finalize_manifest, start_manifest


def _read_metrics(metrics_path: Path) -> Dict[str, object]:
    with metrics_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _load_trades(trades_dir: Path) -> pd.DataFrame:
    trade_files = sorted(trades_dir.glob("trades_*.csv"))
    if not trade_files:
        return pd.DataFrame()
    frames = [pd.read_csv(path) for path in trade_files]
    return pd.concat(frames, ignore_index=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate report")
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()

    run_id = args.run_id
    manifest = start_manifest(run_id, "make_report", ["project/configs/pipeline.yaml"])
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []

    try:
        trades_dir = (
            Path("project")
            / "lake"
            / "trades"
            / "backtests"
            / "vol_compression_expansion_v1"
            / run_id
        )
        metrics_path = trades_dir / "metrics.json"
        metrics = _read_metrics(metrics_path)
        trades = _load_trades(trades_dir)

        inputs.append({"path": str(metrics_path), "rows": 1, "start_ts": None, "end_ts": None})
        inputs.append({"path": str(trades_dir), "rows": len(trades), "start_ts": None, "end_ts": None})

        report_dir = Path("project") / "reports" / "vol_compression_expansion_v1" / run_id
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / "summary.md"

        lines = [
            "# Volatility Compression → Expansion Report",
            "",
            f"Run ID: `{run_id}`",
            "",
            "## Metrics",
            f"- Total trades: {metrics.get('total_trades', 0)}",
            f"- Win rate: {metrics.get('win_rate', 0):.2%}",
            f"- Avg R: {metrics.get('avg_r', 0):.2f}",
            f"- Max drawdown: {metrics.get('max_drawdown', 0):.2%}",
            "",
            "## Trades",
            "",
        ]

        if trades.empty:
            lines.append("No trades for this run.")
        else:
            lines.append(trades.to_markdown(index=False))

        lines.extend(
            [
                "",
                "## Notes",
                "- Fees modeled as 4 bps per side plus 2 bps slippage per fill.",
                "- Fee sensitivity is significant for short-duration trades.",
            ]
        )

        report_path.write_text("\n".join(lines), encoding="utf-8")
        outputs.append({"path": str(report_path), "rows": len(lines), "start_ts": None, "end_ts": None})

        finalize_manifest(manifest, inputs, outputs, "success")
        return 0
    except Exception as exc:
        finalize_manifest(manifest, inputs, outputs, "failed", error=str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
