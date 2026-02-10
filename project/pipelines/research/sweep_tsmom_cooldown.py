from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_ROOT = PROJECT_ROOT.parent / "data"

import sys

sys.path.insert(0, str(PROJECT_ROOT))

from engine.runner import run_engine
from pipelines._lib.io_utils import ensure_dir


def _parse_list(raw: str) -> List[int]:
    return [int(x.strip()) for x in raw.split(",") if x.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Sweep tsmom_v1 cooldown_bars and report gross/cost feasibility")
    parser.add_argument("--run_prefix", required=True)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--band_bps", type=float, default=10.0)
    parser.add_argument("--cooldowns", default="8,16,32,64")
    parser.add_argument("--cost_bps", type=float, default=6.0)
    parser.add_argument("--spread_bps", type=float, default=2.0)
    parser.add_argument("--execution_delay_bars", type=int, default=0)
    parser.add_argument("--execution_min_hold_bars", type=int, default=0)
    parser.add_argument("--data_root", default=str(DEFAULT_DATA_ROOT))
    args = parser.parse_args()

    cooldowns = _parse_list(args.cooldowns)
    data_root = Path(args.data_root)

    rows = []
    for cooldown_bars in cooldowns:
        run_id = f"{args.run_prefix}_cd{cooldown_bars}"
        params = {
            "execution_delay_bars": int(args.execution_delay_bars),
            "execution_min_hold_bars": int(args.execution_min_hold_bars),
            "execution_spread_bps": float(args.spread_bps),
            "strategy_overrides": {
                "tsmom_v1": {
                    "band_bps": float(args.band_bps),
                    "cooldown_bars": int(cooldown_bars),
                }
            },
        }
        result = run_engine(
            run_id=run_id,
            symbols=[args.symbol],
            strategies=["tsmom_v1"],
            params=params,
            cost_bps=float(args.cost_bps),
            data_root=data_root,
        )

        strategy = result["metrics"]["strategies"]["tsmom_v1"]
        diagnostics = result["metrics"]["diagnostics"]["strategies"]["tsmom_v1"]
        effective_bps = float(diagnostics.get("cost_bps_effective", 0.0))
        cost_paid = float(diagnostics.get("cost_paid", 0.0))
        turnover_units = cost_paid / (effective_bps / 10000.0) if effective_bps > 0 else 0.0
        gross_pnl = float(diagnostics.get("gross_pnl", 0.0))
        breakeven_bps = (10000.0 * gross_pnl / turnover_units) if turnover_units > 0 else 0.0

        rows.append(
            {
                "run_id": run_id,
                "symbol": args.symbol,
                "band_bps": float(args.band_bps),
                "cooldown_bars": int(cooldown_bars),
                "gross_pnl": gross_pnl,
                "base_cost_paid": float(diagnostics.get("base_cost_paid", 0.0)),
                "spread_cost_paid": float(diagnostics.get("spread_cost_paid", 0.0)),
                "cost_paid": cost_paid,
                "cost_bps_effective": effective_bps,
                "turnover_units": turnover_units,
                "breakeven_bps": breakeven_bps,
                "avg_turnover_per_bar": float(diagnostics.get("avg_turnover_per_bar", 0.0)),
                "net_total_return": float(strategy.get("total_pnl", 0.0)),
            }
        )

    out_dir = data_root / "runs" / args.run_prefix / "research"
    ensure_dir(out_dir)
    frame = pd.DataFrame(rows).sort_values("cooldown_bars").reset_index(drop=True)
    csv_path = out_dir / "tsmom_cooldown_sweep.csv"
    json_path = out_dir / "tsmom_cooldown_sweep.json"
    frame.to_csv(csv_path, index=False)
    json_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")

    print(frame.to_string(index=False))
    print(f"\nWrote: {csv_path}")
    print(f"Wrote: {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
