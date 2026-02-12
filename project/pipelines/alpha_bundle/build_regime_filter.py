from __future__ import annotations

"""Build volatility regime gating series.

Baseline implementation:
  - Use a market proxy (default: BTCUSDT) returns from perp bars.
  - Compute RV_t = sqrt(252) * std(returns over last rv_window)
  - Compute rolling percentile rank of RV_t over long_window
  - Assign regime labels:
      HIGH_VOL if q >= 0.75
      LOW_VOL  if q <= 0.25
      MID_VOL  otherwise
  - Output gate_scalar:
      HIGH_VOL: 0.6
      MID_VOL:  1.0
      LOW_VOL:  1.2

Output:
  ts_event, regime_label, gate_scalar
"""

import argparse
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir, read_parquet, write_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.validation import ensure_utc_timestamp


def rolling_rank(x: pd.Series, window: int) -> pd.Series:
    """Rolling percentile rank of the last value within the window."""
    def _rank(a: np.ndarray) -> float:
        last = a[-1]
        return float(np.sum(a <= last) / len(a))
    return x.rolling(window=window, min_periods=window).apply(_rank, raw=True)


def main() -> int:
    p = argparse.ArgumentParser(description="Build volatility regime gating series")
    p.add_argument("--run_id", required=True)
    p.add_argument("--cleaned_root", required=False, default=None)
    p.add_argument("--bar_interval", default="15m")
    p.add_argument("--market_symbol", default="BTCUSDT")
    p.add_argument("--rv_window", type=int, default=20 * 24 * 4, help="Default ~20d on 15m bars")
    p.add_argument("--long_window", type=int, default=252 * 24 * 4, help="Default ~252d on 15m bars")
    p.add_argument("--out_dir", default=None)
    args = p.parse_args()

    run_id = args.run_id
    data_root = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
    cleaned_root = Path(args.cleaned_root) if args.cleaned_root else (data_root / "lake" / "cleaned")

    out_dir = Path(args.out_dir) if args.out_dir else (data_root / "feature_store" / "regimes")
    ensure_dir(out_dir)

    stage = "alpha_regime_filter"
    manifest = start_manifest(
        stage,
        run_id,
        params={
            "market_symbol": args.market_symbol,
            "rv_window": args.rv_window,
            "long_window": args.long_window,
            "bar_interval": args.bar_interval,
        },
        inputs=[{"path": str(cleaned_root)}],
        outputs=[{"path": str(out_dir)}],
    )

    bdir = cleaned_root / "perp" / args.market_symbol / f"bars_{args.bar_interval}"
    files = sorted(list(bdir.glob("**/*.parquet"))) if bdir.exists() else []
    if not files:
        raise FileNotFoundError(f"No bars for market_symbol={args.market_symbol} at {bdir}")

    bars = read_parquet([Path(p) for p in files])
    tcol = "ts_event" if "ts_event" in bars.columns else "timestamp"
    bars[tcol] = ensure_utc_timestamp(bars[tcol])
    price_col = "mid" if "mid" in bars.columns else ("close" if "close" in bars.columns else None)
    if price_col is None:
        raise ValueError("bars must contain 'mid' or 'close'")
    bars = bars.sort_values(tcol, kind="mergesort").reset_index(drop=True)
    P = bars[price_col].astype(float)
    r = np.log(P / P.shift(1))

    rv = r.rolling(args.rv_window, min_periods=args.rv_window).std() * np.sqrt(252)
    q = rolling_rank(rv, window=args.long_window)

    regime = pd.Series(index=bars.index, dtype=object)
    regime[q >= 0.75] = "HIGH_VOL"
    regime[q <= 0.25] = "LOW_VOL"
    regime[(q > 0.25) & (q < 0.75)] = "MID_VOL"

    gate = pd.Series(index=bars.index, dtype=float)
    gate[regime == "HIGH_VOL"] = 0.6
    gate[regime == "MID_VOL"] = 1.0
    gate[regime == "LOW_VOL"] = 1.2

    out = pd.DataFrame({"ts_event": bars[tcol], "regime_label": regime, "gate_scalar": gate})
    out = out.dropna().copy()
    if out.empty:
        finalize_manifest(manifest, status="success", stats={"rows": 0, "note": "insufficient history"})
        return 0

    out["ts_event"] = ensure_utc_timestamp(out["ts_event"])
    out = out.sort_values("ts_event", kind="mergesort").reset_index(drop=True)
    out_path = out_dir / "vol_regime.parquet"
    write_parquet(out, out_path)

    finalize_manifest(manifest, status="success", stats={"rows": int(len(out)), "out": str(out_path)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
