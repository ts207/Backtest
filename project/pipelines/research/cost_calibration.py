from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd

from pipelines._lib.io_utils import choose_partition_dir, list_parquet_files, read_parquet, run_scoped_lake_path


@dataclass(frozen=True)
class CandidateCostEstimate:
    cost_bps: float
    fee_bps_per_side: float
    slippage_bps_per_fill: float
    avg_dynamic_cost_bps: float
    turnover_proxy_mean: float
    cost_input_coverage: float
    cost_model_valid: bool
    cost_model_source: str
    regime_multiplier: float


class ToBRegimeCostCalibrator:
    """Estimate candidate execution costs from symbol-level ToB regimes."""

    def __init__(
        self,
        *,
        run_id: str,
        data_root: Path,
        base_fee_bps: float,
        base_slippage_bps: float,
        static_cost_bps: float,
        mode: str = "static",
        min_tob_coverage: float = 0.60,
        tob_tolerance_minutes: int = 10,
    ) -> None:
        self.run_id = str(run_id)
        self.data_root = Path(data_root)
        self.base_fee_bps = float(base_fee_bps)
        self.base_slippage_bps = float(base_slippage_bps)
        self.static_cost_bps = float(static_cost_bps)
        self.mode = str(mode).strip().lower()
        self.min_tob_coverage = float(max(0.0, min(1.0, min_tob_coverage)))
        self.tob_tolerance = pd.Timedelta(minutes=max(1, int(tob_tolerance_minutes)))
        self._symbol_cache: Dict[str, Optional[Dict[str, object]]] = {}

    def estimate(self, *, symbol: str, events_df: pd.DataFrame) -> CandidateCostEstimate:
        if self.mode != "tob_regime":
            return self._static(source="static")

        ts_col = self._event_ts_col(events_df)
        if ts_col is None or events_df.empty:
            return self._static(source="fallback:no_events_or_ts", valid=False)

        profile = self._load_symbol_profile(symbol=symbol)
        if profile is None:
            return self._static(source="fallback:no_tob", valid=False)

        event_ts = pd.to_datetime(events_df[ts_col], utc=True, errors="coerce").dropna().sort_values()
        if event_ts.empty:
            return self._static(source="fallback:no_valid_event_ts", valid=False)

        event_grid = pd.DataFrame({"timestamp": event_ts})
        tob_frame = profile["frame"]
        merged = pd.merge_asof(
            event_grid,
            tob_frame,
            on="timestamp",
            direction="backward",
            tolerance=self.tob_tolerance,
        )
        matched = merged.dropna(subset=["spread_bps", "depth_usd"]).copy()
        coverage = float(len(matched) / len(event_grid)) if len(event_grid) else 0.0
        if matched.empty or coverage < self.min_tob_coverage:
            return self._static(source="fallback:low_tob_coverage", valid=False, coverage=coverage)

        event_spread = float(pd.to_numeric(matched["spread_bps"], errors="coerce").dropna().median())
        event_depth = float(pd.to_numeric(matched["depth_usd"], errors="coerce").dropna().median())
        base_spread = float(profile["base_spread_bps"])
        base_depth = float(profile["base_depth_usd"])
        if not np.isfinite(event_spread) or event_spread <= 0.0:
            return self._static(source="fallback:invalid_event_spread", valid=False, coverage=coverage)
        if not np.isfinite(event_depth) or event_depth <= 0.0:
            return self._static(source="fallback:invalid_event_depth", valid=False, coverage=coverage)

        spread_mult = float(event_spread / max(base_spread, 1e-6))
        depth_mult = float(np.sqrt(max(base_depth, 1e-6) / max(event_depth, 1e-6)))
        regime_mult = float(np.clip((0.7 * spread_mult) + (0.3 * depth_mult), 0.5, 4.0))

        slippage = float(max(0.0, self.base_slippage_bps * regime_mult))
        cost = float(max(0.0, self.base_fee_bps + slippage))
        return CandidateCostEstimate(
            cost_bps=cost,
            fee_bps_per_side=self.base_fee_bps,
            slippage_bps_per_fill=slippage,
            avg_dynamic_cost_bps=cost,
            turnover_proxy_mean=1.0,
            cost_input_coverage=coverage,
            cost_model_valid=True,
            cost_model_source="tob_regime",
            regime_multiplier=regime_mult,
        )

    @staticmethod
    def _event_ts_col(events_df: pd.DataFrame) -> Optional[str]:
        for col in ("enter_ts", "timestamp", "anchor_ts", "event_ts"):
            if col in events_df.columns:
                return col
        return None

    def _load_symbol_profile(self, *, symbol: str) -> Optional[Dict[str, object]]:
        key = str(symbol).strip().upper()
        if key in self._symbol_cache:
            return self._symbol_cache[key]

        candidates = [
            run_scoped_lake_path(self.data_root, self.run_id, "cleaned", "perp", key, "tob_5m_agg"),
            self.data_root / "lake" / "cleaned" / "perp" / key / "tob_5m_agg",
        ]
        selected = choose_partition_dir(candidates)
        files = list_parquet_files(selected) if selected else []
        if not files:
            self._symbol_cache[key] = None
            return None

        frame = read_parquet(files)
        if frame.empty or "timestamp" not in frame.columns:
            self._symbol_cache[key] = None
            return None

        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
        frame = frame.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

        spread_col = None
        for col in ("spread_bps_mean", "spread_bps", "quoted_spread", "spread"):
            if col in frame.columns:
                spread_col = col
                break
        if spread_col is None:
            self._symbol_cache[key] = None
            return None

        spread = pd.to_numeric(frame[spread_col], errors="coerce")
        if {"bid_depth_usd_mean", "ask_depth_usd_mean"}.issubset(frame.columns):
            depth = (
                pd.to_numeric(frame["bid_depth_usd_mean"], errors="coerce")
                + pd.to_numeric(frame["ask_depth_usd_mean"], errors="coerce")
            )
        elif {"bid_depth_usd", "ask_depth_usd"}.issubset(frame.columns):
            depth = (
                pd.to_numeric(frame["bid_depth_usd"], errors="coerce")
                + pd.to_numeric(frame["ask_depth_usd"], errors="coerce")
            )
        else:
            self._symbol_cache[key] = None
            return None

        payload = pd.DataFrame(
            {
                "timestamp": frame["timestamp"],
                "spread_bps": spread,
                "depth_usd": depth,
            }
        )
        payload = payload.replace([np.inf, -np.inf], np.nan).dropna(subset=["spread_bps", "depth_usd"])
        payload = payload[(payload["spread_bps"] > 0.0) & (payload["depth_usd"] > 0.0)].copy()
        if payload.empty:
            self._symbol_cache[key] = None
            return None

        profile = {
            "frame": payload.sort_values("timestamp").reset_index(drop=True),
            "base_spread_bps": float(payload["spread_bps"].median()),
            "base_depth_usd": float(payload["depth_usd"].median()),
        }
        self._symbol_cache[key] = profile
        return profile

    def _static(
        self,
        *,
        source: str,
        valid: bool = True,
        coverage: float = 0.0,
    ) -> CandidateCostEstimate:
        return CandidateCostEstimate(
            cost_bps=float(max(0.0, self.static_cost_bps)),
            fee_bps_per_side=float(max(0.0, self.base_fee_bps)),
            slippage_bps_per_fill=float(max(0.0, self.static_cost_bps - self.base_fee_bps)),
            avg_dynamic_cost_bps=float(max(0.0, self.static_cost_bps)),
            turnover_proxy_mean=1.0,
            cost_input_coverage=float(max(0.0, min(1.0, coverage))),
            cost_model_valid=bool(valid),
            cost_model_source=str(source),
            regime_multiplier=1.0,
        )

