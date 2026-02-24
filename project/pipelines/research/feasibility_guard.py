from __future__ import annotations

import logging
import os
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Tuple

class FeasibilityGuard:
    """
    FeasibilityGuard checks if the required data for a specification (event or feature)
    is available in the data lake before allowing it to be included in a research plan.
    """
    def __init__(self, project_root: Path, data_root: Path, run_id: str):
        self.project_root = project_root
        self.data_root = data_root
        self.run_id = run_id
        self.log = logging.getLogger(__name__)

    def check_feasibility(self, spec_path_str: str, symbol: str) -> Tuple[bool, str]:
        """
        Returns (is_feasible, reason).
        """
        path = self.project_root / spec_path_str
        if not path.exists():
            return False, f"Spec file missing at {spec_path_str}"
        
        try:
            with open(path, "r") as f:
                spec = yaml.safe_load(f)
        except Exception as e:
            return False, f"Failed to parse spec YAML: {e}"

        if spec is None:
            return False, f"Empty spec at {spec_path_str}"

        # 1. Dataset Dependencies
        inputs = spec.get("inputs", [])
        if not isinstance(inputs, list):
            inputs = [inputs]
            
        for inp in inputs:
            if not isinstance(inp, dict): continue
            dataset_id = inp.get("dataset")
            if not dataset_id: continue
            
            if not self._check_dataset_exists(dataset_id, symbol):
                return False, f"Required dataset '{dataset_id}' not found in lake for {symbol}"

        # 2. Market Context Dependencies (for events/conditioned discovery)
        # Some events might implicitly require specific features
        return True, "ready"

    def _check_dataset_exists(self, dataset_id: str, symbol: str) -> bool:
        ds = str(dataset_id).lower()
        
        # Mapping Logic: dataset_id -> physical lake paths
        
        # OHLCV Bars
        if "ohlcv" in ds:
            market = "spot" if "spot" in ds else "perp"
            # Try 1m, 5m, 1h, 1d
            timeframes = ["5m", "1m", "1h", "1d"]
            for tf in timeframes:
                if tf in ds:
                    timeframes = [tf]
                    break
            
            for tf in timeframes:
                candidates = [
                    self.data_root / "lake" / "cleaned" / market / symbol / f"bars_{tf}",
                    self.data_root / "lake" / "runs" / self.run_id / "cleaned" / market / symbol / f"bars_{tf}",
                    # Some files are flat in the directory
                    self.data_root / "lake" / "cleaned" / market / symbol / f"bars_{tf}" / f"{symbol}_{tf}.parquet"
                ]
                if any(c.exists() for c in candidates):
                    return True
            return False

        # Funding
        if "funding" in ds:
            candidates = [
                self.data_root / "lake" / "cleaned" / "perp" / symbol / "funding_5m",
                self.data_root / "lake" / "raw" / "binance" / "perp" / symbol / "funding_rate",
                self.data_root / "lake" / "raw" / "binance" / "perp" / symbol / "funding_info"
            ]
            return any(c.exists() for c in candidates)

        # Top of Book (ToB)
        if "tob" in ds or "top_of_book" in ds:
            candidates = [
                self.data_root / "lake" / "raw" / "binance" / "perp" / symbol / "tob_1s",
                self.data_root / "lake" / "raw" / "binance" / "perp" / symbol / "top_of_book",
                self.data_root / "lake" / "raw" / "binance" / "perp" / symbol / "book_ticker"
            ]
            return any(c.exists() for c in candidates)

        # Open Interest
        if "open_interest" in ds or ds == "oi":
            candidates = [
                self.data_root / "lake" / "raw" / "binance" / "perp" / symbol / "open_interest",
                self.data_root / "lake" / "raw" / "binance" / "perp" / symbol / "oi_hist"
            ]
            return any(c.exists() for c in candidates)

        # Liquidations
        if "liquidation" in ds:
            candidates = [
                self.data_root / "lake" / "raw" / "binance" / "perp" / symbol / "liquidation_snapshot",
                self.data_root / "lake" / "raw" / "binance" / "perp" / symbol / "liquidations"
            ]
            return any(c.exists() for c in candidates)

        # Basis (Perp vs Spot)
        if "basis" in ds:
            candidates = [
                self.data_root / "lake" / "cleaned" / "perp" / symbol / "basis_5m"
            ]
            return any(c.exists() for c in candidates)

        # Unknown dataset IDs must fail closed so missing feasibility mappings do
        # not silently pass and fail much later in the pipeline.
        self.log.warning("Unknown dataset mapping in feasibility guard: dataset_id=%s symbol=%s", dataset_id, symbol)
        return False
