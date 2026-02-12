from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.backtest.backtest_strategies import main


if __name__ == "__main__":
    sys.stderr.write(
        "[deprecated] Use project/pipelines/backtest/backtest_strategies.py; "
        "backtest_vol_compression_v1.py remains as a compatibility shim.\n"
    )
    raise SystemExit(main())
