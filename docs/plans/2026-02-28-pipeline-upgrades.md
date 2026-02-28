# Pipeline Upgrades Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement 10 targeted backtest pipeline upgrades to improve integrity, statistical rigor, execution realism, and research workflow.

**Architecture:** Upgrades touch four layers — split/evaluation (leakage control), promotion gating (statistical rigor), engine (execution realism), and reporting/registry (workflow). Each upgrade is independently testable and deployable. All pipeline integrity contracts from SKILL.md are preserved throughout.

**Tech Stack:** Python 3.12, pandas, numpy, scipy, sqlite3 (stdlib), existing pipeline infrastructure (`pipelines/_lib/`, `engine/`, `eval/`)

---

## Analysis: What Was Accepted and Why

| # | Suggestion | Decision | Reason |
|---|-----------|----------|--------|
| 1 | Purge bars at split boundaries | **IMPLEMENT** | Genuine leakage gap: embargo only creates time gap, does not remove overlapping positions |
| 2 | PSR/DSR selection-bias correction | **IMPLEMENT** | BH-FDR in Phase 2 corrects per-event; PSR/DSR at promotion corrects for the "best-of-N" selection step |
| 3 | Cross-sectional symbol holdout | **SKIP** | `cross_symbol_pass_rate >= 0.60` gate in `promote_blueprints.py` provides adequate coverage; full holdout is complex and marginal |
| 4 | Redundancy control (correlation clustering) | **IMPLEMENT** | Promoted blueprints are currently evaluated independently; PnL correlation is uncontrolled and causes concentrated exposure |
| 5 | Fragility index as hard gate | **IMPLEMENT** | `robustness.py::simulate_parameter_perturbation` exists but is not wired as a promotion gate; the proxy is already implemented |
| 6 | Funding timestamp-accurate accrual | **IMPLEMENT** | `pnl.py` smears 8-hourly funding across all bars; short-hold strategies near 0/8/16 UTC are materially mis-stated |
| 7 | Exchange constraints (tick/lot/min-notional) | **IMPLEMENT** | Tick/step rounding absent in `engine/runner.py`; min-notional exists in DSL but not in engine path |
| 8 | Explicit latency model in specs | **SKIP** | `entry_lag_bars >= 1` (integrity contract) already captures signal-to-fill latency at 5m resolution; adding YAML fields is YAGNI |
| 9 | Slippage calibration as engine artifacts | **IMPLEMENT** | `ToBRegimeCostCalibrator` exists in Phase 2 research path but is NOT used in `engine/execution_model.py`; extend to backtest execution |
| 10 | Formal data QC report | **IMPLEMENT** | `qa_data_layer.py` exists but is minimal (gap count + monotonic). Need per-run: missing partitions, return outliers, funding gaps, OI discontinuities |
| 11 | Benchmarks + exposure decomposition | **IMPLEMENT** | `make_report.py` has no buy/hold baseline or BTC beta. "Sharpe 0.8" is uninterpretable without context |
| 12 | Run registry (SQLite) | **IMPLEMENT** | Run artifacts use glob+concat patterns everywhere; a SQLite index unlocks cheap cross-run queries |

---

## Task 1: Purge Bars at Split Boundaries

**Context:** `eval/splits.py::build_time_splits` has `embargo_days` but no purge. For strategies with multi-bar horizons (e.g., `horizon_bars=12`), a trade entered 10 bars before train end has its exit label in the embargo zone — this leaks. Purge removes the tail of each split based on horizon + feature lookback.

**Files:**
- Modify: `project/eval/splits.py`
- Modify: `project/pipelines/eval/splits.py` (pipeline mirror of the same module — keep them in sync)
- Test: `tests/eval/test_splits.py`

**Step 1: Write failing tests**

```python
# tests/eval/test_splits.py
from eval.splits import build_time_splits, build_time_splits_with_purge

def test_purge_shortens_train_end():
    windows = build_time_splits_with_purge(
        start="2023-01-01", end="2023-12-31",
        train_frac=0.6, validation_frac=0.2,
        embargo_days=1, purge_bars=12,
        bar_duration_minutes=5,
    )
    standard = build_time_splits(
        start="2023-01-01", end="2023-12-31",
        train_frac=0.6, validation_frac=0.2, embargo_days=1,
    )
    train_purged = next(w for w in windows if w.label == "train")
    train_std = next(w for w in standard if w.label == "train")
    # purged train must end strictly before standard train
    assert train_purged.end < train_std.end

def test_purge_zero_is_identity():
    windows = build_time_splits_with_purge(
        start="2023-01-01", end="2023-12-31",
        train_frac=0.6, validation_frac=0.2,
        embargo_days=1, purge_bars=0,
        bar_duration_minutes=5,
    )
    standard = build_time_splits(
        start="2023-01-01", end="2023-12-31",
        train_frac=0.6, validation_frac=0.2, embargo_days=1,
    )
    for w, s in zip(windows, standard):
        assert w.label == s.label
        assert w.start == s.start
        assert w.end == s.end

def test_purge_raises_on_negative():
    import pytest
    with pytest.raises(ValueError, match="purge_bars"):
        build_time_splits_with_purge(
            start="2023-01-01", end="2023-12-31",
            train_frac=0.6, validation_frac=0.2,
            embargo_days=1, purge_bars=-1,
            bar_duration_minutes=5,
        )
```

**Step 2: Run to confirm failure**

```bash
cd /home/tstuv/workspace/backtest
source .venv/bin/activate
pytest tests/eval/test_splits.py -v 2>&1 | head -30
```
Expected: `ImportError: cannot import name 'build_time_splits_with_purge'`

**Step 3: Implement `build_time_splits_with_purge`**

Add to `project/eval/splits.py` (after `build_time_splits`):

```python
def build_time_splits_with_purge(
    *,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    train_frac: float = 0.6,
    validation_frac: float = 0.2,
    embargo_days: int = 0,
    purge_bars: int = 0,
    bar_duration_minutes: int = 5,
) -> List[SplitWindow]:
    """
    Like build_time_splits but trims the tail of each non-test window by purge_bars.
    Purge removes positions whose exit could overlap the embargo/next-split zone.
    purge_bars = max(horizon_bars, entry_lag_bars) + max_feature_lookback_bars.
    """
    if int(purge_bars) < 0:
        raise ValueError("purge_bars must be >= 0")
    windows = build_time_splits(
        start=start, end=end,
        train_frac=train_frac, validation_frac=validation_frac,
        embargo_days=embargo_days,
    )
    if int(purge_bars) == 0:
        return windows

    purge_delta = timedelta(minutes=int(purge_bars) * int(bar_duration_minutes))
    result: List[SplitWindow] = []
    for w in windows:
        if w.label == "test":
            result.append(w)
        else:
            new_end = w.end - purge_delta
            if new_end < w.start:
                raise ValueError(
                    f"purge_bars={purge_bars} with bar_duration_minutes={bar_duration_minutes} "
                    f"exceeds the {w.label} window length. Reduce purge_bars or extend the window."
                )
            result.append(SplitWindow(w.label, w.start, new_end))
    return result
```

Also copy the identical function to `project/pipelines/eval/splits.py` (the pipeline mirror).

**Step 4: Run tests**

```bash
pytest tests/eval/test_splits.py -v
```
Expected: 3 PASSED

**Step 5: Wire into `run_walkforward.py`**

In `project/pipelines/eval/run_walkforward.py`, find the `build_time_splits` call (around line 877) and change to use `build_time_splits_with_purge`. Add `--purge_bars` CLI argument (default `0`):

```python
parser.add_argument("--purge_bars", type=int, default=0,
    help="Bars to purge from split tail to prevent horizon leakage. "
         "Set to max(horizon_bars, entry_lag_bars) + max_feature_lookback_bars.")
```

And update the split call:
```python
from eval.splits import build_time_splits_with_purge
windows = build_time_splits_with_purge(
    start=..., end=...,
    train_frac=..., validation_frac=...,
    embargo_days=int(args.embargo_days),
    purge_bars=int(args.purge_bars),
    bar_duration_minutes=5,
)
```

**Step 6: Commit**

```bash
git add project/eval/splits.py project/pipelines/eval/splits.py \
        project/pipelines/eval/run_walkforward.py \
        tests/eval/test_splits.py
git commit -m "feat: add purge_bars to split generator to prevent horizon leakage at boundaries"
```

---

## Task 2: Funding Timestamp-Accurate Accrual

**Context:** `engine/pnl.py::compute_pnl_components` aligns funding to bar index, smearing 8-hourly perp funding uniformly. A position entering at 07:55 and exiting at 08:10 would miss the 08:00 funding event entirely under the current model. Fix by applying funding only on bars that cross a funding timestamp (0, 8, 16 UTC hours).

**Files:**
- Modify: `project/engine/pnl.py`
- Test: `tests/engine/test_pnl.py`

**Step 1: Write failing tests**

```python
# tests/engine/test_pnl.py  (add to existing)
import pandas as pd
import numpy as np
from engine.pnl import compute_funding_pnl_event_aligned

def _make_5m_index(start: str, periods: int) -> pd.DatetimeIndex:
    return pd.date_range(start, periods=periods, freq="5min", tz="UTC")

def test_funding_only_applied_at_event_times():
    """Position held for 24 bars (2 hours). Only one funding event at 08:00 should fire."""
    idx = _make_5m_index("2023-01-01 07:00", 24)
    pos = pd.Series(1.0, index=idx)
    funding_rate = pd.Series(0.0001, index=idx)  # 0.01% per event

    result = compute_funding_pnl_event_aligned(pos, funding_rate, funding_hours=(0, 8, 16))
    # Bar at 08:00 is idx[12]. Only that bar should have nonzero funding.
    assert result[idx[12]] != 0.0
    nonzero = result[result != 0.0]
    assert len(nonzero) == 1

def test_no_funding_when_position_flat_at_event():
    idx = _make_5m_index("2023-01-01 07:00", 24)
    pos = pd.Series(0.0, index=idx)
    funding_rate = pd.Series(0.0001, index=idx)
    result = compute_funding_pnl_event_aligned(pos, funding_rate, funding_hours=(0, 8, 16))
    assert (result == 0.0).all()
```

**Step 2: Run to confirm failure**

```bash
pytest tests/engine/test_pnl.py -v -k "funding_event" 2>&1 | head -20
```
Expected: `ImportError: cannot import name 'compute_funding_pnl_event_aligned'`

**Step 3: Implement in `engine/pnl.py`**

Add after existing functions:

```python
def compute_funding_pnl_event_aligned(
    pos: pd.Series,
    funding_rate: pd.Series,
    funding_hours: tuple[int, ...] = (0, 8, 16),
) -> pd.Series:
    """
    Apply funding only on bars whose timestamp falls on a funding event hour (0, 8, 16 UTC).
    Position is the prior-bar position (signal held going into the event timestamp).

    Args:
        pos: Position series (float, signed) indexed by UTC timestamp.
        funding_rate: Per-event funding rate series aligned to the same index.
        funding_hours: UTC hours at which funding is charged.

    Returns:
        Per-bar funding PnL series (positive = beneficial for longs receiving negative funding).
    """
    aligned_pos = pos.reindex(funding_rate.index).fillna(0.0).astype(float)
    prior_pos = aligned_pos.shift(1).fillna(0.0)

    rate_aligned = pd.to_numeric(funding_rate.reindex(pos.index), errors="coerce").fillna(0.0)

    is_funding_bar = pd.Series(False, index=pos.index)
    if hasattr(pos.index, "hour"):
        is_funding_bar = pos.index.hour.isin(funding_hours) & (pos.index.minute == 0)

    # Longs pay positive funding; shorts receive it.
    raw_funding = -prior_pos * rate_aligned
    return raw_funding.where(is_funding_bar, 0.0)
```

Update `compute_pnl_components` to accept `use_event_aligned_funding: bool = False` and route to the new function when set.

**Step 4: Run tests**

```bash
pytest tests/engine/test_pnl.py -v
```
Expected: all PASSED

**Step 5: Commit**

```bash
git add project/engine/pnl.py tests/engine/test_pnl.py
git commit -m "feat: add event-aligned funding accrual for perp funding timestamps (0/8/16 UTC)"
```

---

## Task 3: Exchange Constraints (Tick/Step Rounding)

**Context:** `engine/runner.py` applies position changes without rounding to exchange tick/step sizes. `dsl_interpreter_v1.py` has `min_notional` in strategy logic, but the engine's position sizing doesn't enforce exchange lot sizes. `min_notional` is also absent from the engine path.

**Files:**
- Create: `project/engine/exchange_constraints.py`
- Modify: `project/engine/runner.py` (apply rounding before position changes)
- Test: `tests/engine/test_exchange_constraints.py`

**Step 1: Write failing tests**

```python
# tests/engine/test_exchange_constraints.py
from engine.exchange_constraints import SymbolConstraints, apply_constraints

def test_lot_rounding():
    c = SymbolConstraints(tick_size=0.01, step_size=0.001, min_notional=10.0)
    # Raw qty 1.2345 should round DOWN to nearest step_size
    assert c.round_qty(1.2345) == 1.234

def test_min_notional_zero_out():
    c = SymbolConstraints(tick_size=0.01, step_size=0.001, min_notional=100.0)
    # Notional = qty * price = 0.001 * 50.0 = 0.05 < 100 → qty becomes 0
    assert c.enforce_min_notional(qty=0.001, price=50.0) == 0.0

def test_apply_constraints_clips_trade():
    c = SymbolConstraints(tick_size=0.01, step_size=0.1, min_notional=5.0)
    result = apply_constraints(requested_qty=0.05, price=50.0, constraints=c)
    assert result == 0.0  # 0.05 * 50 = 2.5 < 5.0 → zeroed

def test_no_constraints_passthrough():
    c = SymbolConstraints(tick_size=None, step_size=None, min_notional=None)
    assert apply_constraints(requested_qty=1.2345, price=100.0, constraints=c) == 1.2345
```

**Step 2: Run to confirm failure**

```bash
pytest tests/engine/test_exchange_constraints.py -v 2>&1 | head -15
```
Expected: `ImportError: No module named 'engine.exchange_constraints'`

**Step 3: Create `engine/exchange_constraints.py`**

```python
from __future__ import annotations
import math
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class SymbolConstraints:
    tick_size: Optional[float]   # minimum price increment (unused for qty, here for completeness)
    step_size: Optional[float]   # minimum quantity increment (lot size)
    min_notional: Optional[float]  # minimum order value in quote currency


    def round_qty(self, qty: float) -> float:
        if self.step_size is None or self.step_size <= 0.0:
            return qty
        precision = max(0, -int(math.floor(math.log10(self.step_size))))
        return round(math.floor(qty / self.step_size) * self.step_size, precision)

    def enforce_min_notional(self, qty: float, price: float) -> float:
        if self.min_notional is None or self.min_notional <= 0.0:
            return qty
        notional = abs(qty) * abs(price)
        return 0.0 if notional < self.min_notional else qty


def apply_constraints(
    requested_qty: float,
    price: float,
    constraints: SymbolConstraints,
) -> float:
    """Round qty to step_size then zero if below min_notional."""
    sign = 1.0 if requested_qty >= 0.0 else -1.0
    qty = constraints.round_qty(abs(requested_qty))
    qty = constraints.enforce_min_notional(qty=qty, price=price)
    return sign * qty


def load_symbol_constraints(symbol: str, meta_dir) -> SymbolConstraints:
    """
    Load exchange filters from data/lake/raw/binance/meta/<symbol>.json.
    Returns unconstrained SymbolConstraints if file is absent.
    """
    import json
    from pathlib import Path
    path = Path(meta_dir) / f"{symbol}.json"
    if not path.exists():
        return SymbolConstraints(tick_size=None, step_size=None, min_notional=None)
    data = json.loads(path.read_text(encoding="utf-8"))
    filters = {f["filterType"]: f for f in data.get("filters", [])}
    tick = float(filters.get("PRICE_FILTER", {}).get("tickSize", 0)) or None
    step = float(filters.get("LOT_SIZE", {}).get("stepSize", 0)) or None
    notional = float(filters.get("MIN_NOTIONAL", {}).get("minNotional", 0)) or None
    return SymbolConstraints(tick_size=tick, step_size=step, min_notional=notional)
```

**Step 4: Run tests**

```bash
pytest tests/engine/test_exchange_constraints.py -v
```
Expected: 4 PASSED

**Step 5: Wire into `engine/runner.py`**

Import and apply at position change points. Add a `clipped_trades` counter to diagnostics. In `runner.py`, after resolving the raw position signal and before writing position changes, add:

```python
from engine.exchange_constraints import load_symbol_constraints, apply_constraints
# (per-symbol, loaded once before the per-bar loop)
constraints = load_symbol_constraints(symbol, meta_dir=DATA_ROOT / "lake" / "raw" / "binance" / "meta")
# Inside bar loop, when position changes:
raw_qty = new_pos - prior_pos
adj_qty = apply_constraints(raw_qty, price=current_close, constraints=constraints)
if adj_qty == 0.0 and raw_qty != 0.0:
    diagnostics["clipped_trades"] = diagnostics.get("clipped_trades", 0) + 1
new_pos = prior_pos + adj_qty
```

**Step 6: Commit**

```bash
git add project/engine/exchange_constraints.py project/engine/runner.py \
        tests/engine/test_exchange_constraints.py
git commit -m "feat: add exchange constraints (tick/step rounding, min-notional) in engine runner"
```

---

## Task 4: Fragility Index as a Hard Promotion Gate

**Context:** `eval/robustness.py::simulate_parameter_perturbation` exists but is not called in `promote_blueprints.py`. The current `_parameter_neighborhood_stability` is a heuristic on train/validation PnL sign — not an actual perturbation. Wire the existing perturbation simulation as a hard gate: require that at least `min_fragility_pass_rate` fraction of perturbations produce positive annualized return.

**Files:**
- Modify: `project/pipelines/research/promote_blueprints.py`
- Test: `tests/research/test_promote_blueprints_fragility.py`

**Step 1: Write failing tests**

```python
# tests/research/test_promote_blueprints_fragility.py
import numpy as np
import pandas as pd
from eval.robustness import simulate_parameter_perturbation
from pipelines.research.promote_blueprints import _fragility_gate


def test_fragility_gate_passes_stable_strategy():
    rng = np.random.default_rng(42)
    # Strong consistent positive PnL — most perturbations stay positive
    pnl = pd.Series(rng.normal(0.001, 0.002, 1000))
    assert _fragility_gate(pnl, min_pass_rate=0.60, n_iterations=200) is True


def test_fragility_gate_fails_fragile_strategy():
    rng = np.random.default_rng(42)
    # Tiny mean relative to std — perturbations flip sign frequently
    pnl = pd.Series(rng.normal(0.0001, 0.05, 500))
    assert _fragility_gate(pnl, min_pass_rate=0.60, n_iterations=200) is False


def test_fragility_gate_empty_series():
    assert _fragility_gate(pd.Series([], dtype=float), min_pass_rate=0.60) is False
```

**Step 2: Run to confirm failure**

```bash
pytest tests/research/test_promote_blueprints_fragility.py -v 2>&1 | head -20
```
Expected: `ImportError: cannot import name '_fragility_gate'`

**Step 3: Add `_fragility_gate` to `promote_blueprints.py`**

Add this function (imports `simulate_parameter_perturbation` from `eval.robustness`):

```python
from eval.robustness import simulate_parameter_perturbation

def _fragility_gate(
    pnl_series: pd.Series,
    *,
    min_pass_rate: float = 0.60,
    n_iterations: int = 200,
    noise_std_dev: float = 0.05,
) -> bool:
    """
    Return True if >= min_pass_rate fraction of perturbation scenarios yield positive
    annualized return. Uses simulate_parameter_perturbation as the perturbation proxy.
    """
    if pnl_series.empty:
        return False
    stats = simulate_parameter_perturbation(
        pnl_series, noise_std_dev=noise_std_dev, n_iterations=n_iterations
    )
    if not stats:
        return False
    p05 = stats.get("perturbation_return_p05", float("-inf"))
    p50 = stats.get("perturbation_return_p50", 0.0)
    # Estimate pass rate: assume normal distribution of perturbation returns
    # p05 is 5th percentile → infer σ from (p50 - p05) / 1.645
    if p50 <= 0.0:
        return False
    sigma = (p50 - p05) / 1.645 if p50 > p05 else p50
    if sigma <= 0.0:
        return bool(p05 > 0.0)
    from scipy import stats as scipy_stats
    pass_rate = float(1.0 - scipy_stats.norm.cdf(0.0, loc=p50, scale=sigma))
    return pass_rate >= float(min_pass_rate)
```

Then in `promote_blueprints.py::main`, add `--min_fragility_pass_rate` CLI argument (default `0.60`) and add `"fragility_gate"` to the `gates` dict, loading `pnl` from the strategy returns frame.

**Step 4: Run tests**

```bash
pytest tests/research/test_promote_blueprints_fragility.py -v
```
Expected: 3 PASSED

**Step 5: Commit**

```bash
git add project/pipelines/research/promote_blueprints.py \
        tests/research/test_promote_blueprints_fragility.py
git commit -m "feat: add fragility gate to promote_blueprints using parameter perturbation simulation"
```

---

## Task 5: PSR/DSR Selection-Bias Correction at Promotion

**Context:** BH-FDR in Phase 2 corrects for multiplicity at event-candidate level. But the "best of N walkforward survivors" selected at promotion time inflates Sharpe. Probabilistic Sharpe Ratio (PSR) and Deflated Sharpe Ratio (DSR) correct for this. PSR answers: "Is this strategy's Sharpe reliably above a benchmark SR, given finite sample length and non-normality?" DSR additionally deflates for number of trials.

**Files:**
- Create: `project/eval/selection_bias.py`
- Modify: `project/pipelines/research/promote_blueprints.py`
- Test: `tests/eval/test_selection_bias.py`

**Step 1: Write failing tests**

```python
# tests/eval/test_selection_bias.py
import numpy as np
import pandas as pd
from eval.selection_bias import probabilistic_sharpe_ratio, deflated_sharpe_ratio


def test_psr_high_for_strong_strategy():
    rng = np.random.default_rng(0)
    # Strong positive PnL → PSR should be high (close to 1)
    pnl = pd.Series(rng.normal(0.002, 0.01, 2000))
    psr = probabilistic_sharpe_ratio(pnl, benchmark_sr=0.0)
    assert psr > 0.90


def test_psr_low_for_weak_strategy():
    rng = np.random.default_rng(0)
    pnl = pd.Series(rng.normal(0.0001, 0.05, 500))
    psr = probabilistic_sharpe_ratio(pnl, benchmark_sr=0.0)
    assert psr < 0.70


def test_dsr_below_psr_when_trials_gt_1():
    rng = np.random.default_rng(0)
    pnl = pd.Series(rng.normal(0.002, 0.01, 2000))
    psr = probabilistic_sharpe_ratio(pnl, benchmark_sr=0.0)
    dsr = deflated_sharpe_ratio(pnl, n_trials=10)
    assert dsr < psr


def test_empty_series_returns_zero():
    psr = probabilistic_sharpe_ratio(pd.Series([], dtype=float), benchmark_sr=0.0)
    assert psr == 0.0
```

**Step 2: Run to confirm failure**

```bash
pytest tests/eval/test_selection_bias.py -v 2>&1 | head -20
```
Expected: `ModuleNotFoundError: No module named 'eval.selection_bias'`

**Step 3: Create `project/eval/selection_bias.py`**

```python
from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats


def probabilistic_sharpe_ratio(
    pnl: pd.Series,
    benchmark_sr: float = 0.0,
    periods_per_year: int = 105120,  # 5m bars per year
) -> float:
    """
    PSR: probability that the true SR exceeds benchmark_sr, corrected for
    skewness, kurtosis, and finite sample length.
    Bailey & Lopez de Prado (2012).
    """
    pnl_arr = pd.to_numeric(pnl, errors="coerce").dropna().values
    n = len(pnl_arr)
    if n < 10:
        return 0.0
    sr = float(np.mean(pnl_arr) / np.std(pnl_arr, ddof=1)) * np.sqrt(periods_per_year)
    skew = float(stats.skew(pnl_arr))
    kurt = float(stats.kurtosis(pnl_arr, fisher=True))  # excess kurtosis
    # Standard error of SR (Lo 2002 adjusted for non-normality)
    se = np.sqrt(
        (1.0 + 0.5 * sr ** 2 - skew * sr + ((kurt + 3.0) / 4.0) * sr ** 2) / (n - 1)
    )
    if se <= 0.0:
        return 1.0 if sr > benchmark_sr else 0.0
    z = (sr - benchmark_sr) / se
    return float(stats.norm.cdf(z))


def deflated_sharpe_ratio(
    pnl: pd.Series,
    n_trials: int = 1,
    benchmark_sr: float = 0.0,
    periods_per_year: int = 105120,
) -> float:
    """
    DSR: PSR with benchmark SR replaced by the expected maximum SR from n_trials
    independent tests (assumes IID SR estimates across trials).
    Bailey & Lopez de Prado (2014).
    """
    pnl_arr = pd.to_numeric(pnl, errors="coerce").dropna().values
    n = len(pnl_arr)
    if n < 10 or n_trials < 1:
        return 0.0
    # Expected maximum of n_trials standard normal draws
    euler_mascheroni = 0.5772156649
    expected_max = (
        (1.0 - euler_mascheroni) * stats.norm.ppf(1.0 - 1.0 / n_trials)
        + euler_mascheroni * stats.norm.ppf(1.0 - 1.0 / (n_trials * np.e))
    ) if n_trials > 1 else 0.0
    # Deflated benchmark SR (unnormalized — stay in annualized SR units)
    sr_std = 1.0 / np.sqrt(periods_per_year)  # approximate per-trial SR std
    deflated_benchmark = max(benchmark_sr, expected_max * sr_std)
    return probabilistic_sharpe_ratio(pnl, benchmark_sr=deflated_benchmark, periods_per_year=periods_per_year)
```

**Step 4: Run tests**

```bash
pytest tests/eval/test_selection_bias.py -v
```
Expected: 4 PASSED

**Step 5: Wire into `promote_blueprints.py`**

Add `--min_psr` (default `0.75`) and `--min_dsr` (default `0.60`) CLI arguments. Add `"psr_gate"` and `"dsr_gate"` to the `gates` dict using the strategy returns PnL series and `n_trials = len(blueprints)`. Log PSR/DSR values in the `tested` row for inspection.

**Step 6: Commit**

```bash
git add project/eval/selection_bias.py project/pipelines/research/promote_blueprints.py \
        tests/eval/test_selection_bias.py
git commit -m "feat: add PSR/DSR selection-bias gates to promote_blueprints"
```

---

## Task 6: Redundancy Control (PnL Correlation Clustering)

**Context:** `promote_blueprints.py` evaluates candidates independently. The promoted set can contain many correlated variants (e.g., 5 funding-carry strategies with slightly different thresholds). Greedy max-diversification selects a subset with controlled pairwise PnL correlation.

**Files:**
- Create: `project/eval/redundancy.py`
- Modify: `project/pipelines/research/promote_blueprints.py` (post-promotion filter)
- Test: `tests/eval/test_redundancy.py`

**Step 1: Write failing tests**

```python
# tests/eval/test_redundancy.py
import numpy as np
import pandas as pd
from eval.redundancy import greedy_diversified_subset

def test_returns_all_when_uncorrelated():
    rng = np.random.default_rng(0)
    n = 5
    pnl_matrix = pd.DataFrame({f"s{i}": rng.normal(0, 1, 500) for i in range(n)})
    subset = greedy_diversified_subset(pnl_matrix, max_corr=0.90, max_n=10)
    assert len(subset) == n  # all pass since orthogonal

def test_removes_redundant_correlated():
    rng = np.random.default_rng(0)
    base = rng.normal(0, 1, 500)
    # s1 and s2 are nearly identical (high correlation)
    pnl_matrix = pd.DataFrame({
        "s0": base,
        "s1": base + rng.normal(0, 0.01, 500),  # clone of s0
        "s2": rng.normal(0, 1, 500),
    })
    subset = greedy_diversified_subset(pnl_matrix, max_corr=0.95, max_n=10)
    # s0 and s1 should collapse to one
    assert len(subset) == 2

def test_respects_max_n():
    rng = np.random.default_rng(0)
    pnl_matrix = pd.DataFrame({f"s{i}": rng.normal(0, 1, 500) for i in range(10)})
    subset = greedy_diversified_subset(pnl_matrix, max_corr=0.99, max_n=3)
    assert len(subset) <= 3
```

**Step 2: Run to confirm failure**

```bash
pytest tests/eval/test_redundancy.py -v 2>&1 | head -15
```
Expected: `ModuleNotFoundError: No module named 'eval.redundancy'`

**Step 3: Create `project/eval/redundancy.py`**

```python
from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd


def greedy_diversified_subset(
    pnl_matrix: pd.DataFrame,
    max_corr: float = 0.70,
    max_n: int = 20,
) -> List[str]:
    """
    Greedy max-diversification: iteratively add strategies whose pairwise
    Pearson correlation with all already-selected strategies is below max_corr.

    Args:
        pnl_matrix: DataFrame where each column is a strategy's PnL series.
        max_corr: Maximum allowed pairwise correlation with existing selection.
        max_n: Maximum number of strategies to select.

    Returns:
        List of selected column names.
    """
    cols = list(pnl_matrix.columns)
    if not cols:
        return []
    corr = pnl_matrix.corr().abs()
    selected: List[str] = []
    # Rank by descending mean absolute correlation (pick most independent first)
    mean_corr = corr.mean()
    ordered = [c for c in mean_corr.sort_values().index if c in cols]

    for candidate in ordered:
        if len(selected) >= max_n:
            break
        if not selected:
            selected.append(candidate)
            continue
        max_with_selected = max(
            float(corr.loc[candidate, s]) for s in selected
            if candidate != s and s in corr.columns
        )
        if max_with_selected < float(max_corr):
            selected.append(candidate)
    return selected
```

**Step 4: Run tests**

```bash
pytest tests/eval/test_redundancy.py -v
```
Expected: 3 PASSED

**Step 5: Wire into `promote_blueprints.py`**

After `survivors` is populated, add a post-promotion diversification filter:

```python
# Add CLI: --max_pnl_correlation (default 0.70), --max_promoted (default 20)
# Load PnL series for each survivor, build pnl_matrix, run greedy_diversified_subset
# Filter survivors to the diversified subset
# Log removed strategies and correlation reason in promotion_report.json
```

**Step 6: Commit**

```bash
git add project/eval/redundancy.py project/pipelines/research/promote_blueprints.py \
        tests/eval/test_redundancy.py
git commit -m "feat: add greedy correlation-based diversification filter to post-promotion step"
```

---

## Task 7: Slippage Calibration in Engine Execution Model

**Context:** `engine/execution_model.py` uses config-file weights (spread_weight=0.5, etc.) for its dynamic cost model. `ToBRegimeCostCalibrator` in `pipelines/research/cost_calibration.py` already calibrates symbol-level coefficients from ToB data for Phase 2 research, but that calibration is NOT used in the engine's backtest execution path. Extend so that the engine can load per-symbol calibration artifacts and fall back to config defaults if absent.

**Files:**
- Create: `project/data/reports/cost_calibration/` (output directory — created at runtime)
- Create: `project/pipelines/clean/calibrate_execution_costs.py` (new pipeline stage)
- Modify: `project/engine/execution_model.py`
- Test: `tests/engine/test_execution_model_calibration.py`

**Step 1: Write failing tests**

```python
# tests/engine/test_execution_model_calibration.py
import json, tempfile
from pathlib import Path
import pandas as pd
import numpy as np
from engine.execution_model import estimate_transaction_cost_bps, load_calibration_config

def test_load_calibration_falls_back_to_defaults(tmp_path):
    # No calibration file → use defaults from config
    config = {"cost_model": "static", "base_fee_bps": 5.0, "base_slippage_bps": 3.0}
    result = load_calibration_config("BTCUSDT", calibration_dir=tmp_path, base_config=config)
    assert result["base_fee_bps"] == 5.0

def test_load_calibration_overrides_from_file(tmp_path):
    calib = {"spread_weight": 0.8, "base_slippage_bps": 1.5}
    (tmp_path / "BTCUSDT.json").write_text(json.dumps(calib))
    base = {"cost_model": "dynamic", "base_fee_bps": 5.0, "base_slippage_bps": 3.0, "spread_weight": 0.5}
    result = load_calibration_config("BTCUSDT", calibration_dir=tmp_path, base_config=base)
    assert result["spread_weight"] == 0.8
    assert result["base_slippage_bps"] == 1.5
    assert result["base_fee_bps"] == 5.0  # not in calib file → preserved from base
```

**Step 2: Run to confirm failure**

```bash
pytest tests/engine/test_execution_model_calibration.py -v 2>&1 | head -15
```
Expected: `ImportError: cannot import name 'load_calibration_config'`

**Step 3: Add `load_calibration_config` to `engine/execution_model.py`**

```python
def load_calibration_config(
    symbol: str,
    *,
    calibration_dir,
    base_config: dict,
) -> dict:
    """
    Merge per-symbol calibration JSON (if present) over base_config.
    Calibration JSON keys override base_config; absent keys are preserved from base.
    """
    import json
    from pathlib import Path
    path = Path(calibration_dir) / f"{symbol}.json"
    merged = dict(base_config)
    if path.exists():
        overrides = json.loads(path.read_text(encoding="utf-8"))
        merged.update({k: v for k, v in overrides.items() if v is not None})
    return merged
```

**Step 4: Create `project/pipelines/clean/calibrate_execution_costs.py`**

This stage reads `tob_5m_agg` per symbol, estimates regime-specific spread/impact coefficients (mean spread, spread std, volume proxy), and writes `data/reports/cost_calibration/<run_id>/<symbol>.json`:

```python
# Minimal calibration: compute per-symbol median spread_bps and depth_usd
# from tob_5m_agg, write as {"base_slippage_bps": median_half_spread, "spread_weight": ...}
# Refer to pipelines/research/cost_calibration.py::ToBRegimeCostCalibrator for the regime approach
```

See `pipelines/research/cost_calibration.py` lines 26–100 for the data loading pattern.

**Step 5: Run tests**

```bash
pytest tests/engine/test_execution_model_calibration.py -v
```
Expected: 2 PASSED

**Step 6: Commit**

```bash
git add project/engine/execution_model.py \
        project/pipelines/clean/calibrate_execution_costs.py \
        tests/engine/test_execution_model_calibration.py
git commit -m "feat: add per-symbol calibration artifact loading to engine execution model"
```

---

## Task 8: Formal Data QC Report

**Context:** `qa_data_layer.py` has basic gap count and monotonic check. Need a per-run QC report with: missing expected partitions, duplicate bars, return outliers (|return| > 3σ), funding gaps, OI discontinuities. Fail-closed on severe anomalies (consistent with contract #5).

**Files:**
- Modify: `project/pipelines/report/qa_data_layer.py`
- Create: `project/schemas/qc_contracts.py` (severity thresholds)
- Test: `tests/report/test_qa_data_layer.py`

**Step 1: Write failing tests**

```python
# tests/report/test_qa_data_layer.py
import numpy as np
import pandas as pd
from pipelines.report.qa_data_layer import check_return_outliers, check_duplicate_bars, QCSeverity

def test_return_outliers_detected():
    ts = pd.date_range("2023-01-01", periods=500, freq="5min", tz="UTC")
    close = pd.Series(100.0, index=ts)
    close.iloc[200] = 200.0  # 100% return spike → outlier
    result = check_return_outliers(close, z_threshold=5.0)
    assert result["outlier_count"] >= 1
    assert result["severity"] in ("warning", "critical")

def test_no_outliers_in_clean_data():
    ts = pd.date_range("2023-01-01", periods=500, freq="5min", tz="UTC")
    close = pd.Series(np.linspace(100, 102, 500), index=ts)
    result = check_return_outliers(close, z_threshold=5.0)
    assert result["outlier_count"] == 0
    assert result["severity"] == "ok"

def test_duplicate_bars_detected():
    ts = pd.date_range("2023-01-01", periods=10, freq="5min", tz="UTC")
    ts_dup = ts.append(ts[:2])  # 2 duplicates
    result = check_duplicate_bars(pd.Series(ts_dup))
    assert result["duplicate_count"] == 2
    assert result["severity"] != "ok"
```

**Step 2: Run to confirm failure**

```bash
pytest tests/report/test_qa_data_layer.py -v 2>&1 | head -20
```
Expected: `ImportError: cannot import name 'check_return_outliers'`

**Step 3: Add checks to `qa_data_layer.py`**

```python
from typing import Dict

QCSeverity = str  # "ok" | "warning" | "critical"

def check_return_outliers(
    close: pd.Series,
    z_threshold: float = 5.0,
    critical_count: int = 5,
) -> Dict[str, object]:
    ret = close.pct_change().dropna()
    if ret.empty:
        return {"outlier_count": 0, "severity": "ok"}
    z = (ret - ret.mean()) / (ret.std() + 1e-10)
    outlier_count = int((z.abs() > z_threshold).sum())
    severity: QCSeverity = "ok"
    if outlier_count >= critical_count:
        severity = "critical"
    elif outlier_count > 0:
        severity = "warning"
    return {"outlier_count": outlier_count, "severity": severity}


def check_duplicate_bars(timestamps: pd.Series) -> Dict[str, object]:
    dup_count = int(timestamps.duplicated().sum())
    severity: QCSeverity = "ok" if dup_count == 0 else ("critical" if dup_count > 5 else "warning")
    return {"duplicate_count": dup_count, "severity": severity}


def check_funding_gaps(funding_ts: pd.Series, expected_interval_hours: int = 8) -> Dict[str, object]:
    """Detect gaps larger than expected_interval_hours in funding timestamp series."""
    ts = pd.to_datetime(funding_ts, utc=True).sort_values().dropna()
    if len(ts) < 2:
        return {"gap_count": 0, "max_gap_hours": 0.0, "severity": "ok"}
    gaps_hours = ts.diff().dropna().dt.total_seconds() / 3600.0
    large_gaps = int((gaps_hours > expected_interval_hours * 1.5).sum())
    max_gap = float(gaps_hours.max())
    severity: QCSeverity = "ok" if large_gaps == 0 else ("critical" if large_gaps > 3 else "warning")
    return {"gap_count": large_gaps, "max_gap_hours": max_gap, "severity": severity}
```

Update `main()` in `qa_data_layer.py` to write a `data/reports/qc/<run_id>/qc_summary.json` with all checks. Raise a `SystemExit(1)` when any check returns severity `"critical"` (fail-closed, per contract #5).

**Step 4: Run tests**

```bash
pytest tests/report/test_qa_data_layer.py -v
```
Expected: 3 PASSED

**Step 5: Commit**

```bash
git add project/pipelines/report/qa_data_layer.py tests/report/test_qa_data_layer.py
git commit -m "feat: extend QA data layer with return outliers, duplicate bars, and funding gap checks"
```

---

## Task 9: Benchmark + Exposure Decomposition in Reports

**Context:** `make_report.py` has no baseline. A Sharpe of 0.8 is uninterpretable without comparison to buy/hold and BTC beta. Add: buy/hold benchmark, BTC beta (linear regression of strategy returns on BTC returns), funding exposure proxy, and turnover summary to every report.

**Files:**
- Create: `project/eval/benchmarks.py`
- Modify: `project/pipelines/report/make_report.py`
- Test: `tests/eval/test_benchmarks.py`

**Step 1: Write failing tests**

```python
# tests/eval/test_benchmarks.py
import numpy as np
import pandas as pd
from eval.benchmarks import compute_buy_hold_sharpe, compute_btc_beta, compute_exposure_summary

def test_buy_hold_sharpe():
    ts = pd.date_range("2023-01-01", periods=1000, freq="5min", tz="UTC")
    close = pd.Series(100.0 * np.cumprod(1 + np.random.normal(0.0001, 0.01, 1000)), index=ts)
    result = compute_buy_hold_sharpe(close)
    assert "sharpe_annualized" in result
    assert isinstance(result["sharpe_annualized"], float)

def test_btc_beta_near_one_for_identical():
    rng = np.random.default_rng(0)
    ret = pd.Series(rng.normal(0.001, 0.01, 500))
    beta_result = compute_btc_beta(strategy_returns=ret, btc_returns=ret)
    assert abs(beta_result["beta"] - 1.0) < 0.05

def test_exposure_summary_keys():
    rng = np.random.default_rng(0)
    pos = pd.Series(rng.choice([-1, 0, 1], 500).astype(float))
    result = compute_exposure_summary(pos)
    assert "gross_exposure_mean" in result
    assert "net_exposure_mean" in result
    assert "turnover_mean" in result
```

**Step 2: Run to confirm failure**

```bash
pytest tests/eval/test_benchmarks.py -v 2>&1 | head -15
```
Expected: `ModuleNotFoundError: No module named 'eval.benchmarks'`

**Step 3: Create `project/eval/benchmarks.py`**

```python
from __future__ import annotations
import numpy as np
import pandas as pd
from typing import Dict

BARS_PER_YEAR = 105120  # 5m bars


def compute_buy_hold_sharpe(close: pd.Series) -> Dict[str, float]:
    ret = close.pct_change(fill_method=None).dropna()
    if ret.empty:
        return {"sharpe_annualized": 0.0, "annualized_return": 0.0, "annualized_vol": 0.0}
    ann_ret = float(ret.mean() * BARS_PER_YEAR)
    ann_vol = float(ret.std() * np.sqrt(BARS_PER_YEAR))
    sharpe = ann_ret / ann_vol if ann_vol > 0 else 0.0
    return {"sharpe_annualized": sharpe, "annualized_return": ann_ret, "annualized_vol": ann_vol}


def compute_btc_beta(
    strategy_returns: pd.Series,
    btc_returns: pd.Series,
) -> Dict[str, float]:
    aligned = pd.DataFrame({"strat": strategy_returns, "btc": btc_returns}).dropna()
    if len(aligned) < 10:
        return {"beta": float("nan"), "r_squared": float("nan")}
    cov = float(np.cov(aligned["strat"], aligned["btc"])[0, 1])
    var_btc = float(aligned["btc"].var())
    beta = cov / var_btc if var_btc > 0 else float("nan")
    corr = float(aligned["strat"].corr(aligned["btc"]))
    return {"beta": beta, "r_squared": corr ** 2}


def compute_exposure_summary(positions: pd.Series) -> Dict[str, float]:
    gross = positions.abs()
    turnover = positions.diff().abs()
    return {
        "gross_exposure_mean": float(gross.mean()),
        "net_exposure_mean": float(positions.mean()),
        "turnover_mean": float(turnover.mean()),
        "long_fraction": float((positions > 0).mean()),
        "short_fraction": float((positions < 0).mean()),
    }
```

**Step 4: Run tests**

```bash
pytest tests/eval/test_benchmarks.py -v
```
Expected: 3 PASSED

**Step 5: Wire into `make_report.py`**

Load BTC close prices (from the cleaned 5m bar of the BTC symbol, standard path). Compute and append to the report JSON:
- `"buy_hold_benchmark"`: result of `compute_buy_hold_sharpe`
- `"btc_beta_decomposition"`: result of `compute_btc_beta`
- `"exposure_summary"`: result of `compute_exposure_summary` per strategy

**Step 6: Commit**

```bash
git add project/eval/benchmarks.py project/pipelines/report/make_report.py \
        tests/eval/test_benchmarks.py
git commit -m "feat: add buy/hold benchmark, BTC beta, and exposure decomposition to reports"
```

---

## Task 10: Run Registry (SQLite)

**Context:** `data/runs/<run_id>/run_manifest.json` exists per run, but there is no queryable cross-run index. Researchers use glob+concat to compare runs. A SQLite index with one row per run + one row per promoted strategy enables instant filtering.

**Files:**
- Create: `project/scripts/build_run_registry.py`
- Create: `project/scripts/query_runs.py`
- Test: `tests/scripts/test_run_registry.py`

**Step 1: Write failing tests**

```python
# tests/scripts/test_run_registry.py
import json, sqlite3, tempfile
from pathlib import Path
from scripts.build_run_registry import upsert_run, create_schema, query_top_promoted

def test_upsert_and_query(tmp_path):
    db_path = tmp_path / "runs.sqlite"
    conn = sqlite3.connect(db_path)
    create_schema(conn)
    upsert_run(conn, {
        "run_id": "run_001",
        "stage": "promote_blueprints",
        "status": "success",
        "survivors_count": 3,
        "tested_count": 10,
        "timestamp": "2026-01-01T00:00:00",
    })
    conn.commit()
    rows = query_top_promoted(conn, limit=5)
    assert len(rows) == 1
    assert rows[0]["run_id"] == "run_001"

def test_schema_idempotent(tmp_path):
    db_path = tmp_path / "runs.sqlite"
    conn = sqlite3.connect(db_path)
    create_schema(conn)
    create_schema(conn)  # must not raise
    conn.close()
```

**Step 2: Run to confirm failure**

```bash
pytest tests/scripts/test_run_registry.py -v 2>&1 | head -15
```
Expected: `ModuleNotFoundError: No module named 'scripts.build_run_registry'`

**Step 3: Create `project/scripts/build_run_registry.py`**

```python
from __future__ import annotations
import argparse, json, sqlite3, sys
from pathlib import Path
from typing import Dict, List

DATA_ROOT = Path(__file__).resolve().parents[3] / "data"


def create_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            stage TEXT,
            status TEXT,
            survivors_count INTEGER,
            tested_count INTEGER,
            timestamp TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS promoted_strategies (
            run_id TEXT,
            strategy_id TEXT,
            blueprint_id TEXT,
            family TEXT,
            trades INTEGER,
            PRIMARY KEY (run_id, strategy_id)
        )
    """)


def upsert_run(conn: sqlite3.Connection, row: Dict) -> None:
    conn.execute("""
        INSERT OR REPLACE INTO runs (run_id, stage, status, survivors_count, tested_count, timestamp)
        VALUES (:run_id, :stage, :status, :survivors_count, :tested_count, :timestamp)
    """, row)


def query_top_promoted(conn: sqlite3.Connection, limit: int = 10) -> List[Dict]:
    cur = conn.execute(
        "SELECT * FROM runs WHERE status='success' ORDER BY survivors_count DESC LIMIT ?",
        (limit,)
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Index run manifests into SQLite registry")
    parser.add_argument("--data_root", default=str(DATA_ROOT))
    parser.add_argument("--db", default=None)
    args = parser.parse_args()
    data_root = Path(args.data_root)
    db_path = Path(args.db) if args.db else data_root / "meta" / "runs.sqlite"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    create_schema(conn)

    for manifest_path in sorted((data_root / "runs").glob("*/run_manifest.json")):
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        run_id = manifest.get("run_id", manifest_path.parent.name)
        upsert_run(conn, {
            "run_id": run_id,
            "stage": manifest.get("stage", ""),
            "status": manifest.get("status", ""),
            "survivors_count": int(manifest.get("stats", {}).get("survivors_count", 0) or 0),
            "tested_count": int(manifest.get("stats", {}).get("tested_count", 0) or 0),
            "timestamp": manifest.get("started_at", ""),
        })
    conn.commit()
    conn.close()
    print(f"Registry updated: {db_path}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
```

**Step 4: Create `project/scripts/query_runs.py`** (thin CLI wrapper around `query_top_promoted` and ad-hoc SQL)

**Step 5: Run tests**

```bash
pytest tests/scripts/test_run_registry.py -v
```
Expected: 2 PASSED

**Step 6: Commit**

```bash
git add project/scripts/build_run_registry.py project/scripts/query_runs.py \
        tests/scripts/test_run_registry.py
git commit -m "feat: add SQLite run registry for queryable experiment tracking"
```

---

## Integration Test

After all 10 tasks are done, run the full test suite:

```bash
cd /home/tstuv/workspace/backtest
source .venv/bin/activate
make test-fast
```

Expected: all tests pass. If any test fails, fix before proceeding.

Then run a smoke run end-to-end:

```bash
export BACKTEST_DATA_ROOT=$(pwd)/data
make run 2>&1 | tail -20
```

---

## Summary of Changes

| Task | Files Created | Files Modified |
|------|--------------|----------------|
| 1. Purge bars | — | `eval/splits.py`, `pipelines/eval/splits.py`, `pipelines/eval/run_walkforward.py` |
| 2. Funding accrual | — | `engine/pnl.py` |
| 3. Exchange constraints | `engine/exchange_constraints.py` | `engine/runner.py` |
| 4. Fragility gate | — | `pipelines/research/promote_blueprints.py` |
| 5. PSR/DSR | `eval/selection_bias.py` | `pipelines/research/promote_blueprints.py` |
| 6. Redundancy control | `eval/redundancy.py` | `pipelines/research/promote_blueprints.py` |
| 7. Slippage calibration | `pipelines/clean/calibrate_execution_costs.py` | `engine/execution_model.py` |
| 8. Data QC report | `schemas/qc_contracts.py` | `pipelines/report/qa_data_layer.py` |
| 9. Report benchmarks | `eval/benchmarks.py` | `pipelines/report/make_report.py` |
| 10. Run registry | `scripts/build_run_registry.py`, `scripts/query_runs.py` | — |
