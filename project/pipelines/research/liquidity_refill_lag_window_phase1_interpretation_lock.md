# Liquidity Refill / Requote Lag Window — Phase-1 Interpretation Lock

## Decision (binary)
**NO.**

## Final state change
- liquidity_refill_lag_window **FAILED Phase-1**.
- Failure is **structural, not parametric**.
- Event is **permanently frozen for this run**.
- **DRAFT placeholder remains** for provenance only.
- **No Phase-2, no overlays, no backtests.**

## Pivot (immediate next move)
Start a new Phase-1 microstructure frontier — **do not revisit liquidity/volatility families**.

Recommended next candidate (highest orthogonality):
**Directional Exhaustion After Forced Flow**

## Phase-1 definition (structure only)
- **Anchor:** liquidation cluster or extreme taker imbalance.
- **Event:** failure to make a new extreme within **N** bars after the anchor.
- **Window:** [0, N] (start with **N = 16 or 32**).
- **Outcomes (no PnL):**
  - hazard of continuation vs reversal
  - tail-move probability
  - realized range/vol vs matched baseline

## Rules
No actions, no overlays, no tuning loops; **CI + regime + time stability**; interpretation lock at end.

## Next command (when ready)
Design Phase-1 analyzer skeleton for `directional_exhaustion_after_forced_flow`.
