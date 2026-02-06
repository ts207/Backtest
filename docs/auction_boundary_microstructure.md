# Auction Boundary Microstructure Asymmetry (ABMA) — Phase-1 Analyzer Skeleton

This is a **structure-only**, pre-registered Phase-1 analyzer specification. It defines fixed anchors, fixed controls, fixed outputs, and a Phase-1 interpretation lock. No direction, no PnL, and no action timing is permitted.

## Contract summary

- **Module class**: structural context analyzer (not a signal).
- **Definition version**: `abma_def_version = "v1"` (frozen).
- **Primary anchor**: continuous session open (e.g., 09:30:00 local exchange time).
- **Auction boundary types**: **open only** for v1 (no reopen/close/halts).
- **No tuning**: all windows, buckets, and tests are fixed in v1.
- **Inputs required**: trade prints + quotes (best bid/ask + sizes), session calendar, and auction boundary timestamps.

## Scope & anchor

- **Event anchor**: `t0` = auction boundary at session open.
- **Event window**: `t0 + [0s … T]` with **T = 30 minutes** (fixed in v1).
- **Universe**: same instruments as baseline backtest universe.
- **Session calendar**: exchange-local schedule; events only when open auction exists.

## Structural variables (fixed in v1)

v1 uses all three variables below (no subsets allowed).

1. **Effective spread normalization half-life**
   - Time to revert to **session median** effective spread.
2. **Microprice variance decay**
   - EWMA decay rate of microprice variance after `t0` using half-life **H = 60 seconds**.
3. **Trade sign entropy**
   - Shannon entropy of trade signs over rolling **N = 50 trades**.

> No substitution or additions are allowed in v1.

## Controls (mechanically matched)

Controls are sampled **only** from non-auction periods and must be mechanically matched:

- Same **instrument**
- Same **time-of-day offset**
- Same **volatility bucket** (quartiles of baseline RV for that symbol)
- Same **volume percentile** (quartiles of baseline volume for that symbol)
- Same **day-of-week**
- **No auction event** at the control timestamp

Matched controls form a **1:K** set per event with **K = 5** (fixed in v1). Deltas are computed as event minus matched-control mean.

Control timestamps are selected at the **same time-of-day offset** within the same session type (regular session only).

## Required outputs (per split)

Per split (e.g., year, vol regime), the analyzer must emit:

- **Stabilization curves** (event vs control)
- **Effect size + bootstrap CI** (percentile bootstrap, **B = 1000**)
- **Regime-segmented plots**
- **Event/control sample counts**

## Stability tests (hard gates)

All must pass for Phase-1 acceptance:

- **Sign consistency ≥ 70%** across all required splits
- **≤ 1 sign flip** across regimes
- **Monotonic or convex decay** shape test (pre-defined)

Failure on any test **freezes** the frontier (no overlays or extensions).

## Phase-1 interpretation lock (frozen)

**Accept** only if:
- All stability tests pass.
- Event-control separation is consistent and direction-agnostic.

**Reject** if:
- Any stability test fails.
- Separation is non-monotonic or regime-dependent.

**Explicit non-usage (disallowed)**:
- No directional signals.
- No PnL attribution.
- No “best delay” optimization.
- No interaction with other overlays in v1.

## Analyzer skeleton: folder layout

Proposed file layout (v1):

```
project/
  analyzers/
    abma/
      __init__.py
      schema.py          # frozen v1 schema + column definitions
      config_v1.py       # fixed constants: windows, buckets, tests
      events.py          # auction boundary event extraction
      controls.py        # mechanical matching sampler
      metrics.py         # structural variable computations
      evaluate.py        # stabilization curves + tests
      report.py          # plots + tables
docs/
  auction_boundary_microstructure.md
tests/
  analyzers/
    test_abma_v1_contract.py
```

## Output schema (v1)

**Event-level outputs** (one row per event):

- `event_id` (str), `symbol` (str), `session_date` (date)
- `t0` (timestamp), `window_end` (timestamp)
- `abma_def_version` (str)
- `spread_half_life_sec` (float)
- `microprice_var_decay_rate` (float)
- `trade_sign_entropy_mean` (float)
- `controls_used` (int)

**Control summary outputs** (per event):

- `control_spread_half_life_mean` (float)
- `control_microprice_var_decay_mean` (float)
- `control_trade_sign_entropy_mean` (float)

**Delta outputs** (event minus matched control mean):

- `delta_spread_half_life_sec` (float)
- `delta_microprice_var_decay_rate` (float)
- `delta_trade_sign_entropy_mean` (float)

## Pre-registered failure modes

- Sign flips beyond **1** across splits.
- Non-monotonic stabilization curves.
- No measurable separation from matched controls.
- Inconsistent results across years or regimes.

## Notes

- This analyzer is **structural only** and must remain orthogonal to PnL logic.
- Any extension (reopen, close, halt-resume) requires a **new definition version**.

## Next steps (implementation checklist)

1. **Create analyzer module skeleton** under `project/analyzers/abma/` using the folder layout above.
2. **Define schema constants** in `config_v1.py`:
   - `T = 30 minutes`, `H = 60 seconds`, `N = 50 trades`, `K = 5`, `B = 1000`.
3. **Implement event extraction** for open-auction boundaries only.
4. **Implement control sampler** enforcing the mechanical matching rules and non-auction constraint.
5. **Compute structural metrics** and emit the output schema exactly as specified.
6. **Add contract tests** to validate required columns, fixed constants, and gate logic.
