# ABMA v2 Funding Phase-1 Interpretation Lock

This lock fixes the Phase-1 interpretation for ABMA v2 funding to avoid drift
before falsification runs and Phase-2 decisions.

## Scope

- Market: Binance USDT-M perpetuals (24/7).
- Anchor: funding boundary timestamps (t0 = funding event time).
- Sessions: UTC daily session calendar.

## Baseline + controls

- Baseline window: `[t0 - 15m, t0)`.
- Baseline RV: 1-second asof-backward mid-price sampling, log-return RV.
- Baseline volume: sum of trade size over the baseline window.
- Control candidates: fixed time grid within session, baseline validity required.
- Matching: exact `(rv_q, vol_q)` quartile match with deterministic fallback to `rv_q` only.
- Exclusion window around funding events: `[t0 - 10m, t0 + 30m]`.

## Phase-1 gates

- Sign consistency, regime flip count, bootstrap CI exclusion, monotonic decay.
- No PnL metrics or tuning allowed in Phase-1.
