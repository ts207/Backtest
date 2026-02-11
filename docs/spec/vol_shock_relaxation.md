# Vol Shock → Relaxation (VSR) Event Schema

This is a frozen, event-level **context** module (`vsr_def_version="v1"`) with de-overlap and no directional outputs.

## Core construction

- `r_t = log(close_t / close_{t-1})`
- `rv_t`: rolling RMS of returns (`W=12`)
- `rv_base_t`: rolling median of `rv_t` (`B=288`)
- `shock_ratio_t = rv_t / rv_base_t`

Thresholds are distribution-based and frozen in v1:
- `T_shock = quantile(shock_ratio, 0.99)`
- `T_relax = 1.25`
- Relaxed when `shock_ratio <= T_relax` for `K=3` consecutive bars.
- Cooldown after exit: `C=W=12` bars.

## Event-level outcomes (one row per event)

- `time_to_relax`, `rv_peak`, `t_rv_peak`, `rv_decay_half_life`, `auc_excess_rv`
- secondary-shock outcomes: `secondary_shock_within_h`, `time_to_secondary_shock`
- post-horizon shape metrics: `realized_vol_mean_96`, `realized_vol_p90_96`, `range_pct_96`
- relaxation success: `relaxed_within_96`

## Matched baseline controls

For each event, matched non-event controls are sampled by:
- `symbol`
- `vol_regime quartile` (from baseline RV)
- `time-of-day bucket`

Deltas are reported as event minus matched-control mean, with bootstrap CIs.

Secondary-shock timing is measured only after local peak and before relaxation exit (censored otherwise).

## Hazards and phase gate

- Hazard curves by event-age for:
  - secondary shock (absolute + delta vs matched controls)
  - range expansion threshold crossing
- Phase landmarks are checked across required splits (`symbol`, `bull/bear`, `high/low vol`) with tolerance + fractional-time fallback.

## Explicit non-usage

- No directional signal generation.
- No PnL optimization loop in module definition.
- Treated as structural context unless kill-criteria for stable actionable asymmetry is explicitly passed.
