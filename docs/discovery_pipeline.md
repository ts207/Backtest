# Discovery Pipeline (Phase 1 -> Phase 2 -> Strategy Handoff)

## Purpose
This document explains what happens during discovery, how edges are promoted, and why some families can produce zero events.

## Data used for promotion
- Bars/features: `data/lake/features/{perp|spot}/{symbol}/15m/features_v1/...`
- Context: `data/lake/runs/<run_id>/context/{funding_persistence,market_state}/...`
- Phase 1 outputs: `data/reports/<event_type>/<run_id>/...`
- Phase 2 outputs: `data/reports/phase2/<run_id>/<event_type>/...`

## Stage order
1. Ingest and clean market data (`perp`; `spot` added automatically for cross-venue).
2. Build `features_v1` and context features.
3. Phase 1 analyzers generate event/control structure artifacts.
4. Phase 2 evaluates condition/action candidates with gates.
5. Promoted candidates are normalized into edge universe.
6. Strategy builder converts promoted edges into manual-backtest candidates.

## Promotion logic summary
- Phase 1 must have events (`phase1_structure_pass=true`) or Phase 2 freezes with `reason=no_phase1_events`.
- Phase 2 ranks condition/action hypotheses by edge/stability and promotes gate-passing candidates.
- Expectancy + robustness reports are applied after export to avoid strategy decisions from one noisy metric.

## Why cross-venue can show zero events
- `cross_venue_desync` needs both perp and spot feature sets.
- If spot features are missing, phase1 event count is zero and phase2 freezes.
- The cross-venue summary now reports missing perp/spot symbols explicitly.

## Are gates too harsh?
- First check data availability and event count before loosening gates.
- For `liquidity_vacuum`, tune with:
  - `--profile strict|balanced|lenient` (default `balanced`)
  - explicit overrides (`--vol_ratio_floor`, `--range_multiplier`, `--min_vacuum_bars`, etc.)
- Review `liquidity_vacuum_calibration.csv` before changing thresholds globally.
