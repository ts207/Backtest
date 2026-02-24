# Readiness Snapshot

Date: 2026-02-24

This snapshot reflects current code behavior, not historical runs.

## Current Status

The project is ready for controlled research runs when data coverage requirements are met.

## What is production-protective now

- Fail-closed stage behavior for key integrity violations.
- Registry-backed event normalization and dual flag semantics (`*_event`, `*_active`).
- Phase2 entry-timing guard (`entry_lag_bars >= 1`).
- Multiplicity control and downstream gate chaining before compilation/promotion.
- Provenance-aware manifests and repo hygiene checks.

## What still requires operator discipline

- Ensure raw funding coverage is complete for selected symbols/time windows.
- Use bridge and walkforward before relying on discoveries.
- Keep event-family-specific assumptions reviewed (thresholds, sample adequacy).

## Recommended run sequence

1. Build data/features/context.
2. Run one event family discovery.
3. Verify registry flags and Phase2 sample sizes.
4. Run bridge.
5. Compile blueprints.
6. Run backtest and walkforward.
