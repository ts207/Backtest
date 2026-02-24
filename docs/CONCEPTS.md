# Core Concepts

## 1) Point-in-Time (PIT)

No stage may use future information at decision time.

- Features are timestamped and joined backward.
- Event anchoring uses `enter_ts`.
- Phase2 expectancy uses lagged entry (`entry_lag_bars >= 1`).

## 2) Event Semantics

Each event family has two runtime signal forms:

- `*_event`: impulse flag at anchor timestamp.
- `*_active`: active window from `enter_ts` through `exit_ts`.

This allows impulse-triggered entries and window-aware filtering.

## 3) Discovery Pipeline

- Phase1: detect event instances.
- Registry: normalize and align to symbol/time grid.
- Phase2: test conditional hypotheses and compute q-values.
- Bridge: apply tradability/cost constraints.
- Compile: convert candidates to executable blueprints.

## 4) Multiplicity Control

Phase2 applies BH-FDR in two layers:

- family-level correction,
- global correction over family-adjusted values.

Only statistical discoveries can survive final Phase2 gates.

## 5) Economic Validation

Statistical edge is insufficient. Candidates must also pass:

- after-cost expectancy,
- stressed-cost expectancy,
- bridge validation trade-count and tradability constraints.

## 6) Spec-Driven Behavior

Specs in `spec/` define event mappings, gates, and defaults.
Code should derive behavior from specs where possible.
