# Project Report (Detailed)

## Scope of this follow-up

This update addresses review feedback on the recent `vol_compression_v1` hardening work with a focus on consistency, determinism, and auditability.

## What changed

### 1) Exit classification now respects strategy runtime configuration

- Trade extraction in the backtest stage now accepts `max_hold_bars` as an explicit input.
- Time-based exit labeling (`exit_reason = time`) uses that configured value rather than a hardcoded `48` bars.
- This keeps post-trade diagnostics aligned with the strategy's actual execution policy.

### 2) Strategy parameter resolution supports both config layouts

- Backtest stage now resolves parameters from:
  - top-level keys (existing behavior), and
  - nested `strategy_defaults.vol_compression_v1`.
- Consolidated parameters are still overlay-adjusted afterward, preserving existing overlay workflow.

### 3) Adaptive trailing logic removes same-bar look-ahead in trailing baseline

- Adaptive trailing stop baseline now uses prior bars only (`idx-1` backward), not the current bar.
- This prevents the trailing baseline from being informed by the same bar where stop breach is evaluated.

### 4) Test coverage expanded for trailing-stop correctness

- Added a targeted strategy unit test that verifies adaptive trailing stop logic is not triggered by same-bar trailing baseline construction.

## Validation run

- Focused strategy tests covering adaptive behavior and timeout exits passed.
- Pipeline/runner test set also passed.

## Notes

- Existing warnings in tests are unrelated to this change set (pandas deprecations/future warnings in other utility code paths).
- No behavior was changed for unrelated strategies.
