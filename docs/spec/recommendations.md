# Backtest Repository Recommendations (Implementation-Focused)

This document converts high-level guidance into concrete implementation tasks for the current repository. It assumes the recent sample run remained negative-expectancy with low trade count and should be treated as a **delivery checklist**, not just ideas.

## Objectives and promotion standard

Before promoting any strategy variant, require all of the following:

- Positive net return after realistic costs.
- Acceptable drawdown versus baseline.
- Minimum trade count threshold (to avoid sparse-sample conclusions).
- Stability across walk-forward folds and core market regimes.

If any gate fails, keep the variant in research status.

## 1) Strategy logic hardening (first priority)

### What to implement

- Add stricter breakout confirmation rules in `project/strategies/vol_compression_v1.py`.
- Add fast-fail exits when post-entry expansion does not materialize within a bounded time window.
- Add an adaptive exit mode (e.g., trailing logic after expansion confirmation) behind explicit config flags.
- Keep all behavior deterministic and parameterized in config (no hidden constants).

### Definition of done

- Strategy params are externally configurable and documented.
- Backtest output includes clear reason tags for entry/exit path diagnostics.
- Unit tests cover at least: confirmation pass/fail, timeout exit, and adaptive exit behavior.

## 2) Walk-forward parameter research loop

### What to implement

- Add a repeatable sweep driver under `project/pipelines/research/` for bounded parameter grids.
- Use fold-based evaluation (train/validate split per fold; final untouched holdout).
- Rank candidates by multi-objective scoring instead of single-metric return.
- Persist every sweep result under `data/runs/<run_id>/` for auditability.

### Minimum metrics to rank

- Net return after fees.
- Max drawdown.
- Trade count.
- Fold consistency score (dispersion penalty).

### Definition of done

- A single command produces fold-level and aggregate ranking artifacts.
- Holdout performance is reported separately and never used for tuning.
- Promotion/demotion decision is machine-readable in the run output.

## 3) Expand symbol universe with inclusion rules

### What to implement

- Extend ingest/config symbol lists beyond BTCUSDT/ETHUSDT using liquidity filters.
- Add inclusion/exclusion criteria in config (e.g., history length, gap tolerance).
- Ensure reports always show both per-symbol and pooled outcomes.

### Definition of done

- Pipeline runs cleanly on the expanded set.
- Missing-data diagnostics remain within pre-declared thresholds.
- Promotion uses minimum trade-count and symbol-coverage requirements.

## 4) Context/feature enrichment with ablations

### What to implement

- Add context candidates only when tied to explicit hypotheses (e.g., funding persistence, volatility regime).
- Run ablations: baseline vs +feature, holding all other knobs fixed.
- Freeze feature versions once promoted to keep comparisons apples-to-apples.

### Definition of done

- Each added feature has a short hypothesis + result note in run artifacts.
- No feature is promoted without out-of-sample uplift or robustness benefit.

## 5) Robustness and execution realism gates

### What to implement

- Stress-test fee/slippage assumptions and small signal/latency perturbations.
- Add regime-split diagnostics (bull/bear, high/low volatility) to reports.
- Reject variants that only succeed in one narrow regime.

### Definition of done

- Report includes a robustness section with pass/fail gates.
- Registry promotion requires passing robustness gates, not just top-line return.

## 6) Operational workflow upgrades

### What to implement

- Add a reusable research checklist template stored with each run.
- Standardize run manifests and summary outputs as the source of truth.
- Require small, hypothesis-driven PRs rather than broad mixed changes.

### Suggested checklist fields

- Data quality pass/fail.
- Coverage/trade-count sufficiency.
- Walk-forward and holdout outcomes.
- Robustness gate outcomes.
- Promotion decision + rationale.

## Recommended execution order

1. Strategy hardening and deterministic exits.
2. Walk-forward sweep and promotion gate automation.
3. Symbol-universe expansion with quality filters.
4. Feature/context ablations with frozen versions.
5. Robustness gate enforcement in promotion workflow.

