# Product Definition: Backtest

## Vision

Backtest is a spec-first quantitative research and backtesting platform for crypto markets. It prioritizes reproducibility, PIT correctness, and measurable promotion standards over ad-hoc experimentation.

## Target Users

- Quant researchers building and validating event-driven hypotheses.
- Systematic traders evaluating whether statistical signals survive economic constraints.

## Product Outcomes

- Fast, repeatable discovery across event families.
- Strict statistical and economic gating before promotion/execution.
- Auditable run traces from config/spec hashes to artifact outputs.

## Discovery Lifecycle

1. Data ingest and normalization.
2. Feature/context construction.
3. Event detection and registry normalization.
4. Phase2 conditional discovery with multiplicity controls.
5. Bridge and promotion filtering.
6. Optional execution evaluation (backtest/walkforward/report).

## Quality Bar

- No lookahead leakage.
- No hidden fallback in protected measurement flows.
- No unverifiable run outputs.
