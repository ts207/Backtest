# Product Definition: Backtest

## Vision

Backtest is a spec-first quantitative research and backtesting platform for crypto markets. It prioritizes reproducibility, PIT correctness, and measurable promotion standards over ad-hoc experimentation.

## Target Users

- Quant researchers building and validating event-driven hypotheses.
- Systematic traders evaluating whether statistical signals survive economic constraints.

## Product Outcomes

- Fast, repeatable discovery across event families.
- Support for manual DSL strategy hypothesis testing via YAML.
- Built-in microstructure metrics (VPIN, Roll spread) for toxicity and liquidity analysis.
- Multi-dimensional context state vectors for advanced regime conditioning.
- Interaction lift analysis to quantify marginal value of context states.
- Strict statistical and economic gating before promotion/execution.
- Auditable run traces from config/spec hashes to artifact outputs.

## Discovery Lifecycle

1. Data ingest and normalization.
2. Feature/context construction.
3. Microstructure feature extraction.
4. Event detection and registry normalization.
5. Phase2 conditional discovery with multiplicity controls.
6. Bridge and promotion filtering.
7. Interaction lift analysis.
8. Manual DSL strategy definition and testing.
9. Optional execution evaluation (backtest/walkforward/report).

## Quality Bar

- No lookahead leakage.
- No hidden fallback in protected measurement flows.
- No unverifiable run outputs.
