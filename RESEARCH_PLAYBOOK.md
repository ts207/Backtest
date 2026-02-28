# Research Playbook — Backtest

Purpose:
Define how trading strategies are discovered, validated, and promoted.
This document encodes the research philosophy used inside this repository.

Claude must follow this playbook when assisting research.

---

# 1. Research Objective

Goal:
Produce deployable crypto trading strategies that remain profitable
after costs and regime changes.

NOT goals:
- maximizing Sharpe
- fitting historical patterns
- producing many strategies

Preferred outcome:
few robust strategies.

---

# 2. Market Assumptions

Crypto perpetual markets exhibit:

1. liquidity shocks
2. funding imbalances
3. volatility regime transitions
4. leverage crowding cycles
5. reflexive liquidations

Edges are expected to arise from structural behavior,
not prediction accuracy.

---

# 3. Strategy Philosophy

Strategies must be:

- event-driven
- conditional
- regime-aware
- cost-robust
- point-in-time valid

Avoid:
- continuous indicator optimization
- parameter sweeps without hypothesis
- static thresholds across regimes

---

# 4. Primary Edge Families

Claude should prioritize research in this order:

## A. Liquidity Events
Examples:
- liquidity vacuum
- liquidation cascades
- spread expansion

Expectation:
short-horizon mean reversion.

---

## B. Funding Extremes
Examples:
- persistent positive funding
- crowded directional positioning

Expectation:
carry unwind or reversal.

---

## C. Volatility Compression → Expansion

Examples:
- realized vol contraction
- range compression

Expectation:
breakout regimes.

---

## D. Regime Interaction (advanced)

Examples:
event × volatility regime
event × trend regime

Goal:
conditional alpha.

---

# 5. Research Workflow (MANDATORY)

Every strategy must pass:

Phase 1 — Event Detection
    Detect structural phenomenon.

Phase 2 — Conditional Discovery
    Identify statistically significant conditions.

Bridge Evaluation
    Verify tradability after costs.

Promotion
    Only stable candidates survive.

Backtest
    Validate portfolio behavior.

Walk-forward (optional but preferred)
    Confirm temporal robustness.

Skipping stages is forbidden.

---

# 6. Hypothesis Standards

Valid hypothesis format:

"When EVENT occurs under CONDITION,
returns over HORIZON show persistent bias."

Invalid:
"indicator X looks profitable."

---

# 7. Statistical Discipline

Claude must enforce:

- minimum sample size awareness
- delay grids for confirmation-based events
- no forward leakage
- consistent cost model
- regime robustness checks

Reject edges that disappear after small perturbations.

---

# 8. Cost Model Assumption

Default personal execution:

round-trip cost ≈ 6–10 bps.

All discovery and backtests must use same costs.

---

# 9. Promotion Philosophy

Promotion favors:

✓ stability across symbols  
✓ stability across regimes  
✓ moderate but persistent edge  
✓ lower turnover preferred  

Reject:

✗ extreme Sharpe with low sample count  
✗ regime-specific spikes  
✗ fragile parameter dependence  

---

# 10. Personal Risk Constraints

(Adjust later)

Target max drawdown: 20–30%
Preference: smoother equity over peak returns.
Capital preservation > growth rate.

---

# 11. Claude Research Behavior

Claude should:

- suggest smallest next experiment
- debug upstream causes first
- analyze failures before proposing new ideas
- reduce hypothesis space over time

Claude should NOT:
- propose many strategies simultaneously
- optimize parameters blindly
- chase metrics

---

# 12. Definition of Success

A successful strategy:

✓ survives promotion gates  
✓ profitable after costs  
✓ stable across time  
✓ understandable mechanism  
✓ executable without discretion