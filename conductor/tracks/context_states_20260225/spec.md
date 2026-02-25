# Specification: Implement multi-dimensional context state vectors

## Overview
Formalize the "Context State Vector" by implementing a 4-dimensional regime classifier. This allows the Strategy DSL to condition trades based on the confluence of volatility, liquidity, open interest trends, and funding persistence.

## Functional Requirements
- **Volatility Dimension:** Map `vol_regime` to `ms_vol_state` (0: LOW, 1: MID, 2: HIGH, 3: SHOCK).
- **Liquidity Dimension:** Implement `ms_liq_state` based on 24h rolling quantiles of ToB depth (0: THIN, 1: NORMAL, 2: FLUSH).
- **OI Dimension:** Implement `ms_oi_state` based on 1h OI delta z-score (0: DECEL, 1: STABLE, 2: ACCEL).
- **Funding Dimension:** Implement `ms_funding_state` based on trailing 8h sign consistency (0: NEUTRAL, 1: PERSISTENT, 2: EXTREME).
- **Composite State:** Generate `ms_context_state_code` as a unique permutation of the 4 dimensions.

## Acceptance Criteria
- New columns `ms_vol_state`, `ms_liq_state`, `ms_oi_state`, `ms_funding_state`, and `ms_context_state_code` materialized in the market context.
- Strategy DSL can successfully filter using `ms_context_state_code`.
- Verification run shows state transitions align with market regime shifts.
