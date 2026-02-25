# Specification: Implement Amihud and Kyle's Lambda microstructure metrics

## Overview
Implement two additional microstructure metrics to complete the liquidity and market impact feature set: Amihud Illiquidity and Kyle's Lambda. These will be used to detect regime shifts in market depth and order flow toxicity.

## Functional Requirements
- **Amihud Illiquidity (`ms_amihud_24`):**
    - Calculated as $|Return| / (Close 	imes Volume)$.
    - Use a 24-bar rolling window.
- **Kyle's Lambda (`ms_kyle_24`):**
    - Estimated via a 24-bar rolling regression of price changes on net order flow.
    - Net order flow proxy: `taker_buy_volume - taker_sell_volume`.
- **PIT Safety:** Ensure all rolling calculations use only past and current bar data.

## Acceptance Criteria
- New features `ms_amihud_24` and `ms_kyle_24` materialized in the feature lake.
- Calculation logic verified with unit tests using synthetic data.
- Metrics show expected correlation with volatility and spread spikes in historical data.
