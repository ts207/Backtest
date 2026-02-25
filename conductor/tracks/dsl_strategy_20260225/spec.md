# Specification: DSL Strategy - Liquidity Dislocation Mean Reversion

## Overview
Implement a new Strategy DSL pattern focusing on Mean Reversion triggered by Liquidity Dislocations. This strategy aims to capture alpha from temporary price deviations caused by liquidity vacuums, while filtering out high-volatility "shock" regimes to manage risk.

## Functional Requirements
- **Event Ingestion:** Ingest `LIQUIDITY_DISLOCATION` events from the canonical `event_registry.parquet`.
- **Logic Gates:**
    - Filter: `vol_regime != "SHOCK"`.
    - Filter: Event `severity` > 2.0.
- **Action:** Execute the `mean_reversion` operator logic.
- **Execution:** Integrate with `execution_model.py` for dynamic cost estimation and slippage gating based on order-book depth.

## Non-Functional Requirements
- **PIT Correctness:** Ensure all joins and feature lookups are point-in-time safe.
- **Contract Adherence:** Follow `strategy_dsl` v1 contracts.
- **Traceability:** Log all trigger conditions and execution cost estimates in the backtest report.

## Acceptance Criteria
- Strategy successfully detected and backtested using `project/pipelines/run_all.py`.
- Backtest report shows trades are inhibited during "SHOCK" regimes.
- Cost estimates reflect dynamic slippage correctly during dislocation events.
- Strategy meets the promotion gate of minimum 30 samples in the discovery window.

## Out of Scope
- Adding new DSL operators or changing the core DSL grammar.
- Cross-exchange arbitrage or spot/perp basis trades for this specific pattern.
