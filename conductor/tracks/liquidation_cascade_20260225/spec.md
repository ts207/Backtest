# Specification: Liquidation Cascade Event Analyzer

## Overview
Implement a Phase 1 research analyzer to detect Liquidation Cascades. These are defined as sequences of forced liquidations that result in substantial price impact and a notable reduction in market open interest.

## Functional Requirements
- **Detection Logic:** Identify bars where `liquidation_notional` > 3.0 * rolling median AND `oi_delta_1h` < 0.
- **Event Metadata:** Capture `total_liquidation_notional`, `oi_reduction_pct`, and `price_drawdown` for each event cluster.
- **Reporting:** Export events to `data/reports/liquidation_cascade/{run_id}/liquidation_cascade_events.csv`.

## Acceptance Criteria
- Analyzer successfully runs via CLI.
- Detected events are visible in the canonical `event_registry.parquet`.
- Validation on sample runs (e.g. BTCUSDT 2021) shows high alignment with market crashes.
