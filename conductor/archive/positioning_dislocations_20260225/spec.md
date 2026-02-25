# Specification: Funding and Basis Dislocation Analyzers

## Overview
Implement Phase 1 research analyzers to detect "Positioning Shocks": Funding Dislocations and Basis Shocks. These events identify extreme imbalances between spot and perpetual markets, typically indicating forced positioning or arbitrage opportunities.

## Functional Requirements
- **Funding Dislocation (FND_DISLOC):**
    - Detect bars where the perpetual funding rate exceeds an absolute threshold (default: 2 bps).
    - Capture `fr_magnitude`, `fr_sign`, and `ms_funding_state` as metadata.
- **Basis Dislocation (BASIS_DISLOC):**
    - Calculate the normalized basis: `(perp_close - spot_close) / spot_close`.
    - Detect shocks where the basis deviates significantly from its 24h rolling median (z-score > 3.0).
    - Capture `basis_bps`, `basis_zscore`, and `basis_regime` as metadata.
- **Modularity:** Implement as two separate scripts: `analyze_funding_dislocation.py` and `analyze_basis_dislocation.py`.

## Acceptance Criteria
- Both analyzers successfully execute via CLI and produce valid CSV reports.
- Detected events are correctly merged into the canonical `event_registry.parquet`.
- Validation on high-volatility periods shows that Basis Shocks frequently lead or coincide with Liquidation Cascades.
