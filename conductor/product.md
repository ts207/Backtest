# Initial Concept\nA high-performance, event-driven quantitative trading research and backtesting platform specialized for cryptocurrency markets.

# Product Definition: Backtest

## Vision
Backtest is a high-performance, event-driven research and backtesting platform specialized for cryptocurrency markets (Binance Perpetual and Spot). It implements a rigorous, multi-phase discovery pipeline designed to discover, validate, and simulate trading strategies with statistical integrity.

## Target Users
- **Quant Researchers**: Focusing on reproducibility, statistical rigor, and the discovery of new market edges.
- **Algorithmic Traders**: Prioritizing execution simulation, cost modeling, and finding truly tradable strategies.

## Core Functional Goals
- **Automated Discovery**: Rapidly identify new market edges through automated, multi-stage pipelines.
- **Data Integrity (PIT)**: Strictly adhere to Point-in-Time (PIT) guards to prevent lookahead bias and ensure research reliability.
- **Spec-Driven R&D**: Maintain a "Spec-First" approach where YAML specifications for features, events, and validation gates serve as the source of truth.
- **High-Performance Execution**: Efficiently handle high-frequency cryptocurrency market data across perpetual and spot markets.

## Primary Core Priority
**Speed of Discovery**: While maintaining a high standard of statistical rigor and data integrity, the system is optimized to reduce the time-to-market for new trading ideas and hypotheses.

## Discovery Lifecycle
The platform provides full support for the following critical phases:
1. **Ingestion & Cleaning**: Normalizing raw exchange data into high-quality, research-ready "Point-in-Time" (PIT) datasets.
2. **Hypothesis & Events**: Detecting market events (e.g., liquidity vacuums, vol shocks) and generating directional hypotheses.
3. **Statistical Validation**: Applying Multiplicity Control (BH-FDR) to filter out lucky discoveries and ensure statistical significance.
4. **Economic Evaluation**: Simulating execution costs, slippage, and market impact to assess the real-world viability of strategies.
5. **Walkforward Backtesting**: Evaluating strategy robustness across non-overlapping time splits and market regimes.
