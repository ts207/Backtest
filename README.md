# Backtest: Quantitative Trading Research Platform

Backtest is a high-performance, event-driven research and backtesting platform specialized for cryptocurrency markets (Binance Perpetual and Spot). It implements a rigorous, multi-phase pipeline designed to discover, validate, and simulate trading strategies with statistical integrity.

## 🚀 Overview

Unlike traditional backtesters that simply replay price data, this platform focuses on the **Discovery Lifecycle**:
1.  **Ingestion & Cleaning**: Normalizing raw exchange data into high-quality "Point-in-Time" (PIT) datasets.
2.  **Hypothesis Generation**: Detecting market events (e.g., liquidity vacuums, volatility shocks) and generating directional hypotheses.
3.  **Statistical Validation**: Applying Multiplicity Control (BH-FDR) to filter out lucky discoveries and ensure statistical significance.
4.  **Economic Evaluation (Bridge)**: Simulating execution costs, slippage, and market impact to find truly tradable edges.
5.  **Walkforward Backtesting**: Evaluating strategy robustness across non-overlapping time splits and market regimes.

## 📂 Project Structure

- `project/`: The core Python source code.
    - `pipelines/`: Orchestration scripts for the 13+ discovery stages.
    - `engine/`: The event-driven execution engine (P&L, fills, risk).
    - `strategies/`: Implementations of trading logic and the DSL interpreter.
    - `strategy_dsl/`: Schema and logic for deterministic strategy "Blueprints".
- `spec/`: The **Source of Truth**. YAML files defining features, events, and validation gates.
- `data/`: (Gitignored) The local artifact root containing the data lake and run reports.
- `tests/`: Comprehensive test suite ensuring pipeline and engine integrity.

## 📖 Documentation Index

For someone new to the project, we recommend reading in this order:

1.  **[Getting Started](docs/GETTING_STARTED.md)**: How to set up your environment and run your first discovery pipeline.
2.  **[Core Concepts](docs/CONCEPTS.md)**: Understanding Point-in-Time (PIT) logic, Multiplicity, and why we use "Blueprints".
3.  **[Architecture Guide](docs/ARCHITECTURE.md)**: A deep dive into the multi-stage discovery pipeline and data flow.
4.  **[Spec-First Design](docs/SPEC_FIRST.md)**: How to add new features or event types by updating YAML specifications.

## 🛠️ Quick Commands

| Command | Description |
|:---|:---|
| `make run` | Ingest + Clean + Build Features |
| `make discover-edges` | Run full Discovery (Phase 1 + Phase 2) |
| `make test-fast` | Run quick validation tests |

---
*Backtest is designed for professional quantitative research where reproducibility and statistical rigor are paramount.*
