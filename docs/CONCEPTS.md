# Core Concepts

This document explains the fundamental principles and terminology used throughout the Backtest platform.

## 1. Point-in-Time (PIT) Integrity
In quantitative research, "Look-ahead bias" occurs when your model accidentally uses information from the future. Backtest enforces strict **Point-in-Time (PIT)** guards:
- **Feature PIT**: Every feature calculated at timestamp `T` only uses information available *before* `T`.
- **Label PIT**: Forward returns (labels) are strictly calculated starting *after* the event is fully detected.
- **Join Rate**: We verify that features and events align perfectly without gaps or misalignments.

## 2. Multi-Phase Discovery Pipeline
Instead of manually tweaking strategy parameters, the platform uses an automated funnel:

- **Phase 1 (Hypothesis)**: Detects specific market "events" (e.g., a sharp drop in liquidity).
- **Phase 2 (Conditional Analysis)**: Tests if a simple action (buy/sell) following that event has a statistical edge.
- **Multiplicity Control (BH-FDR)**: When testing thousands of hypotheses, some will succeed by random chance. We use the **Benjamini-Hochberg False Discovery Rate (BH-FDR)** procedure to filter these out and keep only discoveries with high confidence (`q-value <= 0.05`).

## 3. Strategy Blueprints & DSL
A **Blueprint** is a deterministic, JSON-serializable definition of a trading strategy. It contains:
- **Entry Logic**: Triggers and conditions.
- **Exit Logic**: Stop-losses, take-profits, and time-stops.
- **Sizing**: How much capital to risk.
- **Lineage**: Metadata tracing exactly which run and cost config produced this strategy.

We use a **Domain Specific Language (DSL)** to interpret these blueprints, ensuring that the exact same logic used during research is executed in the backtest engine without manual translation errors ("Jerry-work").

## 4. The "Bridge" (Economic Validation)
A statistical edge on paper often disappears when costs are added. The **Bridge** stage performs a conservative simulation:
- **Conservative Expectancy**: Subtracts fees and slippage (e.g., 6 bps total round-trip) from the raw edge.
- **Stressed Expectancy**: Tests the edge under 1.5x higher costs to ensure a "buffer" for market volatility.
- **Tradability Gate**: Only candidates that remain profitable under stressed costs are "promoted" to blueprints.

## 5. Walkforward Evaluation
To prevent "overfitting" (creating a strategy that only works on past data), we use **Walkforward splits**:
1.  **Train**: Find parameters/edges on early data.
2.  **Validation**: Verify the edge on a middle "blind" slice.
3.  **Test**: Final evaluation on the most recent, untouched data slice.
Strategies must show "Sign Stability" (consistent direction) across all slices to be considered robust.
