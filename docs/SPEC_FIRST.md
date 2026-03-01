# Spec-First Research Workflow

The Backtest platform is **spec-first**, meaning all research logic (events, features, gates) is defined in YAML files in `spec/` before a single line of execution code is written. 

This ensures that all Alpha discovery is **reproducible, deterministic, and auditable.**

---

## 1. The Research Lifecycle

| Phase | Input Spec | Output Artifact |
| :--- | :--- | :--- |
| **Event Detection** | `spec/events/*.yaml` | `events.parquet` |
| **Feature Engineering** | `spec/features/*.yaml` | `features_v1/` |
| **Hypothesis Generation** | `spec/hypotheses/*.yaml` | `candidate_plans.parquet` |
| **Statistical Gating** | `spec/gates.yaml` | `promoted_candidates.parquet` |
| **Blueprint Compilation** | `spec/strategies/*.yaml` | `blueprints.jsonl` |

---

## 2. Defining a New Event (`spec/events/`)

Events are the "triggers" for your research. Every event spec must define:
- **`logic`**: The detection formula using PIT-safe features.
- **`prevalence`**: Minimum/maximum expected occurrences per year.
- **`horizons`**: The primary look-forward windows (e.g., 5m, 15m, 1h).

```yaml
# Example: spec/events/VOL_SHOCK.yaml
name: VOL_SHOCK
family: VOLATILITY_TRANSITION
logic: "rv_ratio >= 2.5 AND abs(z_score_5m) >= 3.0"
horizons: ["5m", "15m", "60m"]
```

---

## 3. Statistical Gating (`spec/gates.yaml`)

This file defines the gauntlet that a research signal must run before it is promoted to a Blueprint.

- **`gate_v1_phase2`**: The standard research gate.
    - `max_q_value`: 0.05 (BH-FDR control).
    - `min_after_cost_expectancy_bps`: 5.0 (The "Bridge" gate).
    - `min_events`: 100 (Sample size requirement).

---

## 4. Why "Spec-First"?

1. **Anti-P-Hacking**: By requiring you to define your hypothesis and gate thresholds in a spec *before* running discovery, the system prevents you from "tweaking" parameters until a failing strategy looks profitable.
2. **Deterministic Replay**: A discovery run can be perfectly recreated by re-running the pipeline with the same `spec/` hash and data snapshot.
3. **Execution Parity**: Strategy Blueprints contain the exact conditions from the spec, ensuring that the **Research logic** in this repo is exactly what gets executed in **NautilusTrader**.

---

## 5. Adding New Logic

When you want to test a new idea:
1. Create a new YAML in `spec/events/` or `spec/hypotheses/`.
2. Run `make governance` to validate the spec schema.
3. Run `make discover-blueprints` to see if your idea survives the statistical gates.
