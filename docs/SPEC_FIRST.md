# Spec-First Development

In this project, **Specifications are the Source of Truth**. We avoid hardcoding trading logic in Python files. Instead, logic is defined in YAML files within the `spec/` directory and interpreted by the platform.

## Why Spec-First?
1.  **Reproducibility**: You can recreate any research result by providing the exact same Spec files.
2.  **Auditability**: Compliance and risk teams can review trading logic in plain English/YAML without reading complex code.
3.  **Speed**: Researchers can add new features or event types by editing YAML files, often without writing any new Python code.

## Key Spec Files

### 1. Multiplicity Families (`spec/multiplicity/families.yaml`)
Defines the "Universe" of hypotheses to test for a specific event type.
```yaml
families:
  liquidity_vacuum:
    templates:
      - mean_reversion
      - continuation
    horizons:
      - 5m
      - 15m
      - 60m
```

### 2. Validation Gates (`spec/gates.yaml`)
Defines the statistical thresholds a candidate must pass.
```yaml
gate_v1_phase2:
  max_q_value: 0.05
  min_after_cost_expectancy_bps: 0.1
```

### 3. Feature Definitions (`spec/features/`)
Each feature has a YAML definition documenting its mathematical intent and Point-in-Time constraints.

## 🧠 Atlas-Driven Planning
The platform now uses `research_backlog.csv` as the primary driver for candidate generation.
1.  **Claims**: Claims are identified in the backlog (e.g., `CL_0093`).
2.  **Templates**: The Global Planner converts claims into stable Candidate Templates.
3.  **Feasibility**: The Plan Enumerator checks if the required Specs (`spec/events/*.yaml`) and datasets exist before including them in the run.

## Adding a New Logic Component
To add a new indicator or event detection rule:
1.  **Update Backlog**: Add a new row to `research_backlog.csv` with the claim and mechanism.
2.  **Generate Templates**: Run the global planner to see the new `spec_tasks.parquet` entry.
3.  **Draft the Spec**: Create the YAML file (e.g., `spec/events/LIQUIDATION_CASCADE.yaml`).
4.  **Execute**: Run the pipeline with `--atlas_mode 1`. The system will automatically pick up the new spec if it matches a claim in the plan.
