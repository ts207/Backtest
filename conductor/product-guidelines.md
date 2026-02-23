# Product Guidelines: Backtest

## Prose Style & Documentation
- **Tone**: Professional and Technical. Documentation should prioritize clarity, precision, and technical accuracy over narrative flourish.
- **Clarity First**: Explain complex quantitative concepts (e.g., Multiplicity, PIT guards) clearly but without oversimplification.
- **Emoji Usage**: Use standard emojis for top-level headers in markdown (e.g., 🚀, 🧠, 🛠️) to maintain consistency with existing documentation.

## Naming Conventions
- **Market Mechanics**: Names for events and features must be **Microstructure-Focused**, describing the underlying market mechanics (e.g., `liquidity_vacuum`, `vol_shock_relaxation`).
- **Standardization**: Follow the snake_case convention for all identifiers in YAML specs and code.
- **Explicit Horizons**: When naming features or labels with a time component, include the horizon (e.g., `fwd_ret_5m`).

## User Experience (UX) & Reporting
- **Data-Dense Artifacts**: Prioritize the generation of raw, high-quality Parquet artifacts. The primary "interface" for users is programmatic analysis of these datasets.
- **Schema Integrity**: Every pipeline stage must emit artifacts that strictly adhere to defined schemas (e.g., `feature_schema_v1.json`).
- **Traceability**: All reports must include `spec_hashes`, `dataset_hash`, and `code_commit` to ensure results are reproducible and bound to specific thresholds.

## Technical Integrity & Invariants
- **Spec-First Enforcement**: Maintain a **Strict (Linter-Heavy)** relationship between code and specifications. The `spec_qa_linter.py` must be used to ensure that all implementations match their YAML definitions.
- **Point-in-Time (PIT) Guards**: All feature engineering and event detection must implement strict PIT guards to prevent any lookahead bias.
- **Golden Baseline**: Changes to core logic must be verified against the "Certification Batch" (Run ID: `certification_batch`) to prevent regressions.
