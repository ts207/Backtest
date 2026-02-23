# Tech Stack: Backtest

## Core Language & Runtime
- **Python 3.x**: The primary language for the execution engine, pipeline orchestration, and feature engineering.

## Data Engineering & Storage
- **Pandas & NumPy**: Core libraries for vectorised data manipulation and numerical computation.
- **PyArrow (Parquet)**: Used for high-performance, columnar storage of research artifacts (features, events, candidates). Parquet is the standard format for the data lake.
- **Local Artifact Root**: Managed via the `BACKTEST_DATA_ROOT` environment variable, ensuring portability of research environments.

## Configuration & Specifications
- **YAML (PyYAML)**: The "Source of Truth" for system configuration and technical specifications (`spec/`).
- **JSON Schema**: Used for strictly validating the structure of pipeline artifacts.

## Quality Assurance & Testing
- **Pytest**: The primary framework for unit, integration, and contract testing.
- **Custom Linters**: Specialized scripts (e.g., `spec_qa_linter.py`) to enforce "Spec-First" integrity.

## Architecture
- **Monorepo**: A single repository containing all components of the discovery lifecycle.
- **Subprocess-Driven Orchestration**: Pipelines are executed as discrete subprocesses to ensure memory isolation and process-level observability.
- **Event-Driven Engine**: The backtesting core (`project/engine/`) uses an asynchronous, event-driven architecture to simulate market fills and risk management.
