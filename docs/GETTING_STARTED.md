# Getting Started

This guide will walk you through setting up the Backtest platform and running your first automated strategy discovery pipeline.

## 📋 Prerequisites

- **Python 3.10+**
- **pip** (Python package manager)
- **Make** (optional, for shortcut commands)

## ⚙️ Installation

1. **Clone the repository**:

    ```bash
    git clone <repository-url>
    cd Backtest
    ```

2. **Create a virtual environment**:

    ```bash
    python -m venv .venv
    # Windows:
    .venv\Scripts\activate
    # Linux/macOS:
    source .venv/bin/activate
    ```

3. **Install dependencies**:

    ```bash
    pip install -r requirements.txt
    pip install -r requirements-dev.txt
    ```

4. **Set Environment Variables**:
    The platform needs to know where to store data. By default, it uses a `data/` folder in the root.

    ```bash
    # Windows (PowerShell):
    $env:BACKTEST_DATA_ROOT = "$(pwd)\data"
    # Linux/macOS:
    export BACKTEST_DATA_ROOT=$(pwd)/data
    ```

## 🏃 Running Your First Discovery

The platform uses a master orchestrator `project/pipelines/run_all.py` to manage complex research stages.

### 1. Ingest and Clean Data

Run the initial stage to fetch data for BTC and ETH:

```bash
python project/pipelines/run_all.py 
    --run_id my_first_run 
    --symbols BTCUSDT,ETHUSDT 
    --start 2024-01-01 
    --end 2024-01-07
```

### 2. Run Full Discovery

To detect "Liquidity Vacuum" events and validate them statistically:

```bash
python project/pipelines/run_all.py 
    --run_id discovery_run 
    --symbols BTCUSDT,ETHUSDT 
    --start 2024-01-01 
    --end 2024-01-07 
    --run_hypothesis_generator 1 
    --run_phase2_conditional 1 
    --phase2_event_type liquidity_vacuum 
    --run_bridge_eval_phase2 1 
    --run_strategy_blueprint_compiler 1
```

### 3. Run Atlas-Driven Discovery (Automated)

Instead of specifying event types manually, let the Knowledge Atlas drive the queue:

```bash
python project/pipelines/run_all.py \
    --run_id atlas_run \
    --symbols BTCUSDT,ETHUSDT,SOLUSDT \
    --start 2024-01-01 \
    --end 2024-01-07 \
    --run_hypothesis_generator 1 \
    --atlas_mode 1 \
    --run_phase2_conditional 1
```

### 4. Check the Results

After the run finishes, check the following locations:

- **Run Logs**: `data/runs/discovery_run/`
- **Global Templates**: `atlas/template_index.md`
- **Per-Run Plan**: `data/reports/hypothesis_generator/discovery_run/candidate_plan.jsonl`
- **Discovered Edges**: `data/reports/phase2/discovery_run/LIQUIDATION_CASCADE/phase2_candidates.csv`
- **Compiled Blueprints**: `data/reports/strategy_blueprints/discovery_run/blueprints.jsonl`

## ✅ Running Tests

Ensure everything is working correctly by running the fast test suite:

```bash
make test-fast
```

or directly with pytest:

```bash
pytest -m "not slow"
```
