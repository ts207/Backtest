# Backtest Runbook

Operational reference for running, debugging, and researching strategies
inside the Backtest discovery-first quantitative research platform.

This document describes HOW the system is used in practice.

---

# 1. Mental Model

The system is NOT a strategy executor.

It is a research factory:

raw market data
    ↓
features + context
    ↓
event detection
    ↓
conditional discovery (Phase2)
    ↓
tradability gating (Bridge)
    ↓
candidate promotion
    ↓
strategy blueprint
    ↓
backtest + evaluation

Each stage produces artifacts consumed by the next stage.

---

# 2. Directory Map

## Code

project/
    pipelines/        ← orchestration + stages
    features/         ← feature engineering
    events/           ← event registry + flags
    engine/           ← execution + pnl
    strategies/       ← strategy implementations

spec/
    hypotheses/
    events/
    gates/
    ontology/

## Outputs (DO NOT EDIT)

data/
    lake/             ← datasets
    runs/<run_id>/    ← run artifacts
    reports/          ← discovery outputs

---

# 3. Environment Setup

Always run inside repo root:

cd ~/workspace/backtest

export BACKTEST_DATA_ROOT=$(pwd)/data

Activate environment:

source .venv/bin/activate

---

# 4. Smoke Test (First Command)

Confirms pipeline integrity.

python project/pipelines/run_all.py \
  --run_id smoke \
  --symbols BTCUSDT \
  --start 2024-01-01 \
  --end 2024-01-10 \
  --run_phase2_conditional 0 \
  --run_backtest 1 \
  --strategies vol_compression_v1 \
  --cost_bps 6

Expected:
data/runs/smoke/ exists.

---

# 5. Baseline Strategy Validation

Purpose:
Verify costs + engine behavior before discovery.

make baseline \
  RUN_ID=baseline \
  SYMBOLS=BTCUSDT,ETHUSDT \
  START=2022-01-01 \
  END=2024-12-31

Outputs:
data/runs/baseline/backtest_strategies/

---

# 6. Discovery Workflow (Primary Research Loop)

## Step 1 — Generate hypotheses + Phase2

python project/pipelines/run_all.py \
  --run_id discover_lqvac \
  --symbols BTCUSDT,ETHUSDT \
  --start 2022-01-01 \
  --end 2024-12-31 \
  --run_phase2_conditional 1 \
  --phase2_event_type LIQUIDITY_VACUUM \
  --phase2_delay_grid_bars 8,16,30

Check:

data/reports/phase2/discover_lqvac/

---

## Step 2 — Bridge Evaluation

Ensures tradability after costs.

Artifacts:
data/reports/bridge_eval/

If empty → edge not tradable.

---

## Step 3 — Candidate Promotion

Produces survivors only.

Artifacts:
data/reports/candidate_promotion/

If no promoted candidates → adjust hypotheses, NOT backtest.

---

## Step 4 — Blueprint Compilation

Creates executable strategies.

Output:
data/reports/strategy_blueprints/<run_id>/blueprints.jsonl

---

## Step 5 — Backtest Blueprints

python project/pipelines/run_all.py \
  --run_id discover_lqvac \
  --run_backtest 1 \
  --blueprints_path data/reports/strategy_blueprints/discover_lqvac/blueprints.jsonl

---

# 7. Where Failures Usually Occur

| Stage | Typical Cause |
|------|--------------|
| ingest | missing env var |
| features | schema mismatch |
| events | PIT violation |
| phase2 | insufficient samples |
| bridge | costs too high |
| promotion | instability |
| backtest | empty blueprint set |

---

# 8. Debug Procedure

When a run fails:

1. Inspect:
   data/runs/<run_id>/run_manifest.json

2. Identify failing stage.

3. Check upstream artifact exists.

4. Verify schema + timestamps.

5. Fix smallest upstream issue.

Never patch downstream outputs.

---

# 9. Cost Model Guidelines

Default realistic crypto assumption:

round-trip ≈ 6–10 bps

Use:

--cost_bps 6

Discovery and backtest MUST use same cost model.

---

# 10. Strategy Success Criteria

A strategy is valid only if:

✓ survives Phase2 statistical filters  
✓ passes Bridge evaluation  
✓ promoted to blueprint  
✓ profitable after costs  
✓ stable across time splits

High Sharpe alone is NOT success.

---

# 11. Common Commands

Run tests:
make test-fast

Lint + hygiene:
make check-hygiene

List runs:
ls data/runs/

Inspect reports:
ls data/reports/

---

# 12. Golden Rule

Do not optimize backtests.

Optimize discovery validity.