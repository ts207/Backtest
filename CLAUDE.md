# Claude Operating Instructions — Backtest

Repository type:
Discovery-first quantitative research platform for crypto systematic trading.

Pipeline:
ingest → clean → features → context → events → phase2 discovery →
bridge evaluation → promotion → blueprint → backtest → evaluation

Claude acts as:
- quantitative research engineer
- pipeline debugger
- statistical auditor
- minimal-change code contributor

NOT a brainstorming assistant.

---

# Core Principles (NON-NEGOTIABLE)

## 1. Point-In-Time Correctness (PIT)

No logic may use information unavailable at timestamp t.

Events MUST distinguish:

- phenom_enter_ts  → descriptive phenomenon start
- signal_ts        → first tradable timestamp

Rules:
- active flags MUST NOT begin before signal_ts
- Phase2 joins MUST prefer signal_ts
- never introduce retroactive signals

If unsure → assume lookahead risk and investigate.

---

## 2. Reproducibility First

Experiments must be deterministic.

Never:
- edit files under `/data`
- modify generated artifacts
- silently change outputs
- introduce randomness without seeds

All research must be reproducible from `run_id`.

---

## 3. Minimal Patch Philosophy

Before editing:

1. Trace call graph.
2. Identify exact failure point.
3. Modify smallest possible surface.

Avoid:
- refactors
- renames
- architectural changes

unless explicitly requested.

---

## 4. Repository Awareness (Read First)

Primary execution entry:
- project/pipelines/run_all.py

Key systems:
- project/pipelines/stages/*
- project/events/registry.py
- project/pipelines/research/phase2_candidate_discovery.py
- project/engine/*
- project/features/*

Assume strong coupling between stages.

---

## 5. Trading Realism Over Backtest Performance

Prefer realistic results over higher metrics.

Assume strategies must survive:

- transaction costs
- funding payments
- regime shifts
- out-of-sample evaluation
- delayed signal execution

Reject optimizations that increase Sharpe but reduce deployability.

---

# Required Workflow For Any Change

Claude MUST:

1. Explain reasoning briefly.
2. List files to change.
3. Apply minimal patch.
4. Add/update tests when behavior changes.
5. Provide verification commands.

Verification template:

make check-hygiene
make test-fast
python project/pipelines/run_all.py \
  --run_id smoke \
  --symbols BTCUSDT \
  --start 2024-01-01 \
  --end 2024-01-10 \
  --run_phase2_conditional 0 \
  --run_backtest 1 \
  --strategies vol_compression_v1 \
  --cost_bps 6

---

# Forbidden Actions

Do NOT:

- edit `/data`
- fabricate datasets
- invent missing repo features
- bypass pipeline stages
- hardcode research outputs
- introduce forward-looking features

---

# Debugging Protocol

When errors occur:

1. Identify failing stage from run manifest.
2. Trace upstream artifact dependency.
3. Inspect schema + timestamps.
4. Propose smallest fix.
5. Add regression test.

Never guess root causes without code evidence.

---

# Research Mode Guidelines

Goal:
Produce statistically valid tradable strategies.

Claude should prioritize:

- fewer hypotheses
- stronger statistical validation
- stable regime performance
- cost robustness
- PIT safety

Avoid parameter proliferation.

---

# Success Definition

Success is NOT:
high backtest Sharpe.

Success IS:
strategy survives promotion, costs, and walk-forward evaluation
without structural bias.