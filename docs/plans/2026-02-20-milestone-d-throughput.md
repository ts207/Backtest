# Milestone D: Backlog Throughput & Ablation Report Implementation Plan

> **For Gemini:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Execute a large-scale backlog burn-down (500+ candidates) on BTC/ETH/SOL to validate the "Indicator State" hypothesis, producing a data-driven Lift Report.

**Architecture:** Utilize the newly integrated `vol_regime` and `carry_state` indicators to condition `LIQUIDATION_CASCADE` and `liquidity_vacuum` events. Run the full discovery pipeline (Phase 1 → Phase 2 → Ablation) on a 60-day window.

**Tech Stack:** Python, Pandas, Parquet, Make/Bash.

---

### Task 1: Prepare Execution Configuration (`run_milestone_d.py`)

**Files:**
- Create: `project/scripts/run_milestone_d.py`

**Step 1: Implement the orchestrator script**
- **Inputs**: Run ID (`milestone_d_v1`), Symbols (`BTC,ETH,SOL`), Window (`2025-01-01` to `2025-03-01` or similar 60-day recent window).
- **Workflow**:
    1.  `run_slice1_data_layer` (Ingest/Clean) - *skip if data exists*.
    2.  `build_features_v1` (Base features).
    3.  `build_market_context` (Indicators: vol_regime, carry_state).
    4.  `build_event_registry` (Detect LIQUIDATION_CASCADE, liquidity_vacuum).
    5.  `generate_candidate_plan` (Generate conditioned hypotheses).
    6.  `phase2_candidate_discovery` (Execute backtests).
    7.  `ablation.py` (Generate Lift Report).

**Step 2: Commit**
```bash
git add project/scripts/run_milestone_d.py
git commit -m "feat: add milestone d execution script"
```

---

### Task 2: Verify Data Availability (Pre-Flight)

**Files:**
- Modify: `project/scripts/run_milestone_d.py` (add checks)

**Step 1: Add pre-flight checks**
- Verify 1m bars exist for BTC/ETH/SOL for the target window.
- If missing, warn user or trigger ingestion (if API keys available).
- For this plan, assume data exists or fail fast.

**Step 2: Commit**
```bash
git commit -m "feat: add data availability pre-flight checks"
```

---

### Task 3: Execute the Run (Human Action)

**Step 1: Run the script**
```bash
# Example command (adjust dates as needed)
python3 project/scripts/run_milestone_d.py --run_id milestone_d_v1 --start_date 2025-01-01 --end_date 2025-03-01
```

**Step 2: Monitor progress**
- Check logs for "Phase 2 complete".
- Check logs for "Ablation report written".

---

### Task 4: Analyze & Commit Results

**Files:**
- Create: `reports/milestone_d/executive_summary.md`

**Step 1: Synthesize findings**
- Read `data/reports/ablation/milestone_d_v1/lift_summary.csv`.
- Answer:
    - Does `carry_pos` improve `LIQUIDATION_CASCADE`? (Expect: Yes)
    - Does `vol_high` improve `liquidity_vacuum`? (Expect: Yes)
    - What is the overall "Fallback" promotion rate?

**Step 2: Commit results**
```bash
git add reports/milestone_d/
git commit -m "results: milestone d ablation report"
```
