#!/bin/bash
# sweep_all_families.sh
# Orchestrates separate discovery runs for each event family.

set -e

SYMBOLS="BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,BNBUSDT,ADAUSDT,DOGEUSDT,DOTUSDT,MATICUSDT,LINKUSDT"
START="2021-01-01"
END="2023-12-31"
DATA_ROOT="$(pwd)/data"
PYTHON="/home/tstuv/backtest/Backtest/.venv/bin/python"

FAMILIES=(
    "VOL_SHOCK"
    "LIQUIDITY_VACUUM"
    "FORCED_FLOW_EXHAUSTION"
    "CROSS_VENUE_DESYNC"
    "FUNDING_EXTREME_ONSET"
    "FUNDING_PERSISTENCE_TRIGGER"
    "FUNDING_NORMALIZATION_TRIGGER"
    "OI_SPIKE_POSITIVE"
    "OI_SPIKE_NEGATIVE"
    "OI_FLUSH"
)

mkdir -p "${DATA_ROOT}/runs/sequencer"

for FAMILY in "${FAMILIES[@]}"; do
    RUN_ID="sweep_${FAMILY,,}_2021_2023"
    LOG_FILE="${DATA_ROOT}/runs/sequencer/${RUN_ID}.log"
    
    echo "[$(date)] Starting sweep for family: ${FAMILY} (Run ID: ${RUN_ID})"
    echo "[$(date)] Log: ${LOG_FILE}"
    
    export BACKTEST_DATA_ROOT="${DATA_ROOT}"
    
    # Using array for arguments to avoid any parsing issues
    ARGS=(
        project/pipelines/run_all.py
        --run_id "${RUN_ID}"
        --symbols "${SYMBOLS}"
        --start "${START}"
        --end "${END}"
        --run_phase2_conditional 1
        --phase2_event_type "${FAMILY}"
        --run_hypothesis_generator 0
        --run_bridge_eval_phase2 1
        --run_discovery_quality_summary 1
        --run_naive_entry_eval 1
        --run_candidate_promotion 1
        --run_strategy_builder 0
        --run_edge_registry_update 0
        --force 0
    )
    
    ${PYTHON} -u "${ARGS[@]}" > "${LOG_FILE}" 2>&1
    
    EXIT_CODE=$?
    if [ $EXIT_CODE -eq 0 ]; then
        echo "[$(date)] Successfully finished sweep for ${FAMILY}"
    else
        echo "[$(date)] Sweep for ${FAMILY} failed with exit code ${EXIT_CODE}. Check log: ${LOG_FILE}"
    fi
    echo "--------------------------------------------------------"
done

echo "[$(date)] All scheduled sweeps completed."
