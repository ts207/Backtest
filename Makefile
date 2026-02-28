ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
SHARED_VENV_PYTHON := $(abspath $(ROOT_DIR)/../..)/.venv/bin/python
PYTHON ?= $(if $(wildcard $(ROOT_DIR)/.venv/bin/python),$(ROOT_DIR)/.venv/bin/python,$(if $(wildcard $(SHARED_VENV_PYTHON)),$(SHARED_VENV_PYTHON),python3))
PYTHON_COMPILE ?= $(PYTHON)
RUN_ALL := $(ROOT_DIR)/project/pipelines/run_all.py
CLEAN_SCRIPT := $(ROOT_DIR)/project/scripts/clean_data.sh

RUN_ID ?= discovery_2020_2025
SYMBOLS ?= BTCUSDT,ETHUSDT
# Discovery defaults support multi-symbol idea generation under one RUN_ID.
START ?= 2020-06-01
END ?= 2025-07-10
STRATEGIES ?=
ENABLE_CROSS_VENUE_SPOT_PIPELINE ?= 0

.PHONY: help run baseline discover-edges discover-edges-from-raw discover-hybrid discover-hybrid-backtest test test-fast compile clean-runtime clean-all-data clean-repo debloat check-hygiene clean-hygiene

help:
	@echo "Targets:"
	@echo "  run               - ingest+clean+features+context"
	@echo "  baseline          - run + backtest + report"
	@echo "  discover-edges    - run multi-symbol discovery chain (phase1+phase2+export)"
	@echo "  discover-edges-from-raw - discovery chain using existing raw lake partitions (skips ingest)"
	@echo "  discover-hybrid   - discover-edges + expectancy checks (research-speed mode)"
	@echo "  discover-hybrid-backtest - discover-hybrid + backtest + report"
	@echo "                          requires STRATEGIES=... (comma-separated)"
	@echo "  test              - run test suite"
	@echo "  test-fast         - run fast test profile (exclude slow tests)"
	@echo "  compile           - byte-compile all Python modules under project/"
	@echo "  clean-runtime     - clean run/report artifacts"
	@echo "  clean-all-data    - clean all local data artifacts"
	@echo "  clean-repo        - clean local runtime/cache artifacts in repo tree"
	@echo "  debloat           - alias for clean-repo"
	@echo "  check-hygiene     - enforce repo hygiene constraints"
	@echo "  clean-hygiene     - remove local sidecar metadata files"

run:
	$(PYTHON) $(RUN_ALL) \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END)

baseline:
	@test -n "$(STRATEGIES)" || (echo "Set STRATEGIES=... for baseline" && exit 1)
	$(PYTHON) $(RUN_ALL) \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END) \
		--run_backtest 1 \
		--run_make_report 1 \
		--strategies "$(STRATEGIES)"

discover-edges:
	$(PYTHON) $(RUN_ALL) \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END) \
		--run_hypothesis_generator 0 \
		--run_phase2_conditional 1 \
		--phase2_event_type all \
		--run_edge_candidate_universe 1 \
		--run_strategy_builder 0 \
		--run_recommendations_checklist 0 \
		--strategy_blueprint_ignore_checklist 1 \
		--strategy_blueprint_allow_fallback 0 \
		--run_ingest_liquidation_snapshot 0 \
		--run_ingest_open_interest_hist 0

discover-edges-from-raw:
	$(PYTHON) $(RUN_ALL) \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END) \
		--skip_ingest_ohlcv 1 \
		--skip_ingest_funding 1 \
		--skip_ingest_spot_ohlcv 1 \
		--enable_cross_venue_spot_pipeline $(ENABLE_CROSS_VENUE_SPOT_PIPELINE) \
		--run_hypothesis_generator 1 \
		--run_phase2_conditional 1 \
		--phase2_event_type all \
		--run_edge_candidate_universe 1 \
		--run_strategy_builder 0 \
		--run_recommendations_checklist 0

discover-hybrid:
	$(PYTHON) $(RUN_ALL) \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END) \
		--run_hypothesis_generator 1 \
		--run_phase2_conditional 1 \
		--phase2_event_type all \
		--run_edge_candidate_universe 1 \
		--run_expectancy_analysis 1 \
		--run_expectancy_robustness 1

discover-hybrid-backtest:
	@test -n "$(STRATEGIES)" || (echo "Set STRATEGIES=... for discover-hybrid-backtest" && exit 1)
	$(PYTHON) $(RUN_ALL) \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END) \
		--run_hypothesis_generator 1 \
		--run_phase2_conditional 1 \
		--phase2_event_type all \
		--run_edge_candidate_universe 1 \
		--run_expectancy_analysis 1 \
		--run_expectancy_robustness 1 \
		--run_backtest 1 \
		--run_make_report 1 \
		--strategies "$(STRATEGIES)"

test:
	$(PYTHON) -m pytest -q

test-fast:
	$(PYTHON) -m pytest -q -m "not slow" --maxfail=1

monitor:
	$(PYTHON) project/scripts/monitor_data_freshness.py --symbols $(or $(SYMBOLS),BTCUSDT,ETHUSDT) --timeframe 5m --max_staleness_bars 3


compile:
	$(PYTHON_COMPILE) -m compileall $(ROOT_DIR)/project

clean-runtime:
	$(CLEAN_SCRIPT) runtime

clean-all-data:
	$(CLEAN_SCRIPT) all

clean-repo:
	$(CLEAN_SCRIPT) repo

debloat: clean-repo

check-hygiene:
	bash $(ROOT_DIR)/project/scripts/check_repo_hygiene.sh

clean-hygiene:
	find $(ROOT_DIR) -type f \
		\( -name '*:Zone.Identifier' -o -name '*#Uf03aZone.Identifier' -o -name '*#Uf03aZone.Identifier:Zone.Identifier' \) \
		-print -delete
