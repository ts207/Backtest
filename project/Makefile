ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
PYTHON ?= $(ROOT_DIR)/.venv/bin/python
PYTHON_COMPILE ?= $(if $(wildcard $(ROOT_DIR)/.venv/bin/python),$(ROOT_DIR)/.venv/bin/python,python3)
RUN_ALL := $(ROOT_DIR)/project/pipelines/run_all.py
CLEAN_SCRIPT := $(ROOT_DIR)/project/scripts/clean_data.sh

RUN_ID ?= discovery_2020_2025
SYMBOLS ?= BTCUSDT,ETHUSDT
# Discovery defaults support multi-symbol idea generation under one RUN_ID.
START ?= 2020-06-01
END ?= 2025-07-10
STRATEGIES ?=
ENABLE_CROSS_VENUE_SPOT_PIPELINE ?= 0

.PHONY: help run baseline discover-edges discover-edges-from-raw discover-hybrid discover-hybrid-backtest test compile clean-runtime clean-all-data clean-repo debloat check-hygiene clean-hygiene

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
		--run_hypothesis_generator 1 \
		--run_phase2_conditional 1 \
		--phase2_event_type all \
		--run_edge_candidate_universe 1 \
		--run_strategy_builder 0 \
		--run_recommendations_checklist 0

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
	$(ROOT_DIR)/project/scripts/check_repo_hygiene.sh

clean-hygiene:
	find $(ROOT_DIR) -type f \
		\( -name '*:Zone.Identifier' -o -name '*#Uf03aZone.Identifier' -o -name '*#Uf03aZone.Identifier:Zone.Identifier' \) \
		-print -delete
