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

TRACK_NAME ?= research_general

.PHONY: help run baseline discover-edges discover-edges-from-raw discover-hybrid discover-hybrid-backtest test test-fast compile clean-runtime clean-all-data clean-repo debloat check-hygiene clean-hygiene governance update-conductor pre-commit

help:
	@echo "Targets:"
	@echo "  run               - ingest+clean+features+context"
	@echo "  baseline          - run + backtest + report"
	@echo "  governance        - audit specs and sync schemas"
	@echo "  update-conductor  - update conductor tracks after a run"
	@echo "  pre-commit        - run pre-commit quality checks"
	@echo "  test              - run test suite"
	@echo "  test-fast         - run fast test profile (exclude slow tests)"

run:
	$(PYTHON) $(RUN_ALL) \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END)
	$(MAKE) update-conductor TRACK_NAME="Execution: $(RUN_ID)"

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
	$(MAKE) update-conductor TRACK_NAME="Baseline: $(RUN_ID)"

governance:
	$(PYTHON) project/scripts/pipeline_governance.py --audit --sync

update-conductor:
	$(PYTHON) project/scripts/update_conductor.py --track "$(TRACK_NAME)"

pre-commit:
	bash project/scripts/pre_commit.sh

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
