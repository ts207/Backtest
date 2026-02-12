ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
PYTHON ?= $(ROOT_DIR)/.venv/bin/python
RUN_ALL := $(ROOT_DIR)/project/pipelines/run_all.py
CLEAN_SCRIPT := $(ROOT_DIR)/project/scripts/clean_data.sh

RUN_ID ?= discovery_2020_2025
SYMBOLS ?= BTCUSDT,ETHUSDT
START ?= 2020-06-01
END ?= 2025-07-10

.PHONY: help run discover-edges discover-hybrid test clean-runtime clean-all-data clean-repo

help:
	@echo "Targets:"
	@echo "  run               - ingest+clean+features+context"
	@echo "  discover-edges    - run discovery chain (phase1+phase2+export)"
	@echo "  discover-hybrid   - discover-edges + expectancy checks"
	@echo "  test              - run test suite"
	@echo "  clean-runtime     - clean run/report artifacts"
	@echo "  clean-all-data    - clean all local data artifacts"
	@echo "  clean-repo        - clean local runtime/cache artifacts in repo tree"

run:
	$(PYTHON) $(RUN_ALL) \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END)

discover-edges:
	$(PYTHON) $(RUN_ALL) \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END) \
		--run_hypothesis_generator 1 \
		--run_phase2_conditional 1 \
		--phase2_event_type all \
		--run_edge_candidate_universe 1

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

test:
	$(PYTHON) -m pytest -q

clean-runtime:
	$(CLEAN_SCRIPT) runtime

clean-all-data:
	$(CLEAN_SCRIPT) all

clean-repo:
	$(CLEAN_SCRIPT) repo
