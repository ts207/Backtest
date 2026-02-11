RUN_ID ?= discovery_2020_2025
SYMBOLS ?= BTCUSDT,ETHUSDT
START ?= 2020-06-01
END ?= 2025-07-10

help:
	@echo "Targets:"
	@echo "  run               - ingest+clean+features+context"
	@echo "  discover-edges    - run discovery chain (phase1+phase2+export)"
	@echo "  discover-hybrid   - discover-edges + expectancy checks"
	@echo "  test              - run test suite"
	@echo "  clean-runtime     - clean run/report artifacts"
	@echo "  clean-all-data    - clean all local data artifacts"

run:
	./.venv/bin/python pipelines/run_all.py \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END)

discover-edges:
	./.venv/bin/python pipelines/run_all.py \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END) \
		--run_hypothesis_generator 1 \
		--run_phase2_conditional 1 \
		--phase2_event_type all \
		--run_edge_candidate_universe 1

discover-hybrid:
	./.venv/bin/python pipelines/run_all.py \
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
	./.venv/bin/pytest -q

clean-runtime:
	./scripts/clean_data.sh runtime

clean-all-data:
	./scripts/clean_data.sh all
