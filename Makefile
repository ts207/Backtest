.PHONY: help test audit run-core run-research-vsr run-research-deff run-research-lrl run-full clean-reports clean-run

RUN_ID ?= $(shell date +%Y%m%d_%H%M%S)
SYMBOLS ?= BTCUSDT,ETHUSDT
START ?= 2020-06-01
END ?= 2025-07-10
VERIFY_CONTRACT ?= 1
RID ?=

help:
	@echo "Canonical workflow targets:"
	@echo "  make test"
	@echo "  make audit"
	@echo "  make run-core            # cleaned/features/context + backtest + report"
	@echo "  make run-research-vsr    # phase1+phase2 for vol_shock_relaxation"
	@echo "  make run-research-deff   # phase1+phase2 for directional_exhaustion_after_forced_flow"
	@echo "  make run-research-lrl    # phase1+phase2 for liquidity_refill_lag_window"
	@echo "  make run-full            # research + core in canonical order"
	@echo "  make clean-reports RID=<run_id>"
	@echo "  make clean-run RID=<run_id>"

test:
	pytest -q

audit:
	bash scripts/audit_repo.sh

run-core:
	python3 project/pipelines/run_all.py \
		--workflow core \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END) \
		--verify_contract $(VERIFY_CONTRACT)

run-research-vsr:
	python3 project/pipelines/run_all.py \
		--workflow research \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END) \
		--phase2_event_type vol_shock_relaxation \
		--verify_contract $(VERIFY_CONTRACT)

run-research-deff:
	python3 project/pipelines/run_all.py \
		--workflow research \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END) \
		--phase2_event_type directional_exhaustion_after_forced_flow \
		--verify_contract $(VERIFY_CONTRACT)

run-research-lrl:
	python3 project/pipelines/run_all.py \
		--workflow research \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END) \
		--phase2_event_type liquidity_refill_lag_window \
		--verify_contract $(VERIFY_CONTRACT)

run-full:
	python3 project/pipelines/run_all.py \
		--workflow full \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END) \
		--verify_contract $(VERIFY_CONTRACT)

clean-reports:
	@if [ -z "$(RID)" ]; then echo "RID is required (make clean-reports RID=<run_id>)"; exit 2; fi
	rm -rf data/reports/phase2/$(RID)
	rm -rf data/reports/promotion_audits/$(RID)
	rm -rf data/reports/vol_shock_relaxation/$(RID)
	rm -rf data/reports/directional_exhaustion_after_forced_flow/$(RID)
	rm -rf data/reports/liquidity_refill_lag_window/$(RID)
	rm -rf data/reports/vol_compression_expansion_v1/$(RID)

clean-run:
	@if [ -z "$(RID)" ]; then echo "RID is required (make clean-run RID=<run_id>)"; exit 2; fi
	rm -rf data/lake/runs/$(RID)
	rm -rf data/runs/$(RID)
	rm -rf data/reports/by_run/$(RID)
