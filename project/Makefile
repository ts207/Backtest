.PHONY: help test audit run

RUN_ID ?= baseline_2020_2025
SYMBOLS ?= BTCUSDT,ETHUSDT
START ?= 2020-01-01
END ?= 2025-12-31

help:
	@echo "Available targets:"
	@echo "  make test                # run full pytest suite"
	@echo "  make audit               # run standardized repo audit checks"
	@echo "  make run                 # run full pipeline with defaults"
	@echo "  make run RUN_ID=... SYMBOLS=... START=... END=..."

test:
	cd .. && pytest -q

audit:
	cd .. && bash scripts/audit_repo.sh

run:
	python pipelines/run_all.py \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END)
