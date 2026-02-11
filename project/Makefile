RUN_ID ?= baseline_2020_2025
SYMBOLS ?= BTCUSDT,ETHUSDT
START ?= 2020-01-01
END ?= 2025-12-31

run:
	python pipelines/run_all.py \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END)
