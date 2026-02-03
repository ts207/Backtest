RUN_ID ?= 2024-01-01
SYMBOLS ?= BTCUSDT,ETHUSDT
START ?= 2023-01-01
END ?= 2023-12-31

run:
	python project/pipelines/run_all.py \
		--run_id $(RUN_ID) \
		--symbols $(SYMBOLS) \
		--start $(START) \
		--end $(END)
