.PHONY: test audit run

test:
	pytest -q

audit:
	bash scripts/audit_repo.sh

run:
	python3 project/pipelines/run_all.py --symbols BTCUSDT,ETHUSDT --start 2020-06-01 --end 2025-07-10
