.PHONY: help run baseline discover-edges discover-hybrid clean-runtime clean-all-data test

help:
	$(MAKE) -f project/Makefile help

run baseline:
	$(MAKE) -f project/Makefile run

discover-edges:
	$(MAKE) -f project/Makefile discover-edges

discover-hybrid:
	$(MAKE) -f project/Makefile discover-hybrid

clean-runtime:
	$(MAKE) -f project/Makefile clean-runtime

clean-all-data:
	$(MAKE) -f project/Makefile clean-all-data

test:
	$(MAKE) -f project/Makefile test
