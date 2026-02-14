.PHONY: help run discover-edges discover-edges-from-raw discover-hybrid clean-runtime clean-all-data clean-repo test compile

help:
	$(MAKE) -f project/Makefile help

run:
	$(MAKE) -f project/Makefile run

discover-edges:
	$(MAKE) -f project/Makefile discover-edges

discover-edges-from-raw:
	$(MAKE) -f project/Makefile discover-edges-from-raw

discover-hybrid:
	$(MAKE) -f project/Makefile discover-hybrid

clean-runtime:
	$(MAKE) -f project/Makefile clean-runtime

clean-all-data:
	$(MAKE) -f project/Makefile clean-all-data

clean-repo:
	$(MAKE) -f project/Makefile clean-repo

test:
	$(MAKE) -f project/Makefile test

compile:
	$(MAKE) -f project/Makefile compile
