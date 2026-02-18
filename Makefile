.PHONY: help run discover-edges discover-edges-from-raw discover-hybrid clean-runtime clean-all-data clean-repo debloat check-hygiene clean-hygiene test test-fast compile

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

debloat:
	$(MAKE) -f project/Makefile debloat

check-hygiene:
	$(MAKE) -f project/Makefile check-hygiene

clean-hygiene:
	$(MAKE) -f project/Makefile clean-hygiene

test:
	$(MAKE) -f project/Makefile test

test-fast:
	$(MAKE) -f project/Makefile test-fast

compile:
	$(MAKE) -f project/Makefile compile
