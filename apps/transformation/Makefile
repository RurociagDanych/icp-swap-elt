PYTHON ?= python3
PYTHON_TESTS := $(sort $(wildcard test/test_*.py))

.PHONY: help venv test-python docker-build

help:
	@echo "Targets:"
	@echo "  make venv         # create .venv and install test/requirements.txt"
	@echo "  make test-python  # run every test/test_*.py as a standalone script"
	@echo "  make docker-build # build the transformation + quality Lambda container image"

venv:
	$(PYTHON) -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r test/requirements.txt

test-python:
	@set -e; for t in $(PYTHON_TESTS); do \
		echo "==> $$t"; \
		.venv/bin/python -I -B "$$t"; \
	done

docker-build:
	docker build -f Dockerfile -t icp-swap-elt-transformation:latest .
