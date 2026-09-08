# Teton streaming backend.
#
# Upstream challenge targets (example / smoke / baseline / burst / offline /
# adversarial) are preserved so the harness runs unchanged. Everything else is
# for this solution.
#
# Windows without make: use ./tasks.ps1 <target>, which mirrors these.

PYTHON      ?= python
VENV        ?= .venv
VENV_PYTHON := $(VENV)/bin/python
ifeq ($(OS),Windows_NT)
	VENV_PYTHON := $(VENV)/Scripts/python.exe
endif
PY          := $(if $(wildcard $(VENV_PYTHON)),$(VENV_PYTHON),$(PYTHON))

SERVICE_URL ?= http://localhost:8080
DEVICES     ?= 50
PORT        ?= 8080

.PHONY: help install venv migrate run test lint format check \
        load load-burst bench-engine restart-check verify \
        example smoke baseline burst offline adversarial clean

help:
	@echo "Setup"
	@echo "  install        create $(VENV) and install runtime + dev dependencies"
	@echo "  migrate        apply database migrations"
	@echo ""
	@echo "Run"
	@echo "  run            start the service on :$(PORT) (dual-stack)"
	@echo ""
	@echo "Verify"
	@echo "  test           unit tests (56 cases, no server needed)"
	@echo "  restart-check  hard-kill the service and verify recovery end to end"
	@echo "  verify         test + restart-check + smoke"
	@echo "  lint           ruff check"
	@echo ""
	@echo "Measure"
	@echo "  bench-engine   engine throughput without HTTP"
	@echo "  load           end-to-end load + alarm-feed SLA (batch 500)"
	@echo "  load-burst     single-event requests, as the graded generator sends"
	@echo ""
	@echo "Challenge harness (unchanged)"
	@echo "  example        run the reference stub on :8080"
	@echo "  smoke          30s baseline run against \$$SERVICE_URL + scorecard"
	@echo "  baseline       60s baseline"
	@echo "  burst          3min run with two 10x bursts"
	@echo "  offline        2min run with 20% of devices going offline + replaying"
	@echo "  adversarial    4min run combining burst + offline + clock skew"
	@echo ""
	@echo "Override SERVICE_URL=... DEVICES=... PORT=... as needed."

# ------------------------------------------------------------------- setup

install: venv
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -r requirements-dev.txt
	$(PY) manage.py migrate --no-input

venv:
	@test -d $(VENV) || $(PYTHON) -m venv $(VENV)

migrate:
	$(PY) manage.py migrate --no-input

# --------------------------------------------------------------------- run

run: migrate
	PORT=$(PORT) $(PY) run.py

# ------------------------------------------------------------------ verify

test:
	$(PY) -m unittest discover -s tests -t . -v

lint:
	$(PY) -m ruff check .

format:
	$(PY) -m ruff format .

check:
	$(PY) manage.py check

restart-check:
	$(PY) -m client.restart_check

verify: test restart-check smoke

# ----------------------------------------------------------------- measure

bench-engine:
	$(PY) -m client.engine_bench

load:
	$(PY) -m client.loadgen --rate 200000 --duration 20 --devices 5000 --batch 500 --concurrency 4

load-burst:
	$(PY) -m client.loadgen --rate 50000 --duration 20 --devices 5000 --batch 1 --concurrency 32

# -------------------------------------------------- challenge harness (as-is)

example:
	$(PY) example_solution/service.py

smoke:
	$(PY) eval/check.py smoke --target $(SERVICE_URL) --devices $(DEVICES)

baseline:
	$(PY) eval/check.py baseline --target $(SERVICE_URL) --devices $(DEVICES)

burst:
	$(PY) eval/check.py burst --target $(SERVICE_URL) --devices $(DEVICES)

offline:
	$(PY) eval/check.py offline --target $(SERVICE_URL) --devices $(DEVICES)

adversarial:
	$(PY) eval/check.py adversarial --target $(SERVICE_URL) --devices $(DEVICES)

# ------------------------------------------------------------------- clean

clean:
	rm -rf data data-restart-check .ruff_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
