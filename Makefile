.PHONY: help demo demo-br prepare-br7 test logs ps down clean

BR ?= all
PRODUCER_INTERVAL ?= 0
PRODUCER_LIMIT ?=
PRODUCER_ARGS = --interval $(PRODUCER_INTERVAL)$(if $(PRODUCER_LIMIT), --events $(PRODUCER_LIMIT),)

help:
	@echo "Usage:"
	@echo "  make demo             Run full Spark cluster demo for every BR"
	@echo "  make demo-br BR=7     Run the demo for selected BRs only (e.g. BR=7 or BR=3,7)"
	@echo "  make prepare-br7      Download pinned NYC TLC files and prepare BR7 (stack must be up)"
	@echo "  make test             Run unit tests without Docker"
	@echo "  make logs             Tail demo logs; use SVC=grafana for container logs"
	@echo "  make ps               Show containers"
	@echo "  make down             Stop stack"

demo:
	python3 scripts/start.py $(PRODUCER_ARGS) all

demo-br:
	python3 scripts/start.py $(PRODUCER_ARGS) $(BR)

prepare-br7:
	python3 scripts/prepare_br7.py

test:
	python3 -m unittest discover -s tests -v


logs:
	mkdir -p logs/demo
	@if [ -n "$(SVC)" ]; then docker compose logs -f $(SVC); else tail -F logs/demo/*.log; fi

ps:
	docker compose ps

down:
	docker compose --profile full --profile monitoring down

clean:
	docker compose --profile full --profile monitoring down --remove-orphans
