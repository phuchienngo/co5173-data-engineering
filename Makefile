.PHONY: help demo demo-br prepare-br8 test logs ps down clean

BR ?= all
PRODUCER_INTERVAL ?= 0
PRODUCER_LIMIT ?=
PRODUCER_ARGS = --interval $(PRODUCER_INTERVAL)$(if $(PRODUCER_LIMIT), --events $(PRODUCER_LIMIT),)

help:
	@echo "Usage:"
	@echo "  make demo             Run full Spark cluster demo for every BR"
	@echo "  make demo-br BR=8     Run BR8 TripWorld retention demo"
	@echo "  make prepare-br8      Download and prepare TripWorld (Spark container must be up)"
	@echo "  make test             Run BR8 contract tests without Docker"
	@echo "  make logs             Tail demo logs; use SVC=grafana for container logs"
	@echo "  make ps               Show containers"
	@echo "  make down             Stop stack"

demo:
	python3 scripts/start.py $(PRODUCER_ARGS) all

demo-br:
	python3 scripts/start.py $(PRODUCER_ARGS) $(BR)

prepare-br8:
	python3 scripts/prepare_br8.py

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
