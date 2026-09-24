.PHONY: help demo demo-br logs ps down clean

BR ?= all
PRODUCER_INTERVAL ?= 0
PRODUCER_LIMIT ?=
PRODUCER_ARGS = --interval $(PRODUCER_INTERVAL)$(if $(PRODUCER_LIMIT), --events $(PRODUCER_LIMIT),)

help:
	@echo "Usage:"
	@echo "  make demo             Run full Spark cluster demo for every BR"
	@echo "  make demo-br BR=3,4   Run full Spark cluster demo for selected BRs"
	@echo "  make logs             Tail demo logs; use SVC=grafana for container logs"
	@echo "  make ps               Show containers"
	@echo "  make down             Stop stack"

demo:
	python3 scripts/start.py $(PRODUCER_ARGS) all

demo-br:
	python3 scripts/start.py $(PRODUCER_ARGS) $(BR)


logs:
	mkdir -p logs/demo
	@if [ -n "$(SVC)" ]; then docker compose logs -f $(SVC); else tail -F logs/demo/*.log; fi

ps:
	docker compose ps

down:
	docker compose --profile full --profile monitoring down

clean:
	docker compose --profile full --profile monitoring down --remove-orphans
