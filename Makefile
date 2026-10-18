.PHONY: help demo demo-br br5-up br5-fresh br5-import br5-status logs ps down clean

BR ?= all
PRODUCER_INTERVAL ?= 0
PRODUCER_LIMIT ?=
PRODUCER_ARGS = --interval $(PRODUCER_INTERVAL)$(if $(PRODUCER_LIMIT), --events $(PRODUCER_LIMIT),)
MULTIPLIER ?= 1
WRITER ?= partitions
YES ?=
BR5_IMPORT_ARGS = --multiplier $(MULTIPLIER) --writer $(WRITER)$(if $(YES), --yes,)

help:
	@echo "Usage:"
	@echo "  make demo             Run full Spark cluster demo for every BR"
	@echo "  make demo-br BR=3,4   Run full Spark cluster demo for selected BRs"
	@echo "  make br5-up           BR5: start the stack and the app (stored data kept)"
	@echo "  make br5-fresh        BR5: drop BR5 data, import, then start the app"
	@echo "  make br5-import       BR5: import data (asks before replacing; YES=1 skips)"
	@echo "                        options: MULTIPLIER=10 WRITER=partitions|pandas"
	@echo "  make br5-status       BR5: show what data is stored"
	@echo "  make logs             Tail demo logs; use SVC=grafana for container logs"
	@echo "  make ps               Show containers"
	@echo "  make down             Stop stack"

demo:
	python3 scripts/start.py $(PRODUCER_ARGS) all

demo-br:
	python3 scripts/start.py $(PRODUCER_ARGS) $(BR)

br5-up:
	python3 scripts/br5.py up

br5-fresh:
	python3 scripts/br5.py fresh $(BR5_IMPORT_ARGS)

br5-import:
	python3 scripts/br5.py import $(BR5_IMPORT_ARGS)

br5-status:
	python3 scripts/br5.py status

logs:
	mkdir -p logs/demo
	@if [ -n "$(SVC)" ]; then docker compose logs -f $(SVC); else tail -F logs/demo/*.log; fi

ps:
	docker compose ps

down:
	docker compose --profile full --profile monitoring --profile br5 down

clean:
	docker compose --profile full --profile monitoring --profile br5 down --remove-orphans
