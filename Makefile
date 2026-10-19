.PHONY: help demo demo-br br5-up br5-fresh br5-import br5-run br5-status br5-test br5-check-dashboards logs ps down clean

BR ?= all
PRODUCER_INTERVAL ?= 0
PRODUCER_LIMIT ?=
PRODUCER_ARGS = --interval $(PRODUCER_INTERVAL)$(if $(PRODUCER_LIMIT), --events $(PRODUCER_LIMIT),)
MULTIPLIER ?= 1
WRITER ?=
PRESET ?=
YES ?=
HOTEL ?= All
COUNTRY ?= All
ROOM ?= All
BR5_PRESET_ARGS = $(if $(PRESET), --preset $(PRESET),)
BR5_IMPORT_ARGS = --multiplier $(MULTIPLIER)$(if $(WRITER), --writer $(WRITER),)$(BR5_PRESET_ARGS)$(if $(YES), --yes,)

help:
	@echo "Usage:"
	@echo "  make demo             Run full Spark cluster demo for every BR"
	@echo "  make demo-br BR=3,4   Run full Spark cluster demo for selected BRs"
	@echo "  make br5-up           BR5: start the stack and the app (stored data kept)"
	@echo "  make br5-fresh        BR5: drop BR5 data, import, then start the app"
	@echo "  make br5-import       BR5: import data (asks before replacing; YES=1 skips)"
	@echo "                        options: MULTIPLIER=10 WRITER=partitions|pandas PRESET=name"
	@echo "  make br5-run          BR5: run the promotion analysis with the stored settings"
	@echo "                        options: HOTEL=\"City Hotel\" COUNTRY=PRT ROOM=A PRESET=baseline"
	@echo "  make br5-status       BR5: show what data is stored"
	@echo "  make br5-test         BR5: unit tests (rules, settings, monitoring helpers)"
	@echo "  make br5-check-dashboards  BR5: run every Grafana panel query, report errors"
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

br5-run:
	python3 scripts/br5.py run --hotel "$(HOTEL)" --country $(COUNTRY) --room-type $(ROOM)$(BR5_PRESET_ARGS)

br5-status:
	python3 scripts/br5.py status

br5-test:
	docker exec -i br5-app python3 -m unittest discover -s /opt/tests/br5 -v

br5-check-dashboards:
	python3 tests/br5/check_dashboards.py

logs:
	mkdir -p logs/demo
	@if [ -n "$(SVC)" ]; then docker compose logs -f $(SVC); else tail -F logs/demo/*.log; fi

ps:
	docker compose ps

down:
	docker compose --profile full --profile monitoring --profile br5 down

clean:
	docker compose --profile full --profile monitoring --profile br5 down --remove-orphans
