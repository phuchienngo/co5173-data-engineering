.PHONY: help demo demo-br br5-up br5-fresh br5-import br5-run br5-status br5-test br5-check-dashboards br5-bench br5-bench-screens br6-up br6-fresh br6-import br6-run br6-status br6-test br6-check-dashboards br6-bench br6-bench-screens prepare test logs ps down clean

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
CITIES ?=
BR6_IMPORT_ARGS = --multiplier $(MULTIPLIER)$(if $(CITIES), --cities $(CITIES),)$(if $(WRITER), --writer $(WRITER),)$(BR5_PRESET_ARGS)$(if $(YES), --yes,)

help:
	@echo "Usage:"
	@echo "  make demo             Run full Spark cluster demo for every BR"
	@echo "  make demo-br BR=3,4   Run full Spark cluster demo for selected BRs"
	@echo "  make prepare BR=7     Run scripts/prepare_br7.py (Spark container must be up)"
	@echo "  make test             Run unit tests without Docker"
	@echo "  make br5-up           BR5: start the stack and the app (stored data kept)"
	@echo "  make br5-fresh        BR5: drop BR5 data, import, then start the app"
	@echo "  make br5-import       BR5: import data (asks before replacing; YES=1 skips)"
	@echo "                        options: MULTIPLIER=10 WRITER=partitions|pandas PRESET=name"
	@echo "  make br5-run          BR5: run the promotion analysis with the stored settings"
	@echo "                        options: HOTEL=\"City Hotel\" COUNTRY=PRT ROOM=A PRESET=baseline"
	@echo "  make br5-status       BR5: show what data is stored"
	@echo "  make br5-test         BR5: unit tests (rules, settings, monitoring helpers)"
	@echo "  make br5-check-dashboards  BR5: run every Grafana panel query, report errors"
	@echo "  make br5-bench BLOCK=S2     BR5 benchmark: run a block (S1, S2, S3); options: CELLS=I3-L3 ARGS=--clickhouse"
	@echo "  make br5-bench-screens BLOCK=S2  BR5 benchmark: screenshots of a block (needs Playwright)"
	@echo "  make br6-up           BR6: start the stack and the shared app (stored data kept)"
	@echo "  make br6-fresh        BR6: drop BR6 data, import, then start the app"
	@echo "  make br6-import       BR6: import data (asks before replacing; YES=1 skips)"
	@echo "                        options: CITIES=jakarta,tokyo MULTIPLIER=2 WRITER=partitions|pandas PRESET=name"
	@echo "  make br6-run CITY=jakarta START=<venue_id> END=<venue_id>  BR6: one Route Finder run"
	@echo "                        options: STOPS=id,id TYPES='Food & drink,Shopping' VISITOR=<user_id> PRESET=name"
	@echo "  make br6-status       BR6: show what data is stored"
	@echo "  make br6-test         BR6: unit tests (rules, settings, reference files, small dimensions)"
	@echo "  make br6-check-dashboards  BR6: run every Grafana panel query, report errors"
	@echo "  make br6-bench BLOCK=H1     BR6 benchmark: run a block (S1, S2, S3, V, H1, H2, H3); option: CELLS=I1-HL1"
	@echo "  make br6-bench-screens BLOCK=H1  BR6 benchmark: screenshots of a block (needs Playwright)"
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

BR5_CELLS_ARGS = $(if $(CELLS), --cells $(CELLS),)

br5-bench:
	python3 tests/br5/benchmark.py block $(BLOCK)$(BR5_CELLS_ARGS) $(ARGS)

br5-bench-screens:
	python3 tests/br5/benchmark.py screens $(BLOCK)$(BR5_CELLS_ARGS)

br6-up:
	python3 scripts/br6.py up

br6-fresh:
	python3 scripts/br6.py fresh $(BR6_IMPORT_ARGS)

br6-import:
	python3 scripts/br6.py import $(BR6_IMPORT_ARGS)

BR6_RUN_ARGS = $(if $(STOPS), --stops $(STOPS),)$(if $(TYPES), --types "$(TYPES)",)$(if $(VISITOR), --visitor $(VISITOR),)$(if $(PRESET), --preset $(PRESET),)

br6-run:
	python3 scripts/br6.py run --city $(CITY) --start $(START) --end $(END)$(BR6_RUN_ARGS)

br6-status:
	python3 scripts/br6.py status

# pyspark is on the path only under spark-submit, so the tests add Spark's own zips.
br6-test:
	docker exec -i -e PYTHONPATH=/opt/jobs/common:/opt/bitnami/spark/python/lib/pyspark.zip:/opt/bitnami/spark/python/lib/py4j.zip br5-app python3 -m unittest discover -s /opt/tests/br6 -v

br6-check-dashboards:
	python3 tests/br5/check_dashboards.py --br 6

br6-bench:
	python3 tests/br6/benchmark.py block $(BLOCK)$(BR5_CELLS_ARGS) $(ARGS)

br6-bench-screens:
	python3 tests/br6/benchmark.py screens $(BLOCK)$(BR5_CELLS_ARGS)

prepare:
	python3 scripts/prepare_br$(BR).py

test:
	python3 -m unittest discover -s tests -p "test_br*.py" -v

logs:
	mkdir -p logs/demo
	@if [ -n "$(SVC)" ]; then docker compose logs -f $(SVC); else tail -F logs/demo/*.log; fi

ps:
	docker compose ps

down:
	docker compose --profile full --profile monitoring --profile br5 --profile br6 down

clean:
	docker compose --profile full --profile monitoring --profile br5 --profile br6 down --remove-orphans
