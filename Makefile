.PHONY: help demo demo-br br5-up br5-fresh br5-import br5-run br5-status br5-test br5-check-dashboards br3-bench br5-bench br5-bench-screens br6-up br6-fresh br6-import br6-run br6-status br6-test br6-check-dashboards br6-bench br6-bench-screens br8-up br8-import br8-run br8-status br8-settings br8-test br8-bench-presets br8-bench br8-bench-check br8-bench-screens br8-bench-restore br7-up br7-import br7-run br7-status br7-settings br7-test br7-bench-presets br7-bench br7-bench-check br7-bench-screens br7-bench-restore prepare test logs ps down clean


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
	@echo "  make br3-bench REPS=3       BR3 and BR4 benchmark: Spark vs ClickHouse MV (about 40 min)"
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
	@echo "  make br7-up           BR7 benchmark: start the stack (data kept)"
	@echo "  make br7-import       BR7 benchmark: download the TLC months if needed and import them (REPLACE=1 re-imports)"
	@echo "  make br7-run INPUT=I1 BR7 benchmark: one run (I1 12+1 months, I2 6+1, I3 2+1); PRESET=bench-default"
	@echo "  make br7-status       BR7 benchmark: stored data and latest runs"
	@echo "  make br7-test         BR7 benchmark: unit tests (no Docker)"
	@echo "  make br7-bench-presets       BR7 benchmark: save the bench-* presets (once)"
	@echo "  make br7-bench BLOCK=S2      BR7 benchmark: run a block (S1, S2, S3, V, H1, H2, H3); CELLS=I1-L2"
	@echo "  make br7-bench-check BLOCK=S2    BR7 benchmark: check a block's runs (settings, size, same result)"
	@echo "  make br7-bench-screens BLOCK=S2  BR7 benchmark: Grafana and Spark history screenshots (needs Playwright)"
	@echo "  make br7-bench-restore       BR7 benchmark: worker back to 2 cores, ClickHouse to 1 CPU"
	@echo "  make br8-up           BR8 benchmark: start the stack (data kept)"
	@echo "  make br8-import       BR8 benchmark: download TripWorld if needed and import it (REPLACE=1 re-imports)"
	@echo "  make br8-run INPUT=I1 BR8 benchmark: one run (I1 all, I2 Tokyo, I3 Kuala Lumpur); PRESET=bench-default"
	@echo "  make br8-status       BR8 benchmark: stored data and latest runs"
	@echo "  make br8-test         BR8 benchmark: unit tests (no Docker)"
	@echo "  make br8-bench-presets       BR8 benchmark: save the bench-* presets (once)"
	@echo "  make br8-bench BLOCK=S2      BR8 benchmark: run a block (S1, S2, S3, V, H1, H2, H3); CELLS=I1-L2"
	@echo "  make br8-bench-check BLOCK=S2    BR8 benchmark: check a block's runs (settings, size, same result)"
	@echo "  make br8-bench-screens BLOCK=S2  BR8 benchmark: Grafana and Spark history screenshots (needs Playwright)"
	@echo "  make br8-bench-restore       BR8 benchmark: worker back to 2 cores, ClickHouse to 1 CPU"
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

br3-bench:
	python3 tests/br3/benchmark.py $(REPS)

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

INPUT ?= I1
BR8_RUN_ARGS = --input $(INPUT)$(if $(PRESET), --preset $(PRESET),)

br8-up:
	python3 scripts/br8.py up

br8-import:
	python3 scripts/br8.py import$(if $(REPLACE), --replace,)$(if $(WRITER), --writer $(WRITER),)$(BR5_PRESET_ARGS)

br8-run:
	python3 scripts/br8.py run $(BR8_RUN_ARGS)

br8-status:
	python3 scripts/br8.py status

br8-settings:
	python3 scripts/br8.py settings

br8-test:
	python3 -m unittest discover -s tests/br8 -v

br8-bench-presets:
	python3 tests/br8/benchmark.py presets

br8-bench:
	python3 tests/br8/benchmark.py block $(BLOCK)$(BR5_CELLS_ARGS) $(ARGS)

br8-bench-check:
	python3 tests/br8/benchmark.py check $(BLOCK)$(BR5_CELLS_ARGS)

br8-bench-screens:
	python3 tests/br8/benchmark.py screens $(BLOCK)$(BR5_CELLS_ARGS)

br8-bench-restore:
	python3 tests/br8/benchmark.py restore

BR7_RUN_ARGS = --input $(INPUT)$(if $(PRESET), --preset $(PRESET),)

br7-up:
	python3 scripts/br7.py up

br7-import:
	python3 scripts/br7.py import$(if $(REPLACE), --replace,)$(if $(WRITER), --writer $(WRITER),)$(BR5_PRESET_ARGS)

br7-run:
	python3 scripts/br7.py run $(BR7_RUN_ARGS)

br7-status:
	python3 scripts/br7.py status

br7-settings:
	python3 scripts/br7.py settings

br7-test:
	python3 -m unittest discover -s tests/br7 -v

br7-bench-presets:
	python3 tests/br7/benchmark.py presets

br7-bench:
	python3 tests/br7/benchmark.py block $(BLOCK)$(BR5_CELLS_ARGS) $(ARGS)

br7-bench-check:
	python3 tests/br7/benchmark.py check $(BLOCK)$(BR5_CELLS_ARGS)

br7-bench-screens:
	python3 tests/br7/benchmark.py screens $(BLOCK)$(BR5_CELLS_ARGS)

br7-bench-restore:
	python3 tests/br7/benchmark.py restore

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
