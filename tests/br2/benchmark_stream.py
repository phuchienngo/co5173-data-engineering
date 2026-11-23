"""BR2 stream benchmark: Spark Structured Streaming (ETL, jobs/br2_dynamic_pricing/job.py run_stream)
vs ClickHouse Kafka engine + materialized view (ELT, tests/br2/clickhouse_br2.sql), same two
experiments per engine as tests/br3/benchmark.py:
  backlog - N trips already in Kafka, time until every landmark trip is counted in ClickHouse
  live    - pipeline running, RATE trips/s for DURATION s, latency per hourly window
            = ingested_at (Spark: updated_at) - newest Kafka message timestamp in the window

Usage: python3 tests/br2/benchmark_stream.py [REPS]      (REPS 3)
Knobs (env): BENCH_TRIPS 200000 (first trips of the month, sorted by pickup), BENCH_MONTH 2026-07,
  BENCH_RATE 1000 trips/s, BENCH_DURATION 60 s, BENCH_MAX_OFFSETS 20000 (Spark maxOffsetsPerTrigger).
Needs the lake (data/br2/lake/fact_trips and dim_capacity, built by `make demo-br BR=2` once), the
BR2 demo stream stopped, and the stack up (no Grafana needed):
  docker compose --profile full up -d clickhouse kafka spark-master spark-worker spark-local
Host deps are the pinned ones in requirements.txt: clickhouse-connect, kafka-python, pyarrow.
Writes docs/result/br2/benchmark-stream/{benchmark_runs.csv,summary.md,logs/}.
"""
import ast
import csv
import importlib.util
import json
import statistics
import subprocess
import sys
import os
import threading
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import clickhouse_connect
import pyarrow.dataset as ds
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

ROOT = Path(__file__).resolve().parents[2]
REPS = int(sys.argv[1]) if len(sys.argv) > 1 else 3
N_TRIPS = int(os.environ.get("BENCH_TRIPS", 200_000))
MONTH = os.environ.get("BENCH_MONTH", "2026-07")
RATE = int(os.environ.get("BENCH_RATE", 1000))
DURATION = int(os.environ.get("BENCH_DURATION", 60))
MAX_OFFSETS = os.environ.get("BENCH_MAX_OFFSETS", "20000")
TIMEOUT = 1200
OUT = ROOT / "docs" / "result" / "br2" / "benchmark-stream"
FACT = ROOT / "data" / "br2" / "lake" / "fact_trips"
CAPACITY = ROOT / "data" / "br2" / "lake" / "dim_capacity"
SPARK_TABLE = "pricing.demand_ratio_hourly"
CH_TABLE = "pricing.demand_ratio_hourly_ch"
CH_OBJECTS = ["pricing.mv_trips_ch", "pricing.kafka_trips_ch"]

spec = importlib.util.spec_from_file_location("start", ROOT / "scripts" / "start.py")
start_script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(start_script)

CH = clickhouse_connect.get_client(host="localhost", port=8123, username="default", password="clickhouse")
SPARK_MASTER = "spark://spark-master:7077"
CONTAINERS = {"spark": ["spark-local", "spark-worker", "spark-master", "clickhouse"], "clickhouse": ["clickhouse"]}


def landmarks():
    # pricing.py imports pyspark, so read the LANDMARKS literal without importing it
    tree = ast.parse((ROOT / "jobs" / "br2_dynamic_pricing" / "pricing.py").read_text())
    node = next(n for n in tree.body if isinstance(n, ast.Assign) and n.targets[0].id == "LANDMARKS")
    return ast.literal_eval(node.value)


LANDMARKS = landmarks()


def trip_rows():
    table = ds.dataset(FACT, format="parquet", partitioning="hive").to_table(
        columns=["pickup_datetime", "pu_location_id", "do_location_id", "fare_amount"],
        filter=(ds.field("service") == "yellow") & (ds.field("pickup_month") == MONTH))
    rows = []
    for row in table.sort_by("pickup_datetime").slice(0, N_TRIPS).to_pylist():
        pickup = row["pickup_datetime"]
        if pickup.tzinfo is None:
            pickup = pickup.replace(tzinfo=timezone.utc)
        rows.append({"service": "yellow",
                     "pickup_datetime": pickup.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                     "pu_location_id": row["pu_location_id"], "do_location_id": row["do_location_id"],
                     "fare_amount": row["fare_amount"]})
    return rows


def pickup_of(row):
    return datetime.fromisoformat(row["pickup_datetime"])


def producer():
    return KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda v: json.dumps(v).encode())


def topic_size(topic):
    consumer = KafkaConsumer(bootstrap_servers="localhost:9092")
    tps = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic) or set()]
    size = sum(consumer.end_offsets(tps).values()) if tps else 0
    consumer.close()
    return size


def load_topic(topic, rows):
    size = topic_size(topic)
    if 0 < size < len(rows):
        # partial load from an aborted run: a fresh topic avoids duplicated messages
        topic = f"{topic}_{int(time.time())}"
        size = 0
    if size == 0:
        p = producer()
        for row in rows:
            p.send(topic, row)
        p.flush()
    if topic_size(topic) != len(rows):
        sys.exit(f"topic {topic} has {topic_size(topic)} messages, expected {len(rows)}: delete the topic and rerun")
    return topic


class Sampler(threading.Thread):
    def __init__(self, names):
        super().__init__(daemon=True)
        self.names, self.stop, self.mem, self.cpu = names, False, {}, []

    def run(self):
        while not self.stop:
            out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.Name}};{{.CPUPerc}};{{.MemUsage}}"],
                                 capture_output=True, text=True).stdout
            cpu = 0.0
            for line in out.splitlines():
                name, c, m = line.split(";")
                if name in self.names:
                    cpu += float(c.rstrip("%"))
                    used = m.split("/")[0].strip()
                    mib = float(used[:-3]) * (1024 if used.endswith("GiB") else 1 if used.endswith("MiB") else 1 / 1024)
                    self.mem[name] = max(self.mem.get(name, 0), mib)
            self.cpu.append(cpu)

    def result(self, prefix=""):
        self.stop = True
        return {f"{prefix}peak_mem_mib": round(sum(self.mem.values())),
                f"{prefix}mean_cpu_pct": round(statistics.mean(self.cpu), 1) if self.cpu else None}


def table_of(engine):
    return SPARK_TABLE if engine == "spark" else CH_TABLE


def source_filter(engine):
    return "source = 'stream'" if engine == "spark" else "1"


def booked(engine, since_ts=0):
    """Trips counted so far (windows starting at or after since_ts, unix seconds)."""
    return CH.query(f"""SELECT sum(active_bookings) FROM {table_of(engine)} FINAL
                        WHERE {source_filter(engine)} AND toUnixTimestamp(window_start) >= {since_ts}""").result_rows[0][0] or 0


def sql_parts(topic, group):
    sql = ((ROOT / "tests" / "br2" / "clickhouse_br2.sql").read_text().replace("{topic}", topic)
           .replace("{group}", group).replace("{landmark_ids}", ", ".join(map(str, LANDMARKS))))
    for part in sql.split(";"):
        body = "\n".join(l for l in part.splitlines() if not l.strip().startswith("--")).strip()
        if body:
            yield body


def load_dims():
    for body in sql_parts("", ""):
        if "IF NOT EXISTS" in body:
            CH.command(body)
    caps = ds.dataset(CAPACITY, format="parquet").to_table().to_pylist()
    CH.command("TRUNCATE TABLE pricing.dim_capacity_ch")
    CH.insert("pricing.dim_capacity_ch", [[r["location_id"], r["day_type"], r["hour_of_day"], r["capacity"]] for r in caps],
              column_names=["location_id", "day_type", "hour_of_day", "capacity"])
    CH.command("TRUNCATE TABLE pricing.dim_landmark_ch")
    CH.insert("pricing.dim_landmark_ch", [[i, z, l] for i, (z, l) in LANDMARKS.items()],
              column_names=["location_id", "zone", "landmark"])
    return len(caps)


def wait_cluster_idle(timeout=120):
    # a killed driver can keep its cores on the standalone master for a while,
    # which leaves the next app without executors
    deadline = time.time() + timeout
    while time.time() < deadline:
        with urllib.request.urlopen("http://localhost:8082/json/") as r:
            if not json.load(r).get("activeapps"):
                return
        time.sleep(2)
    raise RuntimeError("Spark master still has active apps")


def start_engine(engine, topic, group):
    if engine == "spark":
        # not TRUNCATE: the batch rows feed the dashboard and are only rebuilt with dim_capacity
        CH.command(f"ALTER TABLE {SPARK_TABLE} DELETE WHERE source = 'stream' SETTINGS mutations_sync = 2")
        cmd = start_script.spark_submit_cmd(2, SPARK_MASTER)
        env = ["-e", f"BR2_TOPIC={topic}", "-e", f"MAX_OFFSETS={MAX_OFFSETS}"]
        cmd = cmd[:3] + env + cmd[3:]
        wait_cluster_idle()
        log = open(OUT / "logs" / f"spark-br2-{topic}-{int(time.time())}.log", "w")
        return subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    for obj in CH_OBJECTS + [CH_TABLE]:
        CH.command(f"DROP TABLE IF EXISTS {obj}")
    for body in sql_parts(topic, group):
        if "IF NOT EXISTS" not in body:
            CH.command(body)
    return None


def stop_engine(engine, proc):
    if engine == "spark":
        subprocess.run(["docker", "exec", "spark-local", "pkill", "-f", "jobs/br2_"])
        proc.wait()
    else:
        for obj in CH_OBJECTS:
            CH.command(f"DROP TABLE IF EXISTS {obj}")


def run_backlog(engine, topic, expected, topic_rows, tag):
    sampler = Sampler(CONTAINERS[engine])
    sampler.start()
    start = time.time()
    proc = start_engine(engine, topic, f"backlog_br2_{tag}_{int(start)}")
    first = None
    try:
        while time.time() - start < TIMEOUT:
            c = booked(engine)
            if c and first is None:
                first = time.time()
            if c >= expected:
                break
            time.sleep(0.5)
        else:
            raise TimeoutError(f"{table_of(engine)}: {booked(engine)}/{expected}")
        done = time.time()
    finally:
        stop_engine(engine, proc)
    return {"rows": expected, "topic_rows": topic_rows, "first_row_s": round(first - start, 2),
            "end_to_end_s": round(done - start, 2), "rows_per_s": round(topic_rows / (done - start)), **sampler.result()}


def run_live(engine, rows, tag):
    topic = f"live_br2_{engine}_{tag}_{int(time.time())}"
    # warm-up windows must not share a window with live trips: the warm-up trips are re-sent
    # until the engine writes, and the Spark watermark (2 h) would double-count a shared window
    warm = [r for r in rows if r["do_location_id"] in LANDMARKS][:100]
    warm_end = pickup_of(warm[-1]) + timedelta(hours=3)
    live_start = next((i for i, r in enumerate(rows) if pickup_of(r) >= warm_end), len(rows))
    live = rows[live_start:live_start + RATE * DURATION]
    if len(live) < RATE * DURATION:
        sys.exit(f"only {len(live)} trips after the warm-up gap, need {RATE * DURATION}: raise BENCH_TRIPS or lower BENCH_RATE / BENCH_DURATION")
    first_window = int(pickup_of(live[0]).replace(minute=0, second=0, microsecond=0).timestamp())
    expected = sum(r["do_location_id"] in LANDMARKS for r in live)
    proc = start_engine(engine, topic, topic)
    p = producer()
    try:
        start = time.time()
        while booked(engine) == 0 and time.time() - start < TIMEOUT:
            for row in warm:
                p.send(topic, row)
            p.flush()
            time.sleep(2)
        sampler = Sampler(CONTAINERS[engine])
        sampler.start()
        step = max(1, RATE // 10)
        for i in range(0, len(live), step):
            tick = time.time()
            for row in live[i:i + step]:
                p.send(topic, row)
            time.sleep(max(0.0, 0.1 - (time.time() - tick)))
        p.flush()
        deadline = time.time() + TIMEOUT
        while booked(engine, first_window) < expected and time.time() < deadline:
            time.sleep(0.5)
        res = sampler.result("live_")
        ingested = "updated_at" if engine == "spark" else "ingested_at"
        q = CH.query(f"""SELECT count(),
                quantiles(0.5, 0.95, 0.99)(toUnixTimestamp64Milli({ingested}) - toUnixTimestamp64Milli(event_time))
            FROM {table_of(engine)} FINAL
            WHERE {source_filter(engine)} AND toUnixTimestamp(window_start) >= {first_window}""").result_rows[0]
    finally:
        stop_engine(engine, proc)
    p50, p95, p99 = (round(v) for v in q[1]) if q[0] else (None, None, None)
    return {"live_rows": expected, "live_windows": q[0], "p50_ms": p50, "p95_ms": p95, "p99_ms": p99, **res}


def correctness():
    # Spark's window_start is DateTime in the server timezone, the ClickHouse side is UTC: join on the instant
    return CH.query("""
        SELECT count(),
            countIf(s.active_bookings = c.active_bookings),
            countIf(abs(s.avg_fare - c.avg_fare) < 1e-6),
            countIf((s.capacity IS NULL AND c.capacity IS NULL) OR s.capacity = c.capacity),
            countIf(abs(s.suggested_fare - c.suggested_fare) < 1e-6),
            countIf(s.price_action = c.price_action),
            countIf(s.active_bookings = c.active_bookings AND abs(s.avg_fare - c.avg_fare) < 1e-6
                AND ((s.capacity IS NULL AND c.capacity IS NULL) OR s.capacity = c.capacity)
                AND abs(s.suggested_fare - c.suggested_fare) < 1e-6 AND s.price_action = c.price_action)
        FROM (SELECT *, toUnixTimestamp(window_start) AS ts FROM pricing.demand_ratio_hourly FINAL WHERE source = 'stream') AS s
        INNER JOIN (SELECT *, toUnixTimestamp(window_start) AS ts FROM pricing.v_demand_ratio_ch) AS c
            ON s.location_id = c.location_id AND s.ts = c.ts
        SETTINGS join_use_nulls = 1""").result_rows[0], {
        "spark": CH.query("SELECT count() FROM pricing.demand_ratio_hourly FINAL WHERE source = 'stream'").result_rows[0][0],
        "clickhouse": CH.query("SELECT count() FROM pricing.v_demand_ratio_ch").result_rows[0][0]}


def main():
    (OUT / "logs").mkdir(parents=True, exist_ok=True)
    if subprocess.run(["docker", "exec", "spark-local", "pgrep", "-f", "jobs/br2_"], capture_output=True).stdout.strip():
        sys.exit("the BR2 demo stream is running in spark-local: stop it (make down, or pkill -f jobs/br2_) and rerun")
    start_script.create_clickhouse_tables(2, truncate=False)
    print(f"dimension tables loaded: {load_dims()} capacity slots, {len(LANDMARKS)} landmarks", flush=True)
    rows = trip_rows()
    topic = load_topic(f"bench_br2_{len(rows)}", rows)
    topic_rows = len(rows)
    expected = sum(r["do_location_id"] in LANDMARKS for r in rows)
    print(f"topic {topic} has {topic_size(topic)} messages, {expected} landmark trips expected", flush=True)
    results = []
    for rep in range(REPS):
        for engine in ("spark", "clickhouse"):
            r = {"engine": engine, "rep": rep}
            r.update(run_backlog(engine, topic, expected, topic_rows, rep))
            r.update(run_live(engine, rows, rep))
            results.append(r)
            print(r, flush=True)
    with open(OUT / "benchmark_runs.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(results[0]))
        w.writeheader()
        w.writerows(results)
    for engine in ("spark", "clickhouse"):
        run_backlog(engine, topic, expected, topic_rows, "check")
    (n, ab, fare, cap, sug, act, same), sides = correctness()
    keys = ["end_to_end_s", "first_row_s", "rows_per_s", "peak_mem_mib", "mean_cpu_pct",
            "p50_ms", "p95_ms", "p99_ms", "live_peak_mem_mib", "live_mean_cpu_pct"]
    lines = ["| Engine | Rows | " + " | ".join(keys) + " |", "|---|---|" + "---|" * len(keys)]
    for engine in ("spark", "clickhouse"):
        rs = [r for r in results if r["engine"] == engine]
        med = [statistics.median([r[k] for r in rs if r[k] is not None]) for k in keys]
        lines.append(f"| {engine} | {rs[0]['rows']}/{rs[0]['topic_rows']} | " + " | ".join(f"{m:g}" for m in med) + " |")
    lines += ["", f"Median of {REPS} runs. Spark maxOffsetsPerTrigger = {MAX_OFFSETS}; "
              f"live: {RATE} trips/s for {DURATION} s, latency per hourly window = ingested_at - newest Kafka timestamp.",
              "rows = landmark trips written, topic_rows = Kafka messages consumed; rows_per_s uses topic_rows "
              "(Rows column shows landmark/topic).",
              "ClickHouse Kafka engine flushes blocks every stream_flush_interval_ms (7.5 s default); Spark trigger is 10 s.", "",
              "Correctness (backlog outputs joined on location and hour, Spark vs ClickHouse):",
              f"- {same}/{n} windows identical in every column; active_bookings {ab}/{n}, avg_fare {fare}/{n}, "
              f"capacity {cap}/{n}, suggested_fare {sug}/{n}, price_action {act}/{n}",
              f"- Window counts: {sides}"]
    (OUT / "summary.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
