"""Spark Structured Streaming (ETL) vs ClickHouse Kafka engine + materialized
view (ELT) for BR3 and BR4, two experiments per BR and engine:
  backlog - N messages already in Kafka, time until all are in ClickHouse
  live    - pipeline running, RATE msg/s for DURATION s, per-row latency
            = ingested_at - Kafka message timestamp

Usage: python tests/br3/benchmark.py [REPS]
Needs the stack and both BRs started once with scripts/start.py (it builds
the Spark image, creates the tables and loads the dimension tables).
"""
import csv
import gzip
import importlib.util
import io
import itertools
import json
import os
import statistics
import subprocess
import sys
import threading
import time
import urllib.request
import zipfile
from pathlib import Path

import clickhouse_connect
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

ROOT = Path(__file__).resolve().parents[2]
REPS = int(sys.argv[1]) if len(sys.argv) > 1 else 3
N_REVIEWS = int(os.environ.get("BENCH_REVIEWS", 200_000))
RATE = int(os.environ.get("BENCH_RATE", 1000))
DURATION = int(os.environ.get("BENCH_DURATION", 60))
MAX_OFFSETS_PER_TRIGGER = os.environ.get("BENCH_MAX_OFFSETS_PER_TRIGGER", "10000")
TIMEOUT = 1200
OUT = ROOT / "docs" / "result" / "br3" / "benchmark"

spec = importlib.util.spec_from_file_location("start", ROOT / "scripts" / "start.py")
start_script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(start_script)

CH = clickhouse_connect.get_client(host="localhost", port=8123, username="default", password="clickhouse")
SPARK_MASTER = "spark://spark-master:7077"
CONTAINERS = {"spark": ["spark-local", "spark-worker", "spark-master", "clickhouse"], "clickhouse": ["clickhouse"]}


def review_rows():
    rows = []
    with gzip.open(ROOT / "data" / "br3" / "reviews.csv.gz", "rt", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append({"listing_id": row["listing_id"], "id": row["id"], "comments": row["comments"] or ""})
            if len(rows) == N_REVIEWS:
                break
    return rows


def booking_rows():
    with open(ROOT / "data" / "br4" / "Fact_Bookings.csv", newline="", encoding="utf-8") as f:
        return [{"listing_id": r["Property_ID"], "id": r["Booking_ID"], "revenue": r["Revenue"],
                 "nights_booked": r["Nights_Booked"]} for r in csv.DictReader(f)]


PIPELINES = {
    "3": {"rows": review_rows, "spark_table": "fact_reviews", "ch_table": "fact_reviews_ch",
          "dim": "dim_airbnb_listing", "id": "review_id", "ch_sql": "clickhouse_br3.sql",
          "ch_objects": ["mv_reviews_ch", "kafka_reviews_ch"], "job": "jobs/br3_"},
    "4": {"rows": booking_rows, "spark_table": "fact_bookings", "ch_table": "fact_bookings_ch",
          "dim": "dim_listing_pricing", "id": "booking_id", "ch_sql": "clickhouse_br4.sql",
          "ch_objects": ["mv_bookings_ch", "kafka_bookings_ch"], "job": "jobs/br4_"},
}


def producer():
    return KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda v: json.dumps(v).encode())


def topic_size(topic):
    consumer = KafkaConsumer(bootstrap_servers="localhost:9092")
    tps = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic) or set()]
    size = sum(consumer.end_offsets(tps).values()) if tps else 0
    consumer.close()
    return size


def load_topic(topic, rows):
    if topic_size(topic) >= len(rows):
        return
    p = producer()
    for row in rows:
        p.send(topic, row)
    p.flush()


def load_stopwords():
    jar = subprocess.run(["docker", "exec", "spark-local", "sh", "-c", "ls /opt/bitnami/spark/jars/spark-mllib_*.jar"],
                         capture_output=True, text=True, check=True).stdout.split()[0]
    data = subprocess.run(["docker", "exec", "spark-local", "cat", jar], capture_output=True, check=True).stdout
    words = zipfile.ZipFile(io.BytesIO(data)).read("org/apache/spark/ml/feature/stopwords/english.txt").decode().split()
    CH.command("CREATE TABLE IF NOT EXISTS en_stopwords (word String) ENGINE = MergeTree ORDER BY word")
    CH.command("TRUNCATE TABLE en_stopwords")
    CH.insert("en_stopwords", [[w] for w in words], column_names=["word"])
    return len(words)


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


def count(table, where="1"):
    return CH.query(f"SELECT countIf({where}) FROM {table}").result_rows[0][0]


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


def start_engine(br, cfg, engine, topic, group):
    if engine == "spark":
        CH.command(f"TRUNCATE TABLE {cfg['spark_table']}")
        cmd = start_script.spark_submit_cmd(br, SPARK_MASTER)
        env = ["-e", f"KAFKA_TOPIC={topic}", "-e", "STARTING_OFFSETS=earliest",
               "-e", f"MAX_OFFSETS_PER_TRIGGER={MAX_OFFSETS_PER_TRIGGER}"]
        cmd = cmd[:3] + env + cmd[3:]
        wait_cluster_idle()
        log = open(OUT / "logs" / f"spark-br{br}-{topic}-{int(time.time())}.log", "w")
        return subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    for obj in cfg["ch_objects"] + [cfg["ch_table"]]:
        CH.command(f"DROP TABLE IF EXISTS {obj}")
    sql = (ROOT / "tests" / f"br{br}" / cfg["ch_sql"]).read_text().replace("{topic}", topic).replace("{group}", group)
    for part in sql.split(";"):
        body = "\n".join(l for l in part.splitlines() if not l.strip().startswith("--")).strip()
        if body:
            CH.command(body)
    return None


def stop_engine(cfg, engine, proc):
    if engine == "spark":
        subprocess.run(["docker", "exec", "spark-local", "pkill", "-f", cfg["job"]])
        proc.wait()
    else:
        for obj in cfg["ch_objects"]:
            CH.command(f"DROP TABLE IF EXISTS {obj}")


def run_backlog(br, cfg, engine, topic, expected, tag):
    table = cfg["spark_table"] if engine == "spark" else cfg["ch_table"]
    sampler = Sampler(CONTAINERS[engine])
    sampler.start()
    start = time.time()
    proc = start_engine(br, cfg, engine, topic, f"backlog_br{br}_{tag}_{int(start)}")
    first = None
    try:
        while time.time() - start < TIMEOUT:
            c = count(table)
            if c and first is None:
                first = time.time()
            if c >= expected:
                break
            time.sleep(0.5)
        else:
            raise TimeoutError(f"{table}: {count(table)}/{expected}")
        done = time.time()
    finally:
        stop_engine(cfg, engine, proc)
    return {"rows": expected, "first_row_s": round(first - start, 2), "end_to_end_s": round(done - start, 2),
            "rows_per_s": round(expected / (done - start)), **sampler.result()}


def run_live(br, cfg, engine, rows, dim_ids, tag):
    table = cfg["spark_table"] if engine == "spark" else cfg["ch_table"]
    topic = f"live_br{br}_{engine}_{tag}_{int(time.time())}"
    proc = start_engine(br, cfg, engine, topic, topic)
    p = producer()
    try:
        start = time.time()
        while count(table) == 0 and time.time() - start < TIMEOUT:
            for row in rows[:100]:
                p.send(topic, row)
            p.flush()
            time.sleep(2)
        sampler = Sampler(CONTAINERS[engine])
        sampler.start()
        t0_ms = int(time.time() * 1000)
        live = list(itertools.islice(itertools.cycle(rows[1000:]), RATE * DURATION))
        step = RATE // 10
        for i in range(0, len(live), step):
            tick = time.time()
            for row in live[i:i + step]:
                p.send(topic, row)
            time.sleep(max(0.0, 0.1 - (time.time() - tick)))
        p.flush()
        expected = sum(r["listing_id"] in dim_ids for r in live)
        cond = f"toUnixTimestamp64Milli(event_time) >= {t0_ms}"
        deadline = time.time() + TIMEOUT
        while count(table, cond) < expected and time.time() < deadline:
            time.sleep(0.5)
        res = sampler.result("live_")
        q = CH.query(f"""SELECT countIf({cond}),
                quantilesIf(0.5, 0.95, 0.99)(toUnixTimestamp64Milli(ingested_at) - toUnixTimestamp64Milli(event_time), {cond})
            FROM {table}""").result_rows[0]
    finally:
        stop_engine(cfg, engine, proc)
    return {"live_rows": q[0], "live_expected": expected, "p50_ms": round(q[1][0]), "p95_ms": round(q[1][1]),
            "p99_ms": round(q[1][2]), **res}


def correctness():
    br4 = CH.query("""
        SELECT count(), countIf(abs(s.revenue - c.revenue) < 1e-6 AND s.revenue_bucket = c.revenue_bucket)
        FROM fact_bookings s INNER JOIN fact_bookings_ch c USING (booking_id)""").result_rows[0]
    br3 = CH.query("""
        SELECT count(), countIf(s.keywords = c.keywords), countIf((s.sentiment_label != 'non_english') = (c.is_english = 1))
        FROM fact_reviews s INNER JOIN fact_reviews_ch c USING (review_id)""").result_rows[0]
    sides = {t: CH.query(f"SELECT count(), uniqExact({k}) FROM {t}").result_rows[0]
             for t, k in [("fact_reviews", "review_id"), ("fact_reviews_ch", "review_id"),
                          ("fact_bookings", "booking_id"), ("fact_bookings_ch", "booking_id")]}
    return br3, br4, sides


def main():
    (OUT / "logs").mkdir(parents=True, exist_ok=True)
    print(f"stopwords loaded: {load_stopwords()}", flush=True)
    results, data = [], {}
    for br, cfg in PIPELINES.items():
        rows = cfg["rows"]()
        topic = f"bench_br{br}_{len(rows)}"
        load_topic(topic, rows)
        dim_ids = {r[0] for r in CH.query(f"SELECT listing_id FROM {cfg['dim']}").result_rows}
        expected = sum(r["listing_id"] in dim_ids for r in rows)
        data[br] = (topic, expected)
        print(f"BR{br}: topic {topic} has {topic_size(topic)} messages, {expected} expected after the dim join", flush=True)
        for rep in range(REPS):
            for engine in ("spark", "clickhouse"):
                r = {"br": br, "engine": engine, "rep": rep}
                r.update(run_backlog(br, cfg, engine, topic, expected, rep))
                r.update(run_live(br, cfg, engine, rows, dim_ids, rep))
                results.append(r)
                print(r, flush=True)
    with open(OUT / "benchmark_runs.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(results[0]))
        w.writeheader()
        w.writerows(results)
    for br, cfg in PIPELINES.items():
        for engine in ("spark", "clickhouse"):
            run_backlog(br, cfg, engine, data[br][0], data[br][1], "check")
    br3, br4, sides = correctness()
    keys = ["end_to_end_s", "first_row_s", "rows_per_s", "peak_mem_mib", "mean_cpu_pct",
            "p50_ms", "p95_ms", "p99_ms", "live_peak_mem_mib", "live_mean_cpu_pct"]
    lines = ["| BR | Engine | Rows | " + " | ".join(keys) + " |", "|---|---|---|" + "---|" * len(keys)]
    for br in PIPELINES:
        for engine in ("spark", "clickhouse"):
            rs = [r for r in results if r["br"] == br and r["engine"] == engine]
            med = [statistics.median([r[k] for r in rs if r[k] is not None]) for k in keys]
            lines.append(f"| BR{br} | {engine} | {rs[0]['rows']} | " + " | ".join(f"{m:g}" for m in med) + " |")
    lines += ["", f"Median of {REPS} runs. Spark maxOffsetsPerTrigger = {MAX_OFFSETS_PER_TRIGGER}; "
              f"live: {RATE} msg/s for {DURATION} s, latency = ingested_at - Kafka timestamp.", "",
              "Correctness (backlog outputs joined on id, Spark vs ClickHouse):",
              f"- BR3: {br3[1]}/{br3[0]} rows with identical keywords; {br3[2]}/{br3[0]} with the same English/non-English decision",
              f"- BR4: {br4[1]}/{br4[0]} rows with identical revenue and bucket",
              f"- Row counts (count, distinct ids): {sides}"]
    (OUT / "summary.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
