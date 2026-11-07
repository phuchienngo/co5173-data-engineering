"""BR1 tour route: landmark distance matrix, streaming tour solver, and a naive-vs-optimized benchmark."""

import argparse
import json
import math
import os
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import partial

FARE_FLAG_VND = 11000
FARE_PER_KM_VND = 15000  # ponytail: flat tariff, swap for per-city tariff table when needed
EARTH_KM = 6371.0088
NAIVE_CONF = {
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.adaptive.coalescePartitions.enabled": "false",
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.sql.shuffle.partitions": "200",
}
OPTIMIZED_CONF = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.shuffle.partitions": "8",
    "spark.sql.execution.arrow.pyspark.enabled": "true",
}
REQUEST_DDL = "request_id long, city string, stop_ids array<string>, requested_at string"
FACT_DDL = (
    "request_id long, city string, n_stops int, stop_ids array<string>, stop_names array<string>, "
    "lats array<double>, lons array<double>, total_km double, total_fare_vnd int, total_ticket_vnd int, "
    "total_cost_vnd int, legs array<struct<seq:int,from_id:string,to_id:string,to_name:string,"
    "leg_km:double,leg_fare_vnd:int,ticket_vnd:int,cum_cost_vnd:int>>"
)


def fare_of(km):
    return int(math.floor(FARE_FLAG_VND + FARE_PER_KM_VND * km + 0.5))


def solve_order(d, k):
    """Open path from node 0 through all k nodes: nearest neighbour, then 2-opt until no swap helps."""
    order = [0]
    left = set(range(1, k))
    while left:
        nxt = min(left, key=lambda j: (d[order[-1]][j], j))
        left.remove(nxt)
        order.append(nxt)

    def length(o):
        return sum(d[o[i]][o[i + 1]] for i in range(len(o) - 1))

    best = length(order)
    improved = True
    while improved:
        improved = False
        for i in range(1, k - 1):
            for j in range(i + 1, k):
                cand = order[:i] + order[i:j + 1][::-1] + order[j + 1:]
                cand_len = length(cand)
                if cand_len < best - 1e-12:
                    order, best, improved = cand, cand_len, True
    return order


def plan_row(request_id, city, stops, d):
    """Visiting order and leg costs for one request; stops[0] is the start, d[i][j] km between stops."""
    order = solve_order(d, len(stops))
    first = stops[order[0]]
    legs = [dict(seq=0, from_id=first["osm_id"], to_id=first["osm_id"], to_name=first["name"], leg_km=0.0,
                 leg_fare_vnd=0, ticket_vnd=first["ticket_vnd"], cum_cost_vnd=first["ticket_vnd"])]
    total_km, fare = 0.0, 0
    for seq, (a, b) in enumerate(zip(order, order[1:]), 1):
        km, leg_fare, stop = d[a][b], fare_of(d[a][b]), stops[b]
        total_km += km
        fare += leg_fare
        legs.append(dict(seq=seq, from_id=stops[a]["osm_id"], to_id=stop["osm_id"], to_name=stop["name"],
                         leg_km=km, leg_fare_vnd=leg_fare, ticket_vnd=stop["ticket_vnd"],
                         cum_cost_vnd=legs[-1]["cum_cost_vnd"] + leg_fare + stop["ticket_vnd"]))
    ordered = [stops[i] for i in order]
    tickets = sum(s["ticket_vnd"] for s in stops)
    return dict(request_id=request_id, city=city, n_stops=len(stops),
                stop_ids=[s["osm_id"] for s in ordered], stop_names=[s["name"] for s in ordered],
                lats=[s["lat"] for s in ordered], lons=[s["lon"] for s in ordered],
                total_km=total_km, total_fare_vnd=fare, total_ticket_vnd=tickets,
                total_cost_vnd=fare + tickets, legs=legs)


def naive_plan(request_id, city, stops, edges):
    k = len(stops)
    d = [[0.0] * k for _ in range(k)]
    for e in edges:
        d[e["i"]][e["j"]] = e["dist_km"]
    return plan_row(request_id, city, [s.asDict() for s in stops], d)


def self_test():
    xs = [0.0, 3.0, 1.0, 4.0, 2.0]  # line: optimal open tour from node 0 is 0,2,4,1,3 with length 4
    d = [[abs(a - b) for b in xs] for a in xs]
    order = solve_order(d, len(xs))
    assert order == [0, 2, 4, 1, 3], order
    assert abs(sum(d[order[i]][order[i + 1]] for i in range(4)) - 4.0) < 1e-9
    print("solver self-test OK")


def parse_args():
    env = os.environ.get
    p = argparse.ArgumentParser(description="BR1 tour route: streaming solver plus naive/optimized benchmark")
    p.add_argument("--requests", type=int, default=int(env("BR1_BENCH_REQUESTS", "20000")))
    p.add_argument("--stops-min", type=int, default=4)
    p.add_argument("--stops-max", type=int, default=8)
    p.add_argument("--seed", type=int, default=int(env("BR1_SEED", "42")))
    p.add_argument("--landmarks", default=env("BR1_LANDMARKS", "/opt/data/br1/landmarks.csv"))
    p.add_argument("--lake", default=env("BR1_LAKE", "/opt/data/br1/lake"))
    p.add_argument("--max-landmarks-per-city", type=int, default=int(env("BR1_MAX_LANDMARKS_PER_CITY", "0")))
    p.add_argument("--self-test", action="store_true")
    return p.parse_args()


def ch_config():
    return {
        "url": os.environ.get("CLICKHOUSE_URL", "http://clickhouse:8123"),
        "user": os.environ.get("CLICKHOUSE_USER", "default"),
        "password": os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse"),
    }


def ch_post(ch, query, body=b""):
    url = f"{ch['url']}/?{urllib.parse.urlencode({'query': query})}"
    req = urllib.request.Request(url, data=body, headers={
        "X-ClickHouse-User": ch["user"], "X-ClickHouse-Key": ch["password"]})
    try:
        urllib.request.urlopen(req, timeout=300).read()
    except urllib.error.HTTPError as e:
        raise RuntimeError(e.read().decode(errors="replace")) from e


def insert_partition(rows, table, ch, chunk=20000):
    query = f"INSERT INTO {table} FORMAT JSONEachRow"
    batch = []
    for row in rows:
        batch.append(json.dumps(row.asDict(recursive=True), ensure_ascii=False))
        if len(batch) == chunk:
            ch_post(ch, query, "\n".join(batch).encode("utf-8"))
            batch = []
    if batch:
        ch_post(ch, query, "\n".join(batch).encode("utf-8"))


def insert_rows(df, table, ch):
    df.foreachPartition(partial(insert_partition, table=table, ch=ch))


def write_lake(df, path, partition=None):
    writer = df.write.mode("overwrite")
    if partition:
        writer = writer.partitionBy(partition)
    writer.parquet(path)
    return df.sparkSession.read.parquet(path)


@contextmanager
def timed(stage, mode, timings):
    rec = {"run_mode": mode, "stage": stage, "seconds": 0.0, "rows": 0}
    t0 = time.perf_counter()
    yield rec
    rec["seconds"] = round(time.perf_counter() - t0, 3)
    timings.append(rec)
    print(f"[{mode}] {stage}: {rec['seconds']}s rows={rec['rows']}", flush=True)


def main():
    args = parse_args()
    if args.self_test:
        self_test()
        return

    import numpy as np
    import pandas as pd
    from pyspark.sql import SparkSession, Window, functions as F

    ch = ch_config()
    lake = args.lake
    timings = []
    spark = (SparkSession.builder.appName("br1-tour-route")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.scheduler.mode", "FAIR")
             .config("spark.sql.ansi.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    def set_mode(mode):
        for k, v in (NAIVE_CONF if mode == "naive" else OPTIMIZED_CONF).items():
            spark.conf.set(k, v)

    def haversine_np(la1, lo1, la2, lo2):
        p1, p2 = np.radians(la1), np.radians(la2)
        h = np.sin((p2 - p1) / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(np.radians(lo2 - lo1) / 2) ** 2
        return 2 * EARTH_KM * np.arcsin(np.sqrt(h))

    # Optimized mode only: landmarks feed the stream; dist matrix and ClickHouse dims are built after it starts.
    set_mode("optimized")

    with timed("landmarks", "optimized", timings) as rec:
        lm = (spark.read.option("header", True).option("inferSchema", True).csv(args.landmarks)
              .select("osm_id", "name", "category", "city", F.col("lat").cast("double"),
                      F.col("lon").cast("double"), F.col("ticket_vnd").cast("int"))
              .dropna(subset=["lat", "lon"])
              .withColumn("idx", F.row_number().over(Window.partitionBy("city").orderBy("osm_id")) - 1))
        if args.max_landmarks_per_city:
            lm = lm.where(F.col("idx") < args.max_landmarks_per_city)
        lm = lm.cache()
        rec["rows"] = lm.count()
        by_city = {}
        for r in lm.orderBy("city", "idx").collect():
            by_city.setdefault(r.city, []).append(r)
    # coords[city] = (osm_ids, names, lat, lon, ticket, osm_id -> position); position == idx
    coords = {}
    for city, rs in by_city.items():
        ids = [r.osm_id for r in rs]
        coords[city] = (ids, [r.name for r in rs], np.array([r.lat for r in rs]),
                        np.array([r.lon for r in rs]), np.array([r.ticket_vnd for r in rs]),
                        {oid: i for i, oid in enumerate(ids)})
    coords_bc = sc.broadcast(coords)
    cities = sorted(coords)

    # Stream prep: clear fact tables and the stream checkpoint before the query starts.
    for table in ("tour.fact_tour", "tour.fact_tour_leg"):
        ch_post(ch, f"TRUNCATE TABLE {table}")
    shutil.rmtree(f"{lake}/checkpoints/tour_requests", ignore_errors=True)

    def solve_batches(batches):
        for pdf in batches:
            rows, dropped = [], 0
            for rid, city, stop_ids in zip(pdf["request_id"], pdf["city"], pdf["stop_ids"]):
                if city not in coords_bc.value:
                    dropped += 1
                    continue
                ids, names, lat, lon, ticket, index = coords_bc.value[city]
                known = [index[s] for s in stop_ids if s in index]
                if len(known) < 2:
                    dropped += 1
                    continue
                sel = np.array(known, dtype=int)
                la, lo = lat[sel], lon[sel]
                d = haversine_np(la[:, None], lo[:, None], la[None, :], lo[None, :]).tolist()
                stops = [{"osm_id": ids[i], "name": names[i], "lat": float(lat[i]), "lon": float(lon[i]),
                          "ticket_vnd": int(ticket[i])} for i in sel]
                rows.append(plan_row(int(rid), city, stops, d))
            if dropped:
                print(f"solve_batches: dropped {dropped} rows", flush=True)
            if rows:
                yield pd.DataFrame(rows)

    # Streaming solver: requests from Kafka -> optimized solve -> fact_tour / fact_tour_leg (append only).
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"))
           .option("subscribe", "tour.requests")
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", os.environ.get("BR1_MAX_OFFSETS", "2000"))
           .load())
    requests_stream = (raw.select(F.from_json(F.col("value").cast("string"), REQUEST_DDL).alias("d"))
                       .select("d.request_id", "d.city", "d.stop_ids"))

    def write_batch(batch_df, batch_id):
        valid = batch_df.where(F.col("request_id").isNotNull() & F.col("city").isNotNull()
                               & (F.size("stop_ids") >= 2))
        fact = valid.mapInPandas(solve_batches, FACT_DDL).cache()
        n = fact.count()
        if n:
            insert_rows(fact.drop("legs"), "tour.fact_tour", ch)
            insert_rows(fact.select("request_id", F.explode("legs").alias("l")).select("request_id", "l.*"),
                        "tour.fact_tour_leg", ch)
        fact.unpersist()
        print(f"stream batch {batch_id}: {n} requests", flush=True)

    query = (requests_stream.writeStream.queryName("tour_requests").foreachBatch(write_batch)
             .option("checkpointLocation", f"{lake}/checkpoints/tour_requests")
             .trigger(processingTime="5 seconds")
             .start())
    print("stream started", flush=True)

    with timed("dist_matrix", "optimized", timings) as dist_rec:
        a = lm.select("city", F.col("osm_id").alias("from_id"), F.col("lat").alias("lat1"), F.col("lon").alias("lon1"))
        b = lm.select("city", F.col("osm_id").alias("to_id"), F.col("lat").alias("lat2"), F.col("lon").alias("lon2"))
        pairs = a.join(b, "city").where(F.col("from_id") != F.col("to_id"))
        lat1, lon1, lat2, lon2 = F.col("lat1"), F.col("lon1"), F.col("lat2"), F.col("lon2")
        h = (F.pow(F.sin(F.radians(lat2 - lat1) / 2), 2)
             + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(F.sin(F.radians(lon2 - lon1) / 2), 2))
        dm = pairs.select("city", "from_id", "to_id", (2 * EARTH_KM * F.asin(F.sqrt(h))).alias("dist_km"))
        dm = dm.withColumn("fare_vnd", F.round(FARE_FLAG_VND + FARE_PER_KM_VND * F.col("dist_km")).cast("int"))
        dm = write_lake(dm, f"{lake}/dist_matrix/mode=optimized", "city")
        dist_rec["rows"] = dm.count()

    with timed("sink_clickhouse", "optimized", timings) as rec:
        for table in ("tour.dim_landmark", "tour.dist_matrix"):
            ch_post(ch, f"TRUNCATE TABLE {table}")
        insert_rows(lm.select("osm_id", "name", "category", "city", "lat", "lon", "ticket_vnd"), "tour.dim_landmark", ch)
        insert_rows(dm.select("city", "from_id", "to_id", "dist_km", "fare_vnd"), "tour.dist_matrix", ch)
        rec["rows"] = lm.count() + dist_rec["rows"]

    def build_requests(mode):
        city_n = lm.groupBy("city").agg(F.count("*").alias("n_city"))
        stop_lm = lm.select("city", "idx", "osm_id")
        if mode == "optimized":
            city_n, stop_lm = F.broadcast(city_n), F.broadcast(stop_lm)
        req = spark.range(args.requests).select(F.col("id").alias("request_id"))
        req = req.withColumn("city", F.element_at(F.array(*[F.lit(c) for c in cities]),
                                                  (F.col("request_id") % len(cities) + 1).cast("int")))
        req = req.join(city_n, "city")
        cands = F.array(*[F.pmod(F.hash("request_id", F.lit(args.seed), F.lit(i)), F.col("n_city")).cast("int")
                          for i in range(args.stops_max)])
        n_req = args.stops_min + F.pmod(F.hash("request_id", F.lit(args.seed), F.lit(-1)),
                                        F.lit(args.stops_max - args.stops_min + 1)).cast("int")
        req = (req.withColumn("stop_idx", F.slice(F.array_distinct(cands), 1, n_req))
               .withColumn("n_stops", F.size("stop_idx"))
               .where(F.col("n_stops") >= 2)
               .select("request_id", "city", "n_stops", F.posexplode("stop_idx").alias("pos", "idx")))
        return (req.join(stop_lm, ["city", "idx"])
                .groupBy("request_id", "city", "n_stops")
                .agg(F.sort_array(F.collect_list(F.struct("pos", "osm_id"))).alias("stops"))
                .select("request_id", "city", "n_stops", F.transform("stops", lambda s: s["osm_id"]).alias("stop_ids")))

    def naive_solve(req):
        stops = (req.select("request_id", "city", F.posexplode("stop_ids").alias("pos", "osm_id"))
                 .join(lm.select("city", "osm_id", "name", "lat", "lon", "ticket_vnd"), ["city", "osm_id"])
                 .groupBy("request_id", "city")
                 .agg(F.sort_array(F.collect_list(F.struct("pos", "osm_id", "name", "lat", "lon", "ticket_vnd")))
                      .alias("stops")))
        a_side = stops.select("request_id", "city", "stops", F.explode("stops").alias("a"))
        b_side = stops.select("request_id", F.explode("stops").alias("b"))
        dist = spark.read.parquet(f"{lake}/dist_matrix/mode=optimized").select("city", "from_id", "to_id", "dist_km")
        edges = (a_side.join(b_side, "request_id").where(F.col("a.pos") != F.col("b.pos"))
                 .select("request_id", "city", "stops", F.col("a.pos").alias("i"), F.col("b.pos").alias("j"),
                         F.col("a.osm_id").alias("from_id"), F.col("b.osm_id").alias("to_id"))
                 .join(dist, ["city", "from_id", "to_id"])
                 .groupBy("request_id", "city")
                 .agg(F.first("stops").alias("stops"), F.collect_list(F.struct("i", "j", "dist_km")).alias("edges")))
        plan = F.udf(naive_plan, FACT_DDL)
        return edges.select(plan("request_id", "city", "stops", "edges").alias("f")).select("f.*")

    if args.requests:
        for mode in ("naive", "optimized"):
            set_mode(mode)
            with timed("requests", mode, timings) as rec:
                req = write_lake(build_requests(mode), f"{lake}/tour_requests/mode={mode}")
                rec["rows"] = req.count()
            with timed("solve", mode, timings) as rec:
                if mode == "naive":
                    solved = naive_solve(req)
                else:
                    solved = req.repartition(sc.defaultParallelism * 2).mapInPandas(solve_batches, FACT_DDL)
                fact = write_lake(solved, f"{lake}/fact_tour/mode={mode}")
                rec["rows"] = fact.count()

        for mode in ("naive", "optimized"):
            parts = [r for r in timings if r["run_mode"] == mode and r["stage"] in ("requests", "solve")]
            if parts:
                solve_rows = next(r["rows"] for r in parts if r["stage"] == "solve")
                timings.append({"run_mode": mode, "stage": "total",
                                "seconds": round(sum(r["seconds"] for r in parts), 3), "rows": solve_rows})

    ran_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    body = "\n".join(json.dumps({"run_mode": r["run_mode"], "stage": r["stage"], "seconds": r["seconds"],
                                 "rows": r["rows"], "requests": args.requests, "ran_at": ran_at})
                     for r in timings)
    ch_post(ch, "INSERT INTO tour.job_run FORMAT JSONEachRow", body.encode("utf-8"))

    print(f"{'mode':<10} {'stage':<16} {'seconds':>10} {'rows':>12}")
    for r in timings:
        print(f"{r['run_mode']:<10} {r['stage']:<16} {r['seconds']:>10.3f} {r['rows']:>12}")

    query.awaitTermination()


if __name__ == "__main__":
    main()
