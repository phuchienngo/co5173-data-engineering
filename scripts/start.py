import argparse
import os
import re
import subprocess
import sys
import time
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = Path(os.environ.get("DEMO_LOG_DIR", REPO_ROOT / "logs" / "demo"))
SPARK_KAFKA_PACKAGE = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
    "org.apache.kafka:kafka-clients:3.9.0"
)


def run(cmd):
    print(f"$ {' '.join(str(part) for part in cmd)}")
    subprocess.run([str(part) for part in cmd], check=True, cwd=REPO_ROOT)


def popen(cmd, log_file):
    with log_file.open("w") as log:
        return subprocess.Popen(
            [str(part) for part in cmd], cwd=REPO_ROOT,
            stdout=log, stderr=subprocess.STDOUT,
        )


def container_health(name):
    result = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.Health.Status}}", name],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def wait_healthy(names, timeout=120):
    print(f"Waiting for {', '.join(names)} to be healthy...")
    deadline = time.time() + timeout
    pending = set(names)
    while pending and time.time() < deadline:
        for name in list(pending):
            if container_health(name) == "healthy":
                pending.discard(name)
        if pending:
            time.sleep(3)
    if pending:
        sys.exit(f"Timed out waiting for: {', '.join(pending)}")
    print("OK, all healthy.")


def discover_brs():
    numbers = []
    for f in sorted((REPO_ROOT / "sql" / "clickhouse").glob("ddl_br*.sql")):
        numbers.append(f.stem[len("ddl_br"):])
    return numbers


def parse_selected(values):
    selected = []
    for value in values:
        if value == "all":
            selected.append(value)
            continue
        selected.extend(part for part in value.split(",") if part)
    return selected


def pick_brs(brs):
    print(f"\nAvailable BRs: {', '.join('BR' + n for n in brs)}")
    selected = []
    for n in brs:
        answer = input(f"Run BR{n}? [y/N] ").strip().lower()
        if answer == "y":
            selected.append(n)
    return selected


def tail_file(path, lines=20):
    if not path.exists():
        return ""
    return "\n".join(path.read_text(errors="replace").splitlines()[-lines:])


def report_exited(procs, label, ok_completed=False):
    failed = False
    for name, proc, log_file in procs:
        code = proc.poll()
        if code is None:
            continue
        if code == 0 and ok_completed:
            print(f"\n{name} {label} completed. Last log lines:")
            print(tail_file(log_file))
            continue
        failed = True
        print(f"\n{name} {label} exited with code {code}. Last log lines:")
        print(tail_file(log_file))
    return failed


def build_image():
    run(["docker", "compose", "build", "spark-local"])


def start_stack():
    services = [
        "clickhouse",
        "kafka",
        "spark-local",
        "grafana",
        "prometheus",
        "spark-master",
        "spark-worker",
    ]
    run(["docker", "compose", "--profile", "full", "--profile", "monitoring", "up", "-d", *services])


def create_topics(selected):
    # Only the selected BRs' topics are reset, so other BRs keep their data.
    run([sys.executable, "scripts/topics.py", "--reset", "--br", *selected])


def fact_tables_in(ddl):
    tables = []
    for match in re.finditer(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z0-9_]+)", ddl.read_text(), re.IGNORECASE):
        table = match.group(1)
        if table.startswith("fact_"):
            tables.append(table)
    return tables


def clickhouse_query(sql):
    run([
        "docker",
        "exec",
        "-i",
        "clickhouse",
        "clickhouse-client",
        "--user",
        "default",
        "--password",
        "clickhouse",
        "--query",
        sql,
    ])


def create_clickhouse_tables(br):
    ddl = REPO_ROOT / "sql" / "clickhouse" / f"ddl_br{br}.sql"
    if not ddl.exists():
        sys.exit(f"Missing DDL file: {ddl}")
    subprocess.run([
        "docker", "exec", "-i", "clickhouse", "clickhouse-client",
        "--user", "default", "--password", "clickhouse", "--multiquery",
    ], input=ddl.read_text(), text=True, check=True, cwd=REPO_ROOT)
    for table in fact_tables_in(ddl):
        clickhouse_query(f"TRUNCATE TABLE IF EXISTS {table}")


def prepare_data(br):
    preparation = REPO_ROOT / "scripts" / f"prepare_br{br}.py"
    if preparation.exists():
        run([sys.executable, preparation])


def job_path(br):
    matches = sorted((REPO_ROOT / "jobs").glob(f"br{br}_*/job.py"))
    if not matches:
        sys.exit(f"Missing Spark job for BR{br}: jobs/br{br}_*/job.py")
    if len(matches) > 1:
        sys.exit(f"Multiple Spark jobs found for BR{br}: {', '.join(str(p) for p in matches)}")
    return matches[0]


def py_files_for(job):
    common = sorted((REPO_ROOT / "jobs" / "common").glob("*.py"))
    siblings = sorted(p for p in job.parent.glob("*.py") if p != job)
    files = common + siblings
    return ",".join("/opt/" + str(path.relative_to(REPO_ROOT)) for path in files)


def spark_submit_cmd(br, spark_master):
    job = job_path(br)
    return [
        "docker",
        "exec",
        "-i",
        "-e",
        # A fresh checkpoint per run: topics and fact tables are reset every run.
        f"BR{br}_CHECKPOINT_DIR=/opt/data/br{br}/checkpoints/demo-{uuid.uuid4().hex}",
        "spark-local",
        "spark-submit",
        "--master",
        spark_master,
        "--driver-memory",
        os.environ.get("SPARK_DRIVER_MEMORY", "512m"),
        "--executor-memory",
        os.environ.get("SPARK_EXECUTOR_MEMORY", "512m"),
        "--conf",
        "spark.jars.ivy=/tmp/.ivy2",
        "--conf",
        f"spark.cores.max={os.environ.get('SPARK_CORES_MAX', '1')}",
        "--conf",
        f"spark.sql.shuffle.partitions={os.environ.get('SPARK_SHUFFLE_PARTITIONS', '4')}",
        "--packages",
        SPARK_KAFKA_PACKAGE,
        "--py-files",
        py_files_for(job),
        "/opt/" + str(job.relative_to(REPO_ROOT)),
    ]


def producer_cmd(br, interval, events, loop_producers):
    if loop_producers:
        limit_arg = "--loop"
    elif events is None:
        limit_arg = ""
    else:
        limit_arg = f"--limit {events}"
    return [
        "docker",
        "exec",
        "-i",
        "spark-local",
        "bash",
        "-c",
        (
            "PYTHONPATH=/opt/jobs/common "
            f"python3 /opt/ingestion/producers/produce_br{br}.py "
            f"--interval {interval} {limit_arg}"
        ),
    ]


def wait_for_spark_ready(name, proc, log_file, timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            report_exited([(name, proc, log_file)], "Spark job")
            return False
        tail = tail_file(log_file, lines=80)
        # Jobs print "BR<n>_STREAM_READY"; jobs without a checkpoint log ResolveWriteToStream.
        if f"{name}_STREAM_READY" in tail or "ResolveWriteToStream" in tail:
            print(f"{name} Spark job is ready.")
            return True
        time.sleep(1)
    print(f"Timed out waiting for {name} Spark job to initialize. Last log lines:")
    print(tail_file(log_file))
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("brs", nargs="*", help="BR numbers to run, e.g. 7, 3,7 or all")
    parser.add_argument("--events", type=int, help="Events to produce per BR; omit to send the full CSV once")
    parser.add_argument("--interval", type=float, default=0, help="Seconds between producer events")
    parser.add_argument("--loop-producers", action="store_true", help="Keep producers running forever")
    args = parser.parse_args()
    if args.interval < 0 or (args.events is not None and args.events < 1):
        parser.error("interval must be nonnegative and events must be positive")
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    brs = discover_brs()
    if not brs:
        sys.exit("No BR found (sql/clickhouse/ddl_br*.sql is empty).")

    selected = parse_selected(args.brs) if args.brs else pick_brs(brs)
    if selected == ["all"]:
        selected = brs
    unknown = [n for n in selected if n not in brs]
    if unknown:
        sys.exit(f"Unknown BR: {', '.join(unknown)} (available: {', '.join(brs)})")

    if not selected:
        print("\nNo BR selected.")
        return

    selected = list(dict.fromkeys(selected))
    print("=== 1. Bringing up infra and monitoring ===")
    build_image()
    start_stack()
    wait_healthy(["clickhouse", "kafka"])

    print("\n=== Preparing selected BR datasets (scripts/prepare_br<n>.py) ===")
    for n in selected:
        prepare_data(n)

    print("\n=== 2. Creating selected Kafka topics ===")
    create_topics(selected)

    print(f"\n=== 3. Creating ClickHouse tables for {', '.join('BR' + n for n in selected)} ===")
    for n in selected:
        create_clickhouse_tables(n)

    print(f"\n=== 4. Starting Spark job(s) for {', '.join('BR' + n for n in selected)} ===")
    spark_master = "spark://spark-master:7077"
    spark_procs = []
    for n in selected:
        log_file = LOG_DIR / f"br{n}-spark.log"
        print(f"BR{n} Spark log: {log_file}")
        name = f"BR{n}"
        proc = popen(spark_submit_cmd(n, spark_master), log_file)
        spark_procs.append((name, proc, log_file))
        print(f"Waiting for {name} to resolve packages and start streaming...")
        if not wait_for_spark_ready(name, proc, log_file):
            for _, running_proc, _ in spark_procs:
                if running_proc.poll() is None:
                    running_proc.terminate()
            for _, running_proc, _ in spark_procs:
                running_proc.wait()
            sys.exit(f"{name} Spark startup failed; producers were not started.")

    print(f"\n=== 5. Starting producer(s) for {', '.join('BR' + n for n in selected)} ===")
    producer_procs = []
    for n in selected:
        log_file = LOG_DIR / f"br{n}-producer.log"
        print(f"BR{n} producer log: {log_file}")
        producer_procs.append(
            (
                f"BR{n}",
                popen(producer_cmd(n, args.interval, args.events, args.loop_producers), log_file),
                log_file,
            )
        )

    mode = "Spark standalone cluster"
    print(f"\nRunning with {mode}. Ctrl+C to stop Spark job(s).")
    try:
        while any(proc.poll() is None for _, proc, _ in producer_procs):
            stopped_spark = [name for name, proc, _ in spark_procs if proc.poll() is not None]
            if stopped_spark:
                report_exited(spark_procs, "Spark job")
                for _, proc, _ in producer_procs:
                    if proc.poll() is None:
                        proc.terminate()
                for _, proc, _ in producer_procs:
                    proc.wait()
                sys.exit(f"Spark job stopped while producers were running: {', '.join(stopped_spark)}")
            time.sleep(1)
        if report_exited(producer_procs, "producer", ok_completed=True):
            for _, proc, _ in spark_procs:
                if proc.poll() is None:
                    proc.terminate()
            for _, proc, _ in spark_procs:
                proc.wait()
            sys.exit("Producer failed; stopped Spark jobs.")
        if not args.loop_producers:
            print("\nProducer(s) finished sending available records. Spark job(s) are still running for live queries.")
        for _, p, _ in spark_procs:
            p.wait()
    except KeyboardInterrupt:
        print("\nStopping...")
        for _, p, _ in producer_procs + spark_procs:
            p.terminate()
        for _, p, _ in producer_procs + spark_procs:
            p.wait()


if __name__ == "__main__":
    main()
