import argparse
import ast
import os
import re
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = Path(os.environ.get("DEMO_LOG_DIR", REPO_ROOT / "logs" / "demo"))
SPARK_KAFKA_PACKAGE = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
    "org.apache.kafka:kafka-clients:3.9.0"
)
JOB_MODES = ("stream", "batch")


def run(cmd):
    print(f"$ {' '.join(str(part) for part in cmd)}")
    subprocess.run([str(part) for part in cmd], check=True, cwd=REPO_ROOT)


def popen(cmd, log_file):
    log = log_file.open("w")
    return subprocess.Popen(
        [str(part) for part in cmd],
        cwd=REPO_ROOT,
        stdout=log,
        stderr=subprocess.STDOUT,
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
    # Without build attestations an unchanged build keeps the same image ID,
    # so `compose up` doesn't recreate (and kill jobs in) the Spark containers.
    os.environ.setdefault("BUILDX_NO_DEFAULT_ATTESTATIONS", "1")
    run(["docker", "compose", "build", "spark-local"])


def start_stack():
    services = [
        "clickhouse",
        "kafka",
        "tour-map",
        "spark-local",
        "grafana",
        "prometheus",
        "spark-master",
        "spark-worker",
    ]
    run(["docker", "compose", "--profile", "full", "--profile", "monitoring", "up", "-d", *services])


def create_topics():
    run(["python3", "scripts/topics.py", "--reset"])


def fact_tables_in(ddl):
    tables = []
    for match in re.finditer(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z0-9_.]+)", ddl.read_text(), re.IGNORECASE):
        table = match.group(1)
        if table.split(".")[-1].startswith("fact_"):
            tables.append(table)
    return tables


def clickhouse_query(sql):
    run([
        "docker",
        "exec",
        "-i",
        "clickhouse",
        "bash",
        "-c",
        'exec clickhouse-client --user default --password "$CLICKHOUSE_PASSWORD" --query "$1"',
        "--",
        sql,
    ])


def create_clickhouse_tables(br, truncate=True):
    # Stream BRs start from empty fact tables on every run; batch BRs keep
    # their data and replace it themselves.
    ddl = REPO_ROOT / "sql" / "clickhouse" / f"ddl_br{br}.sql"
    if not ddl.exists():
        sys.exit(f"Missing DDL file: {ddl}")
    cmd = [
        "docker", "exec", "-i", "clickhouse", "bash", "-c",
        'exec clickhouse-client --user default --password "$CLICKHOUSE_PASSWORD" --multiquery',
    ]
    print(f"$ {' '.join(cmd)} < {ddl}")
    subprocess.run(cmd, check=True, cwd=REPO_ROOT, input=ddl.read_text(), text=True)
    if not truncate:
        return
    for table in fact_tables_in(ddl):
        clickhouse_query(f"TRUNCATE TABLE IF EXISTS {table}")


def job_path(br):
    matches = sorted((REPO_ROOT / "jobs").glob(f"br{br}_*/job.py"))
    if not matches:
        sys.exit(f"Missing Spark job for BR{br}: jobs/br{br}_*/job.py")
    if len(matches) > 1:
        sys.exit(f"Multiple Spark jobs found for BR{br}: {', '.join(str(p) for p in matches)}")
    return matches[0]


def job_mode(br):
    # A job declares MODE = "batch" at module level to run to completion
    # without a producer; anything else (or no MODE) is a streaming job.
    job = job_path(br)
    tree = ast.parse(job.read_text())
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "MODE" for target in node.targets):
            continue
        if isinstance(node.value, ast.Constant) and node.value.value in JOB_MODES:
            return node.value.value
        sys.exit(f"{job}: MODE must be one of {', '.join(JOB_MODES)}")
    return "stream"


def run_batch_job(br, spark_master):
    log_file = LOG_DIR / f"br{br}-spark.log"
    name = f"BR{br}"
    print(f"{name} Spark log: {log_file}")
    proc = popen(spark_submit_cmd(br, spark_master), log_file)
    try:
        proc.wait()
    except KeyboardInterrupt:
        print("\nStopping...")
        proc.terminate()
        proc.wait()
        sys.exit(f"{name} batch job interrupted.")
    if report_exited([(name, proc, log_file)], "batch Spark job", ok_completed=True):
        sys.exit(f"{name} batch job failed.")


def py_files_for(job):
    common = sorted((REPO_ROOT / "jobs" / "common").glob("*.py"))
    siblings = sorted(p for p in job.parent.glob("*.py") if p != job)
    files = common + siblings
    return ",".join("/opt/" + path.relative_to(REPO_ROOT).as_posix() for path in files)


def passthrough_env():
    flags = []
    for key, value in sorted(os.environ.items()):
        if re.match(r"^(BR\d+_|MAX_OFFSETS$)", key):
            flags += ["-e", f"{key}={value}"]
    return flags


def spark_submit_cmd(br, spark_master, job_args=(), packages=SPARK_KAFKA_PACKAGE):
    job = job_path(br)
    package_args = ["--packages", packages] if packages else []
    return [
        "docker",
        "exec",
        "-i",
        *passthrough_env(),
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
        "--conf",
        "spark.ui.prometheus.enabled=true",
        "--conf",
        "spark.sql.streaming.metricsEnabled=true",
        "--conf",
        f"spark.metrics.namespace=br{br}",
        "--conf",
        "spark.metrics.conf.*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet",
        "--conf",
        "spark.metrics.conf.*.sink.prometheusServlet.path=/metrics/prometheus",
        "--conf",
        "spark.metrics.conf.master.sink.prometheusServlet.path=/metrics/master/prometheus",
        "--conf",
        "spark.metrics.conf.applications.sink.prometheusServlet.path=/metrics/applications/prometheus",
        *package_args,
        "--py-files",
        py_files_for(job),
        "/opt/" + job.relative_to(REPO_ROOT).as_posix(),
        *job_args,
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
        *passthrough_env(),
        "spark-local",
        "bash",
        "-c",
        (
            "PYTHONPATH=/opt/jobs/common "
            f"python3 /opt/ingestion/producers/produce_br{br}.py "
            f"--interval {interval} {limit_arg}"
        ),
    ]


def wait_for_spark_ready(name, proc, log_file, timeout=int(os.environ.get("SPARK_READY_TIMEOUT", "180"))):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            report_exited([(name, proc, log_file)], "Spark job")
            return False
        if log_file.exists() and "ResolveWriteToStream" in log_file.read_text(errors="replace"):
            print(f"{name} Spark job is ready.")
            return True
        time.sleep(1)
    print(f"Timed out waiting for {name} Spark job to initialize. Last log lines:")
    print(tail_file(log_file))
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("brs", nargs="*", help="BR numbers to run, e.g. 3 4, 3,4, or all")
    parser.add_argument("--events", type=int, help="Events to produce per BR; omit to send the full CSV once")
    parser.add_argument("--interval", type=float, default=0, help="Seconds between producer events")
    parser.add_argument("--loop-producers", action="store_true", help="Keep producers running forever")
    args = parser.parse_args()
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    print("=== 1. Bringing up infra and monitoring ===")
    build_image()
    start_stack()
    wait_healthy(["clickhouse", "kafka"])

    print("\n=== 2. Creating Kafka topics ===")
    create_topics()

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
        print("\nNo BR selected — infra is ready, stopping here.")
        return

    batch_brs = [n for n in selected if job_mode(n) == "batch"]
    stream_brs = [n for n in selected if n not in batch_brs]

    print(f"\n=== 3. Creating ClickHouse tables for {', '.join('BR' + n for n in selected)} ===")
    for n in selected:
        create_clickhouse_tables(n, truncate=n in stream_brs)
    spark_master = "spark://spark-master:7077"

    # Batch jobs run first so they are not starved of cores by streaming
    # jobs, which hold theirs until stopped.
    if batch_brs:
        print(f"\n=== 4a. Running batch Spark job(s) for {', '.join('BR' + n for n in batch_brs)} ===")
        for n in batch_brs:
            run_batch_job(n, spark_master)

    if not stream_brs:
        print("\nBatch job(s) finished; no streaming BR selected.")
        return

    print(f"\n=== 4b. Starting streaming Spark job(s) for {', '.join('BR' + n for n in stream_brs)} ===")
    spark_procs = []
    for n in stream_brs:
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

    print(f"\n=== 5. Starting producer(s) for {', '.join('BR' + n for n in stream_brs)} ===")
    producer_procs = []
    for n in stream_brs:
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
