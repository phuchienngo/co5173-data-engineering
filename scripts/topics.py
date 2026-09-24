import argparse
import ast
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PRODUCER_DIR = REPO_ROOT / "ingestion" / "producers"


def read_topic(path):
    tree = ast.parse(path.read_text())
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "TOPIC" for target in node.targets):
            continue
        if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            return node.value.value
    return None


def discover_topics():
    topics = []
    for path in sorted(PRODUCER_DIR.glob("produce_br*.py")):
        topic = read_topic(path)
        if topic:
            topics.append(topic)
    return sorted(set(topics))


def existing_topics():
    result = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            "kafka",
            "kafka-topics",
            "--bootstrap-server",
            "localhost:9092",
            "--list",
        ],
        check=True,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    return set(result.stdout.splitlines())


def delete_topic(topic):
    print(f"Deleting Kafka topic if it exists: {topic}")
    subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            "kafka",
            "kafka-topics",
            "--bootstrap-server",
            "localhost:9092",
            "--delete",
            "--topic",
            topic,
            "--if-exists",
        ],
        check=True,
        cwd=REPO_ROOT,
    )


def wait_deleted(topics, timeout=30):
    deadline = time.time() + timeout
    pending = set(topics)
    while pending and time.time() < deadline:
        pending = pending.intersection(existing_topics())
        if pending:
            time.sleep(1)
    if pending:
        sys.exit(f"Timed out deleting Kafka topics: {', '.join(sorted(pending))}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--list", action="store_true", help="Print discovered topics without creating them")
    parser.add_argument("--reset", action="store_true", help="Delete discovered topics before creating them")
    args = parser.parse_args()

    topics = discover_topics()
    if not topics:
        sys.exit("No TOPIC constants found in ingestion/producers/produce_br*.py")

    if args.list:
        print("\n".join(topics))
        return

    if args.reset:
        for topic in topics:
            delete_topic(topic)
        wait_deleted(topics)

    for topic in topics:
        print(f"Creating Kafka topic: {topic}")
        subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                "kafka",
                "kafka-topics",
                "--bootstrap-server",
                "localhost:9092",
                "--create",
                "--topic",
                topic,
                "--partitions",
                "1",
                "--replication-factor",
                "1",
                "--if-not-exists",
            ],
            check=True,
            cwd=REPO_ROOT,
        )


if __name__ == "__main__":
    main()
