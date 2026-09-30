#!/usr/bin/env bash
set -euo pipefail

ROLE="${1:-master}"

case "$ROLE" in
  master)
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.master.Master \
      --host spark-master --port 7077 --webui-port 8080
    ;;
  worker)
    exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.worker.Worker \
      "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
      --cores "${SPARK_WORKER_CORES:-2}" \
      --memory "${SPARK_WORKER_MEMORY:-2g}" \
      --webui-port 8081
    ;;
  *)
    echo "Invalid role: $ROLE (expected 'master' or 'worker')" >&2
    exit 1
    ;;
esac
