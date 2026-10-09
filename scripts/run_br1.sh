#!/usr/bin/env bash
# Download BR1 data, build the baseline, replay trips to Kafka, then stream pricing.
# Usage: bash scripts/run_br1.sh [service] [baseline-from] [baseline-to] [replay-month]
set -euo pipefail
cd "$(dirname "$0")/.."

SERVICE="${1:-yellow}"
BASELINE_FROM="${2:-2025-07}"
BASELINE_TO="${3:-2026-06}"
REPLAY_MONTH="${4:-2026-07}"

if [[ ! "$BASELINE_TO" =~ ^[0-9]{4}-(0[1-9]|1[0-2])$ ]] ||
   [[ "$BASELINE_TO" < "$BASELINE_FROM" ]] ||
   [[ ! "$BASELINE_TO" < "$REPLAY_MONTH" ]]; then
	echo "Use YYYY-MM months with baseline-from <= baseline-to < replay-month" >&2
	exit 1
fi

bash scripts/download_tlc.sh "$SERVICE" "$BASELINE_FROM" "$REPLAY_MONTH"
docker compose --profile full --profile monitoring --profile br1 up -d --build --wait \
	clickhouse kafka kafka-ui spark-master spark-worker pyspark prometheus grafana
docker exec -i clickhouse bash -c \
	'exec clickhouse-client --password "$CLICKHOUSE_PASSWORD" --multiquery' < sql/pricing.sql
docker exec pyspark bash /opt/jobs/submit.sh /opt/jobs/batch_fact_capacity.py \
	--service "$SERVICE" --baseline-from "$BASELINE_FROM" --baseline-to "$BASELINE_TO"
docker exec pyspark bash /opt/jobs/submit.sh /opt/jobs/publish_trips.py \
	--service "$SERVICE" --month "$REPLAY_MONTH"
echo "Streaming BR1 demand ratios. To stop the BR1 driver: docker compose restart pyspark"
docker exec -i pyspark bash /opt/jobs/submit.sh /opt/jobs/stream_demand_ratio.py
