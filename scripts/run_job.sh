#!/usr/bin/env bash
# Run a selected pipeline job inside the spark-client container.
# ════════════════════════════════════════════════════════════════════════
#
#      ./scripts/run_job.sh bts --months 12
#      ./scripts/run_job.sh br9_02
#      ./scripts/run_job.sh br9_03 --local
#      ./scripts/run_job.sh produce --month 2026-06 --speedup 120
#
#
# ════════════════════════════════════════════════════════════════════════
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

RED=$'\e[31m'; GRN=$'\e[32m'; YLW=$'\e[33m'; CYN=$'\e[36m'; DIM=$'\e[2m'; RST=$'\e[0m'

declare -A JOB_MODULE=(
    [airports]="ingest.download_airports"
    [bts]="ingest.download_bts"
    [airbnb]="ingest.download_airbnb"
    [profile]="profiling.profile_sources"
    # ── BR9 (Spark) ──
    [br9_01]="batch.br9_01_dim"
    [br9_02]="batch.br9_02_stage_flights"
    [br9_03]="batch.br9_03_flow_baseline"
    [br9_04]="batch.br9_04_occupancy"
    [stream]="streaming.br9_05_stream_arrivals"
    [produce]="producers.flight_event_producer"
    # ── BR10 (Spark) ──
    [br10_01]="batch.br10_01_stays"
    [br10_02]="batch.br10_02_dim_guest"
    [br10_03]="batch.br10_03_marts"
)

declare -A JOB_DESC=(
    [airports]="Download OpenFlights airport reference data (coordinates and time zones)"
    [bts]="Download monthly BTS flights"
    [airbnb]="Download Inside Airbnb data for six destinations"
    [profile]="Profile source data → data/reports/"
    [br9_01]="BR9 · destination and airport dimensions"
    [br9_02]="BR9 · BTS CSV → Parquet and flight events (UTC)"
    [br9_03]="BR9 · p95 baseline, historical 15-minute facts, and alerts"
    [br9_04]="BR9 · Airbnb occupancy by neighborhood and night"
    [stream]="BR9 · Structured Streaming (continuous)"
    [produce]="BR9 · replay flight events to Kafka"
    [br10_01]="BR10 · reviews → stays"
    [br10_02]="BR10 · stays → guest dimension"
    [br10_03]="BR10 · build reporting marts"
)

JOB_ORDER=(airports bts airbnb profile br9_01 br9_02 br9_03 br9_04 stream produce br10_01 br10_02 br10_03)

show_list() {
    echo
    echo "${CYN}  Available jobs (in execution order):${RST}"
    echo
    for k in "${JOB_ORDER[@]}"; do
        printf "    %-9s %s\n" "$k" "${JOB_DESC[$k]}"
    done
    echo
    echo "${YLW}  Examples:${RST}"
    echo "    ./scripts/run_job.sh bts --months 12"
    echo "    ./scripts/run_job.sh br9_03"
    echo "    ./scripts/run_job.sh produce --month 2026-06 --speedup 120"
    echo
}

JOB="${1:-}"
if [[ -z "$JOB" || "$JOB" == "-l" || "$JOB" == "--list" || "$JOB" == "-h" || "$JOB" == "--help" ]]; then
    show_list
    exit 0
fi
shift

if [[ -z "${JOB_MODULE[$JOB]:-}" ]]; then
    echo
    echo "${RED}  Unknown job '$JOB'. List jobs:  ./scripts/run_job.sh -l${RST}"
    echo
    exit 1
fi

if ! docker compose ps --status running --services 2>/dev/null | grep -qx spark-client; then
    echo
    echo "${RED}  The spark-client container is not running. Run:  ./scripts/up.sh${RST}"
    echo
    exit 1
fi

CMD=(python -m "${JOB_MODULE[$JOB]}" "$@")

echo
echo "${CYN}────────────────────────────────────────────────────────────${RST}"
echo "${CYN}  ${JOB_DESC[$JOB]}${RST}"
echo "${DIM}  ${CMD[*]}${RST}"
echo "${CYN}────────────────────────────────────────────────────────────${RST}"

START=$(date +%s)
if [[ "$JOB" == "stream" && -t 0 ]]; then
    docker compose exec spark-client "${CMD[@]}"
else
    docker compose exec -T spark-client "${CMD[@]}"
fi
CODE=$?
SECS=$(( $(date +%s) - START ))

echo
if [[ $CODE -eq 0 ]]; then
    echo "${GRN}  ✓ completed in ${SECS}s${RST}"
else
    echo "${RED}  ✗ failed (exit code $CODE) after ${SECS}s${RST}"
    echo
    echo "${YLW}  The Spark job traceback was logged:${RST}"
    cat <<'EOF'
    docker compose exec clickhouse clickhouse-client --password clickhouse -q \
      "SELECT job_name, step, message FROM tourism.pipeline_log
       WHERE status='FAILED' ORDER BY run_ts DESC LIMIT 1 FORMAT Vertical"
EOF
fi
exit $CODE
