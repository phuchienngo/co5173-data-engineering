#!/usr/bin/env bash
# Create ClickHouse tables and views in dependency order.
# ════════════════════════════════════════════════════════════════════════
#
#      ./scripts/init_clickhouse.sh
#
#
# ════════════════════════════════════════════════════════════════════════
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

RED=$'\e[31m'; GRN=$'\e[32m'; YLW=$'\e[33m'; CYN=$'\e[36m'; DIM=$'\e[2m'; RST=$'\e[0m'

FILES=(
    00_database.sql        # database + pipeline_log
    10_dim.sql             # dim_destination, dim_airport, dim_neighbourhood
    11_kafka.sql           # Kafka Engine + raw_flight_events + MV
    12_br9_flow.sql
    13_br9_occupancy.sql   # fact_occupancy_night
    20_br10_core.sql       # fact_stay + dim_guest
    21_br10_marts.sql      # 5 mart BR10
    14_br9_serving.sql
)

DROP_FIRST=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --only)       FILES=("$2"); shift 2 ;;
        --drop-first) DROP_FIRST=1; shift ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done

ch() { docker compose exec -T clickhouse clickhouse-client --password clickhouse "$@"; }

echo
echo "${CYN}════════════════════════════════════════════════════════════${RST}"
echo "${CYN}  Initialize ClickHouse schema · Data Pipeline${RST}"
echo "${CYN}════════════════════════════════════════════════════════════${RST}"

HEALTH=$(docker inspect --format '{{.State.Health.Status}}' dp-clickhouse 2>/dev/null || echo "not running")
if [[ "$HEALTH" != "healthy" ]]; then
    echo
    echo "${RED}  ClickHouse is not ready (status: $HEALTH)${RST}"
    echo "  Run first:  ./scripts/up.sh"
    exit 1
fi

if [[ $DROP_FIRST -eq 1 ]]; then
    echo
    echo "${YLW}  Drop database 'tourism' ...${RST}"
    ch --query "DROP DATABASE IF EXISTS tourism SYNC" >/dev/null
fi

FAILED=0
for f in "${FILES[@]}"; do
    path="sql/clickhouse/$f"
    if [[ ! -f "$path" ]]; then
        echo "${RED}  ✗ file not found $f${RST}"
        FAILED=$((FAILED + 1))
        continue
    fi
    printf "  → %-24s " "$f"
    if out=$(ch --multiquery < "$path" 2>&1); then
        echo "${GRN}OK${RST}"
        [[ -n "$out" ]] && echo "${DIM}$out${RST}"
    else
        echo "${RED}ERROR${RST}"
        echo "${DIM}$out${RST}"
        FAILED=$((FAILED + 1))
    fi
done

if [[ $FAILED -gt 0 ]]; then
    echo
    echo "${RED}  $FAILED file(s) failed. Fix the issue and rerun.${RST}"
    exit 1
fi

echo
echo "${CYN}  Objects in database 'tourism':${RST}"
ch --query "
SELECT
    multiIf(engine LIKE '%MergeTree',    'Table',
            engine = 'Kafka',            'Kafka Engine',
            engine = 'MaterializedView', 'Materialized View',
            engine = 'View',             'View', engine) AS \`Type\`,
    name AS \`Name\`
FROM system.tables
WHERE database = 'tourism'
ORDER BY \`Type\`, name
FORMAT PrettyCompactMonoBlock
"

echo
echo "${YLW}  Next step:${RST}  ./scripts/run_job.sh profile"
echo "${CYN}════════════════════════════════════════════════════════════${RST}"
