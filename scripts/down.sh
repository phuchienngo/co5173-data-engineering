#!/usr/bin/env bash
# Stop the stack; --purge also deletes Docker volumes.
# ════════════════════════════════════════════════════════════════════════
#
#
# ════════════════════════════════════════════════════════════════════════
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

RED=$'\e[31m'; GRN=$'\e[32m'; DIM=$'\e[2m'; RST=$'\e[0m'

PURGE=0
[[ "${1:-}" == "--purge" ]] && PURGE=1

if [[ $PURGE -eq 1 ]]; then
    echo
    echo "${RED}  ⚠️  WARNING — this permanently deletes:${RST}"
    echo "      · all ClickHouse tables and data"
    echo "      · all Kafka messages"
    echo "      · dashboards edited manually in Grafana"
    echo
    echo "${DIM}      NOT deleted: the local ./data directory${RST}"
    echo "${DIM}      (downloaded files and Parquet remain outside Docker volumes)${RST}"
    echo
    read -r -p "  Type 'delete' to confirm: " ans
    if [[ "$ans" != "delete" ]]; then
        echo "${DIM}  Cancelled.${RST}"
        exit 0
    fi
    docker compose down -v --remove-orphans
    echo
    echo "${GRN}  Containers and volumes deleted.${RST}"
    echo "  Recreate:  ./scripts/up.sh  then  ./scripts/init_clickhouse.sh"
else
    docker compose down --remove-orphans
    echo
    echo "${GRN}  Stopped. Data remains in the volumes.${RST}"
    echo "  Start again:  ./scripts/up.sh"
fi
