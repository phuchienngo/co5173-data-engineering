#!/usr/bin/env bash
# Start the Docker stack and create the required Kafka topics.
# ════════════════════════════════════════════════════════════════════════
#
#      ./scripts/up.sh
#
# ════════════════════════════════════════════════════════════════════════
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

REBUILD=0
TIMEOUT=300
while [[ $# -gt 0 ]]; do
    case "$1" in
        --rebuild) REBUILD=1; shift ;;
        --timeout) TIMEOUT="$2"; shift 2 ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done

RED=$'\e[31m'; GRN=$'\e[32m'; YLW=$'\e[33m'; CYN=$'\e[36m'; DIM=$'\e[2m'; RST=$'\e[0m'

echo
echo "${CYN}════════════════════════════════════════════════════════════${RST}"
echo "${CYN}  Start the stack Data Pipeline · BTS + Inside Airbnb${RST}"
echo "${CYN}════════════════════════════════════════════════════════════${RST}"

#
#
if [[ "$PWD" == /mnt/* ]]; then
    echo
    echo "${YLW}  Note: this project is running on a Windows drive — $PWD${RST}"
    echo "${DIM}  Spark reads and writes large files more slowly than on a Linux drive: 12 BTS months and six cities${RST}"
    echo "${DIM}  The full Airbnb batch pipeline takes about 15–30 minutes.${RST}"
fi

# ── 1. Docker ────────────────────────────────────────────────────────
echo
echo "${YLW}[1/6] Check Docker${RST}"
if ! docker info >/dev/null 2>&1; then
    echo "${RED}  Cannot connect to the Docker daemon.${RST}"
    echo
    echo "  If using Docker Desktop on Windows:"
    echo "    Settings → Resources → WSL Integration → enable this distribution"
    echo "  If Docker Engine is installed in WSL:"
    echo "    sudo service docker start"
    exit 1
fi
echo "${GRN}  OK · $(docker version --format '{{.Server.Version}}')${RST}"

echo
echo "${YLW}[2/6] Check memory${RST}"
MEM_BYTES=$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)
MEM_GB=$(awk "BEGIN {printf \"%.1f\", $MEM_BYTES/1073741824}")
echo "  Docker has ${MEM_GB} GB"
if (( $(awk "BEGIN {print ($MEM_GB < 7)}") )); then
    echo "${YLW}  ⚠️ Below 8 GB. The stack needs about 8 GB for comfortable operation.${RST}"
    echo "${YLW}     WSL2 memory is configured in  C:\\Users\\<your-name>\\.wslconfig  —${RST}"
    echo "${YLW}     see the 'WSL2 memory' in docs/02-runbook.md.${RST}"
    echo "${YLW}     The stack can still run with 12 months.${RST}"
fi

echo
echo "${YLW}[3/6] Check ports${RST}"
BUSY=""
for p in 3000 4040 7077 8080 8082 8083 8123 8501 9000 9090 9092 9363 29092; do
    if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
        exec 3>&- 2>/dev/null || true
        BUSY="$BUSY $p"
    fi
done
if [[ -n "$BUSY" ]]; then
    echo "${YLW}  ⚠️ Ports in use:$BUSY${RST}"
    echo "${YLW}     Ignore these if they belong to a previous run of this stack.${RST}"
    echo "${YLW}     If another program uses them, see Appendix A in the runbook.${RST}"
else
    echo "${GRN}  All ports are available${RST}"
fi

# ── 4. Build ─────────────────────────────────────────────────────────
echo
if [[ $REBUILD -eq 1 ]]; then
    echo "${YLW}[4/6] Rebuild images from scratch (several minutes)${RST}"
    docker compose build --no-cache
else
    echo "${YLW}[4/6] Build missing images${RST}"
    docker compose build
fi

echo
echo "${YLW}[5/6] Start services${RST}"
docker compose up -d

echo
echo "${YLW}      Waiting for ClickHouse and Kafka${RST}"
DEADLINE=$(( $(date +%s) + TIMEOUT ))
READY=0
while [[ $(date +%s) -lt $DEADLINE ]]; do
    CH=$(docker inspect --format '{{.State.Health.Status}}' dp-clickhouse 2>/dev/null || echo "?")
    KF=$(docker inspect --format '{{.State.Health.Status}}' dp-kafka 2>/dev/null || echo "?")
    if [[ "$CH" == "healthy" && "$KF" == "healthy" ]]; then READY=1; break; fi
    echo "${DIM}      clickhouse=$CH  kafka=$KF ...${RST}"
    sleep 5
done

if [[ $READY -eq 0 ]]; then
    echo "${RED}  Timed out. Check logs:${RST}"
    echo "    docker compose logs clickhouse --tail 50"
    echo "    docker compose logs kafka --tail 50"
    exit 1
fi
echo "${GRN}      Both services are healthy${RST}"

# ── 6. Topic Kafka ───────────────────────────────────────────────────
echo
echo "${YLW}[6/6] Create Kafka topics${RST}"
create_topic() {
    docker compose exec -T kafka kafka-topics \
        --bootstrap-server kafka:29092 \
        --create --if-not-exists \
        --topic "$1" --partitions "$2" --replication-factor 1 \
        --config "retention.ms=$3" >/dev/null 2>&1
    echo "${GRN}  topic '$1' · $2 partition${RST}"
}
create_topic flight_events      3 604800000     # retain 7 days; key = airport code
create_topic flight_events_dlq  1 2592000000    # retain 30 days

echo
echo "${CYN}════════════════════════════════════════════════════════════${RST}"
docker compose ps --format "table {{.Service}}\t{{.Status}}"

echo
echo "${CYN}  Interfaces — open in a Windows browser:${RST}"
echo "    Grafana      http://localhost:3000    admin / admin"
echo "    WebApp       http://localhost:8501"
echo "    Kafka UI     http://localhost:8080"
echo "    Spark Master http://localhost:8082"
echo "    Prometheus   http://localhost:9090"
echo "    ClickHouse   http://localhost:8123    default / clickhouse"
echo "${DIM}    (WSL2 forwards these ports to Windows automatically)${RST}"

echo
echo "${YLW}  Next step:${RST}"
echo "    ./scripts/init_clickhouse.sh"
echo "${CYN}════════════════════════════════════════════════════════════${RST}"
