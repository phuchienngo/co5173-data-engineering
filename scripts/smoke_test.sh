#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════════════
#
#      ./scripts/smoke_test.sh
#
# ════════════════════════════════════════════════════════════════════════
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

RED=$'\e[31m'; GRN=$'\e[32m'; YLW=$'\e[33m'; CYN=$'\e[36m'; RST=$'\e[0m'

echo
echo "${CYN}════════════════════════════════════════════════════════════${RST}"
echo "${CYN}  Data quality checks · Data Pipeline${RST}"
echo "${CYN}════════════════════════════════════════════════════════════${RST}"

OUT=$(docker compose exec -T clickhouse clickhouse-client --password clickhouse --multiquery \
        < sql/clickhouse/90_quality_checks.sql 2>&1)
CODE=$?
echo "$OUT"

if [[ $CODE -ne 0 ]]; then
    echo "${RED}  ✗ Quality query failed (exit code $CODE). Usually caused by not running init_clickhouse.sh.${RST}"
    exit 1
fi

FAILS=$(grep -c "FAIL" <<< "$OUT" || true)
echo
if [[ "$FAILS" -gt 0 ]]; then
    echo "${RED}  ✗ $FAILS checks failed.${RST}"
    echo "${YLW}    See the 'expected', then see 'Troubleshooting' in docs/02-runbook.md.${RST}"
    exit 1
fi
echo "${GRN}  ✓ All checks passed.${RST}"
