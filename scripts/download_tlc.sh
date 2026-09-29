#!/usr/bin/env bash
# Download NYC TLC monthly trip parquet files and taxi zone lookup to ./data/raw
# Usage: scripts/download_tlc.sh <service> <YYYY-MM from> <YYYY-MM to>
# Example: scripts/download_tlc.sh yellow 2025-07 2026-07

set -euo pipefail

cd "$(dirname "$0")/.."

SERVICE="${1:?Usage: $0 <service> <YYYY-MM from> <YYYY-MM to>}"
FROM="${2:?Usage: $0 <service> <YYYY-MM from> <YYYY-MM to>}"
TO="${3:?Usage: $0 <service> <YYYY-MM from> <YYYY-MM to>}"

BASE_URL="https://d37ci6vzurychx.cloudfront.net"
DATA_DIR="data/raw"
SERVICE_DIR="$DATA_DIR/$SERVICE"

mkdir -p "$SERVICE_DIR"

# Download taxi zone lookup if missing
if [ ! -f "$DATA_DIR/taxi_zone_lookup.csv" ]; then
	echo "Downloading taxi zone lookup..."
	curl -fL --retry 3 -o "$DATA_DIR/taxi_zone_lookup.csv" "$BASE_URL/misc/taxi_zone_lookup.csv"
fi

# Parse from/to into YYYYMM integers for comparison
FROM_YEAR=${FROM%-*}
FROM_MONTH=${FROM#*-}
TO_YEAR=${TO%-*}
TO_MONTH=${TO#*-}

# Force base 10 to handle leading zeros
FROM_YEAR=$((10#$FROM_YEAR))
FROM_MONTH=$((10#$FROM_MONTH))
TO_YEAR=$((10#$TO_YEAR))
TO_MONTH=$((10#$TO_MONTH))

YEAR=$FROM_YEAR
MONTH=$FROM_MONTH

while true; do
	# Zero-pad month for filename
	PADDED_MONTH=$(printf "%02d" "$MONTH")

	FILE="$SERVICE_DIR/${SERVICE}_tripdata_${YEAR}-${PADDED_MONTH}.parquet"
	URL="$BASE_URL/trip-data/${SERVICE}_tripdata_${YEAR}-${PADDED_MONTH}.parquet"

	if [ -s "$FILE" ]; then
		echo "skip $FILE"
	else
		echo "Downloading $FILE..."
		curl -fL --retry 3 -o "$FILE.part" "$URL"
		mv "$FILE.part" "$FILE"
	fi

	# Stop if we've reached the end month
	if [ "$YEAR" -eq "$TO_YEAR" ] && [ "$MONTH" -eq "$TO_MONTH" ]; then
		break
	fi

	# Advance to next month
	MONTH=$((MONTH + 1))
	if [ "$MONTH" -gt 12 ]; then
		MONTH=1
		YEAR=$((YEAR + 1))
	fi
done

# Count files in service directory
COUNT=$(ls -1 "$SERVICE_DIR"/*.parquet 2>/dev/null | wc -l)
echo "Downloaded $COUNT parquet files to $SERVICE_DIR"
