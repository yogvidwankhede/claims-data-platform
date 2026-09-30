#!/usr/bin/env bash
# Local end-to-end run: generate N days of vendor deliveries, then run each day
# through the whole platform exactly as the Airflow DAG does, in date order.
#
#   scripts/e2e.sh 2026-01-01 5
set -euo pipefail
START=${1:-2026-01-01}
DAYS=${2:-5}
BIN=${CLAIMS_PLATFORM_BIN:-claims-platform}
DBT=${CLAIMS_DBT_BIN:-dbt}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
export CLAIMS_DATA_DIR=${CLAIMS_DATA_DIR:-$ROOT/data}

"$BIN" generate --start "$START" --days "$DAYS" --drift-on-day $(( DAYS > 2 ? DAYS - 1 : 1 ))
for i in $(seq 0 $(( DAYS - 1 ))); do
  day=$(python3 -c "import datetime as d; print(d.date.fromisoformat('$START') + d.timedelta(days=$i))")
  echo "== $day"
  "$BIN" run-day --date "$day" > "$CLAIMS_DATA_DIR/run-$day.log"
  (cd "$ROOT/dbt" && "$DBT" build --profiles-dir . --quiet)
  "$BIN" dq-report --date "$day" > /dev/null || echo "   data quality DEGRADED for $day (see reports/dq/$day.json)"
done
(cd "$ROOT/dbt" && "$DBT" source freshness --profiles-dir . --quiet)
echo "done: $CLAIMS_DATA_DIR"
