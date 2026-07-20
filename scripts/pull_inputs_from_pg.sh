#!/usr/bin/env bash
# Populate the local gitignored input cache from the homelab Postgres.
#
# Uses `kubectl exec` so no database credentials ever land on this machine or
# in this repo. Base64 in transit keeps newlines byte-exact.
#
# Usage: scripts/pull_inputs_from_pg.sh [year] [first_day] [last_day]
set -euo pipefail

YEAR="${1:-2024}"
FIRST="${2:-1}"
LAST="${3:-5}"
NS="${AOC_PG_NAMESPACE:-airflow}"
DEPLOY="${AOC_PG_DEPLOY:-lab-postgres}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST="$ROOT/inputs/$YEAR"
mkdir -p "$DEST"

for day in $(seq "$FIRST" "$LAST"); do
  out="$DEST/$(printf 'day%02d.txt' "$day")"
  if [[ -s "$out" ]]; then
    echo "day $day: cached, skipping"
    continue
  fi
  kubectl -n "$NS" exec "deploy/$DEPLOY" -- bash -c \
    "psql -U \"\$POSTGRES_USER\" -d adventofcode -tAc \
     \"SELECT encode(convert_to(input_text,'UTF8'),'base64') FROM y$YEAR.input_data WHERE day=$day\"" \
    | tr -d '\n' | base64 -d > "$out"
  echo "day $day: $(wc -c < "$out") bytes -> $out"
done
