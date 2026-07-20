#!/usr/bin/env bash
# Expose the cluster's Spark Connect endpoint on localhost:15002.
#
# Only needed when running from outside the cluster (marval, a laptop).
# JupyterLab pods already have SPARK_REMOTE pointing at the in-cluster service.
set -euo pipefail

NS="${SPARK_NAMESPACE:-lakehouse}"
SVC="${SPARK_SERVICE:-spark-connect}"
PORT="${SPARK_PORT:-15002}"

if timeout 1 bash -c "</dev/tcp/localhost/$PORT" 2>/dev/null; then
  echo "localhost:$PORT already open — nothing to do"
  exit 0
fi

echo "forwarding $NS/$SVC :$PORT (ctrl-c to stop)"
exec kubectl -n "$NS" port-forward "svc/$SVC" "$PORT:$PORT"
