"""Spark Connect session helper.

The cluster runs Spark Connect (gRPC :15002) in the `lakehouse` namespace. Two
ways to reach it:

  * from JupyterLab in-cluster -- `SPARK_REMOTE` is already set on the pod to
    `sc://spark-connect.lakehouse.svc.cluster.local:15002`, so `get_spark()`
    picks it up with no arguments;
  * from a laptop / marval -- run `scripts/port-forward.sh` and the default
    `sc://localhost:15002` applies.

Spark Connect is a *thin client*: no JVM starts locally, so importing pyspark
here costs nothing. All the work happens on the cluster executors.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

DEFAULT_REMOTE = "sc://localhost:15002"


def get_spark(app_name: str = "aoc-pyspark") -> SparkSession:
    """Return a Spark Connect session, reusing one if it already exists.

    Honours SPARK_REMOTE when set (the in-cluster JupyterLab case) and falls
    back to a local port-forward otherwise.
    """
    remote = os.environ.get("SPARK_REMOTE", DEFAULT_REMOTE)
    return SparkSession.builder.remote(remote).appName(app_name).getOrCreate()
