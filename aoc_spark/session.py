"""Spark Connect session helper."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

DEFAULT_REMOTE = "sc://localhost:15002"


def get_spark(app_name: str = "aoc-pyspark") -> SparkSession:
    """Reuse or open a Spark Connect session.

    Honours SPARK_REMOTE (set on the JupyterLab pods) and otherwise assumes a
    local port-forward.
    """
    remote = os.environ.get("SPARK_REMOTE", DEFAULT_REMOTE)
    return SparkSession.builder.remote(remote).appName(app_name).getOrCreate()
