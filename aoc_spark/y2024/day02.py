"""2024 Day 2 -- Red-Nosed Reports.

Safety checks done with higher-order array functions rather than UDFs, so the
whole thing stays a native expression tree.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F


def parse(spark: SparkSession, data: str) -> DataFrame:
    lines = spark.createDataFrame([(line,) for line in data.strip().splitlines()], "line STRING")
    return lines.select(
        F.transform(F.split(F.trim(F.col("line")), r"\s+"), lambda x: x.cast("int")).alias("levels")
    )


def _is_safe(levels: Column) -> Column:
    """Monotonic, with every adjacent step in 1..3."""
    size = F.size(levels)
    head = F.slice(levels, F.lit(1), size - 1)
    tail = F.slice(levels, F.lit(2), size - 1)
    diffs = F.zip_with(head, tail, lambda a, b: b - a)
    increasing = F.forall(diffs, lambda d: (d >= 1) & (d <= 3))
    decreasing = F.forall(diffs, lambda d: (d <= -1) & (d >= -3))
    return increasing | decreasing


def _dampened_variants(levels: Column) -> Column:
    """Every version of `levels` with one element removed."""
    size = F.size(levels)
    indices = F.sequence(F.lit(0), size - 1)
    # slice is 1-based, hence i + 2 to skip element i.
    return F.transform(
        indices,
        lambda i: F.concat(
            F.slice(levels, F.lit(1), i),
            F.slice(levels, i + 2, size - i - 1),
        ),
    )


def part1(spark: SparkSession, data: str) -> int:
    df = parse(spark, data)
    safe = df.select(_is_safe(F.col("levels")).alias("safe"))
    return int(safe.agg(F.sum(F.col("safe").cast("int")).alias("n")).collect()[0]["n"])


def part2(spark: SparkSession, data: str) -> int:
    df = parse(spark, data)
    levels = F.col("levels")
    rescued = F.exists(_dampened_variants(levels), _is_safe)
    # OR, not replace: an already-safe report stays safe.
    safe = df.select((_is_safe(levels) | rescued).alias("safe"))
    return int(safe.agg(F.sum(F.col("safe").cast("int")).alias("n")).collect()[0]["n"])
