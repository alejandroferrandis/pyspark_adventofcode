"""2024 Day 7 -- Bridge Repair.

`aggregate` folds each equation left-to-right, carrying the set of reachable
partial results as an array column. No UDFs, no collect.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def parse(spark: SparkSession, data: str) -> DataFrame:
    """Returns [target BIGINT, nums ARRAY<BIGINT>]."""
    lines = spark.createDataFrame(
        [(line,) for line in data.strip().splitlines()], "line STRING"
    )
    halves = F.split(F.col("line"), ": ")
    return lines.select(
        halves.getItem(0).cast("bigint").alias("target"),
        F.transform(F.split(halves.getItem(1), " "), lambda x: x.cast("bigint")).alias("nums"),
    )


def _reachable(ops) -> "F.Column":
    """Fold nums[1:] over the running set of values each operator can produce."""
    # slice() is 1-based; every operand is >= 1, so a partial result above the
    # target can never come back down and is safe to prune.
    return F.aggregate(
        F.slice(F.col("nums"), 2, F.size(F.col("nums")) - 1),
        F.array(F.col("nums").getItem(0)),
        lambda acc, x: F.array_distinct(
            F.filter(
                F.flatten(F.transform(acc, lambda v: F.array(*[op(v, x) for op in ops]))),
                lambda v: v <= F.col("target"),
            )
        ),
    )


def part1(spark: SparkSession, data: str) -> int:
    equations = parse(spark, data)
    solvable = equations.filter(
        F.array_contains(_reachable([lambda v, x: v + x, lambda v, x: v * x]), F.col("target"))
    )
    return int(solvable.agg(F.sum("target").alias("total")).collect()[0]["total"])
