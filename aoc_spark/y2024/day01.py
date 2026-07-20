"""2024 Day 1 -- Historian Hysteria.

Part 1 pairs the two lists by rank; part 2 scores each left value by its
frequency on the right.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def parse(spark: SparkSession, data: str) -> DataFrame:
    lines = spark.createDataFrame([(line,) for line in data.strip().splitlines()], "line STRING")
    parts = F.split(F.trim(F.col("line")), r"\s+")
    return lines.select(
        parts.getItem(0).cast("long").alias("left_id"),
        parts.getItem(1).cast("long").alias("right_id"),
    )


def part1(spark: SparkSession, data: str) -> int:
    df = parse(spark, data)
    # Unpartitioned on purpose: the puzzle needs a global ordering, and a
    # partitioned window would rank within groups instead.
    left = df.select(
        F.row_number().over(Window.orderBy("left_id")).alias("rank"),
        "left_id",
    )
    right = df.select(
        F.row_number().over(Window.orderBy("right_id")).alias("rank"),
        "right_id",
    )
    paired = left.join(right, on="rank")
    total = paired.select(F.sum(F.abs(F.col("left_id") - F.col("right_id"))).alias("total"))
    return int(total.collect()[0]["total"])


def part2(spark: SparkSession, data: str) -> int:
    df = parse(spark, data)
    counts = df.groupBy("right_id").agg(F.count("*").alias("occurrences"))
    # left join + coalesce so values absent from the right score 0 rather than drop.
    scored = df.join(counts, df["left_id"] == counts["right_id"], "left").select(
        (F.col("left_id") * F.coalesce(F.col("occurrences"), F.lit(0))).alias("score")
    )
    return int(scored.agg(F.sum("score").alias("total")).collect()[0]["total"])
