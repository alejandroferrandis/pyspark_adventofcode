"""2024 Day 1 -- Historian Hysteria.  *Spark lesson: window ranking + join.*

Two columns of location IDs. Part 1 pairs them up smallest-with-smallest and
sums the absolute differences. Part 2 scores each left value by how often it
appears on the right.

This is the rare AoC day that is *naturally* relational -- "sort both sides and
pair by rank" is a window function, and "how often does it appear" is a
groupBy + join. No row-at-a-time thinking required.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def parse(spark: SparkSession, data: str) -> DataFrame:
    """One row per line -> two integer columns. Splitting happens in Spark."""
    lines = spark.createDataFrame([(line,) for line in data.strip().splitlines()], "line STRING")
    parts = F.split(F.trim(F.col("line")), r"\s+")
    return lines.select(
        parts.getItem(0).cast("long").alias("left_id"),
        parts.getItem(1).cast("long").alias("right_id"),
    )


def part1(spark: SparkSession, data: str) -> int:
    """Rank each column independently, join on rank, sum |left - right|.

    Note the unpartitioned Window: Spark warns that this pulls all rows into a
    single partition. For 1000 rows that is fine and it is the honest way to
    express a global sort -- a partitioned window would rank within groups,
    which is not what the puzzle asks.
    """
    df = parse(spark, data)
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
    """Similarity score: each left value times its occurrence count on the right.

    A left join keeps left values that never appear on the right; coalesce
    turns their missing count into 0 so they contribute nothing.
    """
    df = parse(spark, data)
    counts = df.groupBy("right_id").agg(F.count("*").alias("occurrences"))
    scored = df.join(counts, df["left_id"] == counts["right_id"], "left").select(
        (F.col("left_id") * F.coalesce(F.col("occurrences"), F.lit(0))).alias("score")
    )
    return int(scored.agg(F.sum("score").alias("total")).collect()[0]["total"])
