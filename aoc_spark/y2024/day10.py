"""2024 Day 10 -- Hoof It.

Trails are walked as nine joins: a frontier of (trailhead, position) pairs is
stepped up one height at a time against the cells at the next height.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

STEPS = [(-1, 0), (1, 0), (0, -1), (0, 1)]


def cells(spark: SparkSession, data: str) -> DataFrame:
    lines = [(r, line) for r, line in enumerate(data.strip().splitlines())]
    rows = spark.createDataFrame(lines, "r INT, line STRING")
    return (
        rows.select("r", F.posexplode(F.split(F.col("line"), "")).alias("c", "ch"))
        .select("r", "c", F.col("ch").try_cast("int").alias("h"))
        # try_cast: split's trailing "" and the examples' impassable "." are not
        # digits, and a plain cast raises under ANSI mode instead of nulling.
        .filter(F.col("h").isNotNull())
    )


def part1(spark: SparkSession, data: str) -> int:
    grid = cells(spark, data).cache()
    steps = spark.createDataFrame(STEPS, "dr INT, dc INT")

    frontier = grid.filter(F.col("h") == 0).select(
        F.col("r").alias("sr"), F.col("c").alias("sc"), "r", "c"
    )

    for height in range(1, 10):
        nxt = grid.filter(F.col("h") == height).select(
            F.col("r").alias("nr"), F.col("c").alias("nc")
        )
        frontier = (
            frontier.crossJoin(steps)
            .join(
                nxt,
                (F.col("nr") == F.col("r") + F.col("dr"))
                & (F.col("nc") == F.col("c") + F.col("dc")),
            )
            .select("sr", "sc", F.col("nr").alias("r"), F.col("nc").alias("c"))
            # A trailhead scores a 9 once however many ways it reaches it.
            .distinct()
        )

    score = frontier.count()
    grid.unpersist()
    return score
