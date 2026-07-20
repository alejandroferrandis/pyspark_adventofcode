"""2024 Day 8 -- Resonant Collinearity.

A self-join on frequency turns every ordered antenna pair into one antinode
row; ordering both ways covers both sides of the pair.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def antennas(spark: SparkSession, data: str) -> DataFrame:
    lines = [(r, line) for r, line in enumerate(data.strip().splitlines())]
    rows = spark.createDataFrame(lines, "r INT, line STRING")
    return (
        rows.select("r", F.posexplode(F.split(F.col("line"), "")).alias("c", "freq"))
        # split(s, "") emits a trailing empty string.
        .filter((F.col("freq") != "") & (F.col("freq") != "."))
    )


def part1(spark: SparkSession, data: str) -> int:
    grid = data.strip().splitlines()
    height, width = len(grid), len(grid[0])

    a = antennas(spark, data)
    b = a.select(
        F.col("r").alias("r2"), F.col("c").alias("c2"), F.col("freq").alias("freq2")
    )
    pairs = a.join(
        b,
        (F.col("freq") == F.col("freq2"))
        & ((F.col("r") != F.col("r2")) | (F.col("c") != F.col("c2"))),
    )

    antinodes = pairs.select(
        (2 * F.col("r2") - F.col("r")).alias("ar"),
        (2 * F.col("c2") - F.col("c")).alias("ac"),
    ).filter(
        F.col("ar").between(0, height - 1) & F.col("ac").between(0, width - 1)
    )
    return antinodes.distinct().count()
