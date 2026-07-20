"""2024 Day 14 -- Restroom Redoubt.

Positions after 100 seconds are closed-form modular arithmetic, so the whole
input collapses to one projection plus a conditional aggregate.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

WIDTH = 101
HEIGHT = 103
SECONDS = 100


def parse(spark: SparkSession, data: str) -> DataFrame:
    lines = [(line,) for line in data.strip().splitlines()]
    nums = F.transform(
        F.regexp_extract_all(F.col("line"), F.lit(r"-?\d+"), 0), lambda x: x.cast("int")
    )
    return spark.createDataFrame(lines, "line STRING").select(
        nums.getItem(0).alias("px"),
        nums.getItem(1).alias("py"),
        nums.getItem(2).alias("vx"),
        nums.getItem(3).alias("vy"),
    )


def part1(spark: SparkSession, data: str, width: int = WIDTH, height: int = HEIGHT) -> int:
    robots = parse(spark, data)
    # Spark's % keeps the sign of the dividend, so wrap into range explicitly.
    moved = robots.select(
        (((F.col("px") + SECONDS * F.col("vx")) % width + width) % width).alias("x"),
        (((F.col("py") + SECONDS * F.col("vy")) % height + height) % height).alias("y"),
    )
    mid_x, mid_y = width // 2, height // 2
    left, right = F.col("x") < mid_x, F.col("x") > mid_x
    top, bottom = F.col("y") < mid_y, F.col("y") > mid_y

    quadrants = moved.agg(
        *[
            F.sum((horizontal & vertical).cast("long")).alias(name)
            for name, horizontal, vertical in [
                ("tl", left, top),
                ("tr", right, top),
                ("bl", left, bottom),
                ("br", right, bottom),
            ]
        ]
    ).collect()[0]

    safety = 1
    for name in ("tl", "tr", "bl", "br"):
        safety *= int(quadrants[name] or 0)
    return safety
