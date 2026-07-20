"""2024 Day 13 -- Claw Contraption.

Each machine is an independent 2x2 integer system, so Cramer's rule solves the
whole input as one vectorised expression.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

MAX_PRESSES = 100
A_COST = 3
B_COST = 1


def parse(spark: SparkSession, data: str) -> DataFrame:
    blocks = [(block,) for block in data.strip().split("\n\n")]
    nums = F.transform(
        F.regexp_extract_all(F.col("block"), F.lit(r"\d+"), 0), lambda x: x.cast("long")
    )
    return spark.createDataFrame(blocks, "block STRING").select(
        nums.getItem(0).alias("ax"),
        nums.getItem(1).alias("ay"),
        nums.getItem(2).alias("bx"),
        nums.getItem(3).alias("by"),
        nums.getItem(4).alias("px"),
        nums.getItem(5).alias("py"),
    )


def part1(spark: SparkSession, data: str) -> int:
    machines = parse(spark, data)
    solved = machines.select(
        (F.col("ax") * F.col("by") - F.col("ay") * F.col("bx")).alias("det"),
        (F.col("px") * F.col("by") - F.col("py") * F.col("bx")).alias("a_num"),
        (F.col("ax") * F.col("py") - F.col("ay") * F.col("px")).alias("b_num"),
    )
    # det == 0 means the two buttons are parallel; AoC inputs contain none, and
    # such a machine would have no unique solution to price.
    integral = solved.filter(
        (F.col("det") != 0)
        & (F.col("a_num") % F.col("det") == 0)
        & (F.col("b_num") % F.col("det") == 0)
    ).select(
        (F.col("a_num") / F.col("det")).cast("long").alias("a"),
        (F.col("b_num") / F.col("det")).cast("long").alias("b"),
    )
    winnable = integral.filter(
        F.col("a").between(0, MAX_PRESSES) & F.col("b").between(0, MAX_PRESSES)
    )
    total = winnable.agg(
        F.sum(A_COST * F.col("a") + B_COST * F.col("b")).alias("total")
    ).collect()[0]["total"]
    return int(total or 0)
