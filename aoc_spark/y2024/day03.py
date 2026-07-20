"""2024 Day 3 -- Mull It Over.

Part 2's do()/don't() state is a last-non-null-value window over the token
positions rather than a sequential scan.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

MUL = r"mul\(\d{1,3},\d{1,3}\)"
TOKEN = r"mul\(\d{1,3},\d{1,3}\)|do\(\)|don't\(\)"
OPERANDS = r"mul\((\d{1,3}),(\d{1,3})\)"


def _memory(spark: SparkSession, data: str) -> DataFrame:
    # One row, not one per line: instructions run across line breaks.
    return spark.createDataFrame([(data,)], "memory STRING")


def _product(token_col: str) -> "F.Column":
    left = F.regexp_extract(F.col(token_col), OPERANDS, 1).cast("long")
    right = F.regexp_extract(F.col(token_col), OPERANDS, 2).cast("long")
    return left * right


def part1(spark: SparkSession, data: str) -> int:
    tokens = _memory(spark, data).select(
        F.explode(F.regexp_extract_all(F.col("memory"), F.lit(MUL), F.lit(0))).alias("token")
    )
    return int(tokens.agg(F.sum(_product("token")).alias("total")).collect()[0]["total"])


def part2(spark: SparkSession, data: str) -> int:
    # regexp_extract_all preserves source order, so posexplode gives a usable
    # ordering key without computing character offsets.
    tokens = _memory(spark, data).select(
        F.posexplode(F.regexp_extract_all(F.col("memory"), F.lit(TOKEN), F.lit(0))).alias(
            "pos", "token"
        )
    )

    toggle = (
        F.when(F.col("token") == "do()", F.lit(1))
        .when(F.col("token") == "don't()", F.lit(0))
        .otherwise(F.lit(None))
    )
    # Must stay unpartitioned: a partitioned window would reset the enabled
    # state at each boundary and quietly give a wrong answer.
    running = Window.orderBy("pos").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    enabled = F.coalesce(F.last(toggle, ignorenulls=True).over(running), F.lit(1))

    scored = tokens.select(
        "token",
        enabled.alias("enabled"),
    ).filter(F.col("token").rlike(f"^{MUL}$") & (F.col("enabled") == 1))

    return int(scored.agg(F.sum(_product("token")).alias("total")).collect()[0]["total"])
