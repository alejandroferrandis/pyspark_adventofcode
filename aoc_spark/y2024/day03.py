"""2024 Day 3 -- Mull It Over.  *Spark lesson: regex extraction + stateful window.*

Corrupted memory holds valid `mul(X,Y)` instructions among garbage. Part 1 sums
every multiplication. Part 2 adds `do()` / `don't()` toggles that enable or
disable the muls that follow.

Part 2 looks hopelessly sequential -- "carry a flag forward while scanning" --
and that is exactly the shape people reach for a UDF to solve. It is really a
**last-non-null-value window**, one of the most useful patterns in Spark:

    last(flag, ignorenulls=True) over (order by pos rows between unbounded preceding and current row)

Tokens carry no flag (null); `do()`/`don't()` set 1/0. The window fills each
mul with the most recent toggle. `coalesce(..., 1)` handles the muls that
appear before any toggle, since multiplication starts enabled.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

MUL = r"mul\(\d{1,3},\d{1,3}\)"
TOKEN = r"mul\(\d{1,3},\d{1,3}\)|do\(\)|don't\(\)"
OPERANDS = r"mul\((\d{1,3}),(\d{1,3})\)"


def _memory(spark: SparkSession, data: str) -> DataFrame:
    """The whole corrupted blob as a single-row DataFrame.

    Line breaks are meaningless in this puzzle -- the instructions run across
    them -- so this is genuinely one string, not one row per line.
    """
    return spark.createDataFrame([(data,)], "memory STRING")


def _product(token_col: str) -> "F.Column":
    """Multiply the two operands out of a `mul(X,Y)` token."""
    left = F.regexp_extract(F.col(token_col), OPERANDS, 1).cast("long")
    right = F.regexp_extract(F.col(token_col), OPERANDS, 2).cast("long")
    return left * right


def part1(spark: SparkSession, data: str) -> int:
    """Every valid mul, toggles ignored."""
    tokens = _memory(spark, data).select(
        F.explode(F.regexp_extract_all(F.col("memory"), F.lit(MUL), F.lit(0))).alias("token")
    )
    return int(tokens.agg(F.sum(_product("token")).alias("total")).collect()[0]["total"])


def part2(spark: SparkSession, data: str) -> int:
    """Only the muls that are enabled at the point they appear.

    `regexp_extract_all` returns matches in source order, so `posexplode` gives
    a reliable ordering key without needing character offsets.
    """
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
    running = Window.orderBy("pos").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    enabled = F.coalesce(F.last(toggle, ignorenulls=True).over(running), F.lit(1))

    scored = tokens.select(
        "token",
        enabled.alias("enabled"),
    ).filter(F.col("token").rlike(f"^{MUL}$") & (F.col("enabled") == 1))

    return int(scored.agg(F.sum(_product("token")).alias("total")).collect()[0]["total"])
