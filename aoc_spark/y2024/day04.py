"""2024 Day 4 -- Ceres Search.  *Spark lesson: modelling a grid as a cell relation.*

Part 1 counts XMAS in all 8 directions; part 2 counts two MAS crossing on a
shared central A.

The move that makes this tractable in Spark is refusing to treat the grid as a
grid. Explode it into a `(row, col, char)` relation and every "look in
direction (dr,dc)" becomes an **equi-join on offset coordinates**. Searching all
8 directions at once is then a cross join against a 8-row directions table --
Spark broadcasts it and the whole search is three joins deep regardless of how
many directions there are.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DIRECTIONS = [(-1, -1), (-1, 0), (-1, 1), (0, -1), (0, 1), (1, -1), (1, 0), (1, 1)]
WORD = "XMAS"


def cells(spark: SparkSession, data: str) -> DataFrame:
    """Grid -> one row per character: (r, c, ch)."""
    lines = [(r, line) for r, line in enumerate(data.strip().splitlines())]
    rows = spark.createDataFrame(lines, "r INT, line STRING")
    return (
        rows.select("r", F.posexplode(F.split(F.col("line"), "")).alias("c", "ch"))
        # split(s, "") yields a trailing empty string; drop it.
        .filter(F.col("ch") != "")
    )


def part1(spark: SparkSession, data: str) -> int:
    """Count XMAS in all 8 directions.

    Start from every X paired with every direction, then join once per
    remaining letter, stepping (dr,dc) further each time. A surviving row is a
    complete match.
    """
    grid = cells(spark, data).cache()
    directions = spark.createDataFrame(DIRECTIONS, "dr INT, dc INT")

    matches = grid.filter(F.col("ch") == WORD[0]).select("r", "c").crossJoin(directions)

    for step, letter in enumerate(WORD[1:], start=1):
        nxt = grid.filter(F.col("ch") == letter).select(
            F.col("r").alias("nr"), F.col("c").alias("nc")
        )
        matches = matches.join(
            nxt,
            (F.col("nr") == F.col("r") + F.col("dr") * step)
            & (F.col("nc") == F.col("c") + F.col("dc") * step),
        ).select("r", "c", "dr", "dc")

    count = matches.count()
    grid.unpersist()
    return count


def part2(spark: SparkSession, data: str) -> int:
    """Count X-MAS: two MAS crossing diagonally on a shared central A.

    Join each A to its four diagonal neighbours, then require each diagonal to
    read M/S in either order.
    """
    grid = cells(spark, data).cache()
    centres = grid.filter(F.col("ch") == "A").select(
        F.col("r").alias("ar"), F.col("c").alias("ac")
    )

    def corner(name: str, dr: int, dc: int) -> DataFrame:
        return grid.select(
            F.col("r").alias(f"{name}_r"), F.col("c").alias(f"{name}_c"), F.col("ch").alias(name)
        ), (F.col(f"{name}_r") == F.col("ar") + dr) & (F.col(f"{name}_c") == F.col("ac") + dc)

    joined = centres
    for name, dr, dc in [("tl", -1, -1), ("br", 1, 1), ("tr", -1, 1), ("bl", 1, -1)]:
        side, condition = corner(name, dr, dc)
        joined = joined.join(side, condition).drop(f"{name}_r", f"{name}_c")

    def is_mas(a: str, b: str) -> "F.Column":
        return ((F.col(a) == "M") & (F.col(b) == "S")) | ((F.col(a) == "S") & (F.col(b) == "M"))

    count = joined.filter(is_mas("tl", "br") & is_mas("tr", "bl")).count()
    grid.unpersist()
    return count
