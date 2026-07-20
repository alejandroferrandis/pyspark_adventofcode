"""2024 Day 12 -- Garden Groups.

Regions are connected components, found by iterative min-label propagation with
pointer doubling; each cell's perimeter is 4 minus its same-plant degree.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def cells(spark: SparkSession, data: str) -> DataFrame:
    """Grid as (node, r, c, ch), node being a row-major id."""
    lines = [(r, line) for r, line in enumerate(data.strip().splitlines())]
    width = max(len(line) for _, line in lines) + 1
    rows = spark.createDataFrame(lines, "r INT, line STRING")
    grid = rows.select(
        "r", F.posexplode(F.split(F.col("line"), "")).alias("c", "ch")
    ).filter(
        # split(s, "") emits a trailing empty string.
        F.col("ch") != ""
    )
    return grid.select(
        (F.col("r").cast("long") * width + F.col("c")).alias("node"), "r", "c", "ch"
    )


def edges(grid: DataFrame) -> DataFrame:
    """Symmetric same-plant adjacency as (u, v)."""
    other = grid.select(
        F.col("node").alias("v"),
        F.col("r").alias("vr"),
        F.col("c").alias("vc"),
        F.col("ch").alias("vch"),
    )
    right_and_down = (
        (F.col("vr") == F.col("r")) & (F.col("vc") == F.col("c") + 1)
    ) | ((F.col("vr") == F.col("r") + 1) & (F.col("vc") == F.col("c")))
    touching = grid.join(other, (F.col("ch") == F.col("vch")) & right_and_down).select(
        F.col("node").alias("u"), "v"
    )
    return touching.union(touching.select(F.col("v").alias("u"), F.col("u").alias("v")))


def components(grid: DataFrame, adjacency: DataFrame) -> DataFrame:
    """(node, p) with p the smallest node id in the same region."""
    parent = grid.select("node", F.col("node").alias("p"))
    while True:
        labelled = (
            adjacency.join(parent.select(F.col("node").alias("u"), F.col("p").alias("pu")), "u")
            .join(parent.select(F.col("node").alias("v"), F.col("p").alias("pv")), "v")
        )
        # Hook both the node and its current parent onto the smallest label one
        # edge away; hooking the parent too is what makes this converge in
        # O(log n) rounds instead of O(diameter).
        proposals = (
            labelled.select(F.col("pu").alias("node"), F.col("pv").alias("cand"))
            .union(labelled.select(F.col("u").alias("node"), F.col("pv").alias("cand")))
            .groupBy("node")
            .agg(F.min("cand").alias("cand"))
        )
        hooked = parent.join(proposals, "node", "left").select(
            "node", F.least("p", F.coalesce("cand", F.col("p"))).alias("p")
        )
        grandparent = hooked.select(F.col("node").alias("gnode"), F.col("p").alias("gp"))
        nxt = (
            hooked.join(grandparent, F.col("p") == F.col("gnode"), "left")
            .select("node", F.least("p", F.coalesce("gp", F.col("p"))).alias("p"))
            .localCheckpoint()
        )

        previous = parent.select(F.col("node").alias("onode"), F.col("p").alias("op"))
        changed = nxt.join(previous, F.col("node") == F.col("onode")).filter(
            F.col("p") != F.col("op")
        ).count()
        parent = nxt
        if changed == 0:
            return parent


def part1(spark: SparkSession, data: str) -> int:
    grid = cells(spark, data).cache()
    adjacency = edges(grid).cache()
    parent = components(grid, adjacency)

    degree = adjacency.groupBy(F.col("u").alias("node")).agg(F.count("*").alias("deg"))
    perimeters = grid.select("node").join(degree, "node", "left").select(
        "node", (F.lit(4) - F.coalesce("deg", F.lit(0))).alias("perimeter")
    )
    regions = (
        perimeters.join(parent, "node")
        .groupBy("p")
        .agg(F.count("*").alias("area"), F.sum("perimeter").alias("perimeter"))
    )
    total = regions.agg(
        F.sum(F.col("area") * F.col("perimeter")).alias("total")
    ).collect()[0]["total"]

    adjacency.unpersist()
    grid.unpersist()
    return int(total)
