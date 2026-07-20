"""2024 Day 5 -- Print Queue.

Violations come from a self-join against the reversed rules. Part 2 finds the
middle page by counting rather than sorting.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def parse(spark: SparkSession, data: str) -> tuple[DataFrame, DataFrame]:
    """Returns (rules[before, after], pages[update_id, pos, page, size])."""
    rules_block, updates_block = data.strip().split("\n\n")

    rule_lines = spark.createDataFrame(
        [(line,) for line in rules_block.splitlines()], "line STRING"
    )
    pair = F.split(F.col("line"), r"\|")
    rules = rule_lines.select(
        pair.getItem(0).cast("int").alias("before"),
        pair.getItem(1).cast("int").alias("after"),
    )

    update_lines = spark.createDataFrame(
        [(i, line) for i, line in enumerate(updates_block.splitlines())],
        "update_id INT, line STRING",
    )
    exploded = update_lines.select(
        "update_id",
        F.posexplode(
            F.transform(F.split(F.col("line"), ","), lambda x: x.cast("int"))
        ).alias("pos", "page"),
    )
    sizes = exploded.groupBy("update_id").agg(F.count("*").alias("size"))
    pages = exploded.join(sizes, on="update_id")
    return rules, pages


def _incorrect_updates(rules: DataFrame, pages: DataFrame) -> DataFrame:
    """update_ids violating at least one ordering rule."""
    a = pages.select(
        F.col("update_id"), F.col("pos").alias("pos_a"), F.col("page").alias("page_a")
    )
    b = pages.select(
        F.col("update_id").alias("uid_b"), F.col("pos").alias("pos_b"), F.col("page").alias("page_b")
    )
    ordered_pairs = a.join(
        b, (F.col("update_id") == F.col("uid_b")) & (F.col("pos_a") < F.col("pos_b"))
    )
    # Rules joined reversed: a hit means the later page should have come first.
    violations = ordered_pairs.join(
        rules, (F.col("before") == F.col("page_b")) & (F.col("after") == F.col("page_a"))
    )
    return violations.select("update_id").distinct()


def part1(spark: SparkSession, data: str) -> int:
    rules, pages = parse(spark, data)
    pages = pages.cache()
    bad = _incorrect_updates(rules, pages)

    middles = (
        pages.join(bad, on="update_id", how="left_anti")
        .filter(F.col("pos") == (F.col("size") / 2).cast("int"))
        .select("page")
    )
    total = int(middles.agg(F.sum("page").alias("total")).collect()[0]["total"])
    pages.unpersist()
    return total


def part2(spark: SparkSession, data: str) -> int:
    """A page preceding k others lands at index n-1-k, so the middle page
    (index n//2) is the one preceding exactly n//2 others.

    Assumes the rules are total within each update, which holds for AoC input
    but is not true in general.
    """
    rules, pages = parse(spark, data)
    pages = pages.cache()
    bad = _incorrect_updates(rules, pages)
    broken = pages.join(bad, on="update_id", how="inner")

    others = broken.select(
        F.col("update_id").alias("uid_o"), F.col("page").alias("other_page")
    )
    pairs = broken.join(
        others, (F.col("update_id") == F.col("uid_o")) & (F.col("page") != F.col("other_page"))
    )

    precedes = (
        pairs.join(
            rules, (F.col("before") == F.col("page")) & (F.col("after") == F.col("other_page"))
        )
        .groupBy("update_id", "page", "size")
        .agg(F.count("*").alias("precedes"))
    )

    middles = precedes.filter(
        F.col("precedes") == (F.col("size") / 2).cast("int")
    ).select("page")

    total = int(middles.agg(F.sum("page").alias("total")).collect()[0]["total"])
    pages.unpersist()
    return total
