"""2024 Day 5 -- Print Queue.  *Spark lesson: self-join for pairwise constraints.*

Ordering rules `X|Y` mean page X must precede page Y. Part 1 sums the middle
page of already-correct updates; part 2 reorders the incorrect ones and sums
their middles.

Two ideas carry this one:

**Part 1 -- a violation is a join.** Explode each update to (update, position,
page), self-join it to itself on `pos_a < pos_b` to get every ordered pair, then
join *that* to the rules reversed. Any surviving row is a broken rule, so an
update is correct exactly when it produces no rows.

**Part 2 -- sorting without a sort.** The single-processing-node instinct is a
comparator (that is what the plain-Python version does). But the correct index
of a page is determined by *counting*: if page p must precede k of the other
pages in its update, p lands at index n-1-k. So the middle page -- index n//2 --
is the page that precedes exactly n//2 others. That is a groupBy, not a sort,
and it never materialises the ordering at all.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def parse(spark: SparkSession, data: str) -> tuple[DataFrame, DataFrame]:
    """Split the two blocks into a rules relation and an exploded pages relation.

    Returns (rules[before, after], pages[update_id, pos, page, size]).
    """
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
    """update_ids that violate at least one ordering rule.

    Pair every page with every later page in the same update, then look for a
    rule saying the later one must come first.
    """
    a = pages.select(
        F.col("update_id"), F.col("pos").alias("pos_a"), F.col("page").alias("page_a")
    )
    b = pages.select(
        F.col("update_id").alias("uid_b"), F.col("pos").alias("pos_b"), F.col("page").alias("page_b")
    )
    ordered_pairs = a.join(
        b, (F.col("update_id") == F.col("uid_b")) & (F.col("pos_a") < F.col("pos_b"))
    )
    violations = ordered_pairs.join(
        rules, (F.col("before") == F.col("page_b")) & (F.col("after") == F.col("page_a"))
    )
    return violations.select("update_id").distinct()


def part1(spark: SparkSession, data: str) -> int:
    """Sum the middle page of every already-correctly-ordered update."""
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
    """Reorder the incorrect updates and sum their middle pages.

    No sorting: for each page count how many other pages *in the same update*
    it must precede. The middle page of an n-page update precedes exactly n//2
    of them.
    """
    rules, pages = parse(spark, data)
    pages = pages.cache()
    bad = _incorrect_updates(rules, pages)
    broken = pages.join(bad, on="update_id", how="inner")

    # Pair each page with every other page in the same update.
    others = broken.select(
        F.col("update_id").alias("uid_o"), F.col("page").alias("other_page")
    )
    pairs = broken.join(
        others, (F.col("update_id") == F.col("uid_o")) & (F.col("page") != F.col("other_page"))
    )

    # Count the pairs where a rule forces this page before the other one.
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
