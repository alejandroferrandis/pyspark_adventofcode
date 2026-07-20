"""2024 Day 19 -- Linen Layout.

Every (design, position, length) prefix is matched against the towel patterns in
one equi-join, then a native `aggregate` folds each design's matches into a
bitmask of reachable positions.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def _reachable_steps(spark: SparkSession, data: str) -> tuple[DataFrame, DataFrame]:
    """Returns (designs[did, dlen], steps[did, pos, lens]) with lens = pattern
    lengths that match the design starting at pos."""
    patterns_block, designs_block = data.strip().split("\n\n")
    patterns = [p.strip() for p in patterns_block.split(",")]
    designs = designs_block.splitlines()

    pattern_df = spark.createDataFrame([(p,) for p in patterns], "pattern STRING")
    designs_df = spark.createDataFrame(
        [(i, d, len(d)) for i, d in enumerate(designs)], "did INT, design STRING, dlen INT"
    )

    shortest, longest = min(map(len, patterns)), max(map(len, patterns))
    prefixes = designs_df.select(
        "did",
        "design",
        F.explode(F.sequence(F.lit(0), F.col("dlen") - 1)).alias("pos"),
    ).select(
        "did",
        "pos",
        "design",
        F.explode(F.sequence(F.lit(shortest), F.lit(longest))).alias("plen"),
    )
    # substring is 1-based; an equal-length key match implies the pattern fits exactly.
    keyed = prefixes.select(
        "did",
        "pos",
        "plen",
        F.substring(F.col("design"), F.col("pos") + 1, F.col("plen")).alias("key"),
    )

    steps = (
        keyed.join(pattern_df, F.col("key") == F.col("pattern"))
        .groupBy("did", "pos")
        .agg(F.collect_set("plen").alias("lens"))
    )
    return designs_df.select("did", "dlen"), steps


def part1(spark: SparkSession, data: str) -> int:
    designs_df, steps = _reachable_steps(spark, data)

    ordered = steps.groupBy("did").agg(
        F.sort_array(F.collect_list(F.struct("pos", "lens"))).alias("steps")
    )

    def bit(index):
        return F.shiftleft(F.lit(1).cast("long"), index)

    # Positions absent from `steps` can never extend a match, so folding over the
    # matching positions in ascending order is a complete DP. Needs dlen < 63.
    reached = F.aggregate(
        F.col("steps"),
        bit(F.lit(0)),
        lambda acc, step: F.when(acc.bitwiseAND(bit(step["pos"])) == 0, acc).otherwise(
            F.aggregate(step["lens"], acc, lambda a, plen: a.bitwiseOR(bit(step["pos"] + plen)))
        ),
    )

    possible = (
        ordered.join(designs_df, on="did")
        .select(reached.bitwiseAND(bit(F.col("dlen"))).alias("hit"))
        .filter(F.col("hit") != 0)
    )
    return possible.count()
