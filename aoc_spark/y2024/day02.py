"""2024 Day 2 -- Red-Nosed Reports.  *Spark lesson: higher-order array functions.*

A report is safe when its levels are strictly monotonic and every adjacent step
is 1..3. Part 2 adds the Problem Dampener: removing a single level may rescue
an unsafe report.

The interesting part is doing this *without* a UDF. Spark's higher-order
functions -- `transform`, `zip_with`, `forall`, `exists`, `slice`, `sequence` --
operate on array columns inside the JVM, so the whole thing stays a native
expression tree. A Python UDF here would serialise every row to a Python worker
and lose an order of magnitude.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F


def parse(spark: SparkSession, data: str) -> DataFrame:
    """One row per report, levels as an array<int> column."""
    lines = spark.createDataFrame([(line,) for line in data.strip().splitlines()], "line STRING")
    return lines.select(
        F.transform(F.split(F.trim(F.col("line")), r"\s+"), lambda x: x.cast("int")).alias("levels")
    )


def _is_safe(levels: Column) -> Column:
    """Boolean column: are these levels monotonic with every step in 1..3?

    `zip_with(levels[1:n-1], levels[2:n])` gives adjacent differences; the
    report is safe when all differences sit in [1,3] or all in [-3,-1].
    """
    size = F.size(levels)
    head = F.slice(levels, F.lit(1), size - 1)
    tail = F.slice(levels, F.lit(2), size - 1)
    diffs = F.zip_with(head, tail, lambda a, b: b - a)
    increasing = F.forall(diffs, lambda d: (d >= 1) & (d <= 3))
    decreasing = F.forall(diffs, lambda d: (d <= -1) & (d >= -3))
    return increasing | decreasing


def _dampened_variants(levels: Column) -> Column:
    """Every version of `levels` with exactly one element removed.

    For each index i (0-based) concatenate the elements before i with those
    after i. `slice` is 1-based, hence the +1/+2 offsets.
    """
    size = F.size(levels)
    indices = F.sequence(F.lit(0), size - 1)
    return F.transform(
        indices,
        lambda i: F.concat(
            F.slice(levels, F.lit(1), i),
            F.slice(levels, i + 2, size - i - 1),
        ),
    )


def part1(spark: SparkSession, data: str) -> int:
    df = parse(spark, data)
    safe = df.select(_is_safe(F.col("levels")).alias("safe"))
    return int(safe.agg(F.sum(F.col("safe").cast("int")).alias("n")).collect()[0]["n"])


def part2(spark: SparkSession, data: str) -> int:
    """Safe outright, or safe after dropping one level.

    `exists` short-circuits inside the JVM, so most reports stop at the first
    successful removal rather than building all n variants.
    """
    df = parse(spark, data)
    levels = F.col("levels")
    rescued = F.exists(_dampened_variants(levels), _is_safe)
    safe = df.select((_is_safe(levels) | rescued).alias("safe"))
    return int(safe.agg(F.sum(F.col("safe").cast("int")).alias("n")).collect()[0]["n"])
