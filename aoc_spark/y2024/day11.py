"""2024 Day 11 -- Plutonian Pebbles.

Order never matters, so the line is kept as (stone, count) and each blink is an
explode of the rules followed by a group-by; distinct stones stay in the low
thousands where the stone count does not.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

BLINKS = 25


def _blink(stones: DataFrame) -> DataFrame:
    text = F.col("stone").cast("string")
    half = (F.length(text) / 2).cast("int")
    children = (
        F.when(F.col("stone") == 0, F.array(F.lit(1).cast("long")))
        .when(
            F.length(text) % 2 == 0,
            F.array(
                # substring is 1-based.
                F.substring(text, F.lit(1), half).cast("long"),
                F.substring(text, half + 1, half).cast("long"),
            ),
        )
        .otherwise(F.array(F.col("stone") * 2024))
    )
    return (
        stones.select(F.explode(children).alias("stone"), "count")
        .groupBy("stone")
        .agg(F.sum("count").alias("count"))
    )


def part1(spark: SparkSession, data: str) -> int:
    rows = [(int(v), 1) for v in data.split()]
    stones = (
        spark.createDataFrame(rows, "stone LONG, count LONG")
        .groupBy("stone")
        .agg(F.sum("count").alias("count"))
    )

    for _ in range(BLINKS):
        # Checkpoint each blink: 25 chained group-bys otherwise plan as one
        # ever-deepening lineage.
        stones = _blink(stones).localCheckpoint()

    return int(stones.agg(F.sum("count").alias("total")).collect()[0]["total"])
