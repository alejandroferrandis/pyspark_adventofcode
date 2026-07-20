"""2024 Day 20 -- Race Condition.

The track is a single corridor, so one walk labels every cell with its distance
from the start; the cheats are then a self-join over the eight offsets two moves
away.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

NEIGHBOURS = [(-1, 0), (1, 0), (0, -1), (0, 1)]
CHEATS = [(-2, 0), (2, 0), (0, -2), (0, 2), (-1, -1), (-1, 1), (1, -1), (1, 1)]


def _distances(data: str) -> dict[tuple[int, int], int]:
    grid = data.strip().splitlines()
    track = {
        (r, c) for r, row in enumerate(grid) for c, ch in enumerate(row) if ch != "#"
    }
    start = next((r, c) for r, row in enumerate(grid) for c, ch in enumerate(row) if ch == "S")

    dist = {start: 0}
    cur, prev = start, None
    while True:
        nxt = next(
            (
                n
                for dr, dc in NEIGHBOURS
                if (n := (cur[0] + dr, cur[1] + dc)) in track and n != prev
            ),
            None,
        )
        if nxt is None:
            return dist
        dist[nxt] = dist[cur] + 1
        cur, prev = nxt, cur


def part1(spark: SparkSession, data: str, threshold: int = 100) -> int:
    dist = _distances(data)
    cells = spark.createDataFrame(
        [(r, c, d) for (r, c), d in dist.items()], "r INT, c INT, d INT"
    )

    offsets = F.array(
        *[F.struct(F.lit(dr).alias("dr"), F.lit(dc).alias("dc")) for dr, dc in CHEATS]
    )
    starts = (
        cells.select("r", "c", F.col("d").alias("d0"), F.explode(offsets).alias("off"))
        .select(
            (F.col("r") + F.col("off.dr")).alias("er"),
            (F.col("c") + F.col("off.dc")).alias("ec"),
            "d0",
        )
    )
    ends = cells.select(
        F.col("r").alias("er"), F.col("c").alias("ec"), F.col("d").alias("d1")
    )

    # Each offset costs 2 picoseconds, so the saving is the distance gap minus 2.
    return starts.join(ends, on=["er", "ec"]).filter(
        F.col("d1") - F.col("d0") - 2 >= threshold
    ).count()
