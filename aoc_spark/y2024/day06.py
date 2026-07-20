"""2024 Day 6 -- Guard Gallivant.

Sequential simulation; Spark adds nothing here.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

# Up, right, down, left -- turning right is (d + 1) % 4.
TURNS = [(-1, 0), (0, 1), (1, 0), (0, -1)]


def part1(spark: SparkSession, data: str) -> int:
    grid = data.strip().splitlines()
    height, width = len(grid), len(grid[0])
    obstacles = {(r, c) for r, line in enumerate(grid) for c, ch in enumerate(line) if ch == "#"}
    start = next(
        (r, c) for r, line in enumerate(grid) for c, ch in enumerate(line) if ch == "^"
    )

    r, c = start
    d = 0
    seen = {start}
    while True:
        dr, dc = TURNS[d]
        nr, nc = r + dr, c + dc
        if not (0 <= nr < height and 0 <= nc < width):
            return len(seen)
        if (nr, nc) in obstacles:
            d = (d + 1) % 4
            continue
        r, c = nr, nc
        seen.add((r, c))
