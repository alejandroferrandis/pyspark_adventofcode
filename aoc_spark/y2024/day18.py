"""2024 Day 18 -- RAM Run.

Sequential BFS over a 71x71 grid; Spark adds nothing here.
"""

from __future__ import annotations

from collections import deque

from pyspark.sql import SparkSession

STEPS = [(-1, 0), (1, 0), (0, -1), (0, 1)]


def part1(spark: SparkSession, data: str, size: int = 71, fallen: int = 1024) -> int:
    """`size` is the side length (coords 0..size-1); `fallen` is how many bytes land."""
    corrupted = set()
    for line in data.strip().splitlines()[:fallen]:
        x, y = line.split(",")
        corrupted.add((int(x), int(y)))

    goal = (size - 1, size - 1)
    queue = deque([((0, 0), 0)])
    seen = {(0, 0)}
    while queue:
        (x, y), steps = queue.popleft()
        if (x, y) == goal:
            return steps
        for dx, dy in STEPS:
            nxt = (x + dx, y + dy)
            if not (0 <= nxt[0] < size and 0 <= nxt[1] < size):
                continue
            if nxt in corrupted or nxt in seen:
                continue
            seen.add(nxt)
            queue.append((nxt, steps + 1))
    return -1
