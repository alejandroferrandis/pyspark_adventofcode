"""2024 Day 16 -- Reindeer Maze.

Dijkstra over (tile, heading). A Spark framing would be iterative join-based
relaxation, one job per frontier hop -- hundreds of jobs for a shortest path a
heap settles in milliseconds, so the priority queue stays.
"""

from __future__ import annotations

import heapq

from pyspark.sql import SparkSession

HEADINGS = [(0, 1), (1, 0), (0, -1), (-1, 0)]
TURN = 1000


def part1(spark: SparkSession, data: str) -> int:
    grid = data.strip("\n").splitlines()
    start = end = None
    for r, row in enumerate(grid):
        for c, ch in enumerate(row):
            if ch == "S":
                start = (r, c)
            elif ch == "E":
                end = (r, c)

    # Heading index 0 is East, matching the puzzle's starting orientation.
    queue = [(0, start[0], start[1], 0)]
    best: dict[tuple[int, int, int], int] = {}

    while queue:
        cost, r, c, h = heapq.heappop(queue)
        if (r, c) == end:
            return cost
        if best.get((r, c, h), cost + 1) <= cost:
            continue
        best[(r, c, h)] = cost

        dr, dc = HEADINGS[h]
        if grid[r + dr][c + dc] != "#":
            heapq.heappush(queue, (cost + 1, r + dr, c + dc, h))
        for turn in (1, 3):
            heapq.heappush(queue, (cost + TURN, r, c, (h + turn) % 4))

    raise ValueError("no path from S to E")
