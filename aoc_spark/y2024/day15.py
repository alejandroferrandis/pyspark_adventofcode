"""2024 Day 15 -- Warehouse Woes.

Ordered state mutation: every move depends on the grid the previous move left
behind, so this is a sequential simulation and Spark adds nothing here.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

DELTAS = {"^": (-1, 0), "v": (1, 0), "<": (0, -1), ">": (0, 1)}


def parse(data: str) -> tuple[list[list[str]], str]:
    grid_block, move_block = data.strip("\n").split("\n\n")
    grid = [list(line) for line in grid_block.splitlines()]
    # Moves are split over several lines purely for copy-pasting; join them.
    return grid, "".join(move_block.split())


def part1(spark: SparkSession, data: str) -> int:
    grid, moves = parse(data)
    r, c = next(
        (r, c)
        for r, row in enumerate(grid)
        for c, ch in enumerate(row)
        if ch == "@"
    )

    for move in moves:
        dr, dc = DELTAS[move]
        nr, nc = r + dr, c + dc
        er, ec = nr, nc
        while grid[er][ec] == "O":
            er, ec = er + dr, ec + dc
        if grid[er][ec] == "#":
            continue
        # Any run of boxes shifts by one, which is the same as teleporting the
        # near box to the far gap.
        grid[er][ec] = "O" if (er, ec) != (nr, nc) else "."
        grid[nr][nc] = "@"
        grid[r][c] = "."
        r, c = nr, nc

    return sum(
        100 * r + c
        for r, row in enumerate(grid)
        for c, ch in enumerate(row)
        if ch == "O"
    )
