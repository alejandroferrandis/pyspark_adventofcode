"""2024 Day 20 reference -- enumerate cheats by their midpoint cell.

A two-move cheat passes through exactly one intermediate cell, so scanning every
cell and pairing its track neighbours finds them all. Diagonal endpoints share
two midpoints, hence the dedup by (start, end).
"""

from __future__ import annotations

from itertools import permutations

NEIGHBOURS = [(-1, 0), (1, 0), (0, -1), (0, 1)]


def part1(data: str, threshold: int = 100) -> int:
    grid = data.strip().splitlines()
    height, width = len(grid), len(grid[0])
    start = next(
        (r, c) for r, row in enumerate(grid) for c, ch in enumerate(row) if ch == "S"
    )

    order = [start]
    seen = {start}
    while True:
        r, c = order[-1]
        step = next(
            (
                n
                for dr, dc in NEIGHBOURS
                if (n := (r + dr, c + dc)) not in seen
                and 0 <= n[0] < height
                and 0 <= n[1] < width
                and grid[n[0]][n[1]] != "#"
            ),
            None,
        )
        if step is None:
            break
        seen.add(step)
        order.append(step)
    dist = {cell: i for i, cell in enumerate(order)}

    cheats = set()
    for mr in range(height):
        for mc in range(width):
            around = [
                n for dr, dc in NEIGHBOURS if (n := (mr + dr, mc + dc)) in dist
            ]
            for a, b in permutations(around, 2):
                if dist[b] - dist[a] - 2 >= threshold:
                    cheats.add((a, b))
    return len(cheats)
