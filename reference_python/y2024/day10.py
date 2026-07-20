"""2024 Day 10 -- Hoof It, cross-check implementation.

Memoised depth-first descent: each cell caches the set of summits it can reach,
and a trailhead's score is the size of that set.
"""

from __future__ import annotations

from functools import lru_cache


def part1(data: str) -> int:
    grid = {
        (r, c): int(ch)
        for r, line in enumerate(data.strip().splitlines())
        for c, ch in enumerate(line)
        if ch.isdigit()
    }

    @lru_cache(maxsize=None)
    def summits(r: int, c: int) -> frozenset[tuple[int, int]]:
        height = grid[(r, c)]
        if height == 9:
            return frozenset({(r, c)})
        reachable: set[tuple[int, int]] = set()
        for nr, nc in ((r - 1, c), (r + 1, c), (r, c - 1), (r, c + 1)):
            if grid.get((nr, nc)) == height + 1:
                reachable |= summits(nr, nc)
        return frozenset(reachable)

    return sum(len(summits(r, c)) for (r, c), height in grid.items() if height == 0)
