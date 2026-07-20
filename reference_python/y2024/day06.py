"""2024 Day 6 reference -- jump straight to the next obstacle instead of stepping.

Per-row and per-column sorted obstacle indices give the stopping point by binary
search, so each leg of the patrol costs one lookup plus one range union.
"""

from __future__ import annotations

import bisect


def part1(data: str) -> int:
    grid = data.strip().splitlines()
    height, width = len(grid), len(grid[0])

    by_row: dict[int, list[int]] = {r: [] for r in range(height)}
    by_col: dict[int, list[int]] = {c: [] for c in range(width)}
    start = (0, 0)
    for r, line in enumerate(grid):
        for c, ch in enumerate(line):
            if ch == "#":
                by_row[r].append(c)
                by_col[c].append(r)
            elif ch == "^":
                start = (r, c)

    r, c = start
    d = 0
    visited = {start}
    while True:
        if d == 0:
            blockers = by_col[c]
            stop = blockers[i - 1] + 1 if (i := bisect.bisect_left(blockers, r)) else 0
            visited.update((rr, c) for rr in range(stop, r))
            leaving, r = stop == 0, stop
        elif d == 2:
            blockers = by_col[c]
            i = bisect.bisect_right(blockers, r)
            stop = blockers[i] - 1 if i < len(blockers) else height - 1
            visited.update((rr, c) for rr in range(r + 1, stop + 1))
            leaving, r = stop == height - 1, stop
        elif d == 1:
            blockers = by_row[r]
            i = bisect.bisect_right(blockers, c)
            stop = blockers[i] - 1 if i < len(blockers) else width - 1
            visited.update((r, cc) for cc in range(c + 1, stop + 1))
            leaving, c = stop == width - 1, stop
        else:
            blockers = by_row[r]
            stop = blockers[i - 1] + 1 if (i := bisect.bisect_left(blockers, c)) else 0
            visited.update((r, cc) for cc in range(stop, c))
            leaving, c = stop == 0, stop

        if leaving:
            return len(visited)
        d = (d + 1) % 4
