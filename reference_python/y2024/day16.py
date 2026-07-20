"""2024 Day 16 -- Reindeer Maze, cross-check.

Queue-based cost relaxation (SPFA) instead of a priority queue: states are
re-enqueued whenever a cheaper cost is found, so no ordering is assumed.
"""

from __future__ import annotations

from collections import deque

HEADINGS = [(0, 1), (1, 0), (0, -1), (-1, 0)]
TURN = 1000


def part1(data: str) -> int:
    grid = data.strip("\n").splitlines()
    cells = {
        (r, c): ch
        for r, row in enumerate(grid)
        for c, ch in enumerate(row)
        if ch != "#"
    }
    start = next(pos for pos, ch in cells.items() if ch == "S")
    end = next(pos for pos, ch in cells.items() if ch == "E")

    dist = {(start, 0): 0}
    pending = deque([(start, 0)])

    while pending:
        state = pending.popleft()
        (r, c), h = state
        cost = dist[state]

        dr, dc = HEADINGS[h]
        moves = [(((r + dr, c + dc), h), cost + 1)] if (r + dr, c + dc) in cells else []
        moves += [(((r, c), (h + t) % 4), cost + TURN) for t in (1, 3)]

        for nxt, ncost in moves:
            if ncost < dist.get(nxt, ncost + 1):
                dist[nxt] = ncost
                pending.append(nxt)

    return min(dist[(end, h)] for h in range(4) if (end, h) in dist)
