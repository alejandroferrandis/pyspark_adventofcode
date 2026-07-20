"""2024 Day 18 reference -- A* with a Manhattan heuristic instead of plain BFS.

Uniform step cost makes the heuristic admissible, so the first pop of the goal
is optimal.
"""

from __future__ import annotations

import heapq

STEPS = [(-1, 0), (1, 0), (0, -1), (0, 1)]


def part1(data: str, size: int = 71, fallen: int = 1024) -> int:
    corrupted = {
        tuple(int(v) for v in line.split(","))
        for line in data.strip().splitlines()[:fallen]
    }

    gx, gy = size - 1, size - 1
    heap = [(gx + gy, 0, 0, 0)]
    best = {(0, 0): 0}
    while heap:
        _, cost, x, y = heapq.heappop(heap)
        if (x, y) == (gx, gy):
            return cost
        if cost > best.get((x, y), 1 << 30):
            continue
        for dx, dy in STEPS:
            nx, ny = x + dx, y + dy
            if not (0 <= nx < size and 0 <= ny < size) or (nx, ny) in corrupted:
                continue
            if cost + 1 < best.get((nx, ny), 1 << 30):
                best[(nx, ny)] = cost + 1
                heapq.heappush(heap, (cost + 1 + abs(gx - nx) + abs(gy - ny), cost + 1, nx, ny))
    return -1
