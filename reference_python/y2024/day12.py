"""2024 Day 12 -- Garden Groups, cross-check.

Explicit BFS flood fill, accumulating perimeter as the fill runs.
"""

from __future__ import annotations

from collections import deque


def part1(data: str) -> int:
    grid = data.strip().splitlines()
    height = len(grid)
    seen: set[tuple[int, int]] = set()
    total = 0

    for r in range(height):
        for c in range(len(grid[r])):
            if (r, c) in seen:
                continue
            plant = grid[r][c]
            seen.add((r, c))
            queue = deque([(r, c)])
            area = perimeter = 0
            while queue:
                cr, cc = queue.popleft()
                area += 1
                for nr, nc in ((cr - 1, cc), (cr + 1, cc), (cr, cc - 1), (cr, cc + 1)):
                    if 0 <= nr < height and 0 <= nc < len(grid[nr]) and grid[nr][nc] == plant:
                        if (nr, nc) not in seen:
                            seen.add((nr, nc))
                            queue.append((nr, nc))
                    else:
                        perimeter += 1
            total += area * perimeter

    return total
