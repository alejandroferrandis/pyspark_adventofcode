"""2024 Day 8 reference -- points as complex numbers, pairs via combinations.

Reflecting p over q is just 2*q - p in the complex plane, so each unordered
pair yields its two antinodes with no coordinate bookkeeping.
"""

from __future__ import annotations

from collections import defaultdict
from itertools import combinations


def part1(data: str) -> int:
    grid = data.strip().splitlines()
    height, width = len(grid), len(grid[0])

    by_freq: dict[str, list[complex]] = defaultdict(list)
    for r, line in enumerate(grid):
        for c, ch in enumerate(line):
            if ch != ".":
                by_freq[ch].append(complex(r, c))

    antinodes = set()
    for points in by_freq.values():
        for p, q in combinations(points, 2):
            antinodes.update((2 * q - p, 2 * p - q))

    return sum(
        1 for z in antinodes if 0 <= z.real < height and 0 <= z.imag < width
    )
