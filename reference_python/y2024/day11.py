"""2024 Day 11 -- Plutonian Pebbles, cross-check implementation.

Top-down instead of breadth-first: a memoised recursion on (stone, blinks left)
returns how many stones that one stone ends up as.
"""

from __future__ import annotations

from functools import lru_cache

BLINKS = 25


@lru_cache(maxsize=None)
def _descendants(stone: int, blinks: int) -> int:
    if blinks == 0:
        return 1
    if stone == 0:
        return _descendants(1, blinks - 1)
    text = str(stone)
    if len(text) % 2 == 0:
        half = len(text) // 2
        return _descendants(int(text[:half]), blinks - 1) + _descendants(
            int(text[half:]), blinks - 1
        )
    return _descendants(stone * 2024, blinks - 1)


def part1(data: str) -> int:
    return sum(_descendants(int(v), BLINKS) for v in data.split())
