"""2024 Day 14 -- Restroom Redoubt, cross-check.

Steps the robots one second at a time rather than jumping straight to t=100.
"""

from __future__ import annotations

import re
from collections import Counter

SECONDS = 100
NUMBER = re.compile(r"-?\d+")


def part1(data: str, width: int = 101, height: int = 103) -> int:
    robots = []
    for line in data.strip().splitlines():
        px, py, vx, vy = (int(n) for n in NUMBER.findall(line))
        robots.append([px, py, vx, vy])

    for _ in range(SECONDS):
        for robot in robots:
            robot[0] = (robot[0] + robot[2]) % width
            robot[1] = (robot[1] + robot[3]) % height

    mid_x, mid_y = width // 2, height // 2
    quadrants: Counter[tuple[bool, bool]] = Counter()
    for px, py, _, _ in robots:
        if px == mid_x or py == mid_y:
            continue
        quadrants[(px > mid_x, py > mid_y)] += 1

    safety = 1
    for corner in ((False, False), (True, False), (False, True), (True, True)):
        safety *= quadrants[corner]
    return safety
