"""2024 Day 13 -- Claw Contraption, cross-check.

Brute forces every A-press count in range instead of solving the system, which
also copes with parallel buttons.
"""

from __future__ import annotations

import re

MAX_PRESSES = 100
NUMBER = re.compile(r"\d+")


def part1(data: str) -> int:
    total = 0
    for block in data.strip().split("\n\n"):
        ax, ay, bx, by, px, py = (int(n) for n in NUMBER.findall(block))
        best = None
        for a in range(MAX_PRESSES + 1):
            rx, ry = px - a * ax, py - a * ay
            if rx < 0 or ry < 0 or bx == 0 or by == 0:
                continue
            b, remainder = divmod(rx, bx)
            if remainder or b > MAX_PRESSES or b * by != ry:
                continue
            cost = 3 * a + b
            best = cost if best is None else min(best, cost)
        total += best or 0
    return total
