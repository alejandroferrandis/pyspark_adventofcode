"""2024 Day 15 -- Warehouse Woes, cross-check.

Sparse sets of walls and boxes keyed by complex numbers rather than a mutable
character grid.
"""

from __future__ import annotations

DELTAS = {"^": -1j, "v": 1j, "<": -1, ">": 1}


def part1(data: str) -> int:
    grid_block, move_block = data.strip("\n").split("\n\n")

    walls, boxes = set(), set()
    robot = 0j
    for y, line in enumerate(grid_block.splitlines()):
        for x, ch in enumerate(line):
            pos = complex(x, y)
            if ch == "#":
                walls.add(pos)
            elif ch == "O":
                boxes.add(pos)
            elif ch == "@":
                robot = pos

    for move in "".join(move_block.split()):
        step = DELTAS[move]
        target = robot + step
        probe = target
        while probe in boxes:
            probe += step
        if probe in walls:
            continue
        if probe != target:
            boxes.remove(target)
            boxes.add(probe)
        robot = target

    return int(sum(100 * p.imag + p.real for p in boxes))
