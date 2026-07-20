"""2024 Day 7 reference -- solve backwards from the target.

Working right-to-left, a trailing operand can only have been multiplied if it
divides the target, which prunes far harder than exploring forwards.
"""

from __future__ import annotations


def _solvable(target: int, nums: list[int]) -> bool:
    if len(nums) == 1:
        return target == nums[0]
    *rest, last = nums
    if target % last == 0 and _solvable(target // last, rest):
        return True
    return target > last and _solvable(target - last, rest)


def part1(data: str) -> int:
    total = 0
    for line in data.strip().splitlines():
        head, tail = line.split(": ")
        target, nums = int(head), [int(n) for n in tail.split()]
        if _solvable(target, nums):
            total += target
    return total
