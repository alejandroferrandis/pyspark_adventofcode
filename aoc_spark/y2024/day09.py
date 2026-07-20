"""2024 Day 9 -- Disk Fragmenter.

Block-by-block compaction is inherently sequential; Spark adds nothing here, so
part 1 is a two-pointer sweep over the expanded block list in plain Python.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

FREE = -1


def blocks(data: str) -> list[int]:
    """One entry per block: a file id, or FREE."""
    out: list[int] = []
    for i, digit in enumerate(data.strip()):
        # Even positions are files, odd positions are gaps; file id is i // 2.
        out.extend([i // 2 if i % 2 == 0 else FREE] * int(digit))
    return out


def part1(spark: SparkSession, data: str) -> int:
    disk = blocks(data)
    left, right = 0, len(disk) - 1
    while left < right:
        if disk[left] != FREE:
            left += 1
        elif disk[right] == FREE:
            right -= 1
        else:
            disk[left], disk[right] = disk[right], FREE

    return sum(pos * fid for pos, fid in enumerate(disk) if fid != FREE)
