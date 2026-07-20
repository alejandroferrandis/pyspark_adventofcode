"""2024 Day 6 against AoC's published example."""

from __future__ import annotations

from aoc_spark.y2024 import day06 as spark_day06
from reference_python.y2024 import day06 as python_day06

EXAMPLE = """\
....#.....
.........#
..........
..#.......
.......#..
..........
.#..^.....
........#.
#.........
......#...
"""

EXPECTED_PART1 = 41


def test_part1_spark(spark):
    assert spark_day06.part1(spark, EXAMPLE) == EXPECTED_PART1


def test_part1_reference():
    assert python_day06.part1(EXAMPLE) == EXPECTED_PART1
