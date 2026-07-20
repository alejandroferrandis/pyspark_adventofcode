"""2024 Day 7 against AoC's published example."""

from __future__ import annotations

from aoc_spark.y2024 import day07 as spark_day07
from reference_python.y2024 import day07 as python_day07

EXAMPLE = """\
190: 10 19
3267: 81 40 27
83: 17 5
156: 15 6
7290: 6 8 6 15
161011: 16 10 13
192: 17 8 14
21037: 9 7 18 13
292: 11 6 16 20
"""

EXPECTED_PART1 = 3749


def test_part1_spark(spark):
    assert spark_day07.part1(spark, EXAMPLE) == EXPECTED_PART1


def test_part1_reference():
    assert python_day07.part1(EXAMPLE) == EXPECTED_PART1
