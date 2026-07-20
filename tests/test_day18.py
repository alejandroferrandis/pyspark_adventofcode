"""2024 Day 18 against AoC's published example (7x7 grid, 12 bytes)."""

from __future__ import annotations

from aoc_spark.y2024 import day18 as spark_day18
from reference_python.y2024 import day18 as py_day18

EXAMPLE = """\
5,4
4,2
4,5
3,0
2,1
6,3
2,4
1,5
0,6
3,3
2,6
5,1
1,2
5,5
2,5
6,5
1,4
0,4
6,4
1,1
6,1
1,0
0,5
1,6
2,0
"""


def test_part1_spark(spark):
    assert spark_day18.part1(spark, EXAMPLE, size=7, fallen=12) == 22


def test_part1_reference():
    assert py_day18.part1(EXAMPLE, size=7, fallen=12) == 22
