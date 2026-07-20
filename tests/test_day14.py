"""2024 Day 14 against AoC's published example, which uses an 11x7 space."""

from __future__ import annotations

from aoc_spark.y2024 import day14 as spark_day14
from reference_python.y2024 import day14 as py_day14

EXAMPLE = """\
p=0,4 v=3,-3
p=6,3 v=-1,-3
p=10,3 v=-1,2
p=2,0 v=2,-1
p=0,0 v=1,3
p=3,0 v=-2,-2
p=7,6 v=-1,-3
p=3,0 v=-1,-2
p=9,3 v=2,3
p=7,3 v=-1,2
p=2,4 v=2,-3
p=9,5 v=-3,-3
"""

WIDTH, HEIGHT = 11, 7
EXPECTED = 12


def test_part1_spark(spark):
    assert spark_day14.part1(spark, EXAMPLE, WIDTH, HEIGHT) == EXPECTED


def test_part1_python():
    assert py_day14.part1(EXAMPLE, WIDTH, HEIGHT) == EXPECTED
