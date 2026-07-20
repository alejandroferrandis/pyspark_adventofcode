"""2024 Day 19 against AoC's published example."""

from __future__ import annotations

from aoc_spark.y2024 import day19 as spark_day19
from reference_python.y2024 import day19 as py_day19

EXAMPLE = """\
r, wr, b, g, bwu, rb, gb, br

brwrr
bggr
gbbr
rrbgbr
ubwu
bwurrg
brgr
bbrgwb
"""


def test_part1_spark(spark):
    assert spark_day19.part1(spark, EXAMPLE) == 6


def test_part1_reference():
    assert py_day19.part1(EXAMPLE) == 6
