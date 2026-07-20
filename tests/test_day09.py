"""2024 Day 9 against AoC's published example."""

from __future__ import annotations

from aoc_spark.y2024 import day09
from reference_python.y2024 import day09 as ref_day09

DAY09 = "2333133121414131402\n"


def test_part1(spark):
    assert day09.part1(spark, DAY09) == 1928


def test_part1_reference():
    assert ref_day09.part1(DAY09) == 1928
