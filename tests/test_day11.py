"""2024 Day 11 against AoC's published example."""

from __future__ import annotations

from aoc_spark.y2024 import day11
from reference_python.y2024 import day11 as ref_day11

DAY11 = "125 17\n"


def test_part1(spark):
    assert day11.part1(spark, DAY11) == 55312


def test_part1_reference():
    assert ref_day11.part1(DAY11) == 55312
