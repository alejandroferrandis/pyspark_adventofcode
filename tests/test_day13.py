"""2024 Day 13 against AoC's published example."""

from __future__ import annotations

from aoc_spark.y2024 import day13 as spark_day13
from reference_python.y2024 import day13 as py_day13

EXAMPLE = """\
Button A: X+94, Y+34
Button B: X+22, Y+67
Prize: X=8400, Y=5400

Button A: X+26, Y+66
Button B: X+67, Y+21
Prize: X=12748, Y=12176

Button A: X+17, Y+86
Button B: X+84, Y+37
Prize: X=7870, Y=6450

Button A: X+69, Y+23
Button B: X+27, Y+71
Prize: X=18641, Y=10279
"""

EXPECTED = 480


def test_part1_spark(spark):
    assert spark_day13.part1(spark, EXAMPLE) == EXPECTED


def test_part1_python():
    assert py_day13.part1(EXAMPLE) == EXPECTED
