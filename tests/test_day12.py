"""2024 Day 12 against AoC's published examples."""

from __future__ import annotations

import pytest

from aoc_spark.y2024 import day12 as spark_day12
from reference_python.y2024 import day12 as py_day12

SMALL = """\
AAAA
BBCD
BBCC
EEEC
"""

NESTED = """\
OOOOO
OXOXO
OOOOO
OXOXO
OOOOO
"""

LARGE = """\
RRRRIICCFF
RRRRIICCCF
VVRRRCCFFF
VVRCCCJFFF
VVVVCJJCFE
VVIVCCJJEE
VVIIICJJEE
MIIIIIJJEE
MIIISIJEEE
MMMISSJEEE
"""

EXAMPLES = [(SMALL, 140), (NESTED, 772), (LARGE, 1930)]


@pytest.mark.parametrize(("data", "expected"), EXAMPLES)
def test_part1_spark(spark, data, expected):
    assert spark_day12.part1(spark, data) == expected


@pytest.mark.parametrize(("data", "expected"), EXAMPLES)
def test_part1_python(data, expected):
    assert py_day12.part1(data) == expected
