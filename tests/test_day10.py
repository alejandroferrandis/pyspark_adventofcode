"""2024 Day 10 against AoC's published examples."""

from __future__ import annotations

import pytest

from aoc_spark.y2024 import day10
from reference_python.y2024 import day10 as ref_day10

DAY10 = """\
89010123
78121874
87430965
96549874
45678903
32019012
01329801
10456732
"""

SMALL = """\
0123
1234
8765
9876
"""

# The "." tiles are impassable and appear only in the examples.
DOTTED = """\
10..9..
2...8..
3...7..
4567654
...8..3
...9..2
.....01
"""


@pytest.mark.parametrize(("data", "expected"), [(DAY10, 36), (SMALL, 1), (DOTTED, 3)])
def test_part1(spark, data, expected):
    assert day10.part1(spark, data) == expected


@pytest.mark.parametrize(("data", "expected"), [(DAY10, 36), (SMALL, 1), (DOTTED, 3)])
def test_part1_reference(data, expected):
    assert ref_day10.part1(data) == expected
