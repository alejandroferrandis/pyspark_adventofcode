"""2024 Day 20 against AoC's published example.

The example's cheat histogram tops out at 64 saved, so the puzzle's own
threshold of 100 yields 0; the >= 2 case sums that histogram to 44.
"""

from __future__ import annotations

import pytest

from aoc_spark.y2024 import day20 as spark_day20
from reference_python.y2024 import day20 as py_day20

EXAMPLE = """\
###############
#...#...#.....#
#.#.#.#.#.###.#
#S#...#.#.#...#
#######.#.#.###
#######.#.#...#
#######.#.###.#
###..E#...#...#
###.#######.###
#...###...#...#
#.#####.#.###.#
#.#...#.#.#...#
#.#.#.#.#.#.###
#...#...#...###
###############
"""


@pytest.mark.parametrize(("threshold", "expected"), [(2, 44), (100, 0)])
def test_part1_spark(spark, threshold, expected):
    assert spark_day20.part1(spark, EXAMPLE, threshold=threshold) == expected


@pytest.mark.parametrize(("threshold", "expected"), [(2, 44), (100, 0)])
def test_part1_reference(threshold, expected):
    assert py_day20.part1(EXAMPLE, threshold=threshold) == expected
