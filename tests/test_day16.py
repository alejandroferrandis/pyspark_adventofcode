"""2024 day 16 against AoC's two published mazes."""

from __future__ import annotations

import pytest

from aoc_spark.y2024 import day16 as spark_day16
from reference_python.y2024 import day16 as py_day16

FIRST = """\
###############
#.......#....E#
#.#.###.#.###.#
#.....#.#...#.#
#.###.#####.#.#
#.#.#.......#.#
#.#.#####.###.#
#...........#.#
###.#.#####.#.#
#...#.....#.#.#
#.#.#.###.#.#.#
#.....#...#.#.#
#.###.#.#.#.#.#
#S..#.....#...#
###############
"""

SECOND = """\
#################
#...#...#...#..E#
#.#.#.#.#.#.#.#.#
#.#.#.#...#...#.#
#.#.#.#.###.#.#.#
#...#.#.#.....#.#
#.#.#.#.#.#####.#
#.#...#.#.#.....#
#.#.#####.#.###.#
#.#.#.......#...#
#.#.###.#####.###
#.#.#...#.....#.#
#.#.#.#####.###.#
#.#.#.........#.#
#.#.#.#########.#
#S#.............#
#################
"""


@pytest.mark.parametrize(("data", "expected"), [(FIRST, 7036), (SECOND, 11048)])
def test_part1_spark(spark, data, expected):
    assert spark_day16.part1(spark, data) == expected


@pytest.mark.parametrize(("data", "expected"), [(FIRST, 7036), (SECOND, 11048)])
def test_part1_reference(data, expected):
    assert py_day16.part1(data) == expected
