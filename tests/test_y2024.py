"""Tests for 2024 days 1-5 against AoC's published examples.

Real inputs are per-user and stay out of git; they live in `inputs/`.
"""

from __future__ import annotations

import pytest

from aoc_spark.y2024 import day01, day02, day03, day04, day05

DAY01 = """\
3   4
4   3
2   5
1   3
3   9
3   3
"""

DAY02 = """\
7 6 4 2 1
1 2 7 8 9
9 7 6 2 1
1 3 2 4 5
8 6 4 4 1
1 3 6 7 9
"""

DAY03_PART1 = "xmul(2,4)%&mul[3,7]!@^do_not_mul(5,5)+mul(32,64]then(mul(11,8)mul(8,5))"
DAY03_PART2 = "xmul(2,4)&mul[3,7]!^don't()_mul(5,5)+mul(32,64](mul(11,8)undo()?mul(8,5))"

DAY04 = """\
MMMSXXMASM
MSAMXMSMSA
AMXSXMAAMM
MSAMASMSMX
XMASAMXAMM
XXAMMXXAMA
SMSMSASXSS
SAXAMASAAA
MAMMMXMMMM
MXMXAXMASX
"""

DAY05 = """\
47|53
97|13
97|61
97|47
75|29
61|13
75|53
29|13
97|29
53|29
61|53
97|53
61|29
47|13
75|47
97|75
47|61
75|61
47|29
75|13
53|13

75,47,61,53,29
97,61,53,29,13
75,29,13
75,97,47,61,53
61,13,29
97,13,75,29,47
"""


@pytest.mark.parametrize(
    ("module", "data", "expected"),
    [
        (day01, DAY01, 11),
        (day02, DAY02, 2),
        (day03, DAY03_PART1, 161),
        (day04, DAY04, 18),
        (day05, DAY05, 143),
    ],
    ids=["day01", "day02", "day03", "day04", "day05"],
)
def test_part1(spark, module, data, expected):
    assert module.part1(spark, data) == expected


@pytest.mark.parametrize(
    ("module", "data", "expected"),
    [
        (day01, DAY01, 31),
        (day02, DAY02, 4),
        (day03, DAY03_PART2, 48),
        (day04, DAY04, 9),
        (day05, DAY05, 123),
    ],
    ids=["day01", "day02", "day03", "day04", "day05"],
)
def test_part2(spark, module, data, expected):
    assert module.part2(spark, data) == expected
