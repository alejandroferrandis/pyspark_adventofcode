"""2024 day 17 against AoC's published examples.

Part 1 answers with a comma-joined string, so the expectations are strings.
"""

from __future__ import annotations

import pytest

from aoc_spark.y2024 import day17 as spark_day17
from reference_python.y2024 import day17 as py_day17

MAIN = """\
Register A: 729
Register B: 0
Register C: 0

Program: 0,1,5,4,3,0
"""

OUT_012 = """\
Register A: 10
Register B: 0
Register C: 0

Program: 5,0,5,1,5,4
"""

LOOP_2024 = """\
Register A: 2024
Register B: 0
Register C: 0

Program: 0,1,5,4,3,0
"""

CASES = [
    (MAIN, "4,6,3,5,6,3,5,2,1,0"),
    (OUT_012, "0,1,2"),
    (LOOP_2024, "4,2,5,6,7,7,7,7,3,1,0"),
]


@pytest.mark.parametrize(("data", "expected"), CASES)
def test_part1_spark(spark, data, expected):
    assert spark_day17.part1(spark, data) == expected


@pytest.mark.parametrize(("data", "expected"), CASES)
def test_part1_reference(data, expected):
    assert py_day17.part1(data) == expected
