"""2024 Day 17 -- Chronospatial Computer.

A three-bit VM stepping one instruction at a time; Spark adds nothing here.
Part 1 answers with a comma-joined string, not a number.
"""

from __future__ import annotations

import re

from pyspark.sql import SparkSession


def parse(data: str) -> tuple[list[int], list[int]]:
    a, b, c, *program = (int(n) for n in re.findall(r"-?\d+", data))
    return [a, b, c], program


def part1(spark: SparkSession, data: str) -> str:
    reg, program = parse(data)
    out: list[int] = []
    ip = 0

    while ip + 1 < len(program):
        opcode, operand = program[ip], program[ip + 1]
        combo = operand if operand < 4 else reg[operand - 4]

        if opcode == 0:
            reg[0] //= 2**combo
        elif opcode == 1:
            reg[1] ^= operand
        elif opcode == 2:
            reg[1] = combo % 8
        elif opcode == 3 and reg[0] != 0:
            ip = operand
            continue
        elif opcode == 4:
            reg[1] ^= reg[2]
        elif opcode == 5:
            out.append(combo % 8)
        elif opcode == 6:
            reg[1] = reg[0] // 2**combo
        elif opcode == 7:
            reg[2] = reg[0] // 2**combo
        ip += 2

    return ",".join(str(n) for n in out)
