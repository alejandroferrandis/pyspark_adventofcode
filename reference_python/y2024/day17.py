"""2024 Day 17 -- Chronospatial Computer, cross-check.

Opcode dispatch table over a register dict, with division as a right shift.
"""

from __future__ import annotations


def _registers(block: str) -> dict[str, int]:
    return {
        line.split(":")[0].split()[-1]: int(line.split(":")[1])
        for line in block.splitlines()
    }


def part1(data: str) -> str:
    reg_block, prog_block = data.strip("\n").split("\n\n")
    reg = _registers(reg_block)
    program = [int(n) for n in prog_block.split(":")[1].split(",")]

    out: list[int] = []
    ip = 0

    def combo(operand: int) -> int:
        return operand if operand < 4 else reg["ABC"[operand - 4]]

    handlers = {
        0: lambda o: reg.__setitem__("A", reg["A"] >> combo(o)),
        1: lambda o: reg.__setitem__("B", reg["B"] ^ o),
        2: lambda o: reg.__setitem__("B", combo(o) & 7),
        4: lambda o: reg.__setitem__("B", reg["B"] ^ reg["C"]),
        5: lambda o: out.append(combo(o) & 7),
        6: lambda o: reg.__setitem__("B", reg["A"] >> combo(o)),
        7: lambda o: reg.__setitem__("C", reg["A"] >> combo(o)),
    }

    while ip + 1 < len(program):
        opcode, operand = program[ip], program[ip + 1]
        if opcode == 3:
            if reg["A"]:
                ip = operand
                continue
        else:
            handlers[opcode](operand)
        ip += 2

    return ",".join(map(str, out))
