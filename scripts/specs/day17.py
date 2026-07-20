"""Notebook content spec for 2024 day 17."""

SPEC = {
    'angle': 'A virtual machine is the archetypal anti-Spark workload, and it is worth naming '
             'why rather than hand-waving at "it\'s sequential".\n'
             '\n'
             'Spark needs **a set of rows whose processing is independent**. A VM has the '
             'opposite shape: there is exactly one row — the machine state `(A, B, C, ip, '
             'output)` — and instruction *n+1* cannot even be *identified* until instruction '
             '*n* has run, because `jnz` decides the next instruction pointer from a register '
             'that the preceding instructions wrote. There is no partitioning key, no set to '
             'fan out over, and no way to speculate ahead. Parallelism of one.\n'
             '\n'
             'You could technically express it as a DataFrame with a single row and loop jobs '
             'until halt — one Spark job per *instruction*, thousands of them, to move three '
             'integers around. The entire program is a handful of shifts, XORs and mod-8s; '
             'CPython runs the whole thing in well under a millisecond.\n'
             '\n'
             'The one implementation detail worth calling out: **part 1 answers with a string**, '
             "not an int — `'7,3,5,7,5,7,4,3,0'`. The `out` instruction emits 3-bit values that "
             'the puzzle asks you to join with commas, so `part1` returns `str`. Anything '
             'downstream that assumes these days return integers will need to cope.',
    'explore_code': "OPS = ['adv', 'bxl', 'bst', 'jnz', 'bxc', 'out', 'bdv', 'cdv']\n"
                    "COMBO = {0: '0', 1: '1', 2: '2', 3: '3', 4: 'A', 5: 'B', 6: 'C', 7: '!'}\n"
                    '\n'
                    'reg, program = day17.parse(EXAMPLE)\n'
                    "print('program:', ','.join(map(str, program)))\n"
                    "print('decoded: ', ' '.join(f'{OPS[program[i]]} {program[i + 1]}' for i in "
                    'range(0, len(program) - 1, 2)))\n'
                    "print(f'start:    A={reg[0]} B={reg[1]} C={reg[2]}')\n"
                    'print()\n'
                    '\n'
                    '# Step the VM by hand, printing state before each instruction. Note how ip\n'
                    '# jumps backwards: instruction n+1 is not knowable until n has executed.\n'
                    "print('step  ip   instr  combo      A   B   C  out')\n"
                    'ip, out, step = 0, [], 0\n'
                    'while ip + 1 < len(program) and step < 18:\n'
                    '    opcode, operand = program[ip], program[ip + 1]\n'
                    '    combo = operand if operand < 4 else reg[operand - 4]\n'
                    "    instr = OPS[opcode] + ' ' + str(operand)\n"
                    "    emitted = ','.join(map(str, out))\n"
                    "    print(f'{step:>4} {ip:>3} {instr:>7} {COMBO[operand]:>5} {reg[0]:>6} "
                    "{reg[1]:>3} {reg[2]:>3}  {emitted}')\n"
                    '    step += 1\n'
                    '    if opcode == 0:\n'
                    '        reg[0] //= 2**combo\n'
                    '    elif opcode == 1:\n'
                    '        reg[1] ^= operand\n'
                    '    elif opcode == 2:\n'
                    '        reg[1] = combo % 8\n'
                    '    elif opcode == 3 and reg[0] != 0:\n'
                    '        ip = operand\n'
                    '        continue\n'
                    '    elif opcode == 4:\n'
                    '        reg[1] ^= reg[2]\n'
                    '    elif opcode == 5:\n'
                    '        out.append(combo % 8)\n'
                    '    elif opcode == 6:\n'
                    '        reg[1] = reg[0] // 2**combo\n'
                    '    elif opcode == 7:\n'
                    '        reg[2] = reg[0] // 2**combo\n'
                    '    ip += 2\n'
                    '\n'
                    'answer = day17.part1(spark, EXAMPLE)\n'
                    "print()\nprint('part 1 returns', repr(answer), '-- a', "
                    "type(answer).__name__ + ', not an int')",
    'explore_md': '### The state dependency, one instruction at a time\n'
                  '\n'
                  'Read the `ip` column downwards. It walks `0, 2, 4` and then **jumps back to '
                  '0** — and which way it jumps is decided by register `A`, which the '
                  'instructions above it just modified. That backward edge is why nothing here '
                  'can be scheduled ahead of time.',
    'lesson': 'Python — a 3-bit virtual machine',
    'notes': "- **Part 1 returns a string**, e.g. `'4,6,3,5,6,3,5,2,1,0'` for the example. Not "
             'an int, not a list. Comparing it to a number will silently be `False`.\n'
             '- `parse` uses `re.findall(r\'-?\\d+\')` across the *whole* input and takes the '
             'first three numbers as A, B, C and the rest as the program. That works because '
             'the AoC format puts the registers first and every remaining number belongs to the '
             "program — but it means the labels (`Register A:`, `Program:`) aren't actually "
             'checked. Reordered input would parse cleanly and give the wrong answer.\n'
             '- **Literal vs combo operands is the classic bug.** `bxl` (1) and `jnz` (3) read '
             'their operand *literally*; `adv`/`bst`/`out`/`bdv`/`cdv` read it as a *combo* '
             'value, where 4/5/6 mean registers A/B/C. Using `combo` for `bxl` gets you a '
             'plausible-looking wrong answer.\n'
             '- `jnz` uses `continue`, deliberately skipping the `ip += 2`. That is the whole '
             'point of a jump — the puzzle states the pointer is *not* advanced when the jump '
             'is taken.\n'
             '- The `elif opcode == 3 and reg[0] != 0` chain is subtle: when `A == 0` the '
             'condition fails, so control falls past every other branch to `ip += 2` and the '
             'jump is correctly ignored. Restructuring that `if`/`elif` ladder into separate '
             'statements breaks it.\n'
             '- Halting is `ip + 1 < len(program)` — reading an opcode *or* its operand past '
             'the end stops the machine. Registers are unbounded Python ints; only the program '
             'values and `out` results are 3-bit.\n'
             '- `combo` operand 7 is reserved and never appears in a valid program, so '
             '`reg[operand - 4]` is never indexed out of range. An invalid program would raise '
             '`IndexError` rather than report anything useful.',
    'summary': 'A tiny 3-bit computer. Three registers `A`, `B`, `C` hold arbitrarily large '
               'integers; the program is a flat list of 3-bit numbers read as alternating '
               '`(opcode, operand)` pairs, with an instruction pointer starting at 0 and '
               'advancing by 2.\n'
               '\n'
               'Operands come in two flavours. A **literal** operand is just its own value. A '
               '**combo** operand means 0–3 literally, and 4/5/6 mean the contents of A/B/C.\n'
               '\n'
               'The eight opcodes:\n'
               '\n'
               '| # | name | effect |\n'
               '|---|------|--------|\n'
               '| 0 | `adv` | `A = A >> combo` (integer divide by `2**combo`) |\n'
               '| 1 | `bxl` | `B ^= literal` |\n'
               '| 2 | `bst` | `B = combo % 8` |\n'
               '| 3 | `jnz` | if `A != 0`, set `ip = literal` and do **not** advance |\n'
               '| 4 | `bxc` | `B ^= C` (reads an operand, ignores it) |\n'
               '| 5 | `out` | emit `combo % 8` |\n'
               '| 6 | `bdv` | `B = A >> combo` |\n'
               '| 7 | `cdv` | `C = A >> combo` |\n'
               '\n'
               'Reading an opcode past the end of the program halts the machine.\n'
               '\n'
               '- **Part 1** — run the program from the given register values and join '
               'everything `out` emitted with commas. The answer is a **string**, not a number.',
    'title': 'Chronospatial Computer',
}
