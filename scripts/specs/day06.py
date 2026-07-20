"""Notebook content spec for 2024 day 06."""

SPEC = {'angle': 'This one is solved in **plain Python inside a Spark project**, and that is a '
          'deliberate call rather than a shortcut.\n'
          '\n'
          'The guard\'s rule set is two lines long, but it is a *state machine*: her position '
          'and facing at step *k* are a function of her position and facing at step *k−1*. '
          'There is no row of a "guard positions" table that can be computed without first '
          'computing the row above it, and no key to partition on — the whole puzzle is one '
          'chain 6082 steps long, with 158 turns in it.\n'
          '\n'
          'What would a Spark version actually cost? The best relational framing is the one '
          '`reference_python/y2024/day06.py` uses: stop stepping cell by cell and *jump to the '
          'next obstacle*, so the patrol becomes 158 legs instead of 6082 steps. Each leg is a '
          'query — "the nearest `#` in this column above this row" — over a 130×130 = 16,900 '
          'row cell relation, and the answer has to come back to the driver before the next '
          'leg can even be described. That is 158 round trips through Spark Connect. At the '
          'tens of milliseconds a trivial Spark job costs, that is several seconds of pure '
          'latency; the Python loop finishes the entire patrol in **2.2 ms**.\n'
          '\n'
          'Iterating a frontier in Spark pays off when each round does a *lot* of work in '
          'parallel — day 10 in this repo is exactly that shape. Here each round decides the '
          'location of a single guard. There is nothing to parallelise, so the distributed '
          'framing buys latency and buys back nothing.',
 'explore_code': 'from aoc_spark.y2024.day06 import TURNS\n'
                 '\n'
                 'grid = EXAMPLE.strip().splitlines()\n'
                 'height, width = len(grid), len(grid[0])\n'
                 "obstacles = {(r, c) for r, line in enumerate(grid) for c, ch in "
                 "enumerate(line) if ch == '#'}\n"
                 "start = next((r, c) for r, line in enumerate(grid) for c, ch in "
                 "enumerate(line) if ch == '^')\n"
                 "print(f'{height}x{width} grid, {len(obstacles)} obstacles, start at "
                 "{start}')\n"
                 '\n'
                 '# Replay the walk, recording every turn. Each entry below is only reachable\n'
                 '# because the entry above it already happened.\n'
                 'r, c = start\n'
                 'd = 0\n'
                 'seen = {start}\n'
                 'turns = []\n'
                 'while True:\n'
                 '    dr, dc = TURNS[d]\n'
                 '    nr, nc = r + dr, c + dc\n'
                 '    if not (0 <= nr < height and 0 <= nc < width):\n'
                 '        break\n'
                 '    if (nr, nc) in obstacles:\n'
                 '        turns.append((len(seen), (r, c), TURNS[d], TURNS[(d + 1) % 4]))\n'
                 '        d = (d + 1) % 4\n'
                 '        continue\n'
                 '    r, c = nr, nc\n'
                 '    seen.add((r, c))\n'
                 '\n'
                 "print(f'{len(turns)} turns, {len(seen)} distinct positions')\n"
                 'for n_seen, at, facing, now in turns[:6]:\n'
                 "    print(f'  after {n_seen:2d} cells seen: blocked at {at}, {facing} -> "
                 "{now}')\n"
                 '\n'
                 '# The path, overlaid on the map -- compare with the puzzle\'s "X" diagram.\n'
                 'print()\n'
                 'for rr in range(height):\n'
                 "    print(''.join('#' if (rr, cc) in obstacles else 'X' if (rr, cc) in seen "
                 "else '.'\n"
                 '                  for cc in range(width)))',
 'explore_md': '### The state you cannot skip ahead in\n'
               '\n'
               'The cell below replays the patrol and prints the first few turns in order. '
               'Read them as a dependency chain: the map alone does not tell you where turn 4 '
               'happens — turns 1 through 3 do.',
 'lesson': 'Python — sequential guard walk',
 'notes': '- `TURNS` is ordered up, right, down, left, which is the *only* reason `(d + 1) % 4` '
          'means "turn right". Reorder that list and the guard quietly starts turning left.\n'
          '- The turn branch `continue`s **without moving**. A guard boxed in on two sides '
          'turns twice on the spot; folding turn-and-step into one iteration breaks exactly '
          'that case.\n'
          '- `seen` is seeded with `start` before the loop, because the puzzle counts the '
          'starting cell. Seed it empty and every answer is one low.\n'
          '- Assumes a rectangular grid — `width` is taken from row 0 and applied to all rows — '
          'and exactly one `^`. `next(...)` raises `StopIteration` if the start marker is '
          'missing.\n'
          '- Assumes the guard **leaves**. There is no step budget, so a map that traps her in '
          'a cycle hangs the notebook rather than erroring. That is safe for part 1 by '
          'construction; it is not a property to rely on if you start editing the map.\n'
          '- `data.strip()` removes the trailing newline. It would also eat leading blank '
          'lines, which would shift every row index — the real input has none.',
 'summary': 'A map of the lab. `#` marks an obstruction, `^` is a guard standing on an open '
            'cell and facing up, and `.` is open floor.\n'
            '\n'
            'The guard follows one rule on repeat: if the cell directly ahead is blocked, turn '
            'right 90°; otherwise step into it. Sooner or later she walks off the edge of the '
            'map.\n'
            '\n'
            '- **Part 1** — count the distinct cells she stands on before leaving, including '
            'the one she started from.',
 'title': 'Guard Gallivant'}
