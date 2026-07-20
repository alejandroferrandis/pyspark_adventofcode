"""Notebook content spec for 2024 day 16."""

SPEC = {
    'angle': 'This one is Python, and the reason is worth being precise about.\n'
             '\n'
             'Dijkstra is **sequentially dependent by construction**: you cannot settle the '
             'next state until you know which unsettled state currently has the lowest cost. '
             'That is a global minimum over the frontier, recomputed after every single pop. A '
             'local binary heap does it in `O(log n)` per step with no coordination.\n'
             '\n'
             'The Spark framing exists — it is iterative join-based relaxation. Keep a '
             '`dist(r, c, h)` frame, join it to a transitions frame, `groupBy` to take the min '
             'cost per state, and loop until nothing improves. Each iteration is a **shuffle '
             'plus an action to test the fixpoint**, so it is one Spark job per frontier hop. '
             'The real maze — 141×141, ~10000 open tiles, ~40000 settled states — runs hundreds '
             'of rounds before the costs stop changing, each paying scheduler latency and a '
             'full shuffle of a frame with a few thousand rows.\n'
             '\n'
             'So the ledger is: hundreds of round-trips to a cluster, versus ~70 ms of heap '
             'operations on the driver, for the same number. There is no data-size argument on '
             'the other side — the whole state space is `open tiles × 4 headings`, low tens of '
             'thousands of rows. Spark would be paying distribution costs to solve a problem '
             'that never needed distributing.',
    'explore_code': 'import heapq\n'
                    'import time\n'
                    '\n'
                    "grid = EXAMPLE.strip('\\n').splitlines()\n"
                    'start = next((r, c) for r, row in enumerate(grid) for c, ch in '
                    "enumerate(row) if ch == 'S')\n"
                    'end = next((r, c) for r, row in enumerate(grid) for c, ch in '
                    "enumerate(row) if ch == 'E')\n"
                    "open_tiles = sum(row.count('.') for row in grid) + 2  # S and E are open "
                    'too\n'
                    "print(f'{len(grid)}x{len(grid[0])} maze, {open_tiles} open tiles -> "
                    "{open_tiles * 4} (tile, heading) states')\n"
                    '\n'
                    '# The same Dijkstra as part1, instrumented: every distinct cost pulled off\n'
                    '# the heap is one frontier hop, i.e. one Spark job in the iterative-join '
                    'framing.\n'
                    'started = time.perf_counter()\n'
                    'queue = [(0, start[0], start[1], 0)]\n'
                    'best = {}\n'
                    'hops, last_cost, cost = 0, -1, None\n'
                    'while queue:\n'
                    '    cost, r, c, h = heapq.heappop(queue)\n'
                    '    if (r, c) == end:\n'
                    '        break\n'
                    '    if best.get((r, c, h), cost + 1) <= cost:\n'
                    '        continue\n'
                    '    if cost != last_cost:\n'
                    '        hops, last_cost = hops + 1, cost\n'
                    '    best[(r, c, h)] = cost\n'
                    '    dr, dc = day16.HEADINGS[h]\n'
                    "    if grid[r + dr][c + dc] != '#':\n"
                    '        heapq.heappush(queue, (cost + 1, r + dr, c + dc, h))\n'
                    '    for turn in (1, 3):\n'
                    '        heapq.heappush(queue, (cost + day16.TURN, r, c, (h + turn) % 4))\n'
                    'elapsed = (time.perf_counter() - started) * 1000\n'
                    '\n'
                    "print(f'settled {len(best)} states over {hops} distinct cost levels')\n"
                    "print(f'answer {cost} in {elapsed:.1f} ms on the driver')\n"
                    "print(f'-> {hops} coordination points: each is a global minimum over the "
                    "frontier,')\n"
                    "print('   which is one shuffle + one action if the frontier lives in a "
                    "DataFrame')",
    'explore_md': '### The cost of distributing this\n'
                  '\n'
                  'The cell below runs the same Dijkstra as `part1`, but counts the frontier '
                  'hops. Each distinct cost level is a point where the algorithm needs a global '
                  'minimum over the whole frontier — a coordination barrier a distributed '
                  'version would pay a job for. The whole search finishes on the driver in '
                  'milliseconds; the real maze takes ~70 ms.',
    'lesson': 'Python — Dijkstra over (tile, heading)',
    'notes': '- The state is **`(row, col, heading)`, not `(row, col)`**. Arriving at a tile '
             'facing north is a different state from arriving facing east, because the 1000-'
             'point turn cost you owe next depends on it. Collapsing the heading out of the key '
             'gives wrong answers on mazes where the cheap approach faces the wrong way.\n'
             '- `HEADINGS[0]` is East, matching the puzzle\'s "the Reindeer starts facing '
             'East". Reorder that list and the start orientation silently changes.\n'
             '- Turns are pushed **unconditionally**, without checking whether the new heading '
             'faces a wall. That is safe — a turn into a wall just settles a state that can '
             'never step forward — and it keeps the inner loop branch-free. Only two turns are '
             'pushed (`+1` and `+3`); a 180° reversal is reachable as two 90° turns at the same '
             'cost, so enumerating it would be redundant.\n'
             '- The `best.get(..., cost + 1) <= cost` guard is the lazy-deletion idiom: stale '
             'heap entries are skipped on pop rather than removed on push. Without it the loop '
             'still terminates but re-expands settled states.\n'
             '- Returning on **pop** of the end tile, not on push, is what makes the first hit '
             'optimal. Checking at push time would return the first path found, not the '
             'cheapest.\n'
             '- The walk indexes `grid[r + dr][c + dc]` with no bounds check. That is safe only '
             'because AoC mazes are fully wall-bordered — the reindeer can never step off the '
             'edge. An unbordered grid would raise `IndexError` or, worse, wrap on a negative '
             'index.',
    'summary': 'A maze of walls (`#`) and open tiles (`.`), with a start `S` and an end `E`. A '
               'reindeer begins on `S` **facing East**.\n'
               '\n'
               'Two moves are available, and they cost different amounts:\n'
               '\n'
               '- step forward one tile — **1 point**\n'
               '- rotate 90° clockwise or counterclockwise in place — **1000 points**\n'
               '\n'
               '- **Part 1** — find the lowest total score of any route from `S` to `E`.\n'
               '\n'
               'The 1000:1 ratio is the whole puzzle. A route that is longer in tiles but '
               'straighter can easily beat a shorter, twistier one, so this is not a shortest-'
               'path problem on the grid — it is a shortest-path problem on `(tile, heading)`.',
    'title': 'Reindeer Maze',
}
