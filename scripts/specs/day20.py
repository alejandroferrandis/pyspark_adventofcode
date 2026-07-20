"""Notebook content spec for 2024 day 20."""

SPEC = {
    'angle': 'This day splits cleanly into a small sequential part and a large relational part, '
             'and the honest solution is a **hybrid** rather than a purist one either way.\n'
             '\n'
             '**The sequential half — labelling the corridor.** The puzzle guarantees a single '
             'path from `S` to `E` with no branches. So there is no search here at all: start '
             'at `S`, and at each step take the one track neighbour that is not where you just '
             'came from. That walk is inherently sequential — step *k+1* is defined by step *k* '
             '— but it is also *linear in the number of track cells* and touches each exactly '
             'once. 9317 cells on the real input, a few milliseconds of Python. There is '
             'nothing to distribute; a Spark version would be one job per step.\n'
             '\n'
             '**The relational half — enumerating cheats.** This is the bulk of the work and it '
             'is a pure join. Once every track cell carries its distance `d` from the start, a '
             'cheat is just a pair of track cells exactly 2 moves apart, and the time saved is '
             '`d1 - d0 - 2`. Explode each cell against the **8 offsets at Manhattan distance '
             '2** — `(±2,0)`, `(0,±2)`, and the four diagonals `(±1,±1)` — and equi-join back '
             'onto the cell table on `(er, ec)`. Cells that land in a wall or off the grid '
             'simply find no match and drop out; no bounds checking needed, the join does it. '
             'Then filter on the saving and count.\n'
             '\n'
             'The framing is worth the trip: 9317 cells × 8 offsets is ~75000 candidate rows '
             'joined against 9317, which is a shape Spark handles without thinking. And '
             '"disable collisions for 2 picoseconds" — a rule about *movement through walls* — '
             'has been reduced to an **offset table**, exactly like the direction table on day '
             '4. The walls never enter the computation at all; they are the absence of rows.',
    'explore_code': 'from pyspark.sql import functions as F\n'
                    '\n'
                    'dist = day20._distances(EXAMPLE)\n'
                    "print(f'{len(dist)} track cells, finish line at distance "
                    "{max(dist.values())}')\n"
                    '\n'
                    '# The corridor, labelled with distance-from-start mod 10. One walk, no '
                    'search.\n'
                    "grid = EXAMPLE.strip().splitlines()\n"
                    'for r, row in enumerate(grid):\n'
                    "    print(''.join(str(dist[(r, c)] % 10) if (r, c) in dist else '#' "
                    'for c in range(len(row))))\n'
                    '\n'
                    'cells = spark.createDataFrame(\n'
                    "    [(r, c, d) for (r, c), d in dist.items()], 'r INT, c INT, d INT'\n"
                    ')\n'
                    'offsets = F.array(\n'
                    "    *[F.struct(F.lit(dr).alias('dr'), F.lit(dc).alias('dc')) for dr, dc "
                    'in day20.CHEATS]\n'
                    ')\n'
                    'starts = cells.select(\n'
                    "    F.col('r').alias('sr'), F.col('c').alias('sc'), "
                    "F.col('d').alias('d0'), F.explode(offsets).alias('off')\n"
                    ').select(\n'
                    "    'sr',\n"
                    "    'sc',\n"
                    "    (F.col('sr') + F.col('off.dr')).alias('er'),\n"
                    "    (F.col('sc') + F.col('off.dc')).alias('ec'),\n"
                    "    'd0',\n"
                    ')\n'
                    "ends = cells.select(F.col('r').alias('er'), F.col('c').alias('ec'), "
                    "F.col('d').alias('d1'))\n"
                    '\n'
                    "print(f'candidate rows before the join: {starts.count()} "
                    "({cells.count()} cells x {len(day20.CHEATS)} offsets)')\n"
                    '\n'
                    "cheats = starts.join(ends, on=['er', 'ec']).withColumn(\n"
                    "    'saved', F.col('d1') - F.col('d0') - 2\n"
                    ").filter(F.col('saved') > 0)\n"
                    '\n'
                    "cheats.orderBy(F.desc('saved')).select('sr', 'sc', 'er', 'ec', 'd0', "
                    "'d1', 'saved').show(6)\n"
                    '\n'
                    "# The puzzle's own histogram, reproduced as a groupBy.\n"
                    "cheats.groupBy('saved').count().orderBy('saved').show(20)",
    'explore_md': '### One walk, then one join\n'
                  '\n'
                  'First the corridor with every cell labelled by its distance from `S` (mod '
                  '10) — that is the sequential half, and you can trace the single path by eye. '
                  "Then the cheats as join output, ending in the puzzle's published histogram: "
                  '14 cheats saving 2 picoseconds, 14 saving 4, and so on.',
    'lesson': 'Spark — cheat enumeration as a self-join on Manhattan-distance-2 offsets',
    'notes': '- **The single-corridor assumption is load-bearing.** `_distances` does not '
             'search — it walks, picking the one neighbour that is not `prev`. If the track '
             'ever branched, `next(...)` would silently pick whichever neighbour came first in '
             '`NEIGHBOURS` and label the rest wrong. The puzzle states there is only one path; '
             'this code depends on it completely.\n'
             '- The walk terminates when no unvisited neighbour exists, i.e. at the dead end '
             'that is `E`. It never checks for `E` explicitly, so a track that continued past '
             'the end tile would keep labelling.\n'
             '- `CHEATS` is the 8 offsets at **Manhattan distance exactly 2**, which is why the '
             'saving is `d1 - d0 - 2` — every one of them costs 2 picoseconds. Manhattan '
             'distance *1* pairs are ordinary moves, not cheats, and are correctly absent.\n'
             '- **Direction matters, and both directions are enumerated.** The explode produces '
             'each ordered pair twice — once as `(a → b)` and once as `(b → a)` — and the '
             'filter `d1 - d0 - 2 >= threshold` keeps only the one that goes forwards. A cheat '
             'is identified by its start *and* end, so this is exactly the puzzle\'s counting '
             'rule, not a double-count.\n'
             '- No bounds or wall checks anywhere: an offset landing in a wall or off the grid '
             'finds no matching row in `ends` and disappears. That is the payoff of modelling '
             'walls as absent rows rather than as a condition.\n'
             '- `threshold` is a parameter defaulting to 100 (the real question); the example '
             'is run with `threshold=2` because a 15×15 maze has no cheat saving 100. The '
             "filter is `>=`, matching the puzzle's \"at least\".\n"
             '- Cells are keyed `(row, col)` throughout, unlike day 18\'s `x,y`. Mixing the two '
             'conventions between days is an easy way to lose an afternoon.',
    'summary': 'A racetrack: walls (`#`) and track (`.`), with `S` and `E` both counting as '
               'track. A program runs from `S` to `E`, one orthogonal move per picosecond. '
               'Crucially, **there is exactly one path** — no branches, no choices — so the '
               'honest race time is fixed.\n'
               '\n'
               'To make it interesting, a program may **cheat exactly once**: disable collision '
               'for up to 2 picoseconds, passing through walls, and it must be standing on '
               'normal track again when the cheat ends. Each cheat is identified by its start '
               'and end position, so two routes between the same pair of cells are the same '
               'cheat.\n'
               '\n'
               '- **Part 1** — count the cheats that save at least 100 picoseconds.\n'
               '\n'
               'Because the track is a single corridor, "time saved" has a clean meaning: if a '
               'cheat jumps from a cell at distance `d0` to one at distance `d1` and costs 2 '
               'picoseconds, it saves `d1 - d0 - 2`.',
    'title': 'Race Condition',
}
