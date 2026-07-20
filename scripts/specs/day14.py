"""Notebook content spec for 2024 day 14."""

SPEC = {'angle': 'The robots **do not interact** — the puzzle says so outright, they pass over and '
          'under each other. That single sentence removes every reason to simulate.\n'
          '\n'
          'Without interaction, position at time *t* is closed form:\n'
          '\n'
          '```\n'
          'x = (px + t·vx) mod width\n'
          'y = (py + t·vy) mod height\n'
          '```\n'
          '\n'
          'No loop over 100 seconds, no intermediate state. The reference implementation steps '
          'one second at a time and does 100× the work for the same answer.\n'
          '\n'
          'So the whole day is **one projection plus one aggregate**. Quadrant counting is not '
          'even a `groupBy` — there are exactly four buckets known at plan time, so four '
          '`sum(predicate.cast("long"))` expressions in a single `agg` scan the data once and '
          'return one row. A `groupBy` on a derived quadrant column would give the same answer '
          'through a shuffle; this way there is none.\n'
          '\n'
          'The robots exactly on a middle row or column belong to no quadrant. That falls out '
          'for free: the predicates are strict (`< mid` and `> mid`, never `<=`), so a robot on '
          'the centre line satisfies neither and is counted nowhere.',
 'explore_code': 'from pyspark.sql import functions as F\n'
                 '\n'
                 '# The example uses an 11x7 floor, not the real 101x103.\n'
                 'width, height = 11, 7\n'
                 'robots = day14.parse(spark, EXAMPLE)\n'
                 '\n'
                 "raw_x = (F.col('px') + 100 * F.col('vx')) % width\n"
                 "raw_y = (F.col('py') + 100 * F.col('vy')) % height\n"
                 '\n'
                 "# Spark's % keeps the dividend's sign -- watch naive_x go negative.\n"
                 'moved = robots.select(\n'
                 "    'px',\n"
                 "    'py',\n"
                 "    'vx',\n"
                 "    'vy',\n"
                 "    raw_x.alias('naive_x'),\n"
                 "    raw_y.alias('naive_y'),\n"
                 "    ((raw_x + width) % width).alias('x'),\n"
                 "    ((raw_y + height) % height).alias('y'),\n"
                 ')\n'
                 'moved.show()\n'
                 '\n'
                 'mid_x, mid_y = width // 2, height // 2\n'
                 "print('midlines: x =', mid_x, ' y =', mid_y)\n"
                 'moved.select(\n'
                 "    'x',\n"
                 "    'y',\n"
                 '    F.when(F.col("x") == mid_x, F.lit("-"))\n'
                 '    .when(F.col("y") == mid_y, F.lit("-"))\n'
                 '    .otherwise(\n'
                 '        F.concat(\n'
                 '            F.when(F.col("y") < mid_y, F.lit("t")).otherwise(F.lit("b")),\n'
                 '            F.when(F.col("x") < mid_x, F.lit("l")).otherwise(F.lit("r")),\n'
                 '        )\n'
                 '    ).alias("quadrant"),\n'
                 ').groupBy("quadrant").count().orderBy("quadrant").show()',
 'explore_md': '### The double mod, and who gets discarded\n'
               '\n'
               'Compare `naive_x` with `x`. Then read the quadrant table: the `-` bucket is the '
               'robots sitting on a midline, which the safety factor throws away.',
 'lesson': 'Spark — closed-form modular arithmetic + conditional aggregation',
 'notes': '- **The double mod is the bug this day is famous for.** Spark\'s `%` follows SQL and '
          'keeps the sign of the **dividend**, so `-7 % 101` is `-7`, not `94`. The input has '
          'negative velocities, `px + 100·vx` goes deeply negative, and a single `%` leaves it '
          'there. `((v % n) + n) % n` is the fix — the second mod is not redundant, it is what '
          'normalises the negative branch.\n'
          '- The regex is `-?\\d+`. Drop the `-?` and the minus signs are silently lost, every '
          'velocity becomes positive, and the answer is wrong without any error.\n'
          '- Grid size is a **parameter**, not a constant, because the published example runs on '
          '11×7 while the real input is 101×103. Running the example at the default 101×103 '
          'gives a wrong answer with no complaint.\n'
          '- Both dimensions are **odd**, so `width // 2` is a genuine middle column with an '
          'equal number of columns either side. The strict `<` / `>` predicates depend on this; '
          'on an even grid there is no middle line to exclude and the split would need '
          'rethinking.\n'
          '- Quadrant sums are `.cast("long")` on booleans — `true` → 1, `false` → 0. Under ANSI '
          'mode boolean-to-long is still a legal cast.\n'
          '- `int(quadrants[name] or 0)` handles an empty quadrant, where `sum` returns null '
          'rather than 0. One null would otherwise poison the whole product.\n'
          '- Positions are `INT`. `px + 100·vx` stays far inside 32 bits at t=100; a much larger '
          '*t* would not.',
 'summary': 'A list of robots on a wrapping grid — the real floor is **101 tiles wide and 103 '
            'tall** (the published example uses 11×7). Each line gives a starting position '
            '`p=x,y` and a velocity `v=x,y` in tiles per second, where `x` counts from the left '
            'and `y` from the top.\n'
            '\n'
            'Robots move in straight lines, teleport to the opposite edge when they run off one '
            'side, and ignore each other completely — several can share a tile.\n'
            '\n'
            '- **Part 1** — run 100 seconds, split the floor into four quadrants, and count the '
            'robots in each. Robots sitting exactly on the middle row or middle column count for '
            'no quadrant. The **safety factor** is the four counts multiplied together.',
 'title': 'Restroom Redoubt'}
