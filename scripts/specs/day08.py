"""Notebook content spec for 2024 day 08."""

SPEC = {'angle': 'Same move as day 4: **stop treating the map as a map.** `posexplode` turns it '
          'into an `(r, c, freq)` relation, and from there the puzzle is one join.\n'
          '\n'
          '"Every pair of antennas sharing a frequency" is an **equi-join of the antenna '
          'relation with itself on `freq`**, minus the rows where a cell pairs with itself. The '
          'grouping the puzzle describes never has to be built — the join key *is* the '
          'grouping.\n'
          '\n'
          'The nice detail is using **ordered** pairs rather than combinations. For a pair '
          '*(p, q)* the antinode on q\'s side is `2q − p`; the one on p\'s side is `2p − q`. '
          'Because the self-join emits both `(p, q)` and `(q, p)`, a single expression '
          '`(2*r2 - r, 2*c2 - c)` produces both antinodes. An unordered-pairs framing would '
          'need two expressions and a `union`.\n'
          '\n'
          'Everything after that is filtering: keep the antinodes inside the grid, `distinct()` '
          'because different pairs can aim at the same cell, `count()`. The join itself is '
          'trivially small — 164 antennas across 42 frequencies, at most 4 sharing any one '
          'frequency — so the whole thing is a few hundred rows.',
 'explore_code': 'from pyspark.sql import functions as F\n'
                 '\n'
                 'grid = EXAMPLE.strip().splitlines()\n'
                 'height, width = len(grid), len(grid[0])\n'
                 '\n'
                 'a = day08.antennas(spark, EXAMPLE)\n'
                 "a.groupBy('freq').agg(\n"
                 "    F.count('*').alias('n'),\n"
                 "    F.sort_array(F.collect_list(F.struct('r', 'c'))).alias('at'),\n"
                 ").orderBy('freq').show(truncate=False)\n"
                 '\n'
                 '# The self-join: one row per *ordered* pair sharing a frequency.\n'
                 "b = a.select(F.col('r').alias('r2'), F.col('c').alias('c2'), "
                 "F.col('freq').alias('freq2'))\n"
                 'pairs = a.join(\n'
                 '    b,\n'
                 "    (F.col('freq') == F.col('freq2'))\n"
                 "    & ((F.col('r') != F.col('r2')) | (F.col('c') != F.col('c2'))),\n"
                 ')\n'
                 '\n'
                 'antinodes = pairs.select(\n'
                 "    'freq',\n"
                 "    F.struct('r', 'c').alias('from'),\n"
                 "    F.struct('r2', 'c2').alias('over'),\n"
                 "    (2 * F.col('r2') - F.col('r')).alias('ar'),\n"
                 "    (2 * F.col('c2') - F.col('c')).alias('ac'),\n"
                 ').withColumn(\n'
                 "    'in_bounds', F.col('ar').between(0, height - 1) & F.col('ac').between(0, "
                 'width - 1)\n'
                 ')\n'
                 "antinodes.orderBy('freq', 'from', 'over').show(12, truncate=False)\n"
                 '\n'
                 "print('ordered pairs:      ', pairs.count())\n"
                 "print('antinodes in bounds:', antinodes.filter('in_bounds').count())\n"
                 "print('distinct locations: ', "
                 "antinodes.filter('in_bounds').select('ar', 'ac').distinct().count())",
 'explore_md': '### Pairs in, antinodes out\n'
               '\n'
               'Each row below is one ordered pair and the single antinode it projects past '
               '`over`. The three counts at the bottom are the point: more pair-rows than '
               'in-bounds antinodes, and more in-bounds antinodes than distinct locations.',
 'lesson': 'Spark — self-join on frequency',
 'notes': "- `split(s, '')` emits a **trailing empty string**, so `antennas()` filters both "
          "`''` and `'.'`. Drop the empty-string filter and every row gains a phantom antenna "
          'at column `width`.\n'
          '- The join excludes self-pairs by comparing **coordinates**, not identity. That is '
          'correct only because no two antennas occupy the same cell — true of the input, but '
          'it is an assumption.\n'
          '- `between(0, height - 1)` is **inclusive on both ends**, which is what you want for '
          'a 0-indexed grid. `between(0, height)` would admit a phantom row.\n'
          '- Grid dimensions are read in Python from the raw string, not derived from the '
          'DataFrame, so a ragged grid would produce wrong bounds rather than an error.\n'
          '- `distinct()` before `count()` is load-bearing: separate pairs frequently project '
          'onto the same cell. On the published example the in-bounds antinode rows outnumber '
          'the 14 distinct locations.\n'
          '- An antinode sitting **on top of an antenna** still counts — nothing in the '
          'pipeline subtracts antenna cells, and the example depends on that (the topmost `A` '
          'is also a `0`-frequency antinode).\n'
          '- Frequencies are case-sensitive: `A` and `a` are different join keys, which is '
          'automatic here but is the kind of thing a `lower()` somewhere upstream would '
          'destroy.',
 'summary': 'A map of a city block. Every non-`.` cell is an antenna, labelled with its '
            'frequency — a single letter or digit, **case-sensitive**, so `A` and `a` are '
            'unrelated.\n'
            '\n'
            'Take any two antennas on the same frequency. Two *antinodes* form on the line '
            'through them, one beyond each antenna, each placed so that one antenna is exactly '
            'twice as far away as the other. Antinodes may land on top of an antenna, and they '
            'only count when they fall inside the map.\n'
            '\n'
            '- **Part 1** — count the distinct in-bounds locations holding an antinode.',
 'title': 'Resonant Collinearity'}
