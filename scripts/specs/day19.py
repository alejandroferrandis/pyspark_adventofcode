"""Notebook content spec for 2024 day 19."""

SPEC = {
    'angle': 'This is the most genuinely Spark-shaped day of the block, because the expensive '
             'part is **matching**, and matching is a join.\n'
             '\n'
             '**Step 1 — every candidate match in one equi-join.** Explode each design into '
             '`(did, pos)` for every position, then again over `plen` from the shortest to the '
             'longest towel pattern. `substring(design, pos + 1, plen)` — with `pos` and `plen` '
             'as *columns*, not Python values — turns each `(design, position, length)` triple '
             'into a fixed string key. A single equi-join of those keys against the pattern '
             'list resolves **every towel match in the whole input at once**. No per-design '
             'loop, no UDF, one shuffle. `groupBy(did, pos)` then collects the set of lengths '
             'that fit at each position.\n'
             '\n'
             '**Step 2 — the DP as a bitmask fold.** "Can I reach the end of this design?" is a '
             'left-to-right reachability scan, which sounds sequential. It becomes a single '
             '`aggregate` over the positions sorted ascending, carrying a **64-bit integer '
             'whose bit *i* means "position *i* is reachable"**. Seed it with bit 0. At each '
             'step: if bit `pos` is clear, the position is unreachable and the accumulator '
             'passes through untouched; if it is set, a **nested `aggregate`** over that '
             "position's matching lengths ORs in bit `pos + plen` for each. The design is "
             'possible exactly when bit `dlen` ends up set.\n'
             '\n'
             'The nesting is the trick — `aggregate` inside `aggregate`, both native Spark '
             'expressions. The outer fold walks positions in order (that is the sequential '
             'part, and it is fine: it runs inside a single row\'s expression evaluation, not '
             'across a cluster), while the *rows* — the designs — stay fully parallel. Spark is '
             'doing what it is good at, one design per partition slot, and the per-design DP '
             'never leaves the JVM.\n'
             '\n'
             'Positions absent from `steps` need no entry: nothing matches there, so they can '
             'never extend a match, and skipping them keeps the fold short.',
    'explore_code': 'from pyspark.sql import functions as F\n'
                    '\n'
                    'patterns_block, designs_block = EXAMPLE.strip().split(\'\\n\\n\')\n'
                    "designs = designs_block.splitlines()\n"
                    "print('patterns:', patterns_block)\n"
                    '\n'
                    'designs_df, steps = day19._reachable_steps(spark, EXAMPLE)\n'
                    'labels = spark.createDataFrame(list(enumerate(designs)), '
                    "'did INT, design STRING')\n"
                    '\n'
                    '# Every towel match in the input, resolved by one equi-join:\n'
                    "# (did, pos) -> the pattern lengths that fit starting there.\n"
                    "steps.join(labels, on='did').orderBy('did', 'pos').select(\n"
                    "    'did', 'design', 'pos', F.sort_array(F.col('lens')).alias('lens')\n"
                    ').show(24)\n'
                    '\n'
                    '# The bitmask fold, exposed in binary. Bit i set = position i is '
                    'reachable.\n'
                    "ordered = steps.groupBy('did').agg(\n"
                    "    F.sort_array(F.collect_list(F.struct('pos', 'lens'))).alias('steps')\n"
                    ')\n'
                    '\n'
                    'def bit(index):\n'
                    "    return F.shiftleft(F.lit(1).cast('long'), index)\n"
                    '\n'
                    'reached = F.aggregate(\n'
                    "    F.col('steps'),\n"
                    '    bit(F.lit(0)),\n'
                    '    lambda acc, step: F.when(acc.bitwiseAND(bit(step[\'pos\'])) == 0, acc)'
                    '.otherwise(\n'
                    "        F.aggregate(step['lens'], acc, lambda a, plen: "
                    "a.bitwiseOR(bit(step['pos'] + plen)))\n"
                    '    ),\n'
                    ')\n'
                    '\n'
                    "ordered.join(designs_df, on='did').join(labels, on='did').select(\n"
                    "    'design',\n"
                    "    'dlen',\n"
                    "    F.lpad(F.bin(reached), 16, '0').alias('reachable_bits'),\n"
                    "    (reached.bitwiseAND(bit(F.col('dlen'))) != 0).alias('possible'),\n"
                    ").orderBy('did').show(truncate=False)",
    'explore_md': '### The join, then the fold\n'
                  '\n'
                  'The first table is every towel match in the input — one row per '
                  '`(design, position)` with the pattern lengths that fit there, all of it out '
                  'of a single equi-join. The second shows the fold\'s result as raw bits: read '
                  '`reachable_bits` right-to-left, and a design is possible when the bit at '
                  'index `dlen` is set.',
    'lesson': 'Spark — prefix-key equi-join + a nested `aggregate` bitmask DP',
    'notes': '- **The bitmask assumes designs are shorter than 63 characters.** The accumulator '
             'is a Spark `long`, so bit `dlen` must fit in a signed 64-bit integer. The real '
             'input tops out at 60 characters, which is why this works — but a longer design '
             'would shift past the sign bit and the design would be judged impossible with **no '
             'error raised**. This fails silently. If you point this at other input, check '
             '`max(len(d))` first.\n'
             '- `substring` is **1-based**, hence `pos + 1`. The `plen` range is bounded by the '
             'shortest and longest actual pattern, so no wasted keys are generated — but note '
             'that near the end of a design `substring` returns a *short* string, which simply '
             'fails to equal any pattern of that length. That is why an equal-length key match '
             'implies the pattern genuinely fits.\n'
             '- `sort_array` on an array of `struct(pos, lens)` sorts by `pos` first, which is '
             'exactly the ascending order the fold needs. That is load-bearing: fold the '
             'positions out of order and reachability propagates wrongly. It works because '
             '`pos` is the struct\'s first field.\n'
             '- The seed is `bit(0)`, i.e. "position 0 is reachable for free" — the empty '
             'prefix. Seeding with 0 makes every design impossible.\n'
             '- The `when(... == 0, acc)` guard is what makes this a correct DP rather than a '
             'greedy scan: an unreachable position must not propagate its matches forward.\n'
             '- Patterns are split on `,` and stripped, so the single space after each comma in '
             'the input is handled. A pattern list with duplicates would produce duplicate join '
             'rows, but `collect_set` on `plen` collapses them.\n'
             '- Part 1 only asks *whether* each design is possible, so a bitmask (one bit per '
             'position) suffices. Counting arrangements would need a per-position integer '
             'instead.',
    'summary': 'Two blocks. First, a comma-separated list of available towel patterns — short '
               'strings over the stripe colours `w`, `u`, `b`, `r`, `g`. Then, after a blank '
               'line, one desired design per line.\n'
               '\n'
               'Every pattern is available in unlimited supply, and towels are laid left to '
               'right with no gaps, overlaps or reversals. So a design is **possible** exactly '
               'when it can be written as a concatenation of patterns.\n'
               '\n'
               '- **Part 1** — count how many of the designs are possible.\n'
               '\n'
               'In the example, 6 of the 8 designs can be built; `ubwu` and `bbrgwb` cannot.',
    'title': 'Linen Layout',
}
