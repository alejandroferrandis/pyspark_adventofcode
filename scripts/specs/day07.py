"""Notebook content spec for 2024 day 07."""

SPEC = {'angle': 'The obvious reading is "try every operator string": *n* operands means 2^(n−1) '
          'combinations, and the longest line here has **12 operands** — 2048 expressions to '
          'evaluate, per line, for 850 lines.\n'
          '\n'
          'The reframing is to stop enumerating *operator strings* and start carrying **the set '
          'of values reachable so far**. After consuming *k* operands there are at most 2^(k−1) '
          'distinct partial results, and usually far fewer — that set is all the state the rest '
          'of the line needs.\n'
          '\n'
          'Which is a fold, and Spark has one that runs on array columns: `aggregate`. Seed '
          'with `array(nums[0])`, and at each operand map every partial *v* to `{v+x, v*x}`, '
          '`flatten`, `array_distinct`. No UDF, no `collect` — the whole line is one native '
          'expression tree, and the driver only ever sees the final `sum`.\n'
          '\n'
          'Two things keep the accumulator small:\n'
          '\n'
          '- **Pruning.** Every operand in this input is ≥ 1, so both `+` and `*` are '
          'monotonically non-decreasing — a partial that has already passed the target can '
          'never come back down. `filter(v <= target)` drops it immediately, which is what '
          'stops the 12-operand lines from ever materialising 2048 partials.\n'
          '- **Dedup.** `array_distinct` collapses branches that converge. 356 of the operands '
          'are `1`, where `v*1 == v` merges straight back into the `+` branch, and cases like '
          '`81+40*27` vs `81*40+27` both landing on 3267 collapse too.',
 'explore_code': 'from pyspark.sql import functions as F\n'
                 '\n'
                 'eq = day07.parse(spark, EXAMPLE)\n'
                 'eq.show(truncate=False)\n'
                 '\n'
                 '\n'
                 'def reachable_after(k):\n'
                 '    """Partials after folding in the first k operands -- no pruning."""\n'
                 '    return F.aggregate(\n'
                 "        F.slice(F.col('nums'), 2, k - 1),\n"
                 "        F.array(F.col('nums').getItem(0)),\n"
                 '        lambda acc, x: F.array_distinct(\n'
                 '            F.flatten(F.transform(acc, lambda v: F.array(v + x, v * x)))\n'
                 '        ),\n'
                 '    )\n'
                 '\n'
                 '\n'
                 '# One line, one operand at a time: watch the reachable set fan out.\n'
                 "eq.filter(F.size('nums') == 4).select(\n"
                 "    'target',\n"
                 "    'nums',\n"
                 "    *[reachable_after(k).alias(f'after_{k}') for k in range(1, 5)],\n"
                 ').show(truncate=False)\n'
                 '\n'
                 '# Now the real thing: pruned at every step, then tested for the target.\n'
                 'ops = [lambda v, x: v + x, lambda v, x: v * x]\n'
                 'reachable = day07._reachable(ops)\n'
                 'eq.select(\n'
                 "    'target',\n"
                 "    'nums',\n"
                 "    reachable.alias('reachable'),\n"
                 "    F.size(reachable).alias('n'),\n"
                 "    F.array_contains(reachable, F.col('target')).alias('solvable'),\n"
                 ').show(truncate=False)',
 'explore_md': '### The fold, one operand at a time\n'
               '\n'
               'The first table below is the fold *without* the pruning filter, so you can '
               'watch the reachable set double (and then not quite double, as duplicates '
               'collapse). The second is what the solution actually evaluates — same fold, '
               'clipped at the target on every step.',
 'lesson': 'Spark — aggregate fold over reachable partials',
 'notes': '- `slice` is **1-based**, so `slice(nums, 2, size - 1)` means "every operand after '
          'the first". The first operand is the fold *seed*, not an element of the fold.\n'
          '- The `v <= target` prune is only correct because **every operand is ≥ 1**. A single '
          '`0` would make `v * 0` collapse a too-large partial back into range, and a negative '
          'operand would do the same for `+`. This input has neither (min operand is 1) — check '
          'before reusing the trick.\n'
          '- Targets run past 2^31 (the real answer is a 13-digit number), so `parse` casts to '
          '`bigint` on both sides. Under ANSI mode an `int` overflow **raises** rather than '
          'wrapping, so a missed cast fails loudly instead of quietly.\n'
          "- `split(line, ': ')` takes a **regex**, not a literal. `': '` happens to contain no "
          'metacharacters; a separator like `|` or `.` would silently match everything.\n'
          '- `_reachable` takes the operator list as an argument rather than hardcoding `+` and '
          '`*`. Adding an operator is a one-line change at the call site, not a rewrite of the '
          'fold.',
 'summary': 'Each line is an equation with its operators stolen: a **test value**, a colon, '
            'then a list of operands.\n'
            '\n'
            '```\n'
            '3267: 81 40 27\n'
            '```\n'
            '\n'
            'Slot `+` or `*` into each gap, in any combination. Evaluation is strictly '
            '**left to right** — no operator precedence — and the operands cannot be '
            'reordered. A line is *solvable* if at least one choice of operators produces the '
            'test value; `81 + 40 * 27` and `81 * 40 + 27` both give 3267, so that line counts '
            'once.\n'
            '\n'
            '- **Part 1** — sum the test values of the solvable lines.',
 'title': 'Bridge Repair'}
