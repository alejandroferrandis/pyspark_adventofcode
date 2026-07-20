"""Notebook content spec for 2024 day 13."""

SPEC = {'angle': 'Each machine is a **2×2 linear system**:\n'
          '\n'
          '```\n'
          'a·ax + b·bx = px\n'
          'a·ay + b·by = py\n'
          '```\n'
          '\n'
          'Two equations, two unknowns. Unless the buttons are parallel, there is exactly **one** '
          'real solution, and Cramer\'s rule writes it down without iterating:\n'
          '\n'
          '```\n'
          'det   = ax·by − ay·bx\n'
          'a     = (px·by − py·bx) / det\n'
          'b     = (ax·py − ay·px) / det\n'
          '```\n'
          '\n'
          'That matters more than it looks. The puzzle\'s framing ("no more than 100 presses") '
          'invites a nested loop, and the reference implementation takes that bait — 101 '
          'candidate A-press counts per machine, checked one at a time. It also invites talk of '
          '"cheapest" solution, but with a unique solution there is nothing to minimise: the '
          'only valid answer is the only answer.\n'
          '\n'
          'Cramer\'s rule collapses all of it into **one projection over the whole input** — no '
          'loop, no search, no per-machine control flow. Every machine is a row, every step is a '
          'column expression, and the answer is a single `sum`. This is the shape Spark is '
          'actually good at: arithmetic that is identical across millions of independent rows.\n'
          '\n'
          'The filters afterwards are just the puzzle\'s constraints restated as predicates — '
          '`det != 0` (buttons not parallel), both numerators divisible by `det` (whole presses, '
          'since you cannot press a button 1.5 times), and both counts within `0..100`.',
 'explore_code': 'from pyspark.sql import functions as F\n'
                 '\n'
                 'machines = day13.parse(spark, EXAMPLE)\n'
                 'machines.show()\n'
                 '\n'
                 "det = F.col('ax') * F.col('by') - F.col('ay') * F.col('bx')\n"
                 "a_num = F.col('px') * F.col('by') - F.col('py') * F.col('bx')\n"
                 "b_num = F.col('ax') * F.col('py') - F.col('ay') * F.col('px')\n"
                 '\n'
                 '# Cramer, then the two reasons a machine is unwinnable, side by side.\n'
                 'machines.select(\n'
                 "    det.alias('det'),\n"
                 "    a_num.alias('a_num'),\n"
                 "    b_num.alias('b_num'),\n"
                 "    (a_num / det).alias('a_exact'),\n"
                 "    (b_num / det).alias('b_exact'),\n"
                 "    ((a_num % det == 0) & (b_num % det == 0)).alias('whole_presses'),\n"
                 "    ((a_num / det).between(0, 100) & (b_num / det).between(0, 100)).alias('in_range'),\n"
                 "    (3 * (a_num / det).cast('long') + (b_num / det).cast('long')).alias('cost_if_won'),\n"
                 ').show()',
 'explore_md': '### Cramer, machine by machine\n'
               '\n'
               'Four rows, four solved systems. Two of them come out fractional — those are the '
               'machines the puzzle says can never be won, and `whole_presses` is exactly why.',
 'lesson': 'Spark — Cramer\'s rule as a single vectorised projection',
 'notes': '- Parsing leans on `regexp_extract_all(block, r"\\d+")` returning the six numbers **in '
          'source order**: `ax, ay, bx, by, px, py`. It never looks at the labels. That is fine '
          'for AoC\'s rigid block format and would be fragile against anything else.\n'
          '- The pattern is `\\d+`, **not** `-?\\d+` — day 13 has no negative numbers, unlike day '
          '14. Reusing day 14\'s pattern here would still work; reusing this one there would '
          'silently drop the minus signs.\n'
          '- Values are `LONG`. `px · by` stays small for part 1, but the products are the first '
          'thing to overflow if the coordinates ever grow.\n'
          '- **`det == 0` is filtered before the modulo, not after.** Order matters: `x % 0` '
          'raises under ANSI mode rather than returning null. AoC inputs contain no parallel-button '
          'machines, so the branch never fires, but leaving it out makes correctness depend on '
          'the input.\n'
          '- `(a_num / det)` produces a **double**; the `.cast("long")` afterwards truncates. It '
          'is only safe because the divisibility filter already guaranteed an exact integer. '
          'Casting first and filtering second would round wrong answers into the total.\n'
          '- `int(total or 0)` guards the case where every machine is filtered out — `sum` over '
          'zero rows is null, not 0.\n'
          '- The `0..100` bound is a part 1 constraint from the puzzle text, not a property of '
          'the maths.',
 'summary': 'A list of claw machines, one block each: button **A** moves the claw by some '
            '`(X, Y)`, button **B** by another, and the prize sits at a fixed `(X, Y)`. The claw '
            'starts at the origin and must land **exactly** on the prize.\n'
            '\n'
            'An A press costs **3 tokens**, a B press costs **1**. No button needs pressing more '
            'than **100** times.\n'
            '\n'
            '- **Part 1** — some machines cannot be won at all. Add up the token cost of winning '
            'every machine that can be.',
 'title': 'Claw Contraption'}
