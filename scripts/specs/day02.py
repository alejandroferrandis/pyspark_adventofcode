"""Notebook content spec for 2024 day 02."""

SPEC = {'angle': 'The interesting constraint here is self-imposed: **solve it without a UDF.**\n'
          '\n'
          'A Python UDF would serialise every row out to a Python worker and back, losing an '
          "order of magnitude and blinding the optimiser. Spark's higher-order functions — "
          '`transform`, `zip_with`, `forall`, `exists`, `slice`, `sequence` — run on array '
          'columns *inside the JVM*, so the whole thing stays one native expression tree.\n'
          '\n'
          '- adjacent differences → `zip_with(levels[1:n-1], levels[2:n], (a,b) -> b-a)`\n'
          '- "every step is legal" → `forall(diffs, d -> d >= 1 AND d <= 3)`\n'
          '- "some single removal rescues it" → `exists(variants, ...)`, which short-circuits '
          'on the first success rather than building all *n* variants.',
 'explore_code': 'from pyspark.sql import functions as F\n'
                 '\n'
                 'df = day02.parse(spark, EXAMPLE)\n'
                 "levels = F.col('levels')\n"
                 'size = F.size(levels)\n'
                 '\n'
                 'df.select(\n'
                 "    'levels',\n"
                 '    F.zip_with(\n'
                 '        F.slice(levels, F.lit(1), size - 1),\n'
                 '        F.slice(levels, F.lit(2), size - 1),\n'
                 '        lambda a, b: b - a,\n'
                 "    ).alias('diffs'),\n"
                 "    day02._is_safe(levels).alias('safe'),\n"
                 '    F.exists(day02._dampened_variants(levels), '
                 "day02._is_safe).alias('rescued'),\n"
                 ').show(truncate=False)',
 'explore_md': '### The expression tree, made visible\n'
               '\n'
               'Every intermediate below is a *column*, not a Python value — nothing has been '
               'pulled to the driver yet.',
 'lesson': 'Spark — higher-order array functions (no UDFs)',
 'notes': '- `slice` is **1-based**, which is why `_dampened_variants` uses `i + 2` to skip '
          'element *i*. Off-by-one here is the classic bug — the example is small enough to '
          'check by eye.\n'
          '- Look at the `rescued` column for row `7 6 4 2 1`: it is already safe, so part 2 '
          'must OR the two conditions rather than replacing one with the other.',
 'summary': 'Each line is a *report*: a list of levels.\n'
            '\n'
            '- **Part 1** — a report is **safe** when its levels are either all increasing or '
            'all decreasing, *and* every adjacent step is between 1 and 3 inclusive. Count the '
            'safe reports.\n'
            '- **Part 2** — the Problem Dampener: a report also counts as safe if removing a '
            'single level would make it safe. Count those too.',
 'title': 'Red-Nosed Reports'}
