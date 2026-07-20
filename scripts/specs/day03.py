"""Notebook content spec for 2024 day 03."""

SPEC = {'angle': 'Part 2 is the one worth the trip. "Scan left to right carrying a flag" sounds '
          'hopelessly sequential, and it is exactly where people reach for a UDF or '
          '`collect()`.\n'
          '\n'
          'It is really a **last-non-null-value window** — one of the most reusable patterns '
          'in Spark:\n'
          '\n'
          '```sql\n'
          'last(flag, ignoreNulls => true) OVER (\n'
          '  ORDER BY pos ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n'
          ')\n'
          '```\n'
          '\n'
          "`mul` tokens carry no flag (null); `do()`/`don't()` carry 1/0. The window fills "
          'each mul with the most recent toggle. `coalesce(..., 1)` covers the muls that '
          'appear before any toggle, since multiplication starts enabled.\n'
          '\n'
          'The ordering key comes free: `regexp_extract_all` returns matches **in source '
          'order**, so `posexplode` gives a reliable position without computing character '
          'offsets.',
 'explore_code': 'from pyspark.sql import Window\n'
                 'from pyspark.sql import functions as F\n'
                 '\n'
                 "memory = spark.createDataFrame([(EXAMPLE_PART2,)], 'memory STRING')\n"
                 'tokens = memory.select(\n'
                 '    F.posexplode(\n'
                 "        F.regexp_extract_all(F.col('memory'), F.lit(day03.TOKEN), F.lit(0))\n"
                 "    ).alias('pos', 'token')\n"
                 ')\n'
                 '\n'
                 'toggle = (\n'
                 "    F.when(F.col('token') == 'do()', F.lit(1))\n"
                 '    .when(F.col(\'token\') == "don\'t()", F.lit(0))\n'
                 '    .otherwise(F.lit(None))\n'
                 ')\n'
                 "running = Window.orderBy('pos').rowsBetween(Window.unboundedPreceding, "
                 'Window.currentRow)\n'
                 '\n'
                 'tokens.select(\n'
                 "    'pos',\n"
                 "    'token',\n"
                 "    toggle.alias('toggle'),\n"
                 '    F.coalesce(F.last(toggle, ignorenulls=True).over(running), '
                 "F.lit(1)).alias('enabled'),\n"
                 ').show(truncate=False)',
 'explore_md': '### The window doing the work\n'
               '\n'
               'This is the whole puzzle in one table — read down the `enabled` column and '
               'watch it carry forward from each toggle.',
 'lesson': 'Spark — regex extraction + a stateful window',
 'notes': '- Part 1 and part 2 use **different published examples** — the part 1 example has '
          'no toggles. Running part 2 on the part 1 example gives 161, not 48.\n'
          '- The whole input is loaded as a **single row**, not one row per line: instructions '
          'run across line breaks, so splitting on newlines would corrupt tokens that straddle '
          'them.\n'
          '- This is the day where a single-partition window is genuinely load-bearing. A '
          'partitioned window would reset the enabled state at each partition boundary and '
          'quietly produce a wrong answer.',
 'summary': 'A block of corrupted memory containing valid `mul(X,Y)` instructions buried in '
            'garbage.\n'
            '\n'
            '- **Part 1** — find every *valid* `mul(X,Y)` (1–3 digits each), multiply, and '
            'sum.\n'
            "- **Part 2** — `do()` and `don't()` instructions toggle whether the muls that "
            'follow count. Multiplication starts **enabled**. Sum only the enabled ones.',
 'title': 'Mull It Over'}
