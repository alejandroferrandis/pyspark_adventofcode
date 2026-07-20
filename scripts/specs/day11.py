"""Notebook content spec for 2024 day 11."""

SPEC = {'angle': 'The whole day turns on one observation: **the puzzle asks how many stones, not '
          'which stones in what order.**\n'
          '\n'
          'The rules are per-stone and depend on nothing but the number engraved on it. A `5` '
          'behaves identically whether it sits at the front of the line or the back, and the '
          'line never interacts with itself. So the order the puzzle so carefully preserves is '
          'decoration — it changes the *arrangement*, never the *count*.\n'
          '\n'
          'That lets the line be stored as a frequency table, `(stone, count)`, instead of a '
          'sequence. One blink becomes:\n'
          '\n'
          '- **explode** — each stone maps to an array of 1 or 2 children (`when` on the three '
          'rules)\n'
          '- **groupBy + sum** — children that collide merge, carrying their counts\n'
          '\n'
          'The payoff is the size difference. After 25 blinks the example line holds 55312 '
          'stones but only a few thousand *distinct* numbers, and that gap widens every blink — '
          'the split rule keeps producing small numbers that everything else eventually falls '
          'into. Simulating the line grows exponentially; the frequency table plateaus.',
 'explore_code': 'from pyspark.sql import functions as F\n'
                 '\n'
                 'rows = [(int(v), 1) for v in EXAMPLE.split()]\n'
                 "stones = spark.createDataFrame(rows, 'stone LONG, count LONG').groupBy('stone').agg(\n"
                 "    F.sum('count').alias('count')\n"
                 ')\n'
                 '\n'
                 '# Distinct rows vs. stones in the line -- the gap is the whole optimisation.\n'
                 'for blink in range(7):\n'
                 "    total = stones.agg(F.sum('count').alias('t')).collect()[0]['t']\n"
                 "    print(f'blink {blink:>2}: {stones.count():>4} distinct  |  {total:>6} "
                 "stones in the line')\n"
                 '    stones = day11._blink(stones).localCheckpoint()\n'
                 '\n'
                 "stones.orderBy('stone').show(10)",
 'explore_md': '### Distinct rows vs. stones\n'
               '\n'
               'Two numbers per blink. The right one is what the puzzle counts; the left one is '
               'what Spark actually stores.',
 'lesson': 'Spark — explode + groupBy counting',
 'notes': '- **`localCheckpoint()` per blink is load-bearing.** Without it the 25 blinks stack '
          'into a single unexecuted plan 25 group-bys deep, and the analyzer alone starts to '
          'crawl before any data moves. The checkpoint truncates the lineage after each round, '
          'which is the standard shape for any iterative Spark job.\n'
          '- Splitting is done on the **string** form: `length(text) % 2 == 0`, then two '
          '`substring` slices cast back to `long`. `substring` is **1-based**, so the halves are '
          '`(1, half)` and `(half + 1, half)`.\n'
          '- The cast back to `long` is what strips leading zeroes for free — `1000` splits to '
          '`10` and `00`, and `00` becomes `0`, exactly as the rules require. Under ANSI mode '
          'this cast is still safe because every substring is pure digits.\n'
          '- Stones are `LONG`, not `INT`. Multiplying by 2024 repeatedly overflows 32 bits '
          'quickly, and Spark would silently wrap (or, under ANSI mode, raise) rather than '
          'widen.\n'
          '- The input is whitespace-separated on **one line**, so `data.split()` with no '
          'argument is the parse — no newline handling needed.\n'
          '- Duplicate stones in the initial line must be folded with a `groupBy` *before* the '
          'first blink; otherwise the same number appears as two rows and the counts, while not '
          'wrong, stop being canonical.',
 'summary': 'A line of stones, each engraved with a number. Every blink, all stones change at '
            'once, each by the **first** rule that applies:\n'
            '\n'
            '1. a `0` becomes a `1`\n'
            '2. a number with an **even** digit count splits into two stones — the left half of '
            'the digits and the right half, with leading zeroes dropped (`1000` → `10` and `0`)\n'
            '3. otherwise the number is multiplied by **2024**\n'
            '\n'
            '- **Part 1** — blink 25 times and count how many stones you end up with.',
 'title': 'Plutonian Pebbles'}
