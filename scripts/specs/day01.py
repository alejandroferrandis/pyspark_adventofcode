"""Notebook content spec for 2024 day 01."""

SPEC = {'angle': 'This is the rare AoC day that is *naturally relational*, which makes it a good '
          'warm-up.\n'
          '\n'
          '"Sort both sides and pair by rank" is a **window function**: `row_number()` over '
          'each column independently gives every value a rank, and joining the two ranked '
          'frames on `rank` reproduces the pairing without ever holding both sorted lists in '
          'memory at once.\n'
          '\n'
          '"How often does it appear" is a **groupBy + join** — a frequency table joined back '
          'onto the left column. The `left` join matters: values that never appear on the '
          'right must score zero, not vanish.',
 'explore_code': 'from pyspark.sql import Window\n'
                 'from pyspark.sql import functions as F\n'
                 '\n'
                 'df = day01.parse(spark, EXAMPLE)\n'
                 'df.show()\n'
                 '\n'
                 'left = '
                 "df.select(F.row_number().over(Window.orderBy('left_id')).alias('rank'), "
                 "'left_id')\n"
                 'right = '
                 "df.select(F.row_number().over(Window.orderBy('right_id')).alias('rank'), "
                 "'right_id')\n"
                 '\n'
                 "left.join(right, on='rank').orderBy('rank').withColumn(\n"
                 "    'distance', F.abs(F.col('left_id') - F.col('right_id'))\n"
                 ').show()',
 'explore_md': '### Watch the pairing happen\n'
               '\n'
               'The two ranked frames are what the join consumes. Notice each column is sorted '
               '*independently* — row 1 of `left` has nothing to do with row 1 of the input.',
 'lesson': 'Spark — window ranking + join',
 'notes': '- The unpartitioned `Window.orderBy(...)` triggers Spark\'s *"No Partition Defined '
          'for Window operation!"* warning. It is correct here — the puzzle needs a **global** '
          'ordering, and a partitioned window would rank within groups instead. At 1000 rows '
          'the single-partition shuffle costs nothing; at a billion it would be the thing to '
          'redesign.\n'
          '- Try swapping the `left` join in part 2 for an `inner` join and confirm the answer '
          'is unchanged *for this input* — then think about why that is luck, not correctness.',
 'summary': 'Two columns of location IDs, one per line.\n'
            '\n'
            '- **Part 1** — pair the two lists up smallest-with-smallest, '
            'second-smallest-with-second-smallest, and so on, then sum the absolute difference '
            'of each pair.\n'
            '- **Part 2** — for each value in the left list, multiply it by the number of '
            'times it appears in the right list, and sum those scores.',
 'title': 'Historian Hysteria'}
