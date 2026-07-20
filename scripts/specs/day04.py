"""Notebook content spec for 2024 day 04."""

SPEC = {'angle': 'The move that makes this tractable in Spark is **refusing to treat the grid as a '
          'grid.**\n'
          '\n'
          'Explode it into a `(row, col, char)` relation and "look in direction (dr,dc)" '
          'becomes an **equi-join on offset coordinates**: join cell *(r,c)* to cell *(r+dr, '
          'c+dc)*. Chain one join per letter and a surviving row is a complete match.\n'
          '\n'
          'Searching all 8 directions at once is then a **cross join against an 8-row '
          'directions table** — Spark broadcasts something that small, so the search stays '
          'three joins deep no matter how many directions you add. That is the payoff of the '
          'relational framing: the direction count moved from *code structure* into *data*.',
 'explore_code': 'from pyspark.sql import functions as F\n'
                 '\n'
                 'grid = day04.cells(spark, EXAMPLE)\n'
                 'grid.show(8)\n'
                 "print('cells:', grid.count())\n"
                 '\n'
                 '# One direction, one step: every X with an M immediately to its right.\n'
                 "xs = grid.filter(F.col('ch') == 'X').select('r', 'c')\n"
                 "ms = grid.filter(F.col('ch') == 'M').select(F.col('r').alias('nr'), "
                 "F.col('c').alias('nc'))\n"
                 "xs.join(ms, (F.col('nr') == F.col('r')) & (F.col('nc') == F.col('c') + "
                 '1)).show()',
 'explore_md': '### From grid to relation\n'
               '\n'
               'The whole trick is the first cell below. Once the grid is a table of '
               'coordinates, everything after it is ordinary SQL.',
 'lesson': 'Spark — modelling a grid as a cell relation',
 'notes': "- `split(line, '')` emits a **trailing empty string**, which is why `cells()` "
          "filters `ch != ''`. Leave it in and every row gains a phantom cell.\n"
          '- The grid is `cache()`d because both parts join against it repeatedly — without it '
          'Spark re-explodes the source on every join.\n'
          "- Part 2's name is a trap: it is *not* about the letters `XMAS` at all.\n"
          '- Worth trying: rewrite part 1 to count each direction in a separate query and sum. '
          'Same answer, ~8× the query plans — a concrete demonstration of why the directions '
          'table is worth it.',
 'summary': 'A grid of letters — a word search.\n'
            '\n'
            '- **Part 1** — count every occurrence of `XMAS`, in all 8 directions: horizontal, '
            'vertical, both diagonals, forwards and backwards. Occurrences may overlap.\n'
            '- **Part 2** — despite the name, find `X-MAS`: two `MAS` (each forwards or '
            'backwards) crossing diagonally on a shared central `A`.',
 'title': 'Ceres Search'}
