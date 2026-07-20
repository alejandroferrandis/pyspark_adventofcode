"""Notebook content spec for 2024 day 05."""

SPEC = {'angle': 'Two ideas carry this one.\n'
          '\n'
          '**Part 1 — a violation is a join.** Explode each update to `(update, position, '
          'page)`, self-join it on `pos_a < pos_b` to get every ordered pair within an update, '
          'then join *that* to the rules **reversed**. Any surviving row is a broken rule, so '
          'an update is correct exactly when it produces no rows — which is a `left_anti` '
          'join.\n'
          '\n'
          '**Part 2 — sorting without a sort.** The single-node instinct is a comparator '
          '(`cmp_to_key`), and that is what a plain-Python solution does. But the correct '
          'index of a page is determined by *counting*: if page *p* must precede *k* of the '
          'other pages in its update, *p* lands at index *n−1−k*. So the middle page — index '
          '*n//2* — is the page that precedes exactly *n//2* others.\n'
          '\n'
          'That is a **groupBy, not a sort**, and it never materialises the ordering at all. '
          'Since the puzzle only ever asks for the middle element, computing the full order '
          'would be wasted work.',
 'explore_code': 'from pyspark.sql import functions as F\n'
                 '\n'
                 'rules, pages = day05.parse(spark, EXAMPLE)\n'
                 'rules.show(5)\n'
                 "pages.orderBy('update_id', 'pos').show(10)\n"
                 '\n'
                 "a = pages.select('update_id', F.col('pos').alias('pos_a'), "
                 "F.col('page').alias('page_a'))\n"
                 'b = pages.select(\n'
                 "    F.col('update_id').alias('uid_b'),\n"
                 "    F.col('pos').alias('pos_b'),\n"
                 "    F.col('page').alias('page_b'),\n"
                 ')\n'
                 "pairs = a.join(b, (F.col('update_id') == F.col('uid_b')) & (F.col('pos_a') < "
                 "F.col('pos_b')))\n"
                 'pairs.join(\n'
                 "    rules, (F.col('before') == F.col('page_b')) & (F.col('after') == "
                 "F.col('page_a'))\n"
                 ").select('update_id', 'page_a', 'page_b', 'before', 'after').show()",
 'explore_md': '### Violations, as rows\n'
               '\n'
               'Each row below is a concrete broken promise: `page_a` sits before `page_b`, '
               'but a rule says `page_b` must come first.',
 'lesson': 'Spark — self-join for pairwise constraints; sorting by counting',
 'notes': '- The counting argument in part 2 relies on the rules being **total** within each '
          "update — every pair of pages in an update is covered by some rule. AoC's input "
          'satisfies this; a general topological sort would not be able to assume it. Worth '
          'checking that assumption on your own input before trusting the shortcut.\n'
          '- `left_anti` is the join to reach for whenever the question is "rows *without* a '
          'match" — it avoids the `distinct()` + `NOT IN` shape that would otherwise appear.\n'
          '- Middle index is `size // 2` with integer division; every AoC update has an odd '
          'length, so there is always an unambiguous middle.',
 'summary': 'Two blocks: ordering rules `X|Y` meaning page X must be printed before page Y, '
            'then a list of updates (each a comma-separated page list).\n'
            '\n'
            '- **Part 1** — find the updates already in a valid order, and sum their '
            '**middle** page numbers.\n'
            '- **Part 2** — take the *incorrectly* ordered updates, reorder each one '
            'correctly, and sum their middle pages.',
 'title': 'Print Queue'}
