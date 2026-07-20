"""Notebook content spec for 2024 day 12."""

SPEC = {'angle': 'Two things fall out of the **same edge list**, which is why building it is the '
          'only parsing step.\n'
          '\n'
          'Explode the grid to `(node, r, c, ch)` and self-join it to its right and down '
          'neighbours where the plant matches. Symmetrise that (union it with itself, swapped) '
          'and you have `(u, v)` for every same-plant adjacency.\n'
          '\n'
          '**Perimeter needs no second pass.** Every plot has four sides; a side is fence '
          'exactly when it does *not* face a same-plant neighbour. So `perimeter = 4 - degree`, '
          'and degree is a `groupBy(u).count()` on the edge list already built. Cells with no '
          'neighbours never appear in the edges at all, hence the `left` join and '
          '`coalesce(deg, 0)` — a lone plot correctly scores 4.\n'
          '\n'
          '**Regions are connected components**, and that is the interesting part. The loop is '
          'min-label propagation: each node starts labelled with its own id, and each round '
          'takes the smallest label one edge away. Done naively that spreads one hop per round, '
          'so a snaking region 200 cells long needs 200 rounds of joins — ruinous.\n'
          '\n'
          'The fix is two extra moves per round:\n'
          '\n'
          '- **hook the parent too** — proposals are emitted for `pu` as well as `u`, so a whole '
          'group of nodes sharing a label is dragged down together rather than one at a time\n'
          '- **pointer doubling** — after hooking, join the label table to itself (`p == gnode`) '
          'and take `least(p, gp)`, halving every chain in one step\n'
          '\n'
          'Together those turn *O(region diameter)* rounds into *O(log n)*. The `140×140` input '
          'converges in a handful of rounds instead of a few hundred.\n'
          '\n'
          '**And it is still the slowest day in the repo: ~6.4 s against ~46 ms for the plain '
          'Python BFS flood fill.** That is not a defect in the Spark code — it is what '
          'iterative graph work costs when every round is a shuffle. 19600 cells fit in L2 '
          'cache; a BFS touches each once and never leaves the CPU. The Spark version pays a '
          'full job launch, several joins and a `count()` barrier *per round*. The relational '
          'framing is worth understanding precisely because it is the one that keeps working at '
          '140 million cells — but at 140×140 it loses, and loses badly.',
 'explore_code': 'from pyspark.sql import functions as F\n'
                 '\n'
                 '# Small example only -- the CC loop is a shuffle per round.\n'
                 'grid = day12.cells(spark, EXAMPLE).cache()\n'
                 'adjacency = day12.edges(grid).cache()\n'
                 "print('cells:', grid.count(), ' directed edges:', adjacency.count())\n"
                 '\n'
                 "# Perimeter is 4 - degree, straight off the edge list. No second pass.\n"
                 "degree = adjacency.groupBy(F.col('u').alias('node')).agg(F.count('*').alias('deg'))\n"
                 'grid.join(degree, "node", "left").select(\n'
                 "    'r',\n"
                 "    'c',\n"
                 "    'ch',\n"
                 "    F.coalesce('deg', F.lit(0)).alias('deg'),\n"
                 "    (F.lit(4) - F.coalesce('deg', F.lit(0))).alias('perimeter'),\n"
                 ").orderBy('r', 'c').show(8)\n"
                 '\n'
                 '# Components: label = smallest node id in the region.\n'
                 'parent = day12.components(grid, adjacency)\n'
                 'regions = (\n'
                 '    grid.join(parent, "node")\n'
                 '    .join(degree, "node", "left")\n'
                 '    .groupBy("p")\n'
                 '    .agg(\n'
                 '        F.first("ch").alias("plant"),\n'
                 '        F.count("*").alias("area"),\n'
                 '        F.sum(F.lit(4) - F.coalesce("deg", F.lit(0))).alias("perimeter"),\n'
                 '    )\n'
                 '    .withColumn("price", F.col("area") * F.col("perimeter"))\n'
                 ')\n'
                 'regions.orderBy("p").show()\n'
                 'print("total:", regions.agg(F.sum("price").alias("t")).collect()[0]["t"])\n'
                 '\n'
                 'adjacency.unpersist()\n'
                 'grid.unpersist()',
 'explore_md': '### One edge list, both answers\n'
               '\n'
               'The degree column below is doing double duty — it is the perimeter *and* it is '
               'the graph the components loop walks. Compare the region table against the '
               "prices the puzzle lists for this example (`R` at 12 × 18 = 216, and so on).",
 'lesson': 'Spark — connected components by min-label propagation',
 'notes': "- `node` is a **row-major id**, `r * width + c`, with `width = longest line + 1`. The "
          '`+ 1` is the guard: without it the last cell of row *r* and the first cell of row '
          '*r+1* can collide into the same id, silently welding two regions together. It is '
          'cast to `long` because a big grid overflows `int`.\n'
          "- `split(line, '')` emits a **trailing empty string**, filtered out in `cells()`. "
          'Same trap as day 4.\n'
          '- Edges are built **right and down only**, then unioned with the swap. Enumerating '
          'all four directions directly would produce every edge twice and double the degree.\n'
          '- The loop\'s termination test is a `count()` of changed labels — an **action**, so '
          'every round is a real job. That is deliberate: there is no way to know a fixpoint has '
          'been reached without looking. It is also why `localCheckpoint()` sits inside the '
          'loop; the lineage would otherwise grow one full round of joins per iteration.\n'
          '- Labels converge to the **smallest node id** in the region, which is stable and '
          'deterministic, but it is an arbitrary identifier — do not read meaning into its '
          'value.\n'
          '- The perimeter formula assumes **grid cells only**. Off-grid neighbours are '
          'perimeter by construction, since a cell outside the grid never produced an edge, so '
          'no explicit boundary handling appears anywhere.\n'
          '- Ragged input (lines of differing length) would break the `width` assumption. AoC '
          'grids are rectangular; the code takes the max length defensively but the geometry '
          'still assumes a rectangle.',
 'summary': 'A map of garden plots, one letter per plot. Plots of the same letter that touch '
            '**horizontally or vertically** form a region — diagonals do not connect, and the '
            'same letter can form several separate regions.\n'
            '\n'
            "Each region has an **area** (how many plots) and a **perimeter** (how many of its "
            "plots' four sides do not touch another plot of the same region — including "
            'sides facing the outside of the map, and sides facing a different region enclosed '
            'within it).\n'
            '\n'
            '- **Part 1** — the price of a region is `area × perimeter`. Sum the price of every '
            'region.',
 'title': 'Garden Groups'}
