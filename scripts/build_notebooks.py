"""Generate the day notebooks from one content spec.

Notebooks import `aoc_spark.y2024.dayNN` rather than inlining it, so the
walkthroughs cannot drift from the tested code.

Regenerating overwrites notebooks/*.ipynb -- do exploratory edits in a copy.
"""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEST = ROOT / "notebooks"

# Published examples, mirrored from tests/test_y2024.py so the notebook shows
# the same data the suite asserts on.
EXAMPLES = {
    1: '"""\\\n3   4\n4   3\n2   5\n1   3\n3   9\n3   3\n"""',
    2: '"""\\\n7 6 4 2 1\n1 2 7 8 9\n9 7 6 2 1\n1 3 2 4 5\n8 6 4 4 1\n1 3 6 7 9\n"""',
    3: None,  # two different examples, handled inline
    4: '"""\\\nMMMSXXMASM\nMSAMXMSMSA\nAMXSXMAAMM\nMSAMASMSMX\nXMASAMXAMM\n'
    'XXAMMXXAMA\nSMSMSASXSS\nSAXAMASAAA\nMAMMMXMMMM\nMXMXAXMASX\n"""',
    5: '"""\\\n47|53\n97|13\n97|61\n97|47\n75|29\n61|13\n75|53\n29|13\n97|29\n53|29\n'
    "61|53\n97|53\n61|29\n47|13\n75|47\n97|75\n47|61\n75|61\n47|29\n75|13\n53|13\n\n"
    '75,47,61,53,29\n97,61,53,29,13\n75,29,13\n75,97,47,61,53\n61,13,29\n97,13,75,29,47\n"""',
}

DAYS = {
    1: {
        "title": "Historian Hysteria",
        "lesson": "window ranking + join",
        "summary": (
            "Two columns of location IDs, one per line.\n\n"
            "- **Part 1** — pair the two lists up smallest-with-smallest, "
            "second-smallest-with-second-smallest, and so on, then sum the absolute "
            "difference of each pair.\n"
            "- **Part 2** — for each value in the left list, multiply it by the number of "
            "times it appears in the right list, and sum those scores."
        ),
        "angle": (
            "This is the rare AoC day that is *naturally relational*, which makes it a good "
            "warm-up.\n\n"
            "\"Sort both sides and pair by rank\" is a **window function**: `row_number()` over "
            "each column independently gives every value a rank, and joining the two ranked "
            "frames on `rank` reproduces the pairing without ever holding both sorted lists in "
            "memory at once.\n\n"
            "\"How often does it appear\" is a **groupBy + join** — a frequency table joined back "
            "onto the left column. The `left` join matters: values that never appear on the "
            "right must score zero, not vanish."
        ),
        "explore_md": (
            "### Watch the pairing happen\n\n"
            "The two ranked frames are what the join consumes. Notice each column is sorted "
            "*independently* — row 1 of `left` has nothing to do with row 1 of the input."
        ),
        "explore_code": (
            "from pyspark.sql import Window\n"
            "from pyspark.sql import functions as F\n\n"
            "df = day01.parse(spark, EXAMPLE)\n"
            "df.show()\n\n"
            "left = df.select(F.row_number().over(Window.orderBy('left_id')).alias('rank'), 'left_id')\n"
            "right = df.select(F.row_number().over(Window.orderBy('right_id')).alias('rank'), 'right_id')\n\n"
            "left.join(right, on='rank').orderBy('rank').withColumn(\n"
            "    'distance', F.abs(F.col('left_id') - F.col('right_id'))\n"
            ").show()"
        ),
        "notes": (
            "- The unpartitioned `Window.orderBy(...)` triggers Spark's *\"No Partition Defined "
            "for Window operation!\"* warning. It is correct here — the puzzle needs a **global** "
            "ordering, and a partitioned window would rank within groups instead. At 1000 rows "
            "the single-partition shuffle costs nothing; at a billion it would be the thing to "
            "redesign.\n"
            "- Try swapping the `left` join in part 2 for an `inner` join and confirm the answer "
            "is unchanged *for this input* — then think about why that is luck, not correctness."
        ),
    },
    2: {
        "title": "Red-Nosed Reports",
        "lesson": "higher-order array functions (no UDFs)",
        "summary": (
            "Each line is a *report*: a list of levels.\n\n"
            "- **Part 1** — a report is **safe** when its levels are either all increasing or all "
            "decreasing, *and* every adjacent step is between 1 and 3 inclusive. Count the safe "
            "reports.\n"
            "- **Part 2** — the Problem Dampener: a report also counts as safe if removing a "
            "single level would make it safe. Count those too."
        ),
        "angle": (
            "The interesting constraint here is self-imposed: **solve it without a UDF.**\n\n"
            "A Python UDF would serialise every row out to a Python worker and back, losing an "
            "order of magnitude and blinding the optimiser. Spark's higher-order functions — "
            "`transform`, `zip_with`, `forall`, `exists`, `slice`, `sequence` — run on array "
            "columns *inside the JVM*, so the whole thing stays one native expression tree.\n\n"
            "- adjacent differences → `zip_with(levels[1:n-1], levels[2:n], (a,b) -> b-a)`\n"
            "- \"every step is legal\" → `forall(diffs, d -> d >= 1 AND d <= 3)`\n"
            "- \"some single removal rescues it\" → `exists(variants, ...)`, which short-circuits "
            "on the first success rather than building all *n* variants."
        ),
        "explore_md": (
            "### The expression tree, made visible\n\n"
            "Every intermediate below is a *column*, not a Python value — nothing has been pulled "
            "to the driver yet."
        ),
        "explore_code": (
            "from pyspark.sql import functions as F\n\n"
            "df = day02.parse(spark, EXAMPLE)\n"
            "levels = F.col('levels')\n"
            "size = F.size(levels)\n\n"
            "df.select(\n"
            "    'levels',\n"
            "    F.zip_with(\n"
            "        F.slice(levels, F.lit(1), size - 1),\n"
            "        F.slice(levels, F.lit(2), size - 1),\n"
            "        lambda a, b: b - a,\n"
            "    ).alias('diffs'),\n"
            "    day02._is_safe(levels).alias('safe'),\n"
            "    F.exists(day02._dampened_variants(levels), day02._is_safe).alias('rescued'),\n"
            ").show(truncate=False)"
        ),
        "notes": (
            "- `slice` is **1-based**, which is why `_dampened_variants` uses `i + 2` to skip "
            "element *i*. Off-by-one here is the classic bug — the example is small enough to "
            "check by eye.\n"
            "- Look at the `rescued` column for row `7 6 4 2 1`: it is already safe, so part 2 "
            "must OR the two conditions rather than replacing one with the other."
        ),
    },
    3: {
        "title": "Mull It Over",
        "lesson": "regex extraction + a stateful window",
        "summary": (
            "A block of corrupted memory containing valid `mul(X,Y)` instructions buried in "
            "garbage.\n\n"
            "- **Part 1** — find every *valid* `mul(X,Y)` (1–3 digits each), multiply, and sum.\n"
            "- **Part 2** — `do()` and `don't()` instructions toggle whether the muls that follow "
            "count. Multiplication starts **enabled**. Sum only the enabled ones."
        ),
        "angle": (
            "Part 2 is the one worth the trip. \"Scan left to right carrying a flag\" sounds "
            "hopelessly sequential, and it is exactly where people reach for a UDF or `collect()`.\n\n"
            "It is really a **last-non-null-value window** — one of the most reusable patterns in "
            "Spark:\n\n"
            "```sql\n"
            "last(flag, ignoreNulls => true) OVER (\n"
            "  ORDER BY pos ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n"
            ")\n"
            "```\n\n"
            "`mul` tokens carry no flag (null); `do()`/`don't()` carry 1/0. The window fills each "
            "mul with the most recent toggle. `coalesce(..., 1)` covers the muls that appear "
            "before any toggle, since multiplication starts enabled.\n\n"
            "The ordering key comes free: `regexp_extract_all` returns matches **in source "
            "order**, so `posexplode` gives a reliable position without computing character "
            "offsets."
        ),
        "explore_md": (
            "### The window doing the work\n\n"
            "This is the whole puzzle in one table — read down the `enabled` column and watch it "
            "carry forward from each toggle."
        ),
        "explore_code": (
            "from pyspark.sql import Window\n"
            "from pyspark.sql import functions as F\n\n"
            "memory = spark.createDataFrame([(EXAMPLE_PART2,)], 'memory STRING')\n"
            "tokens = memory.select(\n"
            "    F.posexplode(\n"
            "        F.regexp_extract_all(F.col('memory'), F.lit(day03.TOKEN), F.lit(0))\n"
            "    ).alias('pos', 'token')\n"
            ")\n\n"
            "toggle = (\n"
            "    F.when(F.col('token') == 'do()', F.lit(1))\n"
            "    .when(F.col('token') == \"don't()\", F.lit(0))\n"
            "    .otherwise(F.lit(None))\n"
            ")\n"
            "running = Window.orderBy('pos').rowsBetween(Window.unboundedPreceding, Window.currentRow)\n\n"
            "tokens.select(\n"
            "    'pos',\n"
            "    'token',\n"
            "    toggle.alias('toggle'),\n"
            "    F.coalesce(F.last(toggle, ignorenulls=True).over(running), F.lit(1)).alias('enabled'),\n"
            ").show(truncate=False)"
        ),
        "notes": (
            "- Part 1 and part 2 use **different published examples** — the part 1 example has no "
            "toggles. Running part 2 on the part 1 example gives 161, not 48.\n"
            "- The whole input is loaded as a **single row**, not one row per line: instructions "
            "run across line breaks, so splitting on newlines would corrupt tokens that straddle "
            "them.\n"
            "- This is the day where a single-partition window is genuinely load-bearing. A "
            "partitioned window would reset the enabled state at each partition boundary and "
            "quietly produce a wrong answer."
        ),
    },
    4: {
        "title": "Ceres Search",
        "lesson": "modelling a grid as a cell relation",
        "summary": (
            "A grid of letters — a word search.\n\n"
            "- **Part 1** — count every occurrence of `XMAS`, in all 8 directions: horizontal, "
            "vertical, both diagonals, forwards and backwards. Occurrences may overlap.\n"
            "- **Part 2** — despite the name, find `X-MAS`: two `MAS` (each forwards or "
            "backwards) crossing diagonally on a shared central `A`."
        ),
        "angle": (
            "The move that makes this tractable in Spark is **refusing to treat the grid as a "
            "grid.**\n\n"
            "Explode it into a `(row, col, char)` relation and \"look in direction (dr,dc)\" "
            "becomes an **equi-join on offset coordinates**: join cell *(r,c)* to cell "
            "*(r+dr, c+dc)*. Chain one join per letter and a surviving row is a complete match.\n\n"
            "Searching all 8 directions at once is then a **cross join against an 8-row "
            "directions table** — Spark broadcasts something that small, so the search stays "
            "three joins deep no matter how many directions you add. That is the payoff of the "
            "relational framing: the direction count moved from *code structure* into *data*."
        ),
        "explore_md": (
            "### From grid to relation\n\n"
            "The whole trick is the first cell below. Once the grid is a table of coordinates, "
            "everything after it is ordinary SQL."
        ),
        "explore_code": (
            "from pyspark.sql import functions as F\n\n"
            "grid = day04.cells(spark, EXAMPLE)\n"
            "grid.show(8)\n"
            "print('cells:', grid.count())\n\n"
            "# One direction, one step: every X with an M immediately to its right.\n"
            "xs = grid.filter(F.col('ch') == 'X').select('r', 'c')\n"
            "ms = grid.filter(F.col('ch') == 'M').select(F.col('r').alias('nr'), F.col('c').alias('nc'))\n"
            "xs.join(ms, (F.col('nr') == F.col('r')) & (F.col('nc') == F.col('c') + 1)).show()"
        ),
        "notes": (
            "- `split(line, '')` emits a **trailing empty string**, which is why `cells()` filters "
            "`ch != ''`. Leave it in and every row gains a phantom cell.\n"
            "- The grid is `cache()`d because both parts join against it repeatedly — without it "
            "Spark re-explodes the source on every join.\n"
            "- Part 2's name is a trap: it is *not* about the letters `XMAS` at all.\n"
            "- Worth trying: rewrite part 1 to count each direction in a separate query and sum. "
            "Same answer, ~8× the query plans — a concrete demonstration of why the directions "
            "table is worth it."
        ),
    },
    5: {
        "title": "Print Queue",
        "lesson": "self-join for pairwise constraints; sorting by counting",
        "summary": (
            "Two blocks: ordering rules `X|Y` meaning page X must be printed before page Y, then "
            "a list of updates (each a comma-separated page list).\n\n"
            "- **Part 1** — find the updates already in a valid order, and sum their **middle** "
            "page numbers.\n"
            "- **Part 2** — take the *incorrectly* ordered updates, reorder each one correctly, "
            "and sum their middle pages."
        ),
        "angle": (
            "Two ideas carry this one.\n\n"
            "**Part 1 — a violation is a join.** Explode each update to `(update, position, "
            "page)`, self-join it on `pos_a < pos_b` to get every ordered pair within an update, "
            "then join *that* to the rules **reversed**. Any surviving row is a broken rule, so "
            "an update is correct exactly when it produces no rows — which is a `left_anti` "
            "join.\n\n"
            "**Part 2 — sorting without a sort.** The single-node instinct is a comparator "
            "(`cmp_to_key`), and that is what a plain-Python solution does. But the correct index "
            "of a page is determined by *counting*: if page *p* must precede *k* of the other "
            "pages in its update, *p* lands at index *n−1−k*. So the middle page — index *n//2* — "
            "is the page that precedes exactly *n//2* others.\n\n"
            "That is a **groupBy, not a sort**, and it never materialises the ordering at all. "
            "Since the puzzle only ever asks for the middle element, computing the full order "
            "would be wasted work."
        ),
        "explore_md": (
            "### Violations, as rows\n\n"
            "Each row below is a concrete broken promise: `page_a` sits before `page_b`, but a "
            "rule says `page_b` must come first."
        ),
        "explore_code": (
            "from pyspark.sql import functions as F\n\n"
            "rules, pages = day05.parse(spark, EXAMPLE)\n"
            "rules.show(5)\n"
            "pages.orderBy('update_id', 'pos').show(10)\n\n"
            "a = pages.select('update_id', F.col('pos').alias('pos_a'), F.col('page').alias('page_a'))\n"
            "b = pages.select(\n"
            "    F.col('update_id').alias('uid_b'),\n"
            "    F.col('pos').alias('pos_b'),\n"
            "    F.col('page').alias('page_b'),\n"
            ")\n"
            "pairs = a.join(b, (F.col('update_id') == F.col('uid_b')) & (F.col('pos_a') < F.col('pos_b')))\n"
            "pairs.join(\n"
            "    rules, (F.col('before') == F.col('page_b')) & (F.col('after') == F.col('page_a'))\n"
            ").select('update_id', 'page_a', 'page_b', 'before', 'after').show()"
        ),
        "notes": (
            "- The counting argument in part 2 relies on the rules being **total** within each "
            "update — every pair of pages in an update is covered by some rule. AoC's input "
            "satisfies this; a general topological sort would not be able to assume it. Worth "
            "checking that assumption on your own input before trusting the shortcut.\n"
            "- `left_anti` is the join to reach for whenever the question is \"rows *without* a "
            "match\" — it avoids the `distinct()` + `NOT IN` shape that would otherwise appear.\n"
            "- Middle index is `size // 2` with integer division; every AoC update has an odd "
            "length, so there is always an unambiguous middle."
        ),
    },
}


def md(text: str) -> dict:
    return {
        "cell_type": "markdown",
        "id": "",  # filled in by build(), nbformat >=4.5 requires it
        "metadata": {},
        "source": text.splitlines(keepends=True),
    }


def code(text: str) -> dict:
    return {
        "cell_type": "code",
        "id": "",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": text.splitlines(keepends=True),
    }


def build(day: int, spec: dict) -> dict:
    nn = f"{day:02d}"
    cells = [
        md(
            f"# AoC 2024 Day {day} — {spec['title']}\n\n"
            f"**Spark lesson: {spec['lesson']}**\n\n"
            f"Puzzle: <https://adventofcode.com/2024/day/{day}>\n\n"
            "---\n\n"
            "> **On puzzle text and inputs.** Advent of Code is Eric Wastl's work, and he asks "
            "that puzzle text and per-user inputs not be redistributed. So this notebook carries "
            "a summary in my own words plus the *published* example, and pulls the real input at "
            "runtime from a local cache that is gitignored. Read the puzzle at the link above.\n\n"
            "---\n\n"
            "## The puzzle\n\n"
            f"{spec['summary']}"
        ),
        md(f"## The Spark angle\n\n{spec['angle']}"),
        md("## Setup\n\nConnect to the cluster's Spark Connect endpoint and import the solution."),
        code(
            "import sys\n"
            "\n"
            "sys.path.insert(0, '..')  # so `aoc_spark` resolves when running from notebooks/\n"
            "\n"
            "from aoc_spark.session import get_spark\n"
            "from aoc_spark.inputs import get_input\n"
            f"from aoc_spark.y2024 import day{nn}\n"
            "\n"
            f"spark = get_spark('aoc-2024-day{nn}')\n"
            "print('Spark', spark.version)"
        ),
    ]

    if day == 3:
        cells += [
            md(
                "## The published example\n\n"
                "Day 3 publishes **two** examples — the part 1 one has no `do()`/`don't()` "
                "toggles, so part 2 needs its own."
            ),
            code(
                "EXAMPLE_PART1 = r\"xmul(2,4)%&mul[3,7]!@^do_not_mul(5,5)+mul(32,64]then(mul(11,8)mul(8,5))\"\n"
                "EXAMPLE_PART2 = r\"xmul(2,4)&mul[3,7]!^don't()_mul(5,5)+mul(32,64](mul(11,8)undo()?mul(8,5))\"\n"
                "\n"
                "print('part 1:', day03.part1(spark, EXAMPLE_PART1), '(expected 161)')\n"
                "print('part 2:', day03.part2(spark, EXAMPLE_PART2), '(expected 48)')"
            ),
        ]
    else:
        expected = {1: (11, 31), 2: (2, 4), 4: (18, 9), 5: (143, 123)}[day]
        cells += [
            md("## The published example\n\nThe same data the test suite asserts on."),
            code(
                f"EXAMPLE = {EXAMPLES[day]}\n"
                "\n"
                f"print('part 1:', day{nn}.part1(spark, EXAMPLE), '(expected {expected[0]})')\n"
                f"print('part 2:', day{nn}.part2(spark, EXAMPLE), '(expected {expected[1]})')"
            ),
        ]

    cells += [
        md(spec["explore_md"]),
        code(spec["explore_code"]),
        md(
            "## The real input\n\n"
            "`get_input` is cache-first: local gitignored file → Postgres → adventofcode.com. "
            "In practice it hits the local file and never touches the network."
        ),
        code(
            "import time\n"
            "\n"
            f"data = get_input(2024, {day})\n"
            "print(f'input: {len(data):,} chars, {len(data.splitlines()):,} lines')\n"
            "\n"
            "for part in (1, 2):\n"
            f"    fn = getattr(day{nn}, f'part{{part}}')\n"
            "    started = time.perf_counter()\n"
            "    answer = fn(spark, data)\n"
            "    print(f'part {part}: {answer}  ({(time.perf_counter() - started) * 1000:.0f} ms)')"
        ),
        md(f"## Notes & gotchas\n\n{spec['notes']}"),
    ]

    # Stable, deterministic ids so regenerating produces no spurious git diff.
    for index, cell in enumerate(cells):
        cell["id"] = f"day{nn}-cell-{index:02d}"

    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.11"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def main() -> None:
    DEST.mkdir(parents=True, exist_ok=True)
    for day, spec in DAYS.items():
        slug = spec["title"].lower().replace(" ", "-")
        path = DEST / f"2024-day{day:02d}-{slug}.ipynb"
        path.write_text(json.dumps(build(day, spec), indent=1) + "\n")
        print(f"wrote {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
