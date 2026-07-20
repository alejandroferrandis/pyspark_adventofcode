# pyspark_adventofcode

[Advent of Code](https://adventofcode.com) solved with **PySpark**, running against the
homelab's Spark Connect cluster — as a way of learning the DataFrame API the way it's
written on Databricks.

The companion repo [`airflow_adventofcode`](https://github.com/alejandroferrandis/airflow_adventofcode)
solves the same puzzles in plain Python, orchestrated by Airflow. That one is about the
*pipeline*; this one is about the *engine*. Their answers are cross-checked against each
other — see [Validation](#validation).

## The point

AoC puzzles are deliberately shaped for single-machine, row-at-a-time thinking, which
makes them an unusually good whetstone for Spark: the naive translation is always a UDF
or a `collect()`, and the interesting work is finding the *relational* framing instead.
Each day is labelled with the technique it teaches.

| Day | Puzzle | Approach |
|-----|--------|----------|
| 1 | Historian Hysteria | **Spark** — window ranking + join |
| 2 | Red-Nosed Reports | **Spark** — higher-order array functions, no UDFs |
| 3 | Mull It Over | **Spark** — regex extraction + a stateful `last(ignoreNulls)` window |
| 4 | Ceres Search | **Spark** — grid as a cell relation; offset equi-joins |
| 5 | Print Queue | **Spark** — self-join for pairwise constraints; sorting by counting |
| 6 | Guard Gallivant | Python — sequential guard walk |
| 7 | Bridge Repair | **Spark** — `aggregate` fold carrying reachable partials |
| 8 | Resonant Collinearity | **Spark** — self-join on frequency |
| 9 | Disk Fragmenter | Python — two-pointer compaction |
| 10 | Hoof It | **Spark** — nine successive frontier joins |
| 11 | Plutonian Pebbles | **Spark** — explode + groupBy counting, not simulation |
| 12 | Garden Groups | **Spark** — iterative connected components with pointer doubling |
| 13 | Claw Contraption | **Spark** — vectorised Cramer's rule |
| 14 | Restroom Redoubt | **Spark** — closed-form modular arithmetic |
| 15 | Warehouse Woes | Python — Sokoban state mutation |
| 16 | Reindeer Maze | Python — Dijkstra |
| 17 | Chronospatial Computer | Python — a sequential VM |
| 18 | RAM Run | Python — BFS shortest path |
| 19 | Linen Layout | **Spark** — one equi-join + nested `aggregate` over a bitmask DP |
| 20 | Race Condition | **Spark** — cheat enumeration as a self-join |

Day 3, 5, 12 and 19 are the ones worth reading. All four look irreducibly sequential and
none of them are.

### Where Spark is the wrong tool

Six days are solved in plain Python, and that is a result rather than a shortcut. Days 6,
9, 15, 17 and 18 are inherently sequential — each step depends on the state the previous
step left, so there is nothing to distribute. Day 16 is Dijkstra: a distributed framing
means one Spark job per frontier hop, hundreds of round-trips for a path a local heap
settles in 70 ms.

Those modules keep the `part1(spark, data)` signature and ignore `spark`, and say in one
line why. Recognising the days where the distributed framing costs more than it buys is
most of the skill this repo is meant to build.

## Layout

```
aoc_spark/
  session.py          # Spark Connect session (honours SPARK_REMOTE)
  inputs.py           # cache-first input: local file -> Postgres -> adventofcode.com
  y2024/
    day01.py .. day20.py   # primary solutions: part1(spark, data)
reference_python/
  y2024/
    day06.py .. day20.py   # independent plain-Python cross-check: part1(data)
notebooks/
  2024-dayNN-*.ipynb  # days 1-5: puzzle summary + example + walkthrough + real answer
tests/
  test_y2024.py       # days 1-5, both parts
  test_dayNN.py       # days 6-20, both implementations vs the published example
scripts/
  port-forward.sh         # reach Spark Connect from outside the cluster
  pull_inputs_from_pg.sh  # populate the local input cache from Postgres
  verify.py               # run every day against the real input, print JSON
  crosscheck.py           # assert both implementations agree on the real input
  build_notebooks.py      # regenerate notebooks/ from one content spec
```

Solutions take `(spark, data)` and return the answer. Keeping them out of the notebooks
means the notebooks import exactly what the tests cover — the walkthrough can't drift
from the implementation.

`reference_python/` exists purely as a correctness oracle. Each module deliberately uses a
*different algorithm* from its counterpart, so agreement is meaningful rather than two
copies of one idea: day 7 folds forwards in Spark and solves backwards in Python, day 12
propagates labels distributed and flood-fills locally, day 19 runs a forward bitmask DP
and a backward trie descent.

## Puzzle text and inputs are not in this repo

AoC is Eric Wastl's work, and he asks that **puzzle text and per-user inputs not be
redistributed**. So:

- notebooks carry a **summary in my own words** and link to the real puzzle;
- example data is AoC's **published** example, which is fine to quote;
- real inputs are fetched at runtime into `inputs/`, which is **gitignored**;
- notebook outputs contain real inputs and answers — **clear them before committing**
  (`jupyter nbconvert --clear-output --inplace notebooks/*.ipynb`).

## Running

### In JupyterLab (the intended path)

<https://jupyter-marvalhomelab.tail164eb.ts.net> already has `SPARK_REMOTE` set and
`pyspark[connect]` installed. Clone the repo into the home PVC, open a notebook, run.

### From marval or a laptop

```bash
uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python -e ".[dev]"

scripts/port-forward.sh &            # Spark Connect on localhost:15002
scripts/pull_inputs_from_pg.sh       # populate inputs/ (uses kubectl exec, no creds)

.venv/bin/python -m pytest -q        # examples only, no real input needed
PYTHONPATH=. .venv/bin/python scripts/verify.py
```

`pull_inputs_from_pg.sh` shells into the Postgres pod with `kubectl exec` rather than
connecting directly, so no database credentials are needed on the client or stored here.

## Scope: days 1-5 are complete, days 6-20 are part 1 only

Advent of Code reveals a day's **Part Two only after a correct Part One submission**. None
of days 6-20 are solved on this account, so part 2 of those puzzles is not readable and
has deliberately not been attempted — writing it from recall would mean inventing the very
example values the tests assert against.

Submitting the part 1 answers below unlocks each part 2, at which point they can be built
on the same footing as the rest.

## Validation

Correctness is asserted two ways, because "it ran" and "it's right" are different claims.

**Days 1-5** have a true oracle: the plain-Python implementation in
`airflow_adventofcode` solved them independently, and all 10 answers match exactly.

| Day | Part 1 | Part 2 | Spark runtime (p1 / p2) |
|-----|--------|--------|--------------------------|
| 1 | 1151792 | 21790168 | 336 ms / 350 ms |
| 2 | 371 | 426 | 177 ms / 177 ms |
| 3 | 190604937 | 82857512 | 95 ms / 200 ms |
| 4 | 2358 | 1737 | 513 ms / 498 ms |
| 5 | 4924 | 6085 | 796 ms / 982 ms |

**Days 6-20 have no oracle**, so correctness rests on two independent implementations
agreeing (`scripts/crosscheck.py`) plus both matching the published example (`pytest`).
That is strong evidence, **not proof** — a shared misreading of the puzzle would survive
both checks, which is why the two implementations use deliberately different algorithms.
These answers are **unverified against AoC**:

| Day | Part 1 | Spark / Python |
|-----|--------|----------------|
| 6 | 5404 | 3 ms / 1 ms |
| 7 | 1289579105366 | 218 ms / 5 ms |
| 8 | 295 | 203 ms / 0 ms |
| 9 | 6398252054886 | 20 ms / 10 ms |
| 10 | 550 | 1338 ms / 60 ms |
| 11 | 233875 | 2193 ms / 2 ms |
| 12 | 1361494 | 6352 ms / 46 ms |
| 13 | 36870 | 106 ms / 10 ms |
| 14 | 230435667 | 88 ms / 10 ms |
| 15 | 1475249 | 12 ms / 11 ms |
| 16 | 99488 | 71 ms / 66 ms |
| 17 | 7,3,5,7,5,7,4,3,0 | 0 ms / 0 ms |
| 18 | 432 | 3 ms / 5 ms |
| 19 | 317 | 565 ms / 6 ms |
| 20 | 1429 | 230 ms / 32 ms |

58 example assertions pass; 15/15 parts agree across implementations.

### Assumptions worth knowing

Three solutions take a shortcut that is valid for this input but not in general. Each is
commented in place, and in every case the reference implementation does *not* share the
assumption, so agreement genuinely tests it:

- **day 7** prunes partial results above the target — valid only because every operand is
  ≥ 1 (verified: min is 1, no zeros).
- **day 13** skips zero-determinant machines — verified 0 of 320 in this input.
- **day 19** packs reachable prefix positions into a 64-bit mask — valid only for designs
  shorter than 63 characters (verified: max is 60). A longer input would break it silently.

### On the runtimes

Spark is slower than plain Python on nearly every day, sometimes by 100×. That is expected
and is the honest finding: these inputs fit in cache, so Spark contributes job latency and
gRPC round-trips rather than throughput. Day 12 is the extreme case — 6.4 s for distributed
connected components against 46 ms for a local flood fill. The value here is the modelling,
not the speed.

Those runtimes are **slower than the plain-Python versions**, which take 0.4–55 ms. That
is the expected and correct result: every one of these inputs fits comfortably in L2
cache, so all Spark contributes at this size is query planning and gRPC round-trips. The
value here is learning the API and the execution model, not throughput — a fact worth
keeping in view before reaching for Spark on small data at work.

## Adding a day

1. Write `aoc_spark/y2024/dayNN.py` with `part1(spark, data)` / `part2(spark, data)`.
2. Add the published example and expected values to `tests/test_y2024.py`.
3. Add an entry to `DAYS` in `scripts/build_notebooks.py` and regenerate.
4. `scripts/pull_inputs_from_pg.sh 2024 NN NN` then `scripts/verify.py NN`.
