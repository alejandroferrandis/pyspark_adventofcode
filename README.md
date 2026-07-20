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

| Day | Puzzle | Spark lesson |
|-----|--------|--------------|
| 1 | Historian Hysteria | window ranking + join |
| 2 | Red-Nosed Reports | higher-order array functions, no UDFs |
| 3 | Mull It Over | regex extraction + a stateful `last(ignoreNulls)` window |
| 4 | Ceres Search | modelling a grid as a cell relation; offset equi-joins |
| 5 | Print Queue | self-join for pairwise constraints; sorting by counting |

Day 3 and day 5 are the two worth reading if you only read two — both look irreducibly
sequential and neither is.

## Layout

```
aoc_spark/
  session.py          # Spark Connect session (honours SPARK_REMOTE)
  inputs.py           # cache-first input: local file -> Postgres -> adventofcode.com
  y2024/
    day01.py .. day05.py   # part1(spark, data) -> int, part2(spark, data) -> int
notebooks/
  2024-dayNN-*.ipynb  # puzzle summary + example + walkthrough + real answer
tests/
  test_y2024.py       # both parts of all five days, on AoC's published examples
scripts/
  port-forward.sh         # reach Spark Connect from outside the cluster
  pull_inputs_from_pg.sh  # populate the local input cache from Postgres
  verify.py               # run every day against the real input, print JSON
  build_notebooks.py      # regenerate notebooks/ from one content spec
```

Solutions take `(spark, data)` and return an `int`. Keeping them out of the notebooks
means the notebooks import exactly what the tests cover — the walkthrough can't drift
from the implementation.

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

## Validation

Two independent checks, because "it ran" and "it's right" are different claims:

1. **`pytest`** — both parts of all five days against AoC's published examples (10 assertions).
2. **`scripts/verify.py`** — both parts against the real inputs, cross-checked against the
   answers the plain-Python implementation had already stored in
   `adventofcode.y2024.results`.

As of the initial commit all 10 example assertions pass and all 10 real answers match
the Python implementation exactly:

| Day | Part 1 | Part 2 | Spark runtime (p1 / p2) |
|-----|--------|--------|--------------------------|
| 1 | 1151792 | 21790168 | 336 ms / 350 ms |
| 2 | 371 | 426 | 177 ms / 177 ms |
| 3 | 190604937 | 82857512 | 95 ms / 200 ms |
| 4 | 2358 | 1737 | 513 ms / 498 ms |
| 5 | 4924 | 6085 | 796 ms / 982 ms |

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
