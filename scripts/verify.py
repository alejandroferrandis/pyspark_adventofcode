"""Run each implemented day against the real input and print JSON.

    python scripts/verify.py          # all days
    python scripts/verify.py 1 3 5    # selected

Needs Spark Connect reachable and the input cache populated.
"""

from __future__ import annotations

import importlib
import json
import sys
import time

from aoc_spark.inputs import get_input
from aoc_spark.session import get_spark

YEAR = 2024
IMPLEMENTED = [1, 2, 3, 4, 5]


def main(days: list[int]) -> None:
    spark = get_spark("aoc-pyspark-verify")
    results: dict[str, dict] = {}

    for day in days:
        module = importlib.import_module(f"aoc_spark.y{YEAR}.day{day:02d}")
        data = get_input(YEAR, day)
        entry: dict[str, dict] = {}
        for part in (1, 2):
            fn = getattr(module, f"part{part}", None)
            if fn is None:
                continue
            started = time.perf_counter()
            answer = fn(spark, data)
            elapsed = round((time.perf_counter() - started) * 1000, 1)
            entry[f"part{part}"] = {"answer": answer, "runtime_ms": elapsed}
        results[f"day{day:02d}"] = entry
        print(f"day {day:02d}: {entry}", file=sys.stderr)

    print(json.dumps(results, indent=2))
    spark.stop()


if __name__ == "__main__":
    requested = [int(a) for a in sys.argv[1:]] or IMPLEMENTED
    main(requested)
