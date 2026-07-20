"""Cross-check the Spark and plain-Python implementations on the real input.

Days 6+ have no known-good answer, so agreement between two independently
written implementations is the correctness signal. Disagreement means at least
one is wrong; agreement is evidence, not proof.

    python scripts/crosscheck.py           # all days with both implementations
    python scripts/crosscheck.py 6 7 8
"""

from __future__ import annotations

import importlib
import sys
import time

from aoc_spark.inputs import get_input
from aoc_spark.session import get_spark

YEAR = 2024


def _load(package: str, day: int):
    try:
        return importlib.import_module(f"{package}.y{YEAR}.day{day:02d}")
    except ModuleNotFoundError:
        return None


def main(days: list[int]) -> int:
    spark = get_spark("aoc-crosscheck")
    rows, mismatches = [], 0

    for day in days:
        primary = _load("aoc_spark", day)
        reference = _load("reference_python", day)
        if primary is None or reference is None:
            continue
        data = get_input(YEAR, day)

        for part in (1, 2):
            pfn = getattr(primary, f"part{part}", None)
            rfn = getattr(reference, f"part{part}", None)
            if pfn is None or rfn is None:
                continue

            started = time.perf_counter()
            got = pfn(spark, data)
            spark_ms = (time.perf_counter() - started) * 1000

            started = time.perf_counter()
            expected = rfn(data)
            py_ms = (time.perf_counter() - started) * 1000

            agree = got == expected
            mismatches += not agree
            rows.append((day, part, got, expected, agree, spark_ms, py_ms))
            print(
                f"day {day:2d} part {part}: {got!s:>18}  "
                f"{'agree' if agree else 'MISMATCH vs ' + str(expected)}  "
                f"({spark_ms:.0f} ms spark / {py_ms:.0f} ms python)",
                flush=True,
            )

    spark.stop()
    print()
    print(f"{len(rows)} parts checked, {mismatches} mismatch(es)")
    return 1 if mismatches else 0


if __name__ == "__main__":
    requested = [int(a) for a in sys.argv[1:]] or list(range(1, 26))
    raise SystemExit(main(requested))
