"""Generate the day notebooks from per-day content specs.

Each day's prose lives in `scripts/specs/dayNN.py`; example data comes from
`scripts/_examples.py` (generated and verified by `scripts/build_examples.py`).
Notebooks import `aoc_spark.y2024.dayNN` rather than inlining it, so the
walkthroughs cannot drift from the tested code.

Days 6-20 render part 1 only -- AoC does not reveal part 2 until part 1 is
solved, so there is nothing to write.

Regenerating overwrites notebooks/*.ipynb -- do exploratory edits in a copy.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEST = ROOT / "notebooks"
SPECS = ROOT / "scripts" / "specs"

# Days 1-5 predate the generated examples module and carry their example inline.
LEGACY_EXAMPLES = {
    1: '"""\\\n3   4\n4   3\n2   5\n1   3\n3   9\n3   3\n"""',
    2: '"""\\\n7 6 4 2 1\n1 2 7 8 9\n9 7 6 2 1\n1 3 2 4 5\n8 6 4 4 1\n1 3 6 7 9\n"""',
    3: None,  # two different examples, handled inline
    4: '"""\\\nMMMSXXMASM\nMSAMXMSMSA\nAMXSXMAAMM\nMSAMASMSMX\nXMASAMXAMM\n'
    'XXAMMXXAMA\nSMSMSASXSS\nSAXAMASAAA\nMAMMMXMMMM\nMXMXAXMASX\n"""',
    5: '"""\\\n47|53\n97|13\n97|61\n97|47\n75|29\n61|13\n75|53\n29|13\n97|29\n53|29\n'
    "61|53\n97|53\n61|29\n47|13\n75|47\n97|75\n47|61\n75|61\n47|29\n75|13\n53|13\n\n"
    '75,47,61,53,29\n97,61,53,29,13\n75,29,13\n75,97,47,61,53\n61,13,29\n97,13,75,29,47\n"""',
}
LEGACY_EXPECTED = {1: (11, 31), 2: (2, 4), 4: (18, 9), 5: (143, 123)}


def load_specs() -> dict[int, dict]:
    specs = {}
    for path in sorted(SPECS.glob("day*.py")):
        day = int(path.stem.removeprefix("day"))
        module_spec = importlib.util.spec_from_file_location(path.stem, path)
        module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(module)
        specs[day] = module.SPEC
    return specs


def load_examples() -> dict[int, dict]:
    path = ROOT / "scripts" / "_examples.py"
    if not path.exists():
        return {}
    module_spec = importlib.util.spec_from_file_location("_examples", path)
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module.EXAMPLES


def md(text: str) -> dict:
    return {
        "cell_type": "markdown",
        "id": "",
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


def _kwargs_call(kwargs: dict) -> str:
    return "".join(f", {k}={v!r}" for k, v in kwargs.items())


def build(day: int, spec: dict, example: dict | None) -> dict:
    nn = f"{day:02d}"
    part1_only = day >= 6

    scope_note = ""
    if part1_only:
        scope_note = (
            "\n> **Part 1 only.** Advent of Code reveals Part Two only after a correct Part "
            "One submission, and this day is unsolved — so part 2's text does not exist to "
            "work from yet. Submitting the answer below unlocks it.\n"
        )

    cells = [
        md(
            f"# AoC 2024 Day {day} — {spec['title']}\n\n"
            f"**{spec['lesson']}**\n\n"
            f"Puzzle: <https://adventofcode.com/2024/day/{day}>\n\n"
            "---\n\n"
            "> **On puzzle text and inputs.** Advent of Code is Eric Wastl's work, and he asks "
            "that puzzle text and per-user inputs not be redistributed. So this notebook carries "
            "a summary in my own words plus the *published* example, and pulls the real input at "
            "runtime from a local cache that is gitignored. Read the puzzle at the link above.\n"
            f"{scope_note}"
            "\n---\n\n"
            "## The puzzle\n\n"
            f"{spec['summary']}"
        ),
        md(f"## The approach\n\n{spec['angle']}"),
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
    elif not part1_only:
        expected = LEGACY_EXPECTED[day]
        cells += [
            md("## The published example\n\nThe same data the test suite asserts on."),
            code(
                f"EXAMPLE = {LEGACY_EXAMPLES[day]}\n"
                "\n"
                f"print('part 1:', day{nn}.part1(spark, EXAMPLE), '(expected {expected[0]})')\n"
                f"print('part 2:', day{nn}.part2(spark, EXAMPLE), '(expected {expected[1]})')"
            ),
        ]
    else:
        call = _kwargs_call(example["kwargs"])
        note = ""
        if example["kwargs"]:
            shown = ", ".join(f"`{k}={v!r}`" for k, v in example["kwargs"].items())
            note = (
                f"\n\nThe example runs at a different scale than the real input, so "
                f"{shown} are passed explicitly rather than hardcoded."
            )
        cells += [
            md(f"## The published example\n\nThe same data the test suite asserts on.{note}"),
            code(
                f"EXAMPLE = {example['data']!r}\n"
                "\n"
                f"print('part 1:', day{nn}.part1(spark, EXAMPLE{call}), "
                f"'(expected {example['expected']})')"
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
    ]

    if part1_only:
        call = _kwargs_call(example["kwargs"]) if example["kwargs"] else ""
        # Real-input defaults differ from the example's, so call with defaults here.
        cells.append(
            code(
                "import time\n"
                "\n"
                f"data = get_input(2024, {day})\n"
                "print(f'input: {len(data):,} chars, {len(data.splitlines()):,} lines')\n"
                "\n"
                "started = time.perf_counter()\n"
                f"answer = day{nn}.part1(spark, data)\n"
                "print(f'part 1: {answer}  ({(time.perf_counter() - started) * 1000:.0f} ms)')"
            )
        )
    else:
        cells.append(
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
                "    print(f'part {part}: {answer}  "
                "({(time.perf_counter() - started) * 1000:.0f} ms)')"
            )
        )

    if part1_only:
        cells.append(
            md(
                "## Cross-check\n\n"
                "Days 6+ have no known-good answer, so correctness rests on an independently "
                "written plain-Python implementation agreeing with the Spark one. That is "
                "evidence, not proof — a shared misreading of the puzzle would survive both."
            )
        )
        cells.append(
            code(
                f"from reference_python.y2024 import day{nn} as reference\n"
                "\n"
                f"cross = reference.part1(data)\n"
                "print('reference:', cross)\n"
                "print('agree:    ', cross == answer)"
            )
        )

    cells.append(md(f"## Notes & gotchas\n\n{spec['notes']}"))

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
    specs = load_specs()
    examples = load_examples()

    for day, spec in sorted(specs.items()):
        slug = spec["title"].lower().replace(" ", "-").replace("'", "")
        path = DEST / f"2024-day{day:02d}-{slug}.ipynb"
        path.write_text(json.dumps(build(day, spec, examples.get(day)), indent=1) + "\n")
        print(f"wrote {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
