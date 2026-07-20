"""2024 Day 19 reference -- trie walk with a backward DP over suffixes.

Descending the trie from each position yields the usable towel lengths without
slicing, and the DP runs end-to-start rather than start-to-end.
"""

from __future__ import annotations

END = "$"


def _build_trie(patterns: list[str]) -> dict:
    root: dict = {}
    for pattern in patterns:
        node = root
        for ch in pattern:
            node = node.setdefault(ch, {})
        node[END] = True
    return root


def _matchable(design: str, trie: dict) -> bool:
    n = len(design)
    # reachable[i] is True when design[i:] can be built; the empty suffix always can.
    reachable = [False] * n + [True]
    for i in range(n - 1, -1, -1):
        node = trie
        for j in range(i, n):
            node = node.get(design[j])
            if node is None:
                break
            if END in node and reachable[j + 1]:
                reachable[i] = True
                break
    return reachable[0]


def part1(data: str) -> int:
    patterns_block, designs_block = data.strip().split("\n\n")
    trie = _build_trie([p.strip() for p in patterns_block.split(",")])
    return sum(_matchable(design, trie) for design in designs_block.splitlines())
