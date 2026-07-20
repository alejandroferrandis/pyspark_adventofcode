"""2024 Day 9 -- Disk Fragmenter, cross-check implementation.

Works on runs rather than individual blocks: gaps are filled from a deque of
files taken off the right, and each placed run is scored with an arithmetic
series instead of a per-block loop.
"""

from __future__ import annotations

from collections import deque


def _run(fid: int, start: int, length: int) -> int:
    """Checksum of `length` blocks of `fid` laid down from position `start`."""
    return fid * (length * start + length * (length - 1) // 2)


def part1(data: str) -> int:
    digits = [int(c) for c in data.strip()]
    files = deque((i // 2, n) for i, n in enumerate(digits) if i % 2 == 0)
    gaps = deque(n for i, n in enumerate(digits) if i % 2 == 1)
    remaining = sum(n for _, n in files)

    checksum = pos = 0
    while remaining:
        fid, length = files.popleft()
        checksum += _run(fid, pos, length)
        pos += length
        remaining -= length

        space = gaps.popleft() if gaps else 0
        while space and remaining:
            fid, length = files.pop()
            taken = min(space, length)
            checksum += _run(fid, pos, taken)
            pos += taken
            space -= taken
            remaining -= taken
            if length > taken:
                files.append((fid, length - taken))

    return checksum
