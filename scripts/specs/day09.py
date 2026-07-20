"""Notebook content spec for 2024 day 09."""

SPEC = {'angle': 'Another plain-Python day, and this time the honest reason is **scale, not '
          'impossibility**.\n'
          '\n'
          'The puzzle *describes* a sequential process: move the rightmost file block into the '
          'leftmost gap, repeat. Taken literally that is a loop with a dependency on every '
          'iteration. But the description collapses — the final layout is just "file blocks in '
          'reverse order, poured into the gaps left to right", which a **two-pointer sweep** '
          'computes in one pass: `left` walks forward looking for free, `right` walks back '
          'looking for occupied, swap. 94,595 blocks, 118,296 pointer moves, 23,702 swaps, '
          '**16 ms**.\n'
          '\n'
          'And it is genuinely expressible in Spark. Explode the disk map to one row per block, '
          '`row_number()` over the free positions ascending and over the occupied blocks '
          'descending, join on that rank, and the two windows *are* the compaction. It would '
          'work. It would also be:\n'
          '\n'
          '- two full sorts and a join over 94,595 rows,\n'
          '- more code than the loop, in a shape that needs a paragraph of explanation, and\n'
          '- slower, because a trivial Spark Connect query costs tens of milliseconds before it '
          'reads a single row, and the entire Python solution — parse, compact, checksum — '
          'finishes in about 20 ms.\n'
          '\n'
          'The crossover would come somewhere north of a few million blocks. This input is not '
          'there, and pretending otherwise would be the actual mistake.',
 'explore_code': "def render(disk):\n"
                 "    return ''.join('.' if b == day09.FREE else str(b) for b in disk)\n"
                 '\n'
                 '\n'
                 "print('12345    ->', render(day09.blocks('12345')))\n"
                 "print('90909    ->', render(day09.blocks('90909')))\n"
                 "print('EXAMPLE  ->', render(day09.blocks(EXAMPLE)))\n"
                 '\n'
                 'disk = day09.blocks(EXAMPLE)\n'
                 "files = sum(1 for b in disk if b != day09.FREE)\n"
                 "print(f'\\n{len(disk)} blocks: {files} used, {len(disk) - files} free')\n"
                 '\n'
                 '# The two pointers, and the disk after each swap. Every line depends\n'
                 '# on the line above it -- the gap you fill next is the one the previous\n'
                 '# swap did not reach.\n'
                 'left, right = 0, len(disk) - 1\n'
                 'swaps = 0\n'
                 'print()\n'
                 'while left < right:\n'
                 '    if disk[left] != day09.FREE:\n'
                 '        left += 1\n'
                 '    elif disk[right] == day09.FREE:\n'
                 '        right -= 1\n'
                 '    else:\n'
                 '        disk[left], disk[right] = disk[right], day09.FREE\n'
                 '        swaps += 1\n'
                 "        print(f'swap {swaps:2d}  left={left:2d} right={right:2d}  "
                 "{render(disk)}')\n"
                 '\n'
                 "checksum = sum(pos * fid for pos, fid in enumerate(disk) if fid != "
                 'day09.FREE)\n'
                 "print(f'\\n{swaps} swaps, pointers met at {left}, checksum {checksum}')",
 'explore_md': '### Watching the pointers close\n'
               '\n'
               'The cell below prints the disk after every single swap on the published '
               'example, alongside where the two pointers sit. Compare the last line with the '
               "puzzle's final layout — and note that neither pointer ever revisits a "
               'position.',
 'lesson': 'Python — two-pointer compaction over the block list',
 'notes': '- The disk map is **positional**: even digit index = a file, odd = a gap. That is '
          'the only reason the file ID is `i // 2`. Miscount the alternation by one and every '
          'ID shifts.\n'
          '- **Zero-length runs are legal** and common — a `0` digit in a gap slot means two '
          'files touch. `blocks()` handles it by extending with an empty list; anything that '
          'assumes every run has length ≥ 1 breaks here.\n'
          '- `FREE` is `-1`, and the checks are `!= FREE` / `== FREE`, never truthiness. File '
          'ID **0 is falsy**, so `if fid:` would silently treat the first file as empty space.\n'
          '- 19,999 digits expand to 94,595 block entries. The expansion is the memory cost of '
          'this approach; the run-based cross-check in `reference_python/y2024/day09.py` avoids '
          'it entirely by scoring each placed run with an arithmetic series.\n'
          '- The input is **one very long line** — `strip()` removes the trailing newline, and '
          "`splitlines()` is never called. Any stray whitespace inside would blow up `int()`.\n"
          '- The checksum is 6,398,252,054,886 — comfortably past 2^32. Python does not care; a '
          'Spark port would need `bigint` throughout.\n'
          '- The loop terminates when the pointers meet. The final `sum` skips `FREE` anyway, '
          'so the exact meeting cell needs no special handling.',
 'summary': 'The input is a single line of digits describing a disk, alternating **file '
            'length** and **free length**, starting with a file. Files are numbered from 0 in '
            'the order they appear, so the file at digit index *i* has ID *i/2*.\n'
            '\n'
            '```\n'
            '12345   ->   0..111....22222\n'
            '```\n'
            '\n'
            'Compact it by repeatedly taking the **rightmost** file block and moving it into '
            'the **leftmost** free block, one block at a time, until no gap remains before the '
            'last file block. The checksum is then the sum of `position × file ID` over every '
            'occupied block, skipping the free ones.\n'
            '\n'
            '- **Part 1** — the checksum after compaction.',
 'title': 'Disk Fragmenter'}
