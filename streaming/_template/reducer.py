#!/usr/bin/env python3
"""Template Hadoop Streaming reducer: sums counts per key (input arrives sorted by key)."""
import sys

key, total = None, 0
for line in sys.stdin:
    k, _, v = line.rstrip("\n").partition("\t")
    if k != key:
        if key is not None:
            print(f"{key}\t{total}")
        key, total = k, 0
    total += int(v)
if key is not None:
    print(f"{key}\t{total}")
