#!/usr/bin/env python3
"""Template Hadoop Streaming mapper: counts price rows per ticker.

Input line: ticker,date,open,high,low,close,volume (illustrative; use the real schema from CORP-04).
Copy this folder to streaming/<job>/ and replace the logic; keep tests/sample.in and tests/expected.out in sync.
"""
import sys

for line in sys.stdin:
    fields = line.rstrip("\n").split(",")
    if len(fields) != 7 or fields[0] == "ticker":
        print("_malformed\t1")
        continue
    print(f"{fields[0]}\t1")
