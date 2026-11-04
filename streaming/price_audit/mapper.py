#!/usr/bin/env python3
"""Hadoop Streaming mapper for the price audit (PIPE-02).

Input: raw daily price CSV lines (date,open,high,low,close,volume,Name), header included.
Output: ticker<TAB>date,open,high,low,close,volume so each reducer sees one ticker's rows together.
Special keys: _header (header lines), _malformed (wrong field count or no ticker).
"""

import sys

for line in sys.stdin:
    line = line.rstrip("\r\n")
    if not line:
        continue
    f = line.split(",")
    if f[0] == "date":
        print("_header\t1")
        continue
    if len(f) != 7 or not f[6].strip():
        print("_malformed\t1")
        continue
    print(f"{f[6].strip()}\t{','.join(x.strip() for x in f[:6])}")
