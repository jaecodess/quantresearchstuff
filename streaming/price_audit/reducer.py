#!/usr/bin/env python3
"""Hadoop Streaming reducer for the price audit (PIPE-02): one audit row per ticker.

Columns: ticker, rows, first_date, last_date, n_missing, n_bad_value, n_ohlc_invalid,
n_zero_volume, n_dup_dates, n_overnight_jumps.
  n_missing       a price or volume field is empty
  n_bad_value     a non-empty field is not a number, or the date is not yyyy-mm-dd
  n_ohlc_invalid  high < low, or open/close outside [low, high], or a price <= 0
  n_dup_dates     extra rows for a date already seen for this ticker
  n_overnight_jumps |ln(open_t / close_t-1)| > 0.25 between consecutive valid rows (split screening)
Special keys _header and _malformed are passed through as counts.
"""

import math
import sys
from datetime import datetime

JUMP = 0.25


def parse(value):
    d, *nums = value.split(",")
    try:
        datetime.strptime(d, "%Y-%m-%d")
    except ValueError:
        return None, None, "bad"
    if any(x == "" for x in nums):
        return d, None, "missing"
    try:
        o, h, lo, c, v = (float(x) for x in nums)
    except ValueError:
        return d, None, "bad"
    return d, (o, h, lo, c, v), None


def audit(ticker, values):
    if ticker in ("_header", "_malformed"):
        print(f"{ticker}\t{sum(int(v) for v in values)}")
        return
    n_missing = n_bad = n_ohlc = n_zero = n_dup = 0
    seen, good, dates = set(), [], []
    for value in values:
        d, nums, problem = parse(value)
        if d is not None:
            dates.append(d)
            if d in seen:
                n_dup += 1
            seen.add(d)
        if problem == "missing":
            n_missing += 1
            continue
        if problem == "bad":
            n_bad += 1
            continue
        o, h, lo, c, v = nums
        if v == 0:
            n_zero += 1
        if h < lo or not (lo <= o <= h) or not (lo <= c <= h) or min(o, h, lo, c) <= 0:
            n_ohlc += 1
            continue
        good.append((d, o, c))
    good.sort()
    jumps = sum(1 for a, b in zip(good, good[1:]) if abs(math.log(b[1] / a[2])) > JUMP)
    valid_dates = sorted(dates)
    first, last = (valid_dates[0], valid_dates[-1]) if valid_dates else ("", "")
    print(f"{ticker}\t{len(values)}\t{first}\t{last}\t{n_missing}\t{n_bad}\t{n_ohlc}\t{n_zero}\t{n_dup}\t{jumps}")


key, values = None, []
for line in sys.stdin:
    k, _, v = line.rstrip("\n").partition("\t")
    if k != key:
        if key is not None:
            audit(key, values)
        key, values = k, []
    values.append(v)
if key is not None:
    audit(key, values)
