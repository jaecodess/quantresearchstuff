#!/usr/bin/env python3
"""Hadoop Streaming reducer for gdelt_daily (PIPE-03): one row per archive_date.

Normally there is one archive per date, so this mostly passes rows through. If the same date
appears twice (a re-acquired or duplicated archive), counts are summed, n_archives shows it,
and source_file lists every file, so the duplication is visible rather than hidden.
Adds tone_wmean = sum_tone_x_articles / sum_articles as the last column. Archive SHA-256 values are
passed through (comma-joined if a date has several archives).
Input arrives sorted by key (archive_date).
"""

import sys

SUM_FIELDS = 23  # columns 1..23 of the mapper output are additive
SUM_TONE, SUM_ART = 16, 15  # positions (0-based, key = 0) of sum_tone_x_articles and sum_articles


def num(s):
    return float(s) if ("." in s or "e" in s.lower()) else int(s)


def emit(key, sums, ids, files, shas):
    tone = sums[SUM_TONE - 1] / sums[SUM_ART - 1] if sums[SUM_ART - 1] else ""
    lo = min(ids) if ids else ""
    hi = max(ids) if ids else ""
    vals = [key] + [repr(round(v, 6)) if isinstance(v, float) else str(v) for v in sums]
    vals += [str(lo), str(hi), ",".join(sorted(files)), ",".join(sorted(shas))]
    vals.append(repr(round(tone, 6)) if tone != "" else "")
    print("\t".join(vals))


key, sums, ids, files, shas = None, None, [], set(), set()
for line in sys.stdin:
    f = line.rstrip("\n").split("\t")
    if len(f) != SUM_FIELDS + 5:
        sys.stderr.write("reporter:counter:gdelt,reducer_bad_line,1\n")
        continue
    if f[0] != key:
        if key is not None:
            emit(key, sums, ids, files, shas)
        key, sums, ids, files, shas = f[0], [0] * SUM_FIELDS, [], set(), set()
    for i in range(SUM_FIELDS):
        sums[i] += num(f[i + 1])
    ids += [int(x) for x in f[SUM_FIELDS + 1 : SUM_FIELDS + 3] if x]
    files.add(f[SUM_FIELDS + 3])
    if f[SUM_FIELDS + 4]:
        shas.add(f[SUM_FIELDS + 4])
if key is not None:
    emit(key, sums, ids, files, shas)
