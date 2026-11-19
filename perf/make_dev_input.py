#!/usr/bin/env python3
"""Build the development-profile price file from the raw price CSV (PERF-03).

Keeps the header and every row whose ticker passes the same deterministic rule as
`spark/clean_prices.py --ticker-mod`: crc32(ticker) % mod == 0, with an empty ticker hashed as "".
Rows with the wrong field count are kept, so the dev run still exercises the rejection path.
The output is a derived input, never written over the raw corpus.

  hdfs dfs -cat <raw_dir>/all_stocks_5yr.csv | python3 perf/make_dev_input.py --ticker-mod 10 \
    | hdfs dfs -put - <dev_dir>/all_stocks_5yr.csv
"""

from __future__ import annotations

import argparse
import sys
import zlib

N_FIELDS = 7  # date,open,high,low,close,volume,Name


def keep(line: str, mod: int) -> bool:
    fields = line.rstrip("\r\n").split(",")
    if len(fields) != N_FIELDS:
        return True
    return zlib.crc32(fields[-1].encode("utf-8")) % mod == 0


def filter_lines(lines, mod: int):
    """Yield the header, then every line kept by the ticker rule."""
    for i, line in enumerate(lines):
        if i == 0 or keep(line, mod):
            yield line


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--ticker-mod", type=int, default=10)
    a = p.parse_args(argv)
    kept = 0
    for line in filter_lines(sys.stdin, a.ticker_mod):
        sys.stdout.write(line)
        kept += 1
    print(f"make_dev_input: ticker_mod={a.ticker_mod} lines_written={kept}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
