import zlib

import make_dev_input as m


def test_rule_matches_spark_crc32_mod():
    # spark/clean_prices.py --ticker-mod keeps crc32(ticker) % mod == 0 (Spark crc32 == zlib.crc32).
    tickers = ["AAL", "AAPL", "ZTS", "MSFT", "GOOG", "A", "IBM", "XOM", "T", "F", "GE", "KO"]
    lines = ["date,open,high,low,close,volume,Name\n"] + [f"2013-02-08,1,2,0.5,1.5,100,{t}\n" for t in tickers]
    kept = list(m.filter_lines(lines, 3))
    assert kept[0] == lines[0]
    assert [ln.rstrip().split(",")[-1] for ln in kept[1:]] == [t for t in tickers if zlib.crc32(t.encode()) % 3 == 0]


def test_keeps_malformed_rows_and_empty_ticker():
    assert m.keep("broken,line\n", 10)
    assert m.keep("2013-02-08,1,2,0.5,1.5,100,\n", 10)  # crc32("") == 0, like Spark's coalesce(ticker, "")


def test_mod_one_keeps_everything():
    lines = ["h\n", "2013-02-08,1,2,0.5,1.5,100,AAL\n", "x\n"]
    assert list(m.filter_lines(lines, 1)) == lines
