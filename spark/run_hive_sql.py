#!/usr/bin/env python3
"""Run a hive/*.sql file through Spark SQL with Hive support (DEVELOPMENT ONLY).

On EMR, Hive scripts run with `hive --hivevar k=v -f file.sql`; that is the assessed path.
Locally there is no Hive, so this helper executes the same file in a local Spark session with a
Derby metastore in the working directory, substituting ${hivevar:k} the way Hive does. It checks the
SQL logic, not Hive-specific behaviour.

  python3 spark/run_hive_sql.py hive/30_calendar_weeks.sql root=/tmp/inf2006 run_id=local-dev
"""

import re
import sys

from pyspark.sql import SparkSession


def statements(text):
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("--")]
    for stmt in "\n".join(lines).split(";"):
        if stmt.strip():
            yield stmt.strip()


def main(argv):
    path, pairs = argv[0], dict(a.split("=", 1) for a in argv[1:])
    sql = open(path).read()
    sql = re.sub(r"\$\{hivevar:(\w+)\}", lambda m: pairs[m.group(1)], sql)
    spark = SparkSession.builder.appName("local_hive_sql").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    for db in ("raw", "clean", "features", "model", "analytics"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    for stmt in statements(sql):
        df = spark.sql(stmt)
        if stmt.lstrip().upper().startswith(("SELECT", "WITH")):
            print(f"-- {stmt.splitlines()[0][:80]}")
            df.show(60, truncate=False)


if __name__ == "__main__":
    main(sys.argv[1:])
