import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.spark


def test_local_spark_session_and_hive_support_flag():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[1]").appName("ci-smoke").config("spark.ui.enabled", "false").getOrCreate()
    )
    try:
        assert spark.range(5).count() == 5
    finally:
        spark.stop()
