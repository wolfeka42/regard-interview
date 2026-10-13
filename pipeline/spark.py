"""SparkSession construction shared by every entry point."""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession


def build_spark(app_name: str, local: bool = False) -> SparkSession:
    """ANSI off and CSV PERMISSIVE keep row values out of Spark's error text.

    `local=True` pins a local master and this interpreter for Python workers; on
    EMR, spark-submit supplies both.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        # Unparseable dates become null instead of raising an upgrade error.
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    )
    if local:
        builder = (
            builder.master("local[*]")
            .config("spark.pyspark.python", sys.executable)
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
