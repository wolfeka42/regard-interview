"""CSV storage contract (AD-2, AD-3): one reader and one writer for every layer.

Paths may be local or `s3://`; all file moves go through the Hadoop FileSystem for
the path, so the code never branches on which.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from pipeline.schemas import TIMESTAMP_FORMAT

# escape='"' makes reads and writes RFC 4180 (doubled quotes) instead of Spark's backslash.
_COMMON = {"header": "true", "escape": '"', "timestampFormat": TIMESTAMP_FORMAT}
READ_OPTIONS = {
    **_COMMON,
    # Fail on a header that differs from the schema instead of mapping by position.
    "enforceSchema": "false",
    "multiLine": "true",
    "mode": "PERMISSIVE",
}
WRITE_OPTIONS = dict(_COMMON)


def read_csv(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    """Read one CSV file under a declared schema; never inferSchema."""
    return spark.read.schema(schema).options(**READ_OPTIONS).csv(path)


def write_csv(df: DataFrame, path: str) -> None:
    """Write `df` as the single CSV file `path`, replacing it (idempotent reruns).

    Steps: clear a leftover `{path}.tmp` -> coalesce(1) write there -> delete the
    target -> rename the one part file to `path` -> delete the temp folder and any
    checksum file beside the target. Errors name paths and counts, never values.
    """
    spark = df.sparkSession
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    HPath = jvm.org.apache.hadoop.fs.Path

    target = HPath(path)
    tmp = HPath(f"{path}.tmp")
    fs = target.getFileSystem(hadoop_conf)

    if fs.exists(tmp):
        fs.delete(tmp, True)
    df.coalesce(1).write.mode("overwrite").options(**WRITE_OPTIONS).csv(f"{path}.tmp")

    parts = fs.globStatus(HPath(f"{path}.tmp/part-*.csv"))
    if parts is None or len(parts) != 1:
        found = 0 if parts is None else len(parts)
        raise RuntimeError(f"Expected one part file under {path}.tmp, found {found}")

    if fs.exists(target):
        fs.delete(target, False)
    if not fs.rename(parts[0].getPath(), target):
        raise RuntimeError(f"Could not move the part file to {path}")
    fs.delete(tmp, True)

    crc = HPath(target.getParent(), f".{target.getName()}.crc")
    if fs.exists(crc):
        fs.delete(crc, False)
