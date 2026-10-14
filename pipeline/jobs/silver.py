"""Silver job: bronze CSV -> {data_root}/silver/clean_{source}.csv (AD-1, AD-4).

Each transform: null sentinels -> normalize -> drop exact duplicates -> drop rows missing required
fields -> keep one row per key -> derive -> conform_to.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipeline.config import Conf, ConfigError, load_conf
from pipeline.hashing import EDGE_WS, make_hash_udf, normalize_id, with_hashed_patient
from pipeline.log import configure_logging, log_counts
from pipeline.schemas import (
    BRONZE_ENCOUNTERS,
    BRONZE_LABS,
    BRONZE_PROVIDERS,
    CLEAN_ENCOUNTERS,
    CLEAN_LABS,
    CLEAN_PROVIDERS,
    SENTINELS,
    TIMESTAMP_FORMAT,
    conform_to,
)
from pipeline.spark import build_spark
from pipeline.storage import read_csv, write_csv

LAYER = "silver"
_RN = "_rn"

ENCOUNTER_KEY = ("customer", "encounter_id", "provider_id")
LAB_KEY = ("customer", "lab_id")
PROVIDER_KEY = ("customer", "provider_id")

ENCOUNTER_REQUIRED = ("customer", "encounter_id", "provider_id", "start_time")
LAB_REQUIRED = ("customer", "lab_id", "encounter_id")
PROVIDER_REQUIRED = ("customer", "provider_id")


_INNER_WS = r"(?U)\s+"


def _clean(col: Column) -> Column:
    """The single entry for every bronze string: strip Unicode whitespace; a sentinel
    (SENTINELS, case-insensitive, empty included) becomes null."""
    stripped = F.regexp_replace(col, EDGE_WS, "")
    return F.when(~F.lower(stripped).isin(*SENTINELS), stripped)


def _id(col: Column) -> Column:
    """Sentinel-null first, then the shared ID normalization."""
    return normalize_id(_clean(col))


def _test_name(col: Column) -> Column:
    """Trimmed, inner Unicode whitespace runs collapsed to one space, lowercased."""
    return F.lower(F.regexp_replace(_clean(col), _INNER_WS, " "))


def parse_ts(col: Column) -> Column:
    """The one timestamp parser; unparseable or blank text gives null (ANSI off)."""
    return F.to_timestamp(_clean(col), TIMESTAMP_FORMAT)


def _status(col: Column) -> Column:
    return F.lower(_clean(col))


def _run_ts(run_ts: datetime | None) -> datetime:
    """The run time; whole seconds so it survives the CSV round trip."""
    if run_ts is None:
        return datetime.now(UTC).replace(microsecond=0)
    return run_ts


def _require(df: DataFrame, required: tuple[str, ...]) -> DataFrame:
    cond = F.lit(True)
    for name in required:
        cond = cond & F.col(name).isNotNull()
    return df.filter(cond)


def _latest_per_key(df: DataFrame, key: tuple[str, ...], first: list[Column]) -> DataFrame:
    """Keep one row per key: order by `first`, then every other column asc nulls last."""
    rest = [F.col(c).asc_nulls_last() for c in df.columns if c not in key]
    window = Window.partitionBy(*key).orderBy(*first, *rest)
    return df.withColumn(_RN, F.row_number().over(window)).filter(F.col(_RN) == 1).drop(_RN)


def _dedupe(df: DataFrame, required, key, first=None) -> DataFrame:
    df = _require(df.dropDuplicates(), required)
    return _latest_per_key(df, key, first or [])


def clean_encounters(df: DataFrame, salt: str, run_ts: datetime | None = None) -> DataFrame:
    hash_udf = make_hash_udf(salt)
    ts = _run_ts(run_ts)
    df = df.select(
        _id(F.col("customer")).alias("customer"),
        _id(F.col("encounter_id")).alias("encounter_id"),
        _id(F.col("patient_id")).alias("patient_id"),
        _id(F.col("provider_id")).alias("provider_id"),
        parse_ts(F.col("start_time")).alias("start_time"),
        parse_ts(F.col("end_time")).alias("end_time"),
        parse_ts(F.col("updated_at")).alias("updated_at"),
        _status(F.col("status")).alias("status"),
    )
    df = _dedupe(
        df, ENCOUNTER_REQUIRED, ENCOUNTER_KEY, [F.col("updated_at").desc_nulls_last()]
    ).drop("updated_at")

    start, end = F.col("start_time"), F.col("end_time")
    df = df.withColumn(
        "duration_days",
        F.when(end >= start, (end.cast("double") - start.cast("double")) / 86400.0),
    )
    df = with_hashed_patient(df, hash_udf)
    df = df.withColumn("patient_id", F.lit(None).cast("string")).withColumn(
        "partition_timestamp", F.lit(ts).cast("timestamp")
    )
    return conform_to(df, CLEAN_ENCOUNTERS)


def clean_labs(df: DataFrame, run_ts: datetime | None = None) -> DataFrame:
    ts = _run_ts(run_ts)
    df = df.select(
        _id(F.col("customer")).alias("customer"),
        _id(F.col("lab_id")).alias("lab_id"),
        _id(F.col("encounter_id")).alias("encounter_id"),
        _test_name(F.col("test_name")).alias("test_name"),
        _clean(F.col("result_value")).cast("double").alias("result_value"),
        _clean(F.col("result_unit")).alias("result_unit"),
        parse_ts(F.col("observed_time")).alias("observed_time"),
        _status(F.col("status")).alias("status"),
    )
    df = _dedupe(df, LAB_REQUIRED, LAB_KEY)
    df = df.withColumn("partition_timestamp", F.lit(ts).cast("timestamp"))
    return conform_to(df, CLEAN_LABS)


def clean_providers(df: DataFrame, run_ts: datetime | None = None) -> DataFrame:
    ts = _run_ts(run_ts)
    df = df.select(
        _id(F.col("customer")).alias("customer"),
        _id(F.col("provider_id")).alias("provider_id"),
        _clean(F.col("provider_name")).alias("provider_name"),
        _clean(F.col("specialty")).alias("specialty"),
        # Spark's cast accepts true/false/t/f/y/n/yes/no/1/0; anything else is null.
        _clean(F.col("active_flag")).cast("boolean").alias("active_flag"),
    )
    df = _dedupe(df, PROVIDER_REQUIRED, PROVIDER_KEY)
    df = df.withColumn("partition_timestamp", F.lit(ts).cast("timestamp"))
    return conform_to(df, CLEAN_PROVIDERS)


def silver_path(data_root: str, source: str) -> str:
    return f"{data_root}/{LAYER}/clean_{source}.csv"


def main(spark: SparkSession, conf: Conf) -> None:
    run_ts = _run_ts(None)  # one partition_timestamp per run
    jobs = (
        (
            "encounters",
            BRONZE_ENCOUNTERS,
            CLEAN_ENCOUNTERS,
            lambda d: clean_encounters(d, conf.salt, run_ts),
        ),
        ("labs", BRONZE_LABS, CLEAN_LABS, lambda d: clean_labs(d, run_ts)),
        ("providers", BRONZE_PROVIDERS, CLEAN_PROVIDERS, lambda d: clean_providers(d, run_ts)),
    )
    for source, bronze_schema, clean_schema, transform in jobs:
        bronze = read_csv(spark, f"{conf.data_root}/bronze/{source}.csv", bronze_schema)
        out_path = silver_path(conf.data_root, source)
        write_csv(transform(bronze), out_path)
        log_counts(LAYER, source, bronze.count(), read_csv(spark, out_path, clean_schema).count())


if __name__ == "__main__":
    configure_logging()
    try:
        _conf = load_conf()
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(2)
    main(build_spark("provider-activity-silver"), _conf)
