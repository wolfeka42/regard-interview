"""Gold job: the three silver files -> {data_root}/gold/provider_daily_activity.csv.

Joins and aggregates only (AD-4); never reads bronze or touches patient_id.
"""

from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.config import Conf, ConfigError, load_conf
from pipeline.log import configure_logging, log_counts
from pipeline.schemas import (
    CLEAN_ENCOUNTERS,
    CLEAN_LABS,
    CLEAN_PROVIDERS,
    GOLD_SCHEMA,
    GOLD_SORT_KEYS,
    conform_to,
)
from pipeline.spark import build_spark
from pipeline.storage import read_csv, write_csv

LAYER = "gold"
GOLD_DATASET = "provider_daily_activity"
CANCELLED_STATUSES = ("cancelled", "canceled")
RETRACTED = "retracted"
DAY_KEY = ("customer", "provider_id", "encounter_date")


def gold_provider_daily_activity(
    encounters: DataFrame, labs: DataFrame, providers: DataFrame
) -> DataFrame:
    """One row per (customer, provider_id, encounter_date); see data-model.md "Gold metrics"."""
    # A null status counts: only an explicit cancelled/canceled is removed.
    active = encounters.filter(~F.coalesce(F.col("status").isin(*CANCELLED_STATUSES), F.lit(False)))
    # AD-4: one row per (customer, encounter_id, provider_id) before any lab join.
    reduced = active.select(
        "customer",
        "encounter_id",
        "provider_id",
        F.to_date("start_time").alias("encounter_date"),
        "hashedpatientid",
        "duration_days",
    ).dropDuplicates(["customer", "encounter_id", "provider_id"])

    encounter_metrics = reduced.groupBy(*DAY_KEY).agg(
        F.countDistinct("encounter_id").alias("encounter_count"),
        F.countDistinct("hashedpatientid").alias("unique_patients"),
        F.coalesce(F.sum("duration_days"), F.lit(0.0)).alias("totaldurationdays"),
    )

    counted_labs = labs.filter(F.col("status").eqNullSafe(RETRACTED) == F.lit(False)).select(
        "customer", "encounter_id", "lab_id"
    )
    lab_metrics = (
        reduced.select("customer", "encounter_id", *DAY_KEY[1:])
        .join(counted_labs, ["customer", "encounter_id"], "inner")
        .groupBy(*DAY_KEY)
        .agg(F.countDistinct("lab_id").alias("lab_count"))
    )

    provider_fields = providers.select(
        "customer", "provider_id", "provider_name", "specialty", "active_flag"
    )
    out = (
        encounter_metrics.join(lab_metrics, list(DAY_KEY), "left")
        .withColumn("lab_count", F.coalesce(F.col("lab_count"), F.lit(0)))
        .join(provider_fields, ["customer", "provider_id"], "left")
    )
    return conform_to(out, GOLD_SCHEMA)


def order_gold(df: DataFrame) -> DataFrame:
    return df.orderBy(*[F.col(k).asc_nulls_last() for k in GOLD_SORT_KEYS])


def gold_path(data_root: str) -> str:
    return f"{data_root}/{LAYER}/{GOLD_DATASET}.csv"


def main(spark: SparkSession, conf: Conf) -> None:
    silver = f"{conf.data_root}/silver"
    encounters = read_csv(spark, f"{silver}/clean_encounters.csv", CLEAN_ENCOUNTERS)
    labs = read_csv(spark, f"{silver}/clean_labs.csv", CLEAN_LABS)
    providers = read_csv(spark, f"{silver}/clean_providers.csv", CLEAN_PROVIDERS)

    out_path = gold_path(conf.data_root)
    write_csv(order_gold(gold_provider_daily_activity(encounters, labs, providers)), out_path)
    log_counts(
        LAYER,
        GOLD_DATASET,
        encounters.count(),
        read_csv(spark, out_path, GOLD_SCHEMA).count(),
    )


if __name__ == "__main__":
    configure_logging()
    try:
        _conf = load_conf()
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(2)
    main(build_spark("provider-activity-gold"), _conf)
