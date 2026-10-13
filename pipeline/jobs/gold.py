"""Gold job: silver events -> {data_root}/gold/patient_history/. Selects, renames, casts only."""

from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.config import Conf, load_conf
from pipeline.log import configure_logging, log_counts
from pipeline.schemas import GOLD_SCHEMA, GOLD_SORT_KEYS
from pipeline.spark import build_spark

LAYER = "gold"
GOLD_DATASET = "patient_history"


def conform_to_gold(df: DataFrame) -> DataFrame:
    """Add missing GOLD_SCHEMA fields as typed nulls, order and cast, drop everything else."""
    present = set(df.columns)
    return df.select(
        *[
            (F.col(f.name) if f.name in present else F.lit(None)).cast(f.dataType).alias(f.name)
            for f in GOLD_SCHEMA.fields
        ]
    )


def gold_encounters(df: DataFrame) -> DataFrame:
    return conform_to_gold(
        df.select(
            F.col("patient_key_v1"),
            F.lit("encounter").alias("event_type"),
            F.col("encounter_date").alias("event_date"),
            F.col("discharge_date").alias("event_end_date"),
            F.col("encounter_type").alias("event_code"),
            F.col("encounter_id"),
        )
    )


def order_gold(df: DataFrame) -> DataFrame:
    return df.orderBy(*[F.col(k).asc_nulls_last() for k in GOLD_SORT_KEYS])


def main(spark: SparkSession, conf: Conf) -> None:
    conformed = []
    counts = []
    for source, build in (("encounters", gold_encounters),):
        silver = spark.read.parquet(f"{conf.data_root}/silver/{source}")
        gold_part = build(silver)
        conformed.append(gold_part)
        counts.append((source, silver.count(), gold_part.count()))

    gold = order_gold(reduce(lambda a, b: a.unionByName(b), conformed))
    gold.write.mode("overwrite").parquet(f"{conf.data_root}/{LAYER}/{GOLD_DATASET}")
    for source, rows_in, rows_out in counts:
        log_counts(LAYER, source, rows_in, rows_out)


if __name__ == "__main__":
    configure_logging()
    _conf = load_conf()
    main(build_spark("patient-history-gold"), _conf)
