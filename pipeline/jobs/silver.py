"""Silver job: bronze -> {data_root}/silver/{source}/ (typed, keyed, identifiers dropped)."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.config import Conf, load_conf
from pipeline.log import configure_logging, log_counts
from pipeline.patient_key import (
    EDGE_WS,
    drop_invalid_identity,
    make_key_udf,
    normalize_facility,
    normalize_mrn,
    with_patient_key,
)
from pipeline.schemas import DATE_FORMAT, SILVER_ENCOUNTERS
from pipeline.spark import build_spark

LAYER = "silver"


def _clean(col: Column) -> Column:
    """Strip Unicode whitespace; empty becomes null."""
    stripped = F.regexp_replace(col, EDGE_WS, "")
    return F.when(stripped != "", stripped)


def parse_date(col: Column) -> Column:
    """The one date parser; unparseable or blank text gives null (ANSI off)."""
    return F.to_date(_clean(col), DATE_FORMAT)


def silver_encounters(df: DataFrame, key_udf) -> DataFrame:
    df = df.withColumn("facility", normalize_facility(F.col("facility"))).withColumn(
        "mrn", normalize_mrn(F.col("mrn"))
    )
    df = drop_invalid_identity(df)

    discharge_text = _clean(F.col("discharge_date"))
    df = (
        df.withColumn("encounter_date", parse_date(F.col("encounter_date")))
        .withColumn("discharge_present", discharge_text.isNotNull())
        .withColumn("discharge_date", parse_date(F.col("discharge_date")))
        .filter(
            F.col("encounter_date").isNotNull()
            & (~F.col("discharge_present") | F.col("discharge_date").isNotNull())
        )
        .withColumn("encounter_id", _clean(F.col("encounter_id")))
        .withColumn("encounter_type", _clean(F.col("encounter_type")))
    )

    df = with_patient_key(df, key_udf)
    return df.select(*[F.col(f.name).cast(f.dataType) for f in SILVER_ENCOUNTERS.fields])


def main(spark: SparkSession, conf: Conf) -> None:
    key_udf = make_key_udf(conf.hmac_secret)
    source = "encounters"
    bronze = spark.read.parquet(f"{conf.data_root}/bronze/{source}")
    out_path = f"{conf.data_root}/{LAYER}/{source}"
    silver_encounters(bronze, key_udf).write.mode("overwrite").parquet(out_path)
    log_counts(LAYER, source, bronze.count(), spark.read.parquet(out_path).count())


if __name__ == "__main__":
    configure_logging()
    _conf = load_conf()
    main(build_spark("patient-history-silver"), _conf)
