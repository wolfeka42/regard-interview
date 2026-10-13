"""Bronze job: raw files -> {data_root}/bronze/{source}/ under an all-text schema."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from pipeline.config import Conf, load_conf
from pipeline.log import configure_logging, log_counts
from pipeline.schemas import INPUT_FORMATS, RAW_ENCOUNTERS
from pipeline.spark import build_spark

LAYER = "bronze"


def bronze_encounters(df: DataFrame) -> DataFrame:
    """No transformation: keep raw text columns as declared."""
    return df.select(*RAW_ENCOUNTERS.fieldNames())


def read_raw(spark: SparkSession, conf: Conf, source: str, schema: StructType) -> DataFrame:
    fmt = INPUT_FORMATS.get(source, "csv")
    path = f"{conf.raw_root}/{source}.{fmt}"
    reader = spark.read.schema(schema).option("mode", "PERMISSIVE")
    if fmt == "csv":
        return (
            reader.option("header", "true")
            # Fail on a header that differs from the schema instead of mapping by position.
            .option("enforceSchema", "false")
            .option("multiLine", "true")
            .csv(path)
        )
    if fmt == "json":
        return reader.json(path)
    raise ValueError(f"Unsupported input format for source {source}")


def main(spark: SparkSession, conf: Conf) -> None:
    source = "encounters"
    raw = read_raw(spark, conf, source, RAW_ENCOUNTERS)
    out_path = f"{conf.data_root}/{LAYER}/{source}"
    bronze_encounters(raw).write.mode("overwrite").parquet(out_path)
    log_counts(LAYER, source, raw.count(), spark.read.parquet(out_path).count())


if __name__ == "__main__":
    configure_logging()
    _conf = load_conf()
    main(build_spark("patient-history-bronze"), _conf)
