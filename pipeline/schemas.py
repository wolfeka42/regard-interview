"""Every declared schema and shared constant lives here (AD-3); see data-model.md.

The day-of schema swap edits this file only.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Interview constants. DEFAULT_SALT is the only allowed default secret (AD-7).
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX"
DEFAULT_SALT = "regard-demo-salt"

# One shared list for every bronze string field: compared lowercase after a Unicode trim.
# "na" is deliberately absent so "Na" (sodium) stays real text (Kailey, 2026-10-14).
SENTINELS = ("", "null", "n/a", "none", "-", "?", "unknown")


def _all_strings(*names: str) -> StructType:
    return StructType([StructField(n, StringType(), True) for n in names])


# Bronze: raw CSV, every column read as text so silver decides what parses.
BRONZE_ENCOUNTERS = _all_strings(
    "customer",
    "encounter_id",
    "patient_id",
    "provider_id",
    "start_time",
    "end_time",
    "updated_at",
    "status",
)
BRONZE_LABS = _all_strings(
    "customer",
    "lab_id",
    "encounter_id",
    "test_name",
    "result_value",
    "result_unit",
    "observed_time",
    "status",
)
BRONZE_PROVIDERS = _all_strings(
    "customer", "provider_id", "provider_name", "specialty", "active_flag"
)

# Silver, exactly as pasted in the interview prompt.
CLEAN_ENCOUNTERS = StructType(
    [
        StructField("customer", StringType(), True),
        StructField("encounter_id", StringType(), True),
        StructField("patient_id", StringType(), True),  # always null (D1)
        StructField("provider_id", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("duration_days", DoubleType(), True),
        StructField("hashedpatientid", StringType(), True),
        StructField("partition_timestamp", TimestampType(), True),
    ]
)
CLEAN_LABS = StructType(
    [
        StructField("customer", StringType(), True),
        StructField("lab_id", StringType(), True),
        StructField("encounter_id", StringType(), True),
        StructField("test_name", StringType(), True),
        StructField("result_value", DoubleType(), True),
        StructField("result_unit", StringType(), True),
        StructField("observed_time", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("partition_timestamp", TimestampType(), True),
    ]
)
CLEAN_PROVIDERS = StructType(
    [
        StructField("customer", StringType(), True),
        StructField("provider_id", StringType(), True),
        StructField("provider_name", StringType(), True),
        StructField("specialty", StringType(), True),
        StructField("active_flag", BooleanType(), True),
        StructField("partition_timestamp", TimestampType(), True),
    ]
)

# Gold: provider_daily_activity. Treated as PHI; no patient-level field.
GOLD_SCHEMA = StructType(
    [
        StructField("customer", StringType(), True),
        StructField("provider_id", StringType(), True),
        StructField("encounter_date", DateType(), True),
        StructField("provider_name", StringType(), True),
        StructField("specialty", StringType(), True),
        StructField("active_flag", BooleanType(), True),
        StructField("encounter_count", LongType(), True),
        StructField("unique_patients", LongType(), True),
        StructField("totaldurationdays", DoubleType(), True),
        StructField("lab_count", LongType(), True),
    ]
)

GOLD_SORT_KEYS = ("customer", "provider_id", "encounter_date")


def conform_to(df: DataFrame, schema: StructType) -> DataFrame:
    """AD-5: add missing fields as typed nulls, select in schema order, cast, drop the rest.

    Lives here, not in a job module, because silver and gold both use it and jobs never
    import each other (AD-1).
    """
    present = set(df.columns)
    return df.select(
        *[
            (F.col(f.name) if f.name in present else F.lit(None)).cast(f.dataType).alias(f.name)
            for f in schema.fields
        ]
    )
