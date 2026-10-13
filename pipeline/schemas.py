"""Every declared schema lives here (AD-3); the day-of schema swap edits this file."""

from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

DATE_FORMAT = "yyyy-MM-dd"

# Raw read and bronze: all text, so silver can count what fails to parse.
RAW_ENCOUNTERS = StructType(
    [
        StructField("facility", StringType(), True),
        StructField("mrn", StringType(), True),
        StructField("encounter_id", StringType(), True),
        StructField("encounter_date", StringType(), True),
        StructField("encounter_type", StringType(), True),
        StructField("discharge_date", StringType(), True),
    ]
)

# Input format per source: "csv" (default) or "json".
INPUT_FORMATS = {
    "encounters": "csv",
}

# Silver events hold the key, never facility or mrn (AD-4).
SILVER_ENCOUNTERS = StructType(
    [
        StructField("patient_key_v1", StringType(), True),
        StructField("encounter_id", StringType(), True),
        StructField("encounter_date", DateType(), True),
        StructField("encounter_type", StringType(), True),
        StructField("discharge_date", DateType(), True),
    ]
)

# Doctor-facing gold. PHI for treatment use: full service dates, no direct identifiers.
GOLD_SCHEMA = StructType(
    [
        StructField("patient_key_v1", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_date", DateType(), True),
        StructField("event_end_date", DateType(), True),
        StructField("event_code", StringType(), True),
        StructField("event_value", StringType(), True),
        StructField("event_unit", StringType(), True),
        StructField("encounter_id", StringType(), True),
        StructField("age_at_encounter", IntegerType(), True),
        StructField("sex", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip3", StringType(), True),
    ]
)

GOLD_SORT_KEYS = ("patient_key_v1", "event_date", "event_type", "event_code")
