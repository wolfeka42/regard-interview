"""Sole owner of identity normalization and the patient key (AD-6).

Regexes use (?U) so that \\s matches Unicode whitespace such as NBSP; Java's plain
\\s and Spark's trim are ASCII-only.
"""

from __future__ import annotations

import hashlib
import hmac

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

KEY_COLUMN = "patient_key_v1"

EDGE_WS = r"(?U)^\s+|\s+$"
_ANY_WS = r"(?U)\s+"
_LEADING_ZEROS = r"^0+"


def normalize_facility(col: Column) -> Column:
    """Strip surrounding whitespace, then uppercase."""
    return F.upper(F.regexp_replace(col, EDGE_WS, ""))


def normalize_mrn(col: Column) -> Column:
    """Remove all whitespace, then strip leading zeros."""
    return F.regexp_replace(F.regexp_replace(col, _ANY_WS, ""), _LEADING_ZEROS, "")


def drop_invalid_identity(df: DataFrame) -> DataFrame:
    """Drop rows whose normalized facility or MRN is null or empty, before any hashing."""
    facility = normalize_facility(F.col("facility"))
    mrn = normalize_mrn(F.col("mrn"))
    valid = facility.isNotNull() & (facility != "") & mrn.isNotNull() & (mrn != "")
    return df.filter(valid)


def make_key_udf(secret: str):
    """Build the HMAC-SHA256 UDF on the driver. Returns 64 lowercase hex chars.

    Null input gives null; the function never raises, so no row value can reach
    a traceback.
    """
    key = secret.encode()

    def _patient_key(facility, mrn):
        if facility is None or mrn is None:
            return None
        try:
            message = f"{facility}:{mrn}".encode()
            return hmac.new(key, message, hashlib.sha256).hexdigest()
        except Exception:  # noqa: BLE001 -- never let a row value reach a traceback
            return None

    return F.udf(_patient_key, StringType(), useArrow=False)


def with_patient_key(df: DataFrame, key_udf) -> DataFrame:
    """Add patient_key_v1 from already-normalized facility and mrn columns."""
    return df.withColumn(KEY_COLUMN, key_udf(F.col("facility"), F.col("mrn")))
