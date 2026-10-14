"""Sole owner of ID normalization and the patient-ID hash (AD-6).

Regexes use (?U) so that \\s matches Unicode whitespace such as NBSP; Java's plain
\\s and Spark's trim are ASCII-only.
"""

from __future__ import annotations

import hashlib
import hmac

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

HASH_COLUMN = "hashedpatientid"
EDGE_WS = r"(?U)^\s+|\s+$"


def normalize_id(col: Column) -> Column:
    """Trim Unicode whitespace, lowercase; blank becomes null. Leading zeros are kept."""
    stripped = F.lower(F.regexp_replace(col, EDGE_WS, ""))
    return F.when(stripped != "", stripped)


def make_hash_udf(salt: str):
    """Build the HMAC-SHA256 UDF on the driver. Returns 64 lowercase hex chars.

    Null input gives null; the function never raises, so no row value can reach
    a traceback.
    """
    key = salt.encode()

    def _hash(customer, patient_id):
        if customer is None or patient_id is None:
            return None
        try:
            message = f"{customer}:{patient_id}".encode()
            return hmac.new(key, message, hashlib.sha256).hexdigest()
        except Exception:  # noqa: BLE001 -- never let a row value reach a traceback
            return None

    return F.udf(_hash, StringType(), useArrow=False)


def with_hashed_patient(df: DataFrame, hash_udf) -> DataFrame:
    """Add hashedpatientid over normalized `customer:patient_id`."""
    return df.withColumn(
        HASH_COLUMN,
        hash_udf(normalize_id(F.col("customer")), normalize_id(F.col("patient_id"))),
    )
