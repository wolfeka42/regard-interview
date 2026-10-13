import hashlib
import hmac
import re

from pyspark.sql import functions as F

from pipeline.patient_key import (
    drop_invalid_identity,
    make_key_udf,
    normalize_facility,
    normalize_mrn,
    with_patient_key,
)
from tests.conftest import TEST_SECRET

HEX64 = re.compile(r"^[0-9a-f]{64}$")
NBSP = "\u00a0"


def _keys(spark, key_udf, pairs):
    df = spark.createDataFrame(pairs, "facility string, mrn string")
    df = df.select(
        normalize_facility(F.col("facility")).alias("facility"),
        normalize_mrn(F.col("mrn")).alias("mrn"),
    )
    df = drop_invalid_identity(df)
    return [r["patient_key_v1"] for r in with_patient_key(df, key_udf).collect()]


def test_normalization(spark):
    df = spark.createDataFrame(
        [(" fac1 ", "00123"), (f"{NBSP}fac1{NBSP}", f" 1{NBSP}2 3 ")],
        "facility string, mrn string",
    )
    rows = df.select(
        normalize_facility(F.col("facility")).alias("f"), normalize_mrn(F.col("mrn")).alias("m")
    ).collect()
    assert [(r.f, r.m) for r in rows] == [("FAC1", "123"), ("FAC1", "123")]


def test_same_patient_different_formatting_same_key(spark, key_udf):
    keys = _keys(spark, key_udf, [(" fac1 ", "00123"), ("FAC1", "1 23")])
    assert len(keys) == 2
    assert keys[0] == keys[1]


def test_same_mrn_different_facility_different_keys(spark, key_udf):
    keys = _keys(spark, key_udf, [("FAC1", "123"), ("FAC2", "123")])
    assert keys[0] != keys[1]


def test_key_is_64_hex_hmac_and_deterministic(spark, key_udf):
    keys = _keys(spark, key_udf, [("FAC1", "123")])
    again = _keys(spark, make_key_udf(TEST_SECRET), [("fac1", "0123")])
    expected = hmac.new(TEST_SECRET.encode(), b"FAC1:123", hashlib.sha256).hexdigest()
    assert keys == [expected] == again
    assert HEX64.match(keys[0])
    # Not a plain hash.
    assert keys[0] != hashlib.sha256(b"FAC1:123").hexdigest()


def test_different_secret_gives_different_key(spark, key_udf):
    a = _keys(spark, key_udf, [("FAC1", "123")])
    b = _keys(spark, make_key_udf("another-synthetic-key"), [("FAC1", "123")])
    assert a != b


def test_blank_identities_dropped_never_hashed(spark, key_udf):
    invalid = [
        ("FAC1", ""),
        ("FAC1", "   "),
        ("FAC1", NBSP),
        ("FAC1", f"{NBSP} {NBSP}"),
        ("FAC1", "0000"),
        ("FAC1", " 0 0 "),
        ("FAC1", None),
        ("", "123"),
        ("  ", "123"),
        (NBSP, "123"),
        (None, "123"),
    ]
    df = spark.createDataFrame(invalid + [("FAC1", "123")], "facility string, mrn string")
    kept = drop_invalid_identity(df).collect()
    assert len(kept) == 1


def test_udf_null_input_returns_null(spark, key_udf):
    df = spark.createDataFrame([(None, "1"), ("F", None)], "facility string, mrn string")
    out = with_patient_key(df, key_udf).collect()
    assert [r["patient_key_v1"] for r in out] == [None, None]
