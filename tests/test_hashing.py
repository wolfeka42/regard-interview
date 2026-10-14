import hashlib
import hmac

from pyspark.sql import functions as F

from pipeline.hashing import HASH_COLUMN, make_hash_udf, normalize_id, with_hashed_patient
from tests.assertions import HEX64
from tests.datasets import NBSP, SALT


def _hashes(spark, hash_udf, pairs):
    df = spark.createDataFrame(pairs, "customer string, patient_id string")
    return [r[HASH_COLUMN] for r in with_hashed_patient(df, hash_udf).collect()]


def _hmac(salt, message):
    return hmac.new(salt.encode(), message.encode(), hashlib.sha256).hexdigest()


def test_normalize_id_trims_unicode_lowercases_keeps_zeros(spark):
    df = spark.createDataFrame(
        [(f"{NBSP} Alpha\t",), ("007",), ("ABCDEF-12",), (f" {NBSP} ",), (None,)], "v string"
    )
    out = [r.v for r in df.select(normalize_id(F.col("v")).alias("v")).collect()]
    assert out == ["alpha", "007", "abcdef-12", None, None]


def test_hash_is_hmac_of_customer_colon_patient(spark, hash_udf):
    (got,) = _hashes(spark, hash_udf, [("alpha", "1")])
    assert got == _hmac(SALT, "alpha:1")
    assert HEX64.match(got)
    assert got != hashlib.sha256(b"alpha:1").hexdigest()  # not a plain hash


def test_hash_is_deterministic_across_formatting(spark, hash_udf):
    uuid = "9B2A7C4D-0000-4000-8000-0000000000A1"
    a, b = _hashes(spark, hash_udf, [(" ALPHA", f"{NBSP}{uuid}"), ("alpha", uuid.lower())])
    assert a == b == _hmac(SALT, f"alpha:{uuid.lower()}")


def test_hash_is_customer_scoped(spark, hash_udf):
    a, b = _hashes(spark, hash_udf, [("alpha", "1"), ("beta", "1")])
    assert a != b


def test_different_salt_gives_different_hash(spark, hash_udf):
    a = _hashes(spark, hash_udf, [("alpha", "1")])
    b = _hashes(spark, make_hash_udf("another-synthetic-salt"), [("alpha", "1")])
    assert a != b


def test_null_or_blank_input_gives_null(spark, hash_udf):
    out = _hashes(spark, hash_udf, [(None, "1"), ("alpha", None), ("alpha", "  "), ("", "1")])
    assert out == [None, None, None, None]
