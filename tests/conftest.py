"""Shared fixtures. All data in tests is synthetic."""

import pytest

from pipeline.patient_key import make_key_udf
from pipeline.spark import build_spark

TEST_SECRET = "synthetic-test-key-not-a-real-secret"

FORBIDDEN_GOLD_COLUMNS = {
    "name",
    "first_name",
    "last_name",
    "ssn",
    "mrn",
    "facility",
    "phone",
    "email",
    "street",
    "address",
    "city",
    "dob",
    "date_of_birth",
    "birth_date",
    "zip",
}


@pytest.fixture(scope="session")
def spark():
    session = build_spark("tests", local=True)
    yield session
    session.stop()


@pytest.fixture(scope="session")
def key_udf():
    return make_key_udf(TEST_SECRET)
