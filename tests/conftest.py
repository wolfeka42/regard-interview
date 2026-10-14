"""Shared fixtures. All data in tests is synthetic."""

import pytest

from pipeline.hashing import make_hash_udf
from pipeline.spark import build_spark
from tests.datasets import SALT


@pytest.fixture(scope="session")
def spark():
    session = build_spark("tests", local=True)
    yield session
    session.stop()


@pytest.fixture(scope="session")
def hash_udf():
    return make_hash_udf(SALT)
