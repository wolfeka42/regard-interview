"""Reusable output checks (D9). All data passed through them is synthetic."""

from __future__ import annotations

import hashlib
import hmac
import re
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

HEX64 = re.compile(r"^[0-9a-f]{64}$")


def assert_schema(df: DataFrame, schema: StructType) -> None:
    """Exact column names, types and order."""
    actual = [(f.name, f.dataType) for f in df.schema.fields]
    expected = [(f.name, f.dataType) for f in schema.fields]
    assert actual == expected, f"schema mismatch:\n  actual   {actual}\n  expected {expected}"


def assert_content(df: DataFrame, expected_rows) -> None:
    """Exact, order-independent, null-aware row match (multiplicity included).

    Compared inside Spark so timestamps never pass through the local Python timezone.
    """
    nullable = StructType([StructField(f.name, f.dataType, True) for f in df.schema.fields])
    expected = df.sparkSession.createDataFrame(list(expected_rows), nullable)
    actual = df.select(*[F.col(c) for c in df.columns])
    missing = expected.exceptAll(actual).collect()
    unexpected = actual.exceptAll(expected).collect()
    # Counts and column names only: never row values in exception text.
    assert not missing and not unexpected, (
        f"content mismatch on {df.columns}: {len(missing)} missing, {len(unexpected)} unexpected rows"
    )


def assert_deduped(df: DataFrame, keys) -> None:
    """No key appears twice."""
    repeated = df.groupBy(*keys).count().filter("count > 1").count()
    assert repeated == 0, f"{repeated} repeated keys on {list(keys)}"


# Independent copy of data-model.md's sentinel list (test_silver checks it matches
# pipeline.schemas.SENTINELS). "na" is not a sentinel.
SENTINELS = frozenset({"", "null", "n/a", "none", "-", "?", "unknown"})


def clean_text(value: str | None) -> str | None:
    """Independent Python twin of silver's _clean: Unicode trim; a sentinel becomes null."""
    if value is None:
        return None
    stripped = value.strip()
    return None if stripped.lower() in SENTINELS else stripped


def normalize(value: str | None) -> str | None:
    """Independent Python twin of silver's ID cleaning: sentinel-null, trim, lowercase."""
    cleaned = clean_text(value)
    return cleaned.lower() if cleaned is not None else None


def expected_hash(salt: str, customer: str, patient_id: str | None) -> str | None:
    """HMAC-SHA256(salt, customer:patient_id), recomputed with the stdlib."""
    customer, patient_id = normalize(customer), normalize(patient_id)
    if customer is None or patient_id is None:
        return None
    message = f"{customer}:{patient_id}".encode()
    return hmac.new(salt.encode(), message, hashlib.sha256).hexdigest()


_TS = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(Z|[+-]\d{2}(\d{2})?)$")


def parse_ts(value: str | None) -> datetime | None:
    """Independent Python twin of silver's TIMESTAMP_FORMAT parse (Z, +05, +0530)."""
    s = clean_text(value) or ""
    m = _TS.match(s)
    if not m:
        return None
    offset = {"Z": "+0000"}.get(m.group(2), m.group(2))
    if len(offset) == 3:
        offset += "00"
    try:
        return datetime.strptime(m.group(1) + offset, "%Y-%m-%dT%H:%M:%S%z")
    except ValueError:
        return None


def _asc(value):
    """Ascending, nulls last; datetimes compared as instants."""
    if value is None:
        return (True, "")
    return (False, value.timestamp() if isinstance(value, datetime) else value)


def kept_encounter_versions(bronze_rows) -> dict[tuple, tuple]:
    """Per normalized (customer, encounter_id, provider_id), the bronze version silver keeps.

    Same rules as silver: rows missing a required field are ignored; latest parsed
    updated_at wins (nulls last), then every other column ascending, nulls last.
    """
    best: dict[tuple, tuple] = {}
    for r in bronze_rows:
        key = (normalize(r.customer), normalize(r.encounter_id), normalize(r.provider_id))
        start = parse_ts(r.start_time)
        if None in key or start is None:
            continue
        upd = parse_ts(r.updated_at)
        status = clean_text(r.status)
        status = status.lower() if status is not None else None
        rest = (normalize(r.patient_id), start, parse_ts(r.end_time), upd, status)
        rank = ((upd is None, -upd.timestamp() if upd else 0), *[_asc(v) for v in rest])
        if key not in best or rank < best[key][0]:
            best[key] = (rank, r)
    return {k: r for k, (_rank, r) in best.items()}


def assert_hashed(silver_df: DataFrame, bronze_df: DataFrame, salt: str) -> None:
    """patient_id all null; each hashedpatientid is the HMAC of the bronze version kept.

    Joins on normalized (customer, encounter_id, provider_id). A null hash is allowed only
    where the kept bronze version's patient_id was null, blank or a sentinel.
    """
    not_null = silver_df.filter(F.col("patient_id").isNotNull()).count()
    assert not_null == 0, f"{not_null} silver rows with patient_id not null"

    kept = kept_encounter_versions(bronze_df.collect())
    rows = silver_df.select("customer", "encounter_id", "provider_id", "hashedpatientid").collect()
    unmatched = wrong = malformed = 0
    for r in rows:
        source = kept.get((r.customer, r.encounter_id, r.provider_id))
        if source is None:
            unmatched += 1
            continue
        if r.hashedpatientid != expected_hash(salt, source.customer, source.patient_id):
            wrong += 1
        if r.hashedpatientid is not None and not HEX64.match(r.hashedpatientid):
            malformed += 1
    # Counts only: never keys or hash values in exception text.
    assert unmatched == 0, f"{unmatched} silver keys with no bronze row"
    assert wrong == 0, f"{wrong} silver keys with a wrong hash"
    assert malformed == 0, f"{malformed} hashes not 64 lowercase hex"
