import datetime as dt
import hashlib
import hmac

from pipeline.jobs.silver import silver_encounters
from pipeline.schemas import RAW_ENCOUNTERS, SILVER_ENCOUNTERS
from tests.conftest import TEST_SECRET

NBSP = "\u00a0"


def _raw(spark, rows):
    return spark.createDataFrame(rows, RAW_ENCOUNTERS)


def _row(**kw):
    base = {
        "facility": "FAC1",
        "mrn": "123",
        "encounter_id": "E1",
        "encounter_date": "2024-01-05",
        "encounter_type": "INPATIENT",
        "discharge_date": "2024-01-08",
    }
    base.update(kw)
    return tuple(base[f] for f in RAW_ENCOUNTERS.fieldNames())


def test_schema_matches_and_identifiers_dropped(spark, key_udf):
    out = silver_encounters(_raw(spark, [_row()]), key_udf)
    assert [(f.name, f.dataType) for f in out.schema.fields] == [
        (f.name, f.dataType) for f in SILVER_ENCOUNTERS.fields
    ]
    assert "mrn" not in out.columns
    assert "facility" not in out.columns


def test_valid_row_is_typed(spark, key_udf):
    out = silver_encounters(
        _raw(spark, [_row(discharge_date=None, encounter_type=" ER ")]), key_udf
    )
    (r,) = out.collect()
    assert r.encounter_date == dt.date(2024, 1, 5)
    assert r.discharge_date is None
    assert r.encounter_type == "ER"
    assert len(r.patient_key_v1) == 64


def test_bad_dates_dropped_and_counted(spark, key_udf):
    rows = [
        _row(encounter_id="OK"),
        _row(encounter_date=None),
        _row(encounter_date=""),
        _row(encounter_date="not-a-date"),
        _row(encounter_date="2024-13-45"),
        _row(encounter_date="2024-1-5"),
        _row(discharge_date="2024-02-30"),
        _row(discharge_date="soon"),
        _row(encounter_id="BLANK_DISCHARGE", discharge_date="  "),  # absent, not bad
    ]
    df = _raw(spark, rows)
    out = silver_encounters(df, key_udf)
    rows_in, rows_out = df.count(), out.count()
    assert rows_out == 2
    assert rows_in - rows_out == 7
    survivors = {r.encounter_id: r.discharge_date for r in out.collect()}
    assert survivors == {"OK": dt.date(2024, 1, 8), "BLANK_DISCHARGE": None}


def test_formatting_variants_get_one_key(spark, key_udf):
    rows = [_row(facility=" fac1 ", mrn="00123"), _row(facility="FAC1", mrn="1 23")]
    out = silver_encounters(_raw(spark, rows), key_udf).collect()
    expected = hmac.new(TEST_SECRET.encode(), b"FAC1:123", hashlib.sha256).hexdigest()
    assert [r.patient_key_v1 for r in out] == [expected, expected]


def test_blank_identity_dropped_and_counted(spark, key_udf):
    rows = [
        _row(),
        _row(mrn=""),
        _row(mrn=None),
        _row(mrn=f" {NBSP} "),
        _row(mrn="0000"),
        _row(facility=""),
        _row(facility=NBSP),
    ]
    df = _raw(spark, rows)
    out = silver_encounters(df, key_udf)
    assert df.count() - out.count() == 6
    assert out.filter("patient_key_v1 is null").count() == 0
