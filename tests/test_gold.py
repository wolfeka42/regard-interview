import datetime as dt
import itertools
import random

from pipeline.jobs.gold import conform_to_gold, gold_encounters, order_gold
from pipeline.schemas import GOLD_SCHEMA, GOLD_SORT_KEYS, SILVER_ENCOUNTERS
from tests.conftest import FORBIDDEN_GOLD_COLUMNS

K1 = "a" * 64
K2 = "b" * 64


def _silver(spark, rows):
    return spark.createDataFrame(rows, SILVER_ENCOUNTERS)


def test_conformed_schema_is_exact_and_has_no_forbidden_column(spark):
    df = spark.createDataFrame(
        [(K1, "123", "Jane Synthetic", "FAC1", "2024-01-01")],
        "patient_key_v1 string, mrn string, name string, facility string, event_date string",
    )
    out = conform_to_gold(df)
    assert [(f.name, f.dataType) for f in out.schema.fields] == [
        (f.name, f.dataType) for f in GOLD_SCHEMA.fields
    ]
    assert not FORBIDDEN_GOLD_COLUMNS & set(out.columns)


def test_gold_encounters_mapping(spark):
    silver = _silver(spark, [(K1, "E1", dt.date(2024, 1, 5), "INPATIENT", dt.date(2024, 1, 8))])
    out = gold_encounters(silver)
    assert [(f.name, f.dataType) for f in out.schema.fields] == [
        (f.name, f.dataType) for f in GOLD_SCHEMA.fields
    ]
    (r,) = out.collect()
    assert r.patient_key_v1 == K1
    assert r.event_type == "encounter"
    assert r.event_date == dt.date(2024, 1, 5)
    assert r.event_end_date == dt.date(2024, 1, 8)
    assert r.event_code == "INPATIENT"
    assert r.encounter_id == "E1"
    # Demographics are typed nulls until story 2.
    for col in ("event_value", "event_unit", "age_at_encounter", "sex", "state", "zip3"):
        assert r[col] is None


def _key(row):
    # (is_null, value) per key: nulls sort last.
    return tuple((row[k] is None, row[k] if row[k] is not None else "") for k in GOLD_SORT_KEYS)


def test_order_gold_never_decreases_with_nulls_last(spark):
    rows = []
    for key in (K1, K2):
        for day in (1, 2, 3):
            for code in ("OUTPATIENT", "INPATIENT", None):
                rows.append((key, "E", dt.date(2024, 1, day), code, None))
        rows.append((key, "E", None, "OUTPATIENT", None))
    rows.append((None, "E", dt.date(2024, 1, 1), "OUTPATIENT", None))
    random.Random(7).shuffle(rows)
    shuffled = gold_encounters(_silver(spark, rows).repartition(5))

    out = order_gold(shuffled).collect()
    keys = [_key(r) for r in out]
    assert len(out) == len(rows)
    assert all(a <= b for a, b in itertools.pairwise(keys))
    assert out[-1]["patient_key_v1"] is None
