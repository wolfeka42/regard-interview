import datetime as dt
import itertools

from pipeline.jobs.gold import gold_provider_daily_activity, order_gold
from pipeline.schemas import CLEAN_ENCOUNTERS, CLEAN_LABS, CLEAN_PROVIDERS, GOLD_SCHEMA
from tests import datasets as ds
from tests.assertions import assert_content, assert_schema

T = ds.t
TS = ds.RUN_TS


def _silver(spark, encounters, labs=(), providers=()):
    return (
        spark.createDataFrame(list(encounters), CLEAN_ENCOUNTERS),
        spark.createDataFrame(list(labs), CLEAN_LABS),
        spark.createDataFrame(list(providers), CLEAN_PROVIDERS),
    )


def _enc(enc_id, provider, start, end, status="completed", patient="1", customer="a"):
    duration = (end - start).total_seconds() / 86400 if end and end >= start else None
    hashed = ds.h(customer, patient)
    return (customer, enc_id, None, provider, start, end, status, duration, hashed, TS)


def _lab(lab_id, enc_id, status="final", customer="a"):
    return (customer, lab_id, enc_id, "glucose", 1.0, "mg/dL", T(9, 9), status, TS)


def test_gold_from_expected_silver(spark):
    out = gold_provider_daily_activity(
        *_silver(
            spark, ds.EXPECTED_CLEAN_ENCOUNTERS, ds.EXPECTED_CLEAN_LABS, ds.EXPECTED_CLEAN_PROVIDERS
        )
    )
    assert_schema(out, GOLD_SCHEMA)
    assert_content(out, ds.EXPECTED_GOLD)


def test_duration_not_multiplied_by_labs(spark):
    encounters = [_enc("1", "p", T(1, 0), T(1, 12))]
    labs = [_lab("l1", "1"), _lab("l2", "1"), _lab("l3", "1")]
    out = gold_provider_daily_activity(*_silver(spark, encounters, labs))
    assert_schema(out, GOLD_SCHEMA)
    assert_content(out, [("a", "p", dt.date(2024, 1, 1), None, None, None, 1, 1, 0.5, 3)])


def test_null_patient_counts_encounter_not_patient(spark):
    out = gold_provider_daily_activity(
        *_silver(spark, [_enc("1", "p", T(1, 0), None, patient=None)])
    )
    assert_schema(out, GOLD_SCHEMA)
    assert_content(out, [("a", "p", dt.date(2024, 1, 1), None, None, None, 1, 0, 0.0, 0)])


def test_only_cancelled_day_has_no_row_and_null_status_counts(spark):
    encounters = [
        _enc("1", "p", T(1, 0), None, status="cancelled"),
        _enc("2", "p", T(1, 1), None, status="canceled"),
        _enc("3", "q", T(1, 0), None, status=None),
    ]
    labs = [_lab("l1", "1"), _lab("l3", "3", status=None), _lab("l4", "3", status="retracted")]
    out = gold_provider_daily_activity(*_silver(spark, encounters, labs))
    assert_schema(out, GOLD_SCHEMA)
    assert_content(out, [("a", "q", dt.date(2024, 1, 1), None, None, None, 1, 1, 0.0, 1)])


def test_lab_counted_on_encounter_day_and_customer_scoped(spark):
    encounters = [
        _enc("1", "p", T(1, 22), T(2, 4)),
        _enc("1", "p", T(1, 22), T(2, 4), customer="b"),
    ]
    labs = [_lab("l1", "1")]  # customer a only, observed on 2024-01-09
    out = gold_provider_daily_activity(*_silver(spark, encounters, labs))
    assert_schema(out, GOLD_SCHEMA)
    assert_content(
        out,
        [
            ("a", "p", dt.date(2024, 1, 1), None, None, None, 1, 1, 0.25, 1),
            ("b", "p", dt.date(2024, 1, 1), None, None, None, 1, 1, 0.25, 0),
        ],
    )


def test_order_gold_sorts_by_keys_nulls_last(spark):
    out = gold_provider_daily_activity(
        *_silver(
            spark, ds.EXPECTED_CLEAN_ENCOUNTERS, ds.EXPECTED_CLEAN_LABS, ds.EXPECTED_CLEAN_PROVIDERS
        )
    )
    ordered = order_gold(out.repartition(5))
    assert_schema(ordered, GOLD_SCHEMA)
    rows = ordered.collect()
    keys = [(r.customer, r.provider_id, r.encounter_date) for r in rows]
    assert len(rows) == len(ds.EXPECTED_GOLD)
    assert all(a <= b for a, b in itertools.pairwise(keys))
