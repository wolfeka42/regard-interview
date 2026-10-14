import csv
import logging
import re
from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import functions as F

from pipeline.config import load_conf
from pipeline.jobs import silver
from pipeline.jobs.silver import (
    ENCOUNTER_KEY,
    LAB_KEY,
    PROVIDER_KEY,
    clean_encounters,
    clean_labs,
    clean_providers,
)
from pipeline.schemas import (
    BRONZE_ENCOUNTERS,
    BRONZE_LABS,
    BRONZE_PROVIDERS,
    CLEAN_ENCOUNTERS,
    CLEAN_LABS,
    CLEAN_PROVIDERS,
    SENTINELS,
)
from pipeline.storage import read_csv
from synth.generate import HEADERS
from tests import datasets as ds
from tests.assertions import SENTINELS as TEST_SENTINELS
from tests.assertions import assert_content, assert_deduped, assert_hashed, assert_schema

COUNTS_LINE = re.compile(r"^layer=\w+ source=\w+ rows_in=\d+ rows_out=\d+ dropped=\d+$")


@pytest.fixture(scope="module")
def bronze(spark):
    return {
        "encounters": spark.createDataFrame(ds.BRONZE_ENCOUNTER_ROWS, BRONZE_ENCOUNTERS),
        "labs": spark.createDataFrame(ds.BRONZE_LAB_ROWS, BRONZE_LABS),
        "providers": spark.createDataFrame(ds.BRONZE_PROVIDER_ROWS, BRONZE_PROVIDERS),
    }


def test_clean_encounters(bronze):
    out = clean_encounters(bronze["encounters"], ds.SALT, ds.RUN_TS)
    assert_schema(out, CLEAN_ENCOUNTERS)
    assert_content(out, ds.EXPECTED_CLEAN_ENCOUNTERS)
    assert_deduped(out, ENCOUNTER_KEY)
    assert_hashed(out, bronze["encounters"], ds.SALT)


def test_clean_labs(bronze):
    out = clean_labs(bronze["labs"], ds.RUN_TS)
    assert_schema(out, CLEAN_LABS)
    assert_content(out, ds.EXPECTED_CLEAN_LABS)
    assert_deduped(out, LAB_KEY)


def test_clean_providers(bronze):
    out = clean_providers(bronze["providers"], ds.RUN_TS)
    assert_schema(out, CLEAN_PROVIDERS)
    assert_content(out, ds.EXPECTED_CLEAN_PROVIDERS)
    assert_deduped(out, PROVIDER_KEY)


def test_dropped_rows_are_exactly_duplicates_and_missing_required(bronze):
    # encounters: 1 exact dup, 1 older version, 4 missing/invalid required fields,
    # 4 sentinel required fields (provider_id, start_time, customer, encounter_id).
    enc = clean_encounters(bronze["encounters"], ds.SALT, ds.RUN_TS)
    assert bronze["encounters"].count() - enc.count() == 10
    # labs: 1 exact dup, 1 repeated lab_id, 1 missing encounter_id,
    # 3 sentinel required fields (encounter_id, customer, lab_id).
    assert bronze["labs"].count() - clean_labs(bronze["labs"], ds.RUN_TS).count() == 6
    # providers: 1 exact dup, 1 repeated provider_id, 1 missing provider_id,
    # 2 sentinel required fields (provider_id, customer).
    assert (
        bronze["providers"].count() - clean_providers(bronze["providers"], ds.RUN_TS).count() == 5
    )


def test_latest_updated_at_compared_as_timestamp(spark):
    rows = [
        # As strings "2024-01-07T09:00:00+01" > "2024-01-07T08:30:00Z", but it is earlier.
        ("a", "1", "p", "1", "2024-01-07T07:00:00Z", "", "2024-01-07T09:00:00+01", "old"),
        ("a", "1", "p", "1", "2024-01-07T07:00:00Z", "", "2024-01-07T08:30:00Z", "new"),
    ]
    out = clean_encounters(spark.createDataFrame(rows, BRONZE_ENCOUNTERS), ds.SALT, ds.RUN_TS)
    assert_schema(out, CLEAN_ENCOUNTERS)
    assert_content(
        out,
        [("a", "1", None, "1", ds.t(7, 7), None, "new", None, ds.h("a", "p"), ds.RUN_TS)],
    )


def test_default_run_ts_is_now_in_whole_seconds(bronze):
    before = datetime.now(UTC).replace(microsecond=0)
    out = clean_providers(bronze["providers"])
    stamps = out.select(F.unix_timestamp("partition_timestamp").alias("s")).distinct().collect()
    assert len(stamps) == 1
    stamp = datetime.fromtimestamp(stamps[0].s, UTC)
    assert before <= stamp <= datetime.now(UTC) + timedelta(seconds=1)
    frac = out.select(F.date_format("partition_timestamp", "SSS").alias("f")).first().f
    assert frac == "000"


def test_silver_main_writes_three_checked_files(spark, tmp_path, caplog):
    ds.write_bronze_csvs(tmp_path, HEADERS)
    conf = load_conf({"DATA_ROOT": str(tmp_path), "PATIENT_ID_SALT": ds.SALT})
    with caplog.at_level(logging.INFO, logger="pipeline"):
        silver.main(spark, conf)

    silver_dir = tmp_path / "silver"
    assert sorted(p.name for p in silver_dir.iterdir()) == [
        "clean_encounters.csv",
        "clean_labs.csv",
        "clean_providers.csv",
    ]
    checks = (
        ("encounters", CLEAN_ENCOUNTERS, ENCOUNTER_KEY, ds.EXPECTED_CLEAN_ENCOUNTERS, 10),
        ("labs", CLEAN_LABS, LAB_KEY, ds.EXPECTED_CLEAN_LABS, 9),
        ("providers", CLEAN_PROVIDERS, PROVIDER_KEY, ds.EXPECTED_CLEAN_PROVIDERS, 6),
    )
    for source, schema, key, expected, n_cols in checks:
        path = silver_dir / f"clean_{source}.csv"
        header = path.read_text(encoding="utf-8").splitlines()[0].split(",")
        assert header == schema.fieldNames() and len(header) == n_cols
        df = read_csv(spark, str(path), schema)
        assert_schema(df, schema)
        assert_deduped(df, key)
        # Expected rows carry the fixed RUN_TS; the file carries this run's stamp.
        assert_content(df.drop("partition_timestamp"), [row[:-1] for row in expected])
        assert df.select("partition_timestamp").distinct().count() == 1
    bronze_enc = read_csv(spark, str(tmp_path / "bronze" / "encounters.csv"), BRONZE_ENCOUNTERS)
    enc = read_csv(spark, str(silver_dir / "clean_encounters.csv"), CLEAN_ENCOUNTERS)
    assert_hashed(enc, bronze_enc, ds.SALT)

    # Quote, comma and newline survive the CSV round trip unchanged.
    with (silver_dir / "clean_providers.csv").open(newline="", encoding="utf-8") as fh:
        names = [row[2] for row in csv.reader(fh)][1:]
    assert ds.BEN_NAME in names

    lines = [r.getMessage() for r in caplog.records if r.name == "pipeline"]
    assert lines == [
        "layer=silver source=encounters rows_in=24 rows_out=14 dropped=10",
        "layer=silver source=labs rows_in=19 rows_out=13 dropped=6",
        "layer=silver source=providers rows_in=14 rows_out=9 dropped=5",
    ]
    assert all(COUNTS_LINE.match(line) for line in lines)
    assert ds.SALT not in caplog.text


# ---- Story 1 part 2: one test per quality-rule matrix row --------------------------
NBSP = ds.NBSP
TS = ds.RUN_TS
T = ds.t


def _check_encounters(spark, rows, expected):
    bronze_df = spark.createDataFrame(rows, BRONZE_ENCOUNTERS)
    out = clean_encounters(bronze_df, ds.SALT, TS)
    assert_schema(out, CLEAN_ENCOUNTERS)
    assert_content(out, expected)
    assert_deduped(out, ENCOUNTER_KEY)
    assert_hashed(out, bronze_df, ds.SALT)


def _check_labs(spark, rows, expected):
    out = clean_labs(spark.createDataFrame(rows, BRONZE_LABS), TS)
    assert_schema(out, CLEAN_LABS)
    assert_content(out, expected)
    assert_deduped(out, LAB_KEY)


def _check_providers(spark, rows, expected):
    out = clean_providers(spark.createDataFrame(rows, BRONZE_PROVIDERS), TS)
    assert_schema(out, CLEAN_PROVIDERS)
    assert_content(out, expected)
    assert_deduped(out, PROVIDER_KEY)


def _enc_row(enc_id="1", patient="p", provider="1", start="2024-01-09T08:00:00Z", **kw):
    return (
        kw.get("customer", "a"),
        enc_id,
        patient,
        provider,
        start,
        kw.get("end", "2024-01-09T14:00:00Z"),
        kw.get("updated", "2024-01-09T15:00:00Z"),
        kw.get("status", "completed"),
    )


def _lab_row(lab_id="1", enc_id="1", name="glucose", value="90", unit="mg/dL", **kw):
    obs = kw.get("observed", "2024-01-09T09:00:00Z")
    return (
        kw.get("customer", "a"),
        lab_id,
        enc_id,
        name,
        value,
        unit,
        obs,
        kw.get("status", "final"),
    )


def _lab_out(lab_id="1", name="glucose", value=90.0, unit="mg/dL", status="final"):
    return ("a", lab_id, "1", name, value, unit, T(9, 9), status, TS)


def test_sentinels_constant_is_the_agreed_list():
    assert SENTINELS == ("", "null", "n/a", "none", "-", "?", "unknown")
    assert set(SENTINELS) == TEST_SENTINELS
    assert "na" not in SENTINELS


def test_sentinel_in_optional_field_becomes_null_row_kept(spark):
    _check_encounters(
        spark,
        [_enc_row(end="unknown")],
        [("a", "1", None, "1", T(9, 8), None, "completed", None, ds.h("a", "p"), TS)],
    )
    _check_labs(spark, [_lab_row(unit="-", status="NULL")], [_lab_out(unit=None, status=None)])
    _check_providers(
        spark,
        [("a", "1", "Dr. Test Synthetic", "N/A", "true")],
        [("a", "1", "Dr. Test Synthetic", None, True, TS)],
    )


@pytest.mark.parametrize("variant", [" n/a ", "Null", "UNKNOWN", "none", "?", f"{NBSP}-{NBSP}"])
def test_sentinel_variants_are_trimmed_and_case_insensitive(spark, variant):
    _check_labs(
        spark,
        [_lab_row(unit=variant, status=variant)],
        [_lab_out(unit=None, status=None)],
    )
    _check_encounters(
        spark,
        [_enc_row(status=variant, updated=variant)],
        [("a", "1", None, "1", T(9, 8), T(9, 14), None, 0.25, ds.h("a", "p"), TS)],
    )


@pytest.mark.parametrize("value", ["NA", "Na", "na"])
def test_na_is_not_a_sentinel(spark, value):
    _check_labs(
        spark,
        [_lab_row(name=value, unit=value)],
        [_lab_out(name="na", unit=value)],
    )


def test_sentinel_in_required_field_drops_row(spark):
    kept_enc = _enc_row(enc_id="9")
    _check_encounters(
        spark,
        [
            kept_enc,
            _enc_row(provider="unknown"),
            _enc_row(enc_id="none"),
            _enc_row(start="NULL"),
            _enc_row(customer="-"),
        ],
        [("a", "9", None, "1", T(9, 8), T(9, 14), "completed", 0.25, ds.h("a", "p"), TS)],
    )
    _check_labs(
        spark,
        [
            _lab_row(lab_id="9"),
            _lab_row(enc_id="N/A"),
            _lab_row(lab_id="?"),
            _lab_row(customer="-"),
        ],
        [_lab_out(lab_id="9")],
    )
    _check_providers(
        spark,
        [
            ("a", "9", "Dr. Kept Synthetic", "Oncology", "true"),
            ("a", "unknown", "Dr. Dropped", "Oncology", "true"),
            (" NULL ", "1", "Dr. Dropped", "Oncology", "true"),
        ],
        [("a", "9", "Dr. Kept Synthetic", "Oncology", True, TS)],
    )


def test_sentinel_patient_keeps_encounter_with_null_hash(spark):
    _check_encounters(
        spark,
        [_enc_row(patient="unknown"), _enc_row(enc_id="2", patient=f"{NBSP}N/A ")],
        [
            ("a", "1", None, "1", T(9, 8), T(9, 14), "completed", 0.25, None, TS),
            ("a", "2", None, "1", T(9, 8), T(9, 14), "completed", 0.25, None, TS),
        ],
    )


def test_test_name_is_trimmed_collapsed_and_lowercased(spark):
    _check_labs(
        spark,
        [
            _lab_row(lab_id="1", name="  Hemoglobin   A1C "),
            _lab_row(lab_id="2", name="GLUCOSE"),
            _lab_row(lab_id="3", name=f"Glu{NBSP}{NBSP}\tCose"),
        ],
        [
            _lab_out(lab_id="1", name="hemoglobin a1c"),
            _lab_out(lab_id="2", name="glucose"),
            _lab_out(lab_id="3", name="glu cose"),
        ],
    )


@pytest.mark.parametrize("specialty", ["", "   ", f"{NBSP}\t"])
def test_empty_optional_specialty_is_null_and_kept(spark, specialty):
    _check_providers(
        spark,
        [("a", "1", "Dr. Test Synthetic", specialty, "true")],
        [("a", "1", "Dr. Test Synthetic", None, True, TS)],
    )


def test_active_flag_parsing(spark):
    flags = [("1", "Yes", True), ("2", "N", False), ("3", " TRUE ", True)]
    flags += [("4", "maybe", None), ("5", "N/A", None)]
    _check_providers(
        spark,
        [("a", pid, "Dr. Test Synthetic", "Oncology", raw) for pid, raw, _ in flags],
        [("a", pid, "Dr. Test Synthetic", "Oncology", out, TS) for pid, _, out in flags],
    )


def test_result_value_parsing_keeps_row(spark):
    _check_labs(
        spark,
        [
            _lab_row(lab_id="1", value=" 4.1 "),
            _lab_row(lab_id="2", value="abc"),
            _lab_row(lab_id="3", value="N/A"),
        ],
        [
            _lab_out(lab_id="1", value=4.1),
            _lab_out(lab_id="2", value=None),
            _lab_out(lab_id="3", value=None),
        ],
    )
