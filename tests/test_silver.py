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
)
from pipeline.storage import read_csv
from synth.generate import HEADERS
from tests import datasets as ds
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
    # encounters: 1 exact dup, 1 older version, 4 missing/invalid required fields.
    enc = clean_encounters(bronze["encounters"], ds.SALT, ds.RUN_TS)
    assert bronze["encounters"].count() - enc.count() == 6
    # labs: 1 exact dup, 1 repeated lab_id, 1 missing encounter_id.
    assert bronze["labs"].count() - clean_labs(bronze["labs"], ds.RUN_TS).count() == 3
    # providers: 1 exact dup, 1 repeated provider_id, 1 missing provider_id.
    assert (
        bronze["providers"].count() - clean_providers(bronze["providers"], ds.RUN_TS).count() == 3
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
        "layer=silver source=encounters rows_in=18 rows_out=12 dropped=6",
        "layer=silver source=labs rows_in=12 rows_out=9 dropped=3",
        "layer=silver source=providers rows_in=8 rows_out=5 dropped=3",
    ]
    assert all(COUNTS_LINE.match(line) for line in lines)
    assert ds.SALT not in caplog.text
