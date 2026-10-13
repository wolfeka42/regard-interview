import csv
import logging
import re

import pytest

from pipeline.__main__ import run_local
from pipeline.config import load_conf
from pipeline.jobs.bronze import read_raw
from pipeline.schemas import GOLD_SCHEMA, RAW_ENCOUNTERS
from synth.generate import write_encounters
from tests.conftest import FORBIDDEN_GOLD_COLUMNS, TEST_SECRET

HEX64 = re.compile(r"^[0-9a-f]{64}$")
INVALID_SYNTH_ROWS = 11

COUNTS_LINE = re.compile(
    r"^layer=(bronze|silver|gold) source=encounters rows_in=(\d+) rows_out=(\d+) dropped=(\d+)$"
)


def test_local_run_end_to_end(spark, tmp_path, caplog):
    raw_root = tmp_path / "raw"
    data_root = tmp_path / "lake"
    csv_path = write_encounters(raw_root)
    with csv_path.open(newline="", encoding="utf-8") as fh:
        source_rows = sum(1 for _ in csv.DictReader(fh))

    conf = load_conf(
        {
            "DATA_ROOT": str(data_root),
            "RAW_ROOT": str(raw_root),
            "PATIENT_KEY_HMAC_SECRET": TEST_SECRET,
        }
    )
    with caplog.at_level(logging.INFO, logger="pipeline"):
        run_local(spark, conf)

    bronze = spark.read.parquet(f"{data_root}/bronze/encounters")
    silver = spark.read.parquet(f"{data_root}/silver/encounters")
    gold = spark.read.parquet(f"{data_root}/gold/patient_history")

    gold_rows = gold.count()
    assert bronze.count() == source_rows
    assert gold_rows > 0
    assert 0 < silver.count() < source_rows  # synth includes invalid rows
    for df in (silver, gold):
        assert "mrn" not in df.columns
        assert "facility" not in df.columns
    assert not FORBIDDEN_GOLD_COLUMNS & set(gold.columns)
    assert [(f.name, f.dataType) for f in gold.schema.fields] == [
        (f.name, f.dataType) for f in GOLD_SCHEMA.fields
    ]
    keys = [r.patient_key_v1 for r in gold.select("patient_key_v1").collect()]
    assert all(k is not None and HEX64.match(k) for k in keys)
    assert gold_rows == silver.count()
    # Silver drops exactly the invalid synthetic rows (encounter ids X00000...).
    assert silver.count() == source_rows - INVALID_SYNTH_ROWS
    assert silver.filter(silver.encounter_id.startswith("X")).count() == 0

    # One counts-only line per job for the one source; never the key value.
    lines = [r.getMessage() for r in caplog.records if r.name == "pipeline"]
    matches = [COUNTS_LINE.match(line) for line in lines]
    assert all(matches), lines
    assert [m.group(1) for m in matches] == ["bronze", "silver", "gold"]
    for m in matches:
        assert int(m.group(2)) - int(m.group(3)) == int(m.group(4))
    assert TEST_SECRET not in caplog.text

    # Rerun is idempotent (overwrite).
    run_local(spark, conf)
    assert spark.read.parquet(f"{data_root}/gold/patient_history").count() == gold_rows


def test_bronze_rejects_reordered_header(spark, tmp_path):
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    (raw_root / "encounters.csv").write_text(
        "mrn,facility,encounter_id,encounter_date,encounter_type,discharge_date\n"
        "123,FAC1,E1,2024-01-05,OUTPATIENT,\n",
        encoding="utf-8",
    )
    conf = load_conf(
        {
            "DATA_ROOT": str(tmp_path / "lake"),
            "RAW_ROOT": str(raw_root),
            "PATIENT_KEY_HMAC_SECRET": TEST_SECRET,
        }
    )
    with pytest.raises(Exception, match="(?i)header"):
        read_raw(spark, conf, "encounters", RAW_ENCOUNTERS).collect()


def test_bronze_keeps_quoted_newline_in_one_row(spark, tmp_path):
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    (raw_root / "encounters.csv").write_text(
        "facility,mrn,encounter_id,encounter_date,encounter_type,discharge_date\n"
        'FAC1,123,E1,2024-01-05,"OUT\nPATIENT",\n',
        encoding="utf-8",
    )
    conf = load_conf(
        {
            "DATA_ROOT": str(tmp_path / "lake"),
            "RAW_ROOT": str(raw_root),
            "PATIENT_KEY_HMAC_SECRET": TEST_SECRET,
        }
    )
    assert read_raw(spark, conf, "encounters", RAW_ENCOUNTERS).count() == 1
