import csv
import logging
import re
import shutil

import pytest

from pipeline.__main__ import run_local
from pipeline.config import load_conf
from pipeline.jobs import gold
from pipeline.jobs.silver import ENCOUNTER_KEY, LAB_KEY, PROVIDER_KEY
from pipeline.schemas import (
    BRONZE_ENCOUNTERS,
    BRONZE_PROVIDERS,
    CLEAN_ENCOUNTERS,
    CLEAN_LABS,
    CLEAN_PROVIDERS,
    GOLD_SCHEMA,
)
from pipeline.storage import read_csv
from synth.generate import write_bronze
from tests import datasets as ds
from tests.assertions import assert_content, assert_deduped, assert_hashed, assert_schema

COUNTS_LINE = re.compile(r"^layer=\w+ source=\w+ rows_in=\d+ rows_out=\d+ dropped=\d+$")
SILVER_FILES = ["clean_encounters.csv", "clean_labs.csv", "clean_providers.csv"]


def _bronze_dicts(data_root):
    out = {}
    for source in ("encounters", "labs", "providers"):
        with (data_root / "bronze" / f"{source}.csv").open(newline="", encoding="utf-8") as fh:
            out[source] = list(csv.DictReader(fh))
    return out


def test_local_run_end_to_end(spark, tmp_path, caplog):
    data_root = tmp_path / "lake"
    write_bronze(data_root)
    conf = load_conf({"DATA_ROOT": str(data_root), "PATIENT_ID_SALT": ds.SALT})

    with caplog.at_level(logging.INFO, logger="pipeline"):
        run_local(spark, conf)

    # Exactly the named files: no part, .crc, _SUCCESS or temp entries.
    assert sorted(p.name for p in (data_root / "silver").iterdir()) == SILVER_FILES
    assert [p.name for p in (data_root / "gold").iterdir()] == ["provider_daily_activity.csv"]

    silver = data_root / "silver"
    for name, schema, key in (
        ("clean_encounters.csv", CLEAN_ENCOUNTERS, ENCOUNTER_KEY),
        ("clean_labs.csv", CLEAN_LABS, LAB_KEY),
        ("clean_providers.csv", CLEAN_PROVIDERS, PROVIDER_KEY),
    ):
        df = read_csv(spark, str(silver / name), schema)
        assert_schema(df, schema)
        assert_deduped(df, key)
        assert df.count() > 0
    assert_hashed(
        read_csv(spark, str(silver / "clean_encounters.csv"), CLEAN_ENCOUNTERS),
        read_csv(spark, str(data_root / "bronze" / "encounters.csv"), BRONZE_ENCOUNTERS),
        ds.SALT,
    )

    gold_df = read_csv(spark, str(data_root / "gold" / "provider_daily_activity.csv"), GOLD_SCHEMA)
    assert_schema(gold_df, GOLD_SCHEMA)
    expected = ds.oracle_gold(_bronze_dicts(data_root))
    assert len(expected) > 10
    assert_content(gold_df, expected)

    n_enc = read_csv(spark, str(silver / "clean_encounters.csv"), CLEAN_ENCOUNTERS).count()
    n_gold = gold_df.count()
    lines = [r.getMessage() for r in caplog.records if r.name == "pipeline"]
    assert lines[-1] == (
        f"layer=gold source=provider_daily_activity rows_in={n_enc} rows_out={n_gold} "
        f"dropped={n_enc - n_gold}"
    )
    assert all(COUNTS_LINE.match(line) for line in lines), lines
    assert [line.split()[0:2] for line in lines] == [
        ["layer=silver", "source=encounters"],
        ["layer=silver", "source=labs"],
        ["layer=silver", "source=providers"],
        ["layer=gold", "source=provider_daily_activity"],
    ]
    assert ds.SALT not in caplog.text

    # Gold reads only the three silver files: it still runs with bronze gone.
    shutil.rmtree(data_root / "bronze")
    gold.main(spark, conf)
    rerun = read_csv(spark, str(data_root / "gold" / "provider_daily_activity.csv"), GOLD_SCHEMA)
    assert_content(rerun, expected)
    assert [p.name for p in (data_root / "gold").iterdir()] == ["provider_daily_activity.csv"]


def test_rerun_is_idempotent(spark, tmp_path):
    data_root = tmp_path / "lake"
    write_bronze(data_root)
    conf = load_conf({"DATA_ROOT": str(data_root), "PATIENT_ID_SALT": ds.SALT})
    run_local(spark, conf)
    first = (data_root / "gold" / "provider_daily_activity.csv").read_text(encoding="utf-8")
    run_local(spark, conf)
    assert (data_root / "gold" / "provider_daily_activity.csv").read_text(encoding="utf-8") == first
    assert sorted(p.name for p in (data_root / "silver").iterdir()) == SILVER_FILES


def test_gold_file_is_sorted(spark, tmp_path):
    data_root = tmp_path / "lake"
    write_bronze(data_root)
    run_local(spark, load_conf({"DATA_ROOT": str(data_root)}))
    with (data_root / "gold" / "provider_daily_activity.csv").open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    keys = [(r["customer"], r["provider_id"], r["encounter_date"]) for r in rows]
    assert keys == sorted(keys)


def test_reordered_bronze_header_is_rejected(spark, tmp_path):
    path = tmp_path / "providers.csv"
    path.write_text(
        "provider_id,customer,provider_name,specialty,active_flag\n1,a,Dr. X,,true\n",
        encoding="utf-8",
    )
    with pytest.raises(Exception, match="(?i)header"):
        read_csv(spark, str(path), BRONZE_PROVIDERS).collect()
