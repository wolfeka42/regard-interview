import dataclasses
import os
import subprocess
import sys
from pathlib import Path

import pytest

import pipeline.__main__ as cli
from pipeline.config import DATA_ROOT, PATIENT_ID_SALT, Conf, ConfigError, load_conf
from pipeline.schemas import DEFAULT_SALT

SALT = "synthetic-salt-value"
ROOT = Path(__file__).resolve().parent.parent


def test_salt_unset_uses_default():
    assert load_conf({DATA_ROOT: "/tmp/lake"}) == Conf("/tmp/lake", DEFAULT_SALT)


def test_salt_from_env():
    assert load_conf({DATA_ROOT: "/tmp/lake", PATIENT_ID_SALT: SALT}).salt == SALT


@pytest.mark.parametrize("bad", ["", "   ", SALT + "\n", " " + SALT, SALT + "\t"])
def test_blank_or_padded_salt_fails_naming_variable_not_value(bad):
    with pytest.raises(ConfigError) as exc:
        load_conf({DATA_ROOT: "/tmp/lake", PATIENT_ID_SALT: bad})
    assert PATIENT_ID_SALT in str(exc.value)
    assert SALT not in str(exc.value)


@pytest.mark.parametrize("bad", [None, "", "   "])
def test_missing_data_root_fails_naming_variable(bad):
    env = {PATIENT_ID_SALT: SALT}
    if bad is not None:
        env[DATA_ROOT] = bad
    with pytest.raises(ConfigError) as exc:
        load_conf(env)
    assert DATA_ROOT in str(exc.value)
    assert SALT not in str(exc.value)


def test_repr_masks_salt():
    conf = load_conf({DATA_ROOT: "/tmp/lake", PATIENT_ID_SALT: SALT})
    assert SALT not in repr(conf)
    assert SALT not in str(conf)
    assert "***" in repr(conf)
    assert DEFAULT_SALT not in repr(load_conf({DATA_ROOT: "/tmp/lake"}))


def test_conf_is_frozen():
    conf = load_conf({DATA_ROOT: "/tmp/lake"})
    with pytest.raises(dataclasses.FrozenInstanceError):
        conf.salt = "other"  # type: ignore[misc]


def test_root_stored_without_trailing_slash():
    assert load_conf({DATA_ROOT: "/tmp/lake/"}).data_root == "/tmp/lake"
    assert load_conf({DATA_ROOT: "s3://bucket/lake/"}).data_root == "s3://bucket/lake"


@pytest.mark.parametrize("value", [None, "", "  "])
def test_cli_fails_fast_without_spark(monkeypatch, capsys, value):
    monkeypatch.setenv(PATIENT_ID_SALT, SALT)
    if value is None:
        monkeypatch.delenv(DATA_ROOT, raising=False)
    else:
        monkeypatch.setenv(DATA_ROOT, value)

    def _no_spark(*_a, **_k):
        raise AssertionError("SparkSession must not be created when conf is invalid")

    monkeypatch.setattr(cli.spark_mod, "build_spark", _no_spark)
    assert cli.main(["run", "--target", "local"]) == 2
    err = capsys.readouterr().err
    assert DATA_ROOT in err
    assert SALT not in err


def test_cli_has_no_emr_target_in_story_1():
    with pytest.raises(SystemExit) as exc:
        cli.main(["run", "--target", "emr"])
    assert exc.value.code != 0


def test_cli_success_path_runs_silver_then_gold(monkeypatch):
    monkeypatch.setenv(DATA_ROOT, "/tmp/lake")
    monkeypatch.delenv(PATIENT_ID_SALT, raising=False)

    class StubSpark:
        stopped = 0

        def stop(self):
            self.stopped += 1

    stub = StubSpark()
    calls = []
    monkeypatch.setattr(cli.spark_mod, "build_spark", lambda *_a, **_k: stub)
    monkeypatch.setattr(cli.silver, "main", lambda spark, conf: calls.append("silver"))
    monkeypatch.setattr(cli.gold, "main", lambda spark, conf: calls.append("gold"))
    assert cli.main(["run", "--target", "local"]) == 0
    assert calls == ["silver", "gold"]
    assert stub.stopped == 1


@pytest.mark.parametrize("job", ["silver", "gold"])
def test_job_entry_fails_fast_without_data_root(job):
    env = {k: v for k, v in os.environ.items() if k not in (DATA_ROOT, PATIENT_ID_SALT)}
    env[PATIENT_ID_SALT] = SALT
    proc = subprocess.run(
        [sys.executable, "-m", f"pipeline.jobs.{job}"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert proc.returncode == 2
    assert DATA_ROOT in proc.stderr
    assert SALT not in proc.stderr + proc.stdout
    assert "Setting default log level" not in proc.stderr  # Spark never started


def test_no_bronze_job_module():
    assert not (ROOT / "pipeline" / "jobs" / "bronze.py").exists()
