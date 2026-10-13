import dataclasses
import os
import subprocess
import sys
from pathlib import Path

import pytest

import pipeline.__main__ as cli
from pipeline.config import REQUIRED_VARS, Conf, ConfigError, load_conf

SECRET = "synthetic-secret-value"
GOOD_ENV = {
    "DATA_ROOT": "/tmp/lake",
    "RAW_ROOT": "/tmp/raw",
    "PATIENT_KEY_HMAC_SECRET": SECRET,
}


def test_load_conf_reads_all_vars():
    conf = load_conf(GOOD_ENV)
    assert conf == Conf("/tmp/lake", "/tmp/raw", SECRET)


@pytest.mark.parametrize("name", REQUIRED_VARS)
@pytest.mark.parametrize("bad", [None, "", "   "])
def test_missing_or_empty_var_fails_naming_variable_not_value(name, bad):
    env = dict(GOOD_ENV)
    if bad is None:
        del env[name]
    else:
        env[name] = bad
    with pytest.raises(ConfigError) as exc:
        load_conf(env)
    assert name in str(exc.value)
    assert SECRET not in str(exc.value)


def test_repr_masks_key():
    conf = load_conf(GOOD_ENV)
    assert SECRET not in repr(conf)
    assert SECRET not in str(conf)
    assert "***" in repr(conf)


def test_conf_is_frozen():
    conf = load_conf(GOOD_ENV)
    with pytest.raises(dataclasses.FrozenInstanceError):
        conf.hmac_secret = "other"  # type: ignore[misc]


@pytest.mark.parametrize("name", REQUIRED_VARS)
def test_cli_fails_fast_without_spark(monkeypatch, capsys, name):
    for var, value in GOOD_ENV.items():
        monkeypatch.setenv(var, value)
    monkeypatch.delenv(name)

    def _no_spark(*_a, **_k):
        raise AssertionError("SparkSession must not be created when conf is invalid")

    monkeypatch.setattr(cli.spark_mod, "build_spark", _no_spark)
    code = cli.main(["run", "--target", "local"])
    assert code != 0
    err = capsys.readouterr().err
    assert name in err
    assert SECRET not in err


def test_cli_rejects_emr_target_in_story_1():
    with pytest.raises(SystemExit) as exc:
        cli.main(["run", "--target", "emr"])
    assert exc.value.code != 0


@pytest.mark.parametrize("secret", [SECRET + "\n", " " + SECRET, SECRET + "\t"])
def test_secret_with_surrounding_whitespace_fails(secret):
    env = dict(GOOD_ENV, PATIENT_KEY_HMAC_SECRET=secret)
    with pytest.raises(ConfigError) as exc:
        load_conf(env)
    assert "PATIENT_KEY_HMAC_SECRET" in str(exc.value)
    assert SECRET not in str(exc.value)


def test_roots_stored_without_trailing_slash():
    conf = load_conf(dict(GOOD_ENV, DATA_ROOT="/tmp/lake/", RAW_ROOT="s3://bucket/raw/"))
    assert conf.data_root == "/tmp/lake"
    assert conf.raw_root == "s3://bucket/raw"


def test_cli_success_path(monkeypatch):
    for var, value in GOOD_ENV.items():
        monkeypatch.setenv(var, value)

    class StubSpark:
        stopped = 0

        def stop(self):
            self.stopped += 1

    stub = StubSpark()
    calls = []
    monkeypatch.setattr(cli.spark_mod, "build_spark", lambda *_a, **_k: stub)
    monkeypatch.setattr(cli, "run_local", lambda spark, conf: calls.append((spark, conf)))
    assert cli.main(["run", "--target", "local"]) == 0
    assert calls == [(stub, load_conf(GOOD_ENV))]
    assert stub.stopped == 1


@pytest.mark.parametrize("job", ["bronze", "silver", "gold"])
def test_job_entry_fails_fast_without_key(job):
    env = {k: v for k, v in os.environ.items() if k not in REQUIRED_VARS}
    env.update(DATA_ROOT="/tmp/lake", RAW_ROOT="/tmp/raw")
    proc = subprocess.run(
        [sys.executable, "-m", f"pipeline.jobs.{job}"],
        cwd=Path(__file__).resolve().parent.parent,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert proc.returncode != 0
    assert "PATIENT_KEY_HMAC_SECRET" in proc.stderr
    assert SECRET not in proc.stderr + proc.stdout
    # Spark never started.
    assert "Setting default log level" not in proc.stderr
