"""report/__main__.py main: failures are still reported, then the exit code is nonzero.

Subprocesses are faked; all CSV content is synthetic.
"""

import subprocess

import pytest

import report.__main__ as rm

JUNIT = """<?xml version="1.0"?><testsuites><testsuite>
<testcase classname="tests.test_x" name="test_ok" time="0.1"/>
<testcase classname="tests.test_x" name="test_bad" time="0.1"><failure message="m"/></testcase>
</testsuite></testsuites>"""
GOLD_LINE = "layer=gold source=provider_daily_activity rows_in=1 rows_out=1 dropped=0"
BRONZE_MARKER = "bronze-only-synthetic-value"


def _write_lake(root):
    for layer, name, body in (
        ("bronze", "encounters.csv", f"customer,encounter_id\na,{BRONZE_MARKER}\n"),
        ("silver", "clean_encounters.csv", "customer,encounter_id\na,1\n"),
        ("gold", "provider_daily_activity.csv", "customer,provider_id\na,p\n"),
    ):
        (root / layer).mkdir(parents=True, exist_ok=True)
        (root / layer / name).write_text(body, encoding="utf-8")


@pytest.fixture
def fake(monkeypatch, tmp_path):
    reports = tmp_path / "reports"
    monkeypatch.setattr(rm, "REPORTS", reports)
    calls = []
    codes = {"pytest": 0, "pipeline": 0}

    def run(args, **_kw):
        if args[0] == "git":
            out = "abc123\n" if "rev-parse" in args else ""  # clean tree
            return subprocess.CompletedProcess(args, 0, stdout=out, stderr="")
        if "pytest" in args:
            calls.append("pytest")
            junit = next(a for a in args if a.startswith("--junitxml=")).split("=", 1)[1]
            with open(junit, "w", encoding="utf-8") as fh:
                fh.write(JUNIT)
            return subprocess.CompletedProcess(args, codes["pytest"], stdout="", stderr="")
        calls.append("pipeline")
        return subprocess.CompletedProcess(args, codes["pipeline"], stdout="", stderr=GOLD_LINE)

    monkeypatch.setattr(rm.subprocess, "run", run)
    return reports, calls, codes


@pytest.mark.parametrize("failing", ["pytest", "pipeline"])
def test_failure_is_reported_then_exit_1(fake, monkeypatch, tmp_path, failing):
    reports, calls, codes = fake
    lake = tmp_path / "lake"
    _write_lake(lake)
    monkeypatch.setenv("DATA_ROOT", str(lake))
    codes[failing] = 1

    assert rm.main() == 1
    assert calls == ["pytest", "pipeline"]
    tests_md = (reports / "test-results.md").read_text(encoding="utf-8")
    output_md = (reports / "pipeline-output.md").read_text(encoding="utf-8")
    assert "Run start (UTC)" in tests_md and "Commit: `abc123`" in tests_md
    assert "| tests.test_x::test_bad | failed |" in tests_md
    assert "Run start (UTC)" in output_md
    assert ("**FAIL** (exit code 1)" in output_md) == (failing == "pipeline")
    assert "| bronze | encounters.csv | 1 |" in output_md
    assert "### silver/clean_encounters.csv" in output_md
    assert "### gold/provider_daily_activity.csv" in output_md
    assert "| gold | provider_daily_activity | 1 | 1 | 0 |" in output_md
    # Bronze is counted, never rendered.
    assert "### bronze" not in output_md
    assert BRONZE_MARKER not in output_md


def test_missing_data_root_writes_not_run_and_exits_2(fake, monkeypatch, capsys):
    reports, calls, _codes = fake
    monkeypatch.delenv("DATA_ROOT", raising=False)

    assert rm.main() == 2
    assert calls == []  # no tests, no pipeline
    output_md = (reports / "pipeline-output.md").read_text(encoding="utf-8")
    assert "**NOT RUN**" in output_md
    assert "DATA_ROOT" in output_md
    assert "DATA_ROOT" in capsys.readouterr().err
