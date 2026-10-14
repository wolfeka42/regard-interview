"""`python -m report`: run the tests and the pipeline, then write two Markdown reports.

- reports/test-results.md: run start, duration, commit, totals, one row per test.
- reports/pipeline-output.md: run start, duration, per-layer counts, every silver and
  gold table (capped at 200 rows each).

Requires DATA_ROOT and renders whatever it holds; only run it on synthetic data.
Each step runs as a subprocess with the current environment, so each has its own JVM.
A failing test or pipeline run is still reported, then the command exits nonzero.
"""

from __future__ import annotations

import csv
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

from pipeline.config import ConfigError, load_conf
from report.render import (
    RunInfo,
    Table,
    parse_count_lines,
    parse_junit,
    render_pipeline_output,
    render_test_results,
)

ROOT = Path(__file__).resolve().parent.parent
REPORTS = ROOT / "reports"
SOURCES = ("encounters", "labs", "providers")


def git_commit() -> str:
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=True
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain"], cwd=ROOT, capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return f"{sha} (dirty tree)" if dirty else sha


def _read_csv(path: Path) -> tuple[list[str], list[list[str]]] | None:
    if not path.is_file():
        return None
    with path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.reader(fh))
    return (rows[0], rows[1:]) if rows else ([], [])


def run_tests() -> tuple[int, list, RunInfo]:
    started, t0 = datetime.now(UTC), time.monotonic()
    with tempfile.TemporaryDirectory() as tmp:
        junit = Path(tmp) / "junit.xml"
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", f"--junitxml={junit}"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        cases = parse_junit(junit.read_text(encoding="utf-8")) if junit.is_file() else []
    return proc.returncode, cases, RunInfo(started, time.monotonic() - t0, git_commit())


def run_pipeline(data_root: str) -> tuple[str, int]:
    started, t0 = datetime.now(UTC), time.monotonic()
    proc = subprocess.run(
        [sys.executable, "-m", "pipeline", "run", "--target", "local"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    run = RunInfo(started, time.monotonic() - t0, git_commit())
    root = Path(data_root)
    if not root.is_absolute():
        root = ROOT / root

    files = [("bronze", f"{s}.csv", root / "bronze" / f"{s}.csv") for s in SOURCES]
    files += [("silver", f"clean_{s}.csv", root / "silver" / f"clean_{s}.csv") for s in SOURCES]
    files += [
        ("gold", "provider_daily_activity.csv", root / "gold" / "provider_daily_activity.csv")
    ]

    file_counts, tables = [], []
    for layer, name, path in files:
        data = _read_csv(path)
        file_counts.append((layer, name, None if data is None else len(data[1])))
        if layer != "bronze" and data is not None:
            tables.append(Table(f"{layer}/{name}", data[0], data[1]))
    return render_pipeline_output(
        run, proc.returncode, file_counts, parse_count_lines(proc.stderr), tables
    ), proc.returncode


def main() -> int:
    REPORTS.mkdir(exist_ok=True)
    try:
        conf = load_conf()
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        run = RunInfo(datetime.now(UTC), 0.0, git_commit())
        (REPORTS / "pipeline-output.md").write_text(
            render_pipeline_output(run, None, [], [], [], note=str(exc)), encoding="utf-8"
        )
        return 2

    test_code, cases, test_run = run_tests()
    (REPORTS / "test-results.md").write_text(
        render_test_results(test_run, cases, test_code), encoding="utf-8"
    )
    text, pipeline_code = run_pipeline(conf.data_root)
    (REPORTS / "pipeline-output.md").write_text(text, encoding="utf-8")
    print(f"wrote {REPORTS / 'test-results.md'} and {REPORTS / 'pipeline-output.md'}")
    return 0 if test_code == 0 and pipeline_code == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
