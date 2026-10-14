"""Pure Markdown rendering for `python -m report`. No I/O, no Spark.

Reports are outputs for a reader, not logs (AD-8). They render synthetic data only.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime

MAX_ROWS = 200
COUNTS_LINE = re.compile(
    r"layer=(?P<layer>\w+) source=(?P<source>\w+) rows_in=(?P<rows_in>\d+) "
    r"rows_out=(?P<rows_out>\d+) dropped=(?P<dropped>\d+)"
)


@dataclass(frozen=True)
class RunInfo:
    started: datetime  # UTC
    duration_s: float
    commit: str


@dataclass(frozen=True)
class CaseResult:
    name: str
    outcome: str  # passed | failed | error | skipped
    duration_s: float


@dataclass(frozen=True)
class Table:
    name: str
    header: list[str]
    rows: list[list[str]] = field(default_factory=list)


def escape_cell(value: str | None) -> str:
    if value is None:
        return ""
    return value.replace("\\", "\\\\").replace("|", "\\|").replace("\r", "").replace("\n", "<br>")


def md_table(header: list[str], rows: list[list[str]]) -> str:
    lines = [
        "| " + " | ".join(escape_cell(h) for h in header) + " |",
        "|" + "|".join("---" for _ in header) + "|",
    ]
    lines += ["| " + " | ".join(escape_cell(c) for c in row) + " |" for row in rows]
    return "\n".join(lines)


def parse_junit(xml_text: str) -> list[CaseResult]:
    """Test cases from a pytest --junitxml file."""
    root = ET.fromstring(xml_text)
    cases = []
    for tc in root.iter("testcase"):
        if tc.find("failure") is not None:
            outcome = "failed"
        elif tc.find("error") is not None:
            outcome = "error"
        elif tc.find("skipped") is not None:
            outcome = "skipped"
        else:
            outcome = "passed"
        name = f"{tc.get('classname', '')}::{tc.get('name', '')}".lstrip(":")
        cases.append(CaseResult(name, outcome, float(tc.get("time") or 0.0)))
    return cases


def parse_count_lines(log_text: str) -> list[dict[str, str]]:
    """The counts-only pipeline log lines (layer, source, rows_in, rows_out, dropped)."""
    return [m.groupdict() for m in COUNTS_LINE.finditer(log_text)]


def _header(title: str, run: RunInfo) -> list[str]:
    return [
        f"# {title}",
        "",
        f"- Run start (UTC): {run.started.strftime('%Y-%m-%dT%H:%M:%SZ')}",
        f"- Duration: {run.duration_s:.1f} s",
        f"- Commit: `{run.commit}`",
    ]


def render_test_results(run: RunInfo, cases: list[CaseResult], exit_code: int) -> str:
    totals = {
        k: sum(1 for c in cases if c.outcome == k) for k in ("passed", "failed", "error", "skipped")
    }
    status = "PASS" if exit_code == 0 and not (totals["failed"] or totals["error"]) else "FAIL"
    lines = _header("Test results", run)
    lines += [
        f"- Result: **{status}** (pytest exit code {exit_code})",
        (
            f"- Totals: {len(cases)} tests, {totals['passed']} passed, "
            f"{totals['failed']} failed, {totals['error']} errors, {totals['skipped']} skipped"
        ),
        "",
        md_table(
            ["Test", "Outcome", "Time (s)"],
            [[c.name, c.outcome, f"{c.duration_s:.2f}"] for c in cases],
        ),
        "",
    ]
    return "\n".join(lines)


def _capped(table: Table) -> list[str]:
    shown = table.rows[:MAX_ROWS]
    out = [f"### {table.name}", ""]
    if len(table.rows) > MAX_ROWS:
        out += [f"Showing the first {MAX_ROWS} of {len(table.rows)} rows.", ""]
    out += [md_table(table.header, shown), ""]
    return out


def render_pipeline_output(
    run: RunInfo,
    exit_code: int | None,
    file_counts: list[tuple[str, str, int | None]],
    job_counts: list[dict[str, str]],
    tables: list[Table],
    note: str | None = None,
) -> str:
    """file_counts: (layer, file, rows or None when missing)."""
    if exit_code is None:
        result = "NOT RUN"
    else:
        result = "PASS" if exit_code == 0 else "FAIL"
    code = "" if exit_code is None else f" (exit code {exit_code})"
    lines = _header("Pipeline output", run)
    lines += [f"- Result: **{result}**{code}"]
    if note:
        lines += [f"- Note: {note}"]
    lines += [
        "- Data: synthetic only. Every layer is treated as PHI; bronze is counted, never shown.",
        "",
        "## Rows per layer",
        "",
        md_table(
            ["Layer", "File", "Rows"],
            [[layer, f, "missing" if n is None else str(n)] for layer, f, n in file_counts],
        ),
        "",
        "## Job counts (from the counts-only log)",
        "",
        md_table(
            ["Layer", "Source", "Rows in", "Rows out", "Dropped"],
            [
                [j["layer"], j["source"], j["rows_in"], j["rows_out"], j["dropped"]]
                for j in job_counts
            ],
        ),
        "",
        "## Tables",
        "",
    ]
    for table in tables:
        lines += _capped(table)
    return "\n".join(lines)
