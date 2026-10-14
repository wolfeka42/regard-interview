from datetime import UTC, datetime

from report.render import (
    MAX_ROWS,
    CaseResult,
    RunInfo,
    Table,
    escape_cell,
    md_table,
    parse_count_lines,
    parse_junit,
    render_pipeline_output,
    render_test_results,
)

RUN = RunInfo(datetime(2026, 10, 14, 9, 30, 0, tzinfo=UTC), 12.34, "abc123 (dirty tree)")

JUNIT = """<?xml version="1.0" encoding="utf-8"?>
<testsuites><testsuite name="pytest" tests="4">
  <testcase classname="tests.test_a" name="test_ok" time="0.10"/>
  <testcase classname="tests.test_a" name="test_bad" time="0.20"><failure message="x"/></testcase>
  <testcase classname="tests.test_b" name="test_err" time="0.00"><error message="y"/></testcase>
  <testcase classname="tests.test_b" name="test_skip" time="0.00"><skipped message="z"/></testcase>
</testsuite></testsuites>"""


def test_escape_cell():
    assert escape_cell(None) == ""
    assert escape_cell("a|b\nc") == "a\\|b<br>c"


def test_md_table():
    assert md_table(["a", "b"], [["1", ""], ["x|y", "2"]]) == (
        "| a | b |\n|---|---|\n| 1 |  |\n| x\\|y | 2 |"
    )


def test_parse_junit_outcomes():
    cases = parse_junit(JUNIT)
    assert [(c.name, c.outcome) for c in cases] == [
        ("tests.test_a::test_ok", "passed"),
        ("tests.test_a::test_bad", "failed"),
        ("tests.test_b::test_err", "error"),
        ("tests.test_b::test_skip", "skipped"),
    ]
    assert cases[1].duration_s == 0.2


def test_parse_count_lines_ignores_other_output():
    text = (
        "2026-10-14 INFO pipeline layer=silver source=labs rows_in=5 rows_out=4 dropped=1\n"
        "WARN some spark noise\n"
        "2026-10-14 INFO pipeline layer=gold source=provider_daily_activity rows_in=4 "
        "rows_out=2 dropped=2\n"
    )
    assert parse_count_lines(text) == [
        {"layer": "silver", "source": "labs", "rows_in": "5", "rows_out": "4", "dropped": "1"},
        {
            "layer": "gold",
            "source": "provider_daily_activity",
            "rows_in": "4",
            "rows_out": "2",
            "dropped": "2",
        },
    ]


def test_render_test_results_header_totals_and_rows():
    md = render_test_results(RUN, parse_junit(JUNIT), exit_code=1)
    assert "- Run start (UTC): 2026-10-14T09:30:00Z" in md
    assert "- Duration: 12.3 s" in md
    assert "- Commit: `abc123 (dirty tree)`" in md
    assert "**FAIL**" in md
    assert "4 tests, 1 passed, 1 failed, 1 errors, 1 skipped" in md
    assert "| tests.test_a::test_bad | failed | 0.20 |" in md


def test_render_test_results_pass():
    md = render_test_results(RUN, [CaseResult("t::x", "passed", 0.0)], exit_code=0)
    assert "**PASS**" in md


def test_render_pipeline_output_counts_tables_and_cap():
    big = Table("silver/clean_labs.csv", ["lab_id"], [[str(i)] for i in range(MAX_ROWS + 5)])
    small = Table("gold/provider_daily_activity.csv", ["customer", "lab_count"], [["a", "3"]])
    md = render_pipeline_output(
        RUN,
        0,
        [
            ("bronze", "labs.csv", 210),
            ("silver", "clean_labs.csv", MAX_ROWS + 5),
            ("gold", "g", None),
        ],
        [
            {
                "layer": "silver",
                "source": "labs",
                "rows_in": "210",
                "rows_out": "205",
                "dropped": "5",
            }
        ],
        [big, small],
    )
    assert "- Run start (UTC): 2026-10-14T09:30:00Z" in md
    assert "**PASS** (exit code 0)" in md
    assert "| bronze | labs.csv | 210 |" in md
    assert "| gold | g | missing |" in md
    assert "| silver | labs | 210 | 205 | 5 |" in md
    assert f"Showing the first {MAX_ROWS} of {MAX_ROWS + 5} rows." in md
    assert f"| {MAX_ROWS - 1} |" in md and f"| {MAX_ROWS} |" not in md
    assert "| a | 3 |" in md


def test_render_pipeline_output_not_run():
    md = render_pipeline_output(RUN, None, [], [], [], note="DATA_ROOT is unset")
    assert "**NOT RUN**" in md
    assert "DATA_ROOT is unset" in md


def test_pipeline_never_imports_report():
    from pathlib import Path

    pipeline_dir = Path(__file__).resolve().parent.parent / "pipeline"
    for path in pipeline_dir.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert "import report" not in text and "from report" not in text, path
