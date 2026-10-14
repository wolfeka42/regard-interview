---
title: 'Story 1 (part 2): Silver cleans every source quality issue'
type: 'feature'
created: '2026-10-14'
status: 'done'
baseline_commit: '601f1ce4336d16bef262db73f7e3adbd053a3ec1'
route: 'dispatch'
review_loop_iteration: 0
context:
  - '{project-root}/_bmad-output/project-context.md'
  - '{project-root}/_bmad-output/specs/spec-patient-history-pipeline/data-model.md'
  - '{project-root}/_bmad-output/implementation-artifacts/spec-1-steel-thread-provider-daily-activity.md'
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** Silver still passes sentinel values ("N/A", "unknown", "NULL") through as real text, even in required IDs, and leaves `test_name` in mixed case and spacing. Kailey decided story 2's quality rules belong to story 1, implemented and tested in the silver layer.

**Approach:** Add one shared `SENTINELS` list and apply it to every bronze string field before any other silver step; normalize `test_name`; make every quality rule (sentinels, formatting, optional nulls, `active_flag`, `result_value`, required-field drops, dedupe) an explicit, tested silver rule with exact dropped counts, using the existing chained dummy datasets and check functions.

## Boundaries & Constraints

**Always:** Silver only (`pipeline/jobs/silver.py`, `pipeline/schemas.py`); gold and the silver/gold schemas are unchanged. Sentinel nulling runs first, before ID normalization, hashing, required-field drops and dedupe. Every rule is checked with `assert_schema`, `assert_content`, `assert_deduped` and, for encounters, `assert_hashed`. Counts-only logs.

**Never:** Numeric sentinels (for example `-999`) or per-source sentinel lists; changing gold metrics; story 3 (bad-input counts per reason, malformed rows); quarantining rows with values.

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|---|---|---|---|
| Sentinel in optional field | `specialty="N/A"`, `result_unit="-"`, lab `status="NULL"`, `end_time="unknown"` | Field null, row kept | N/A |
| Sentinel variants | `" n/a "`, `"Null"`, `"UNKNOWN"`, `"none"`, `"?"`, NBSP-padded `"-"` | Treated as sentinel (trimmed, case-insensitive) | N/A |
| Sentinel in required field | `provider_id="unknown"`, lab `encounter_id="N/A"`, `start_time="NULL"`, `customer="-"` | Row dropped | Counted in `dropped` |
| Sentinel patient | `patient_id="unknown"` | Encounter kept; `patient_id` and `hashedpatientid` null | N/A |
| `test_name` formatting | `"  Hemoglobin   A1C "`, `"GLUCOSE"`, `"glu cose"`-style inner NBSP | `"hemoglobin a1c"`, `"glucose"`; inner whitespace runs become one space | N/A |
| Empty optional | `specialty=""` or whitespace only | null; provider kept | N/A |
| `active_flag` | `"Yes"`, `"N"`, `" TRUE "`, `"maybe"`, `"N/A"` | true, false, true, null, null | N/A |
| `result_value` | `" 4.1 "`, `"abc"`, `"N/A"` | 4.1, null, null; row kept | N/A |
| Dropped counts | Each source mixes exact duplicates, superseded versions and rows missing required fields | `rows_in - rows_out` per source equals the exact number of such rows | Counts only, never values |

**Decision (NA, Kailey 2026-10-14):** `"NA"` is not a sentinel, so `"Na"` (sodium) stays real text. `SENTINELS` = `""`, `null`, `n/a`, `none`, `-`, `?`, `unknown`, compared after trimming, case-insensitive.

</frozen-after-approval>

## Code Map

- `pipeline/schemas.py` -- add `SENTINELS` (a tuple of lowercase strings) per `data-model.md` silver rules.
- `pipeline/jobs/silver.py` -- `_clean` (line 43) is the single entry for every bronze string: extend it to null `SENTINELS` (compare `lower(trimmed)`), so `parse_ts`, `_status` and casts inherit it; wrap the `normalize_id(...)` inputs (lines 88-91, 116-118, 133-134) as `normalize_id(_clean(...))`; add `_test_name` (trim, collapse `(?U)\s+` to one space, lowercase) for line 119. Keep the existing normalize -> exact duplicates -> required -> dedupe order.
- `pipeline/hashing.py` -- unchanged (`normalize_id` stays the ID owner; sentinel nulling happens before it).
- `tests/datasets.py` -- extend `BRONZE_*_ROWS` and `EXPECTED_CLEAN_*` (and `EXPECTED_GOLD` only where a changed row reaches gold); extend the oracle's `_text` to null sentinels the same way.
- `synth/generate.py` -- add each matrix issue to the synthetic bronze CSVs.
- `tests/test_silver.py` -- one test per matrix row, plus exact per-source dropped counts in `test_silver_main_writes_three_checked_files`.

## Tasks & Acceptance

**Execution:**
- [x] `pipeline/schemas.py`, `pipeline/jobs/silver.py` -- `SENTINELS`, sentinel-aware `_clean`, `_test_name`
- [x] `tests/datasets.py`, `synth/generate.py` -- every matrix issue in dummy and synthetic bronze, with expected silver rows
- [x] `tests/test_silver.py` -- a test per matrix row; each output checked with `assert_schema`, `assert_content`, `assert_deduped` (and `assert_hashed` for encounters)
- [x] `_bmad-output/specs/spec-patient-history-pipeline/stories.yaml` -- fold story 2 into story 1; renumber stories 3 and 4 to 2 and 3

**Acceptance Criteria:**
- Given the extended dummy bronze datasets, when `clean_encounters`, `clean_labs` and `clean_providers` run, then all four check functions pass against the expected `clean_*` datasets.
- Given those datasets written as bronze CSVs, when `silver.main` runs, then each source's log line has the exact expected `rows_in`, `rows_out` and `dropped`, and the three files pass the same checks.
- Given synthetic bronze CSVs containing every matrix issue, when `python -m pipeline run --target local` runs, then the end-to-end test still passes, gold included.

## Implementation Notes

- `normalize_id(_clean(...))` is wrapped as a small `_id` helper in `silver.py`; `_clean` nulls `SENTINELS` on `lower(trimmed)`, so `parse_ts`, `_status`, casts and `_test_name` inherit it.
- `tests/assertions.py` gains an independent `SENTINELS` copy and `clean_text`; `normalize`, `parse_ts` and `kept_encounter_versions` use it, so `assert_hashed` and the gold oracle expect null for a sentinel patient. `test_sentinels_constant_is_the_agreed_list` ties both copies to the decision.
- Dummy data: encounters 24 in / 14 out / 10 dropped, labs 19 / 13 / 6, providers 14 / 9 / 5. One new gold row (alpha, provider 3, 2024-01-09). The synthetic generator carries the same case rows.
- Also updated `data-model.md` "Required tests" line (story 2 -> story 1 part 2). Not touched (outside the silver-only boundary): `pipeline/__main__.py` docstring still says "EMR arrives in story 4" (now story 3).
- Verified: ruff clean; `pytest -q` 75 passed; mutation check (sentinel nulling off, `_test_name` off) fails 15 and 5 tests respectively; synth + `python -m report` PASS. Reports are stamped `601f1ce (dirty tree)`: regenerate after commit.

## Spec Change Log

## Review Triage Log

## Verification

**Commands:**
- `.venv/bin/ruff format --check . && .venv/bin/ruff check .` -- expected: clean
- `JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/pytest -q` -- expected: all pass
- `export JAVA_HOME=/opt/homebrew/opt/openjdk@17 DATA_ROOT=data/lake && .venv/bin/python -m synth.generate && .venv/bin/python -m report` -- expected: both reports regenerated, PASS
