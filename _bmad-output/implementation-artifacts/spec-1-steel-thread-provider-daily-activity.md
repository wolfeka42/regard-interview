---
title: 'Story 1: Steel thread - gold shows how busy each provider was each day'
type: 'feature'
created: '2026-10-14'
status: 'done'
baseline_commit: '8917d3a11d2cbb0c5c12516d1700d8108db5b764'
route: 'dispatch'
review_loop_iteration: 0
context:
  - '{project-root}/_bmad-output/project-context.md'
  - '{project-root}/_bmad-output/planning-artifacts/architecture/architecture-interview-test-2026-10-12/ARCHITECTURE-SPINE.md'
  - '{project-root}/_bmad-output/specs/spec-patient-history-pipeline/data-model.md'
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** The code builds a patient-history gold table; the interview asks "How busy was each provider on each day?" across encounters, labs and providers from several hospital customers.

**Approach:** Rework story 1's scaffold into: synthetic bronze CSVs → silver `clean_encounters`, `clean_labs`, `clean_providers` → gold `provider_daily_activity`, all CSV, with the exact schemas, hashing and metric rules in `data-model.md`. Tests use chained dummy datasets per layer and check every output with `assert_schema` and `assert_content`; silver tests also check the output files are de-duplicated and hashed. `python -m report` writes `reports/test-results.md` and `reports/pipeline-output.md`, stamped with the run time, so a GitHub reader can verify results.

## Boundaries & Constraints

**Always:** Spine AD-1 to AD-11 and `data-model.md` bind this story. `customer` is in every join, grouping and dedupe key. Silver transforms are `clean_encounters(df: DataFrame, salt: str, run_ts=None) -> DataFrame`, `clean_labs(df, run_ts=None)` and `clean_providers(df, run_ts=None)`; `run_ts` defaults to now (UTC) and tests pass a fixed one. Counts-only logs.

**Never:** Story 2 rules (sentinel list, `test_name` normalization, empty-`specialty` handling beyond blank → null), story 3, `--target emr`, JSON input, Parquet.

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|---|---|---|---|
| Happy path | Provider P1 at customer A, 2 encounters on one day, 2 patients, 3 labs | One gold row: `encounter_count=2`, `unique_patients=2`, `lab_count=3`, `totaldurationdays`=sum, provider fields joined | N/A |
| Multi-provider | Encounter E1 has rows for P1 and P2 | E1 counted once for P1 and once for P2, with its labs for each | N/A |
| Duplicates | Exact duplicate rows in any source; two versions of `(customer, encounter_id, provider_id)` with different `updated_at`; repeated `(customer, lab_id)` or `(customer, provider_id)` | Silver keeps one row per key: the latest `updated_at` for encounters, a deterministic pick for labs and providers; gold counts once and labs are not doubled | Counted in `dropped` |
| Cancelled | Status `" Cancelled"` or `canceled` | Kept in silver with status lowercased; excluded from every gold metric, with its labs; a provider-day with only cancelled encounters has no gold row | N/A |
| Lab status | `retracted` / null / `final` | Retracted not counted; null and final counted | N/A |
| Lab day | Lab observed the day after its encounter | Counted on the encounter's `encounter_date` | N/A |
| Cross-customer IDs | `patient_id=1` and `provider_id=1` at customers A and B | Different `hashedpatientid`; separate gold rows | N/A |
| Unknown provider | Encounter provider absent from providers | Gold row kept; provider fields null | N/A |
| Duration | `end_time` null or before `start_time` | `duration_days` null; `totaldurationdays` sums the rest, 0.0 when none | N/A |
| Missing required | Encounter without `start_time` (or unparseable), lab without `encounter_id`, provider without `provider_id` | Dropped in silver | Counted in `dropped` |
| Salt | `PATIENT_ID_SALT` unset / set / padded with whitespace | `DEFAULT_SALT` / env value / `ConfigError` | Error names the variable, never the value |
| Missing `DATA_ROOT` | Unset or blank | No SparkSession; exit 2 | Error names the variable |
| Report | `python -m report` | `reports/test-results.md`: run start (UTC), duration, commit, pass/fail totals and one row per test. `reports/pipeline-output.md`: run start, duration, per-layer counts, and every silver and gold table as a Markdown table | A failing test or run is still reported, then exit nonzero |

**Decision (layer files, Kailey 2026-10-14):** silver outputs exactly three CSV files, `clean_encounters.csv`, `clean_labs.csv` and `clean_providers.csv`, under `{DATA_ROOT}/silver/`; gold reads only those three files and writes `{DATA_ROOT}/gold/provider_daily_activity.csv`.

**Decision (silver checks, Kailey 2026-10-14):** silver de-duplication moves from story 2 into this story. Silver unit tests include check functions that confirm each silver output file matches its schema exactly as pasted (`clean_encounters` 10 columns, `clean_labs` 9, `clean_providers` 6; `data-model.md`), is de-duplicated on its key, and for `clean_encounters` has `patient_id` null and every `hashedpatientid` equal to HMAC-SHA256(salt, `customer:patient_id`).

**Decision (report data, Kailey 2026-10-14):** `python -m report` renders whatever `DATA_ROOT` holds, synthetic or supplied. Supplied data may be reported only because the interviewer confirms it is synthetic; the report never runs against real records.

</frozen-after-approval>

## Code Map

Reuse from `665ae5b`; keep the patterns named, replace the rest.

- `pipeline/config.py` -- keep `require_env`, `ConfigError`, masked `__repr__`, root `rstrip`, padded-secret check. `Conf(data_root, salt)`: drop `RAW_ROOT` and `PATIENT_KEY_HMAC_SECRET`; optional `PATIENT_ID_SALT`, fallback `DEFAULT_SALT`.
- `pipeline/patient_key.py` -> `pipeline/hashing.py` -- keep `EDGE_WS` and the UDF pattern (driver-built, null-safe, never raises, `useArrow=False`). New: `normalize_id`, `make_hash_udf(salt)`, `with_hashed_patient(df, hash_udf)` over `customer:patient_id`.
- `pipeline/schemas.py` -- replace with `BRONZE_*`, `CLEAN_*`, `GOLD_SCHEMA`, `GOLD_SORT_KEYS`, `TIMESTAMP_FORMAT`, `DEFAULT_SALT`, exactly as `data-model.md`.
- `pipeline/jobs/silver.py` -- keep `_clean`; `parse_date` -> `parse_ts`; move gold's `conform_to_gold` here as `conform_to(df, schema)`; bronze read options from `jobs/bronze.py` (`header`, `enforceSchema=false`, `multiLine=true`, PERMISSIVE). New: `clean_encounters(df, salt, run_ts=None)` (builds the hash UDF from `salt`; dedupes on latest `updated_at`, then drops it), `clean_labs`, `clean_providers`, `main` (one `run_ts` per run).
- `pipeline/jobs/gold.py` -- keep `order_gold`; new `gold_provider_daily_activity(encounters, labs, providers)` and `main`.
- `pipeline/jobs/bronze.py` -- delete (AD-1). `pipeline/__main__.py` -- silver then gold. `spark.py`, `log.py` -- unchanged.
- `synth/generate.py` -- rewrite: seeded `{DATA_ROOT}/bronze/{source}.csv` covering every matrix row, two customers, integer and UUID IDs.
- `tests/conftest.py` -- keep `spark`; `key_udf` -> `hash_udf`; drop `FORBIDDEN_GOLD_COLUMNS`. `tests/test_patient_key.py` -> `tests/test_hashing.py`; rewrite the other test files.
- Stale names to update: docstrings in `pipeline/__init__.py`, `pipeline/jobs/__init__.py`, `pipeline/__main__.py`, app names (`patient-history-*`), `pyproject.toml` description. `README.md` does not exist: create it.
- `SENTINELS` is declared in story 2, not here.

## Tasks & Acceptance

**Execution:**
- [x] `pipeline/` -- schemas, config, hashing, silver, gold, CLI, bronze-job removal as in the Code Map -- AD-1 to AD-7, AD-11
- [x] `synth/generate.py` -- bronze CSVs -- CAP-6
- [x] `tests/assertions.py` -- `assert_schema(df, schema)` (names, types, order), `assert_content(df, expected_rows)` (exact, order-independent, null-aware), `assert_deduped(df, keys)` (no repeated key), `assert_hashed(silver_df, bronze_df, salt)` (`patient_id` all null; joins on normalized `(customer, encounter_id, provider_id)` and requires `hashedpatientid` == `hmac` HMAC-SHA256(salt, `customer:patient_id`) recomputed in the test, null only where bronze `patient_id` was null) -- D9
- [x] `tests/datasets.py` -- chained dummy datasets: bronze rows -> expected `clean_*` -> expected gold -- D9
- [x] `tests/test_{config,hashing,silver,gold,end_to_end}.py` -- every matrix row; each output checked with `assert_schema` and `assert_content`; each silver output also with `assert_deduped` on its key and, for encounters, `assert_hashed`; expected hashes computed independently with `hmac` in the test
- [x] `report/__main__.py` -- run pytest (`--junitxml` to a temp file) and the pipeline, render both `.md` files; Markdown rendering is pure and unit-tested; not imported by `pipeline/` -- reports are outputs, not logs (AD-8)
- [x] `README.md` -- create; link both reports and the command that regenerates them

**Acceptance Criteria:**
- Given the dummy bronze datasets, when each silver function runs, then `assert_schema`, `assert_content`, `assert_deduped` and (encounters) `assert_hashed` pass against the expected `clean_*` datasets.
- Given the expected `clean_*` datasets, when `gold_provider_daily_activity` runs, then both assertions pass against the expected gold dataset.
- Given synthetic bronze CSVs and `DATA_ROOT`, when `python -m pipeline run --target local` runs, then `silver/` holds exactly `clean_encounters.csv`, `clean_labs.csv`, `clean_providers.csv` and `gold/` exactly `provider_daily_activity.csv` (no part, `.crc`, `_SUCCESS` or temp entries), gold is computed only from those three files, each read back under its schema passes `assert_schema`, the silver files pass `assert_deduped` and `assert_hashed`, gold read back passes `assert_content` against the expected rows for the synthetic data, and each job logs counts-only lines (caplog; every pipeline line matches `^layer=\w+ source=\w+ rows_in=\d+ rows_out=\d+ dropped=\d+$`).
- Given `python -m report`, when it runs, then both `.md` files exist with run start (UTC), duration, commit and result rows, and its Markdown rendering functions pass unit tests on small in-memory inputs.

## Implementation Notes

- Departures from the Code Map: `conform_to` lives in `pipeline/schemas.py` (gold uses it too; jobs never import each other); read options and `write_csv` live in new `pipeline/storage.py`; every CSV read/write adds `escape='"'` (RFC 4180 doubled quotes).
- `tests/datasets.py` also holds a pure-Python gold oracle used by the end-to-end test on synthetic data; `report/render.py` holds the pure Markdown rendering.
- Gold's log line uses `rows_in` = clean_encounters rows, so its `dropped` counts rows merged by aggregation.
- Verified (orchestrator rerun): ruff clean; `pytest -q` 53 passed; fresh synth + `run --target local` exit 0, `silver/` holds exactly the three `clean_*.csv`, `gold/` exactly `provider_daily_activity.csv`; `python -m report` wrote both reports (implementer run).
- Not exercised: `write_csv` on `s3://`.

## Spec Change Log

- 2026-10-14, pre-implementation spec review (15 findings): amended Code Map, Tasks, ACs, Design Notes and Verification only (dedupe order, gold aggregation without fan-out, null-status handling, ID normalization scope, `write_csv` steps, timestamp round trip, report mechanics, `assert_hashed` signature). Frozen block unchanged. KEEP: matrix and decisions as approved.

## Review Triage Log

| # | Layer | Finding | Verdict | Evidence | Route |
|---|---|---|---|---|---|
| 1 | phi, blind | `python -m report` publishes silver rows incl. `hashedpatientid` (public salt) to a git-tracked file; only a docstring guards it | false | Frozen decision (report data) explicitly renders every silver table from whatever `DATA_ROOT` holds; risk raised with Kailey before approval | reject |
| 2 | phi | `assert_content`/`assert_hashed` failure messages print row values and keys | medium | `tests/assertions.py` messages interpolate `missing`/`unexpected` rows and keys; helpers run on files read from `DATA_ROOT`; policy: no values in exception text | patch |
| 3 | phi, edge | Gold tests (null patient, cancelled, lab day, order) skip `assert_schema`; silver timestamp test uses neither assertion | low | `tests/test_gold.py` 3 tests call only `assert_content`, order test neither; Task says each output gets both | patch |
| 4 | phi | Synth silver files in end-to-end test not content-checked | low | Covered on files by `test_silver_main_writes_three_checked_files` (dummy data); an extra silver oracle adds code | reject |
| 5 | phi, blind, verification-gap, edge | `docs/hipaa-context.md:15` "Gold now" truncated to "ld now" | low | Edit not made by this build (implementer and orchestrator); likely an editor slip; surfaced to the human instead of reverting | defer |
| 6 | blind | Default salt used silently, no log notice | false | D5: `DEFAULT_SALT` fallback is the interview requirement; nothing to log without the value | reject |
| 7 | blind | Reports stamped with baseline commit + dirty tree, not the code that ran | medium | `reports/*.md` show `8917d3a… (dirty tree)`; generated before commit | patch (regenerate after commit, no code change) |
| 8 | blind, edge | Report treats `s3://` or cwd-relative `DATA_ROOT` as local paths | low | `report/__main__.py` resolves against repo root, same as the pipeline subprocess (cwd=ROOT); S3 reports not in scope | reject |
| 9 | blind, edge | `write_csv` deletes target before rename; crash leaves file missing or `.tmp` behind | low | Spec Design Notes prescribe these steps; non-atomic overwrite already deferred (story 1 v1 #10) | reject |
| 10 | blind, verification-gap | `escape='"'` and `multiLine` never exercised; quoted-newline test deleted | medium | No bronze value contains a quote, comma or newline; removing `escape` passes all tests | patch |
| 11 | blind, edge | `assert_hashed` accepts the hash of any bronze version of a key, incl. null | medium | `allowed` is a set over every version; a stale-version hash would pass | patch |
| 12 | blind | `assert_schema` after `read_csv(schema)` is tautological | low | `enforceSchema=false` already validates header names and order (reordered-header test); types are imposed by design | reject |
| 13 | blind | `test_gold_file_is_sorted` weak | low | `test_order_gold_sorts_by_keys_nulls_last` sorts a 5-partition shuffle; gold keys are never null | reject |
| 14 | blind | Gold `dropped` reads as data loss in the report | low | Documented in Implementation Notes; relabeling adds a branch | reject |
| 15 | blind | Orphan labs and unsupported timestamp offsets vanish uncounted | medium | Inner lab join and `parse_ts` nulls; per-reason bad-input counts are story 3 | defer |
| 16 | verification-gap | Gold counts log line values never asserted | low | Only prefix and regex checked in `test_local_run_end_to_end` | patch |
| 17 | verification-gap, edge | `report/__main__.py` main/exit codes untested (matrix Report row) | medium | No test imports `report.__main__`; matrix row requires nonzero exit on failure | patch |
| 18 | verification-gap | Padded `DATA_ROOT` strip untested | low | Unlikely mis-set env; test-only | reject |
| 19 | verification-gap, edge | Oracle uses local-offset date, gold uses UTC date | low | `tests/datasets.py` `start.date()`; agrees only because synth emits `Z`; one-line fix | patch |
| 20 | edge | `:` in customer/patient_id collides hashes | low | `customer:patient_id` is frozen D5; customer codes have no `:` | reject |
| 21 | edge | Caller-supplied `run_ts` with microseconds or naive tz | low | Only tests pass `run_ts`, with whole-second UTC values | reject |
| 22 | edge | Target path exists as a directory | low | Targets are `clean_*.csv`; no code creates such directories | reject |
| 23 | edge | Silver read-back row with null `start_time`/`provider_id` reaches gold | false | Silver drops rows missing required fields before writing; gold reads only silver | reject |
| 24 | edge | `DATA_ROOT` of `s3://` or only slashes | low | Mis-set env; fails loudly | reject |
| 25 | edge | Missing bronze file gives traceback and half-updated silver | medium | `silver.main` reads sources in turn; error text names the path; bad-input handling is story 3 | defer |
| 26 | edge | Empty CSV renders a broken Markdown table | low | Spark writes a header for empty output; cosmetic | reject |
| 27 | edge | Truncated junit XML crashes the report | low | Only if pytest is killed mid-write | reject |
| 28 | edge | Report runs the whole suite before checking `DATA_ROOT` | low | `main` calls `load_conf` after `run_tests`; direct reorder | patch |
| 29 | edge | Oracle tie-break order differs from silver on `updated_at` ties | low | No ties in synth; test-only | reject |
| 30 | edge | Python `strip()` twin vs Java `(?U)\s` | low | Differs only for control characters absent from synthetic data | reject |
| 31 | edge | Legacy `silver/encounters/`, `gold/patient_history/` left in a reused `DATA_ROOT` | low | `data/` is git-ignored; tests use fresh folders; noted for the human | reject |

## Design Notes

- **Silver order, every source:** normalize (`normalize_id` on `customer` and every ID column: `encounter_id`, `patient_id`, `provider_id`, `lab_id`; status trim + lowercase; parse types) -> drop exact duplicates -> drop rows missing required fields -> dedupe by key with `row_number()`: encounters order by parsed `updated_at` desc nulls last, then every other column asc nulls last; labs and providers order by every non-key column asc nulls last. `updated_at` is compared as a parsed timestamp, never a string.
- **Gold:** remove cancelled with `NOT coalesce(status IN ('cancelled','canceled'), false)`, so a null status counts. Reduce to one row per `(customer, encounter_id, provider_id)` (AD-4). Aggregate encounter metrics on that reduced set. Separately join non-retracted labs (`IS DISTINCT FROM 'retracted'`) to the reduced set on `(customer, encounter_id)` and count per `(customer, provider_id, encounter_date)`. Left-join the two aggregates, then providers. Never `sum(duration_days)` after the lab join; test: one encounter with a duration and 3 labs gives `totaldurationdays` = that duration.
- `unique_patients` counts distinct non-null hashes: a provider-day whose only encounter has a null `patient_id` has `encounter_count=1`, `unique_patients=0`.
- `encounter_date` is the UTC date of `start_time` (accepted assumption).
- Spark's string -> boolean cast already accepts `true/false/t/f/y/n/yes/no/1/0`; `"maybe"` becomes null.
- **Timestamps:** every CSV read and write passes `timestampFormat=TIMESTAMP_FORMAT`. The pattern accepts `Z`, `+05` and `+0530`; anything else (for example `+05:30`, fractional seconds, no offset) is null. The generator emits `Z`. The default `run_ts` is truncated to whole seconds so values survive the CSV round trip.
- **`write_csv(df, path)`:** one helper for silver and gold, via the Hadoop `FileSystem` for the path (works on `s3://`). Steps: delete a leftover temp folder `{path}.tmp` -> `coalesce(1)` write there -> delete the existing target -> rename the single `part-*.csv` to `path` -> delete the temp folder and any `.crc` file beside the target. Gold reads only `silver/clean_{source}.csv`.
- **Salt:** `PATIENT_ID_SALT` set but blank, or padded with whitespace, raises `ConfigError` naming the variable.
- **`python -m report`:** requires `DATA_ROOT` (no default). It runs `pytest --junitxml=<tmp>` and `python -m pipeline run --target local` as subprocesses with the current env, so each has its own JVM. It stamps `git rev-parse HEAD` plus a dirty-tree flag, and caps each rendered table at 200 rows, saying when it truncates. `report/` sits outside `pipeline/`, so AD-8's no-`collect` rule for logging does not apply to it.

## Verification

**Commands:**
- `.venv/bin/ruff format --check . && .venv/bin/ruff check .` -- expected: clean
- `JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/pytest -q` -- expected: all pass
- `export JAVA_HOME=/opt/homebrew/opt/openjdk@17 DATA_ROOT=data/lake && .venv/bin/python -m synth.generate && .venv/bin/python -m pipeline run --target local` -- expected: exit 0, gold CSV written
- `JAVA_HOME=/opt/homebrew/opt/openjdk@17 DATA_ROOT=data/lake .venv/bin/python -m report` -- expected: both reports written
