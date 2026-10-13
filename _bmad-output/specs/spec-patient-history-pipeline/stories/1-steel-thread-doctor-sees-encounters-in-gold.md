---
title: 'Story 1: Steel thread - a doctor sees one patient''s encounters in gold'
type: 'feature'
created: '2026-10-13'
status: 'done'
route: 'dispatch'
review_loop_iteration: 0
baseline_commit: 'beab0791cdbbc00a943d0a11eaadac5a25bb812f'
context:
  - '{project-root}/_bmad-output/project-context.md'
  - '{project-root}/_bmad-output/planning-artifacts/architecture/architecture-interview-test-2026-10-12/ARCHITECTURE-SPINE.md'
  - '{project-root}/_bmad-output/specs/spec-patient-history-pipeline/data-model.md'
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** No code exists yet. Doctors need a gold table of a patient's encounters, with full dates, keyed by a pseudonymous patient key and never by the MRN.

**Approach:** Build the smallest end-to-end slice: `python -m pipeline run --target local` takes a synthetic `encounters.csv` through bronze, silver and gold, as three jobs that hand off through Parquet. One source only, with no joins. Every schema lives in `pipeline/schemas.py` so the real data's schema can be swapped in on the day.

**Decision (runtime):** The agent installs JDK 17 (`brew install openjdk@17`) and recreates `.venv` with uv on Python 3.11 from the pinned `pyproject.toml`, replacing the current Python 3.10 / pyspark 4.2.0 venv (AD-10).

## Boundaries & Constraints

**Always:** The spine (AD-1 to AD-11) and `project-context.md` bind this story. In this story, the demographic gold columns are typed nulls.

**Never:** The patients source, joins, age, zip3, other event sources (stories 2 to 4), or `--target emr` (story 6).

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|---|---|---|---|
| Happy path | Valid encounter rows | One gold row per encounter: `event_type='encounter'`, `event_date`=encounter_date, `event_end_date`=discharge_date, `event_code`=encounter_type, a 64-hex `patient_key_v1` | N/A |
| Same patient, different formatting | `" fac1 "`/`"00123"` and `"FAC1"`/`"1 23"` | Same key | N/A |
| Same MRN, different facility | `FAC1:123` and `FAC2:123` | Different keys | N/A |
| Blank identity | MRN of `""`, whitespace (including NBSP) or `"0000"`; or a blank facility | Row dropped in silver, never hashed | Counted in `dropped` |
| Bad dates | encounter_date missing or unparseable, or discharge_date present but unparseable | Row dropped in silver | Counted in `dropped` |
| Missing env var | `DATA_ROOT`, `RAW_ROOT` or `PATIENT_KEY_HMAC_SECRET` unset or empty | No SparkSession created; nonzero exit | Error names the variable, never its value |

</frozen-after-approval>

## Code Map

- No Python code exists yet; planning docs in `context` are the only constraints.
- `.gitignore` -- already ignores `data/`; use `DATA_ROOT=data/lake`, `RAW_ROOT=data/raw` locally.
- `.claude/settings.json` -- hook runs `ruff format` on each edited `.py`.

## Tasks & Acceptance

**Execution:**
- [x] `pyproject.toml` -- pins: `pyspark==3.5.8`, `pytest==9.1.1`, `ruff==0.17.0`; Python 3.11; ruff `py311`; pytest `testpaths=["tests"]`
- [x] `pipeline/__init__.py`, `pipeline/jobs/__init__.py` -- packages
- [x] `pipeline/config.py` -- frozen `Conf(data_root, raw_root, hmac_secret)`, masked `repr`; `load_conf()` raises `ConfigError` naming the variable
- [x] `pipeline/schemas.py` -- `RAW_ENCOUNTERS`, `INPUT_FORMATS`, `SILVER_ENCOUNTERS`, `GOLD_SCHEMA`, `GOLD_SORT_KEYS`, `DATE_FORMAT="yyyy-MM-dd"`
- [x] `pipeline/patient_key.py` -- `normalize_facility`, `normalize_mrn`, `drop_invalid_identity`, `make_key_udf`, `with_patient_key`
- [x] `pipeline/log.py` -- `log_counts(layer, source, rows_in, rows_out)` via stdlib logging
- [x] `pipeline/spark.py` -- `build_spark(app_name)`: ANSI off, session time zone UTC
- [x] `pipeline/jobs/{bronze,silver,gold}.py` -- `bronze_encounters`; `parse_date` + `silver_encounters(df, key_udf)`; `gold_encounters`, `conform_to_gold`, `order_gold`; each with `main(spark, conf)` and a `__main__` entry
- [x] `pipeline/__main__.py` -- `run --target local` only; conf, then Spark, then bronze, silver, gold
- [x] `synth/__init__.py`, `synth/generate.py` -- seed 42; about 200 encounters for about 40 patients in 2 facilities, including padded MRNs, blank and `0000` MRNs, and bad dates
- [x] `tests/` -- `conftest.py` (session Spark), plus `test_config`, `test_patient_key`, `test_silver`, `test_gold`, `test_end_to_end`: the matrix above and the ACs below

**Acceptance Criteria:**
- Given a conformed gold DataFrame, when its schema is read, then the `(name, type)` pairs equal `GOLD_SCHEMA` exactly and no forbidden column appears.
- Given a shuffled gold DataFrame, when `order_gold` runs, then the `GOLD_SORT_KEYS` tuples in `collect()` never decrease, with nulls last.
- Given synthetic raw data and the env vars set, when the run command runs, then bronze row count equals source row count, gold is written to `{DATA_ROOT}/gold/patient_history/`, each job logs one counts-only line per source, and no silver or gold output has `mrn` or `facility`.

## Implementation Notes

- Runtime: `brew install openjdk@17` (keg-only, not linked; export `JAVA_HOME=/opt/homebrew/opt/openjdk@17`); `.venv` rebuilt by uv on Python 3.11.17 with pyspark 3.5.8; `uv.lock` added. pyproject also sets pytest `pythonpath=["."]` and ruff `extend-exclude` for `.claude`, `_bmad`, `_bmad-output`, `data`.
- `build_spark` also sets `spark.sql.legacy.timeParserPolicy=CORRECTED` so a non-padded date like `2024-1-5` becomes null (dropped) instead of raising Spark's upgrade error, whose text can carry the row value. `local=True` adds `local[*]` master and pins the Python worker interpreter; EMR gets these from spark-submit (story 6).
- `config.require_env` lets `synth.generate` read `RAW_ROOT` without the key; a whitespace-only variable counts as missing.
- Silver nulls blank `encounter_id`/`encounter_type`; a whitespace-only discharge date counts as absent, not bad.
- Verified: ruff clean; `pytest -q` 31 passed, no skips; local run bronze 211→211, silver 211→200 (11 dropped), gold 200→200, 40 distinct keys.
- Incident: an early `ruff format .` reformatted tracked `.claude/skills/` files; reverted with `git checkout -- .claude` before hand-back (confirmed no tracked diff).

## Spec Change Log

## Review Triage Log

| # | Layer | Finding | Verdict | Evidence | Route |
|---|---|---|---|---|---|
| 1 | edge, blind | Bronze CSV maps columns by position (`enforceSchema` default true); reordered header swaps MRN/facility silently | high | Reproduced: header `mrn,facility` gave `facility='123', mrn='FAC1'`; `enforceSchema=false` raises | patch |
| 2 | edge | Quoted field with embedded newline splits one record into two | medium | Reproduced: count 2 by default, 1 with `multiLine=true`; breaks CAP-1 bronze count | patch |
| 3 | edge | `:` inside facility/MRN can collide keys | low | Real but unlikely in facility/MRN codes; fix changes the `facility:mrn` contract in data-model | reject |
| 4 | edge | Facility inner whitespace variants give different keys | low | Real; AD-6 defines strip-surrounding only; changing normalization goes through bmad-architecture | defer |
| 5 | edge, blind | HMAC secret kept with surrounding whitespace, so keys differ by environment | medium | `config.py:41` returns value unstripped; only whitespace-only is rejected | patch (reject, never strip) |
| 6 | blind | DATA_ROOT/RAW_ROOT not whitespace-stripped | low | Unlikely; path would fail loudly | reject |
| 7 | edge, blind | discharge_date before encounter_date reaches gold | medium | Real; spine Deferred: per-source validity rules beyond required dates are set after day-of data review | defer |
| 8 | edge, blind | Duplicate encounter_id rows reach gold; blank encounter_id kept | medium | Real; dedupe/validity policy is a data-review decision (spine Deferred) | defer |
| 9 | edge | Ties on all GOLD_SORT_KEYS reorder between runs | low | Same-day same-type encounters only; fix alters spine's GOLD_SORT_KEYS | reject |
| 10 | edge | Overwrite failure mid-write leaves a layer partial or empty (bronze, silver, gold) | medium | Real; AD-2 prescribes plain overwrite; atomic promote not in scope | defer |
| 11 | blind | Malformed CSV rows (wrong field count) are padded, not counted | medium | Real under PERMISSIVE; bad-input counting is story 5 | defer |
| 12 | blind | No top-level guard on Spark exceptions; tracebacks could carry values | maybe-false | Needs a Spark error path that embeds row values (ANSI off, PERMISSIVE, UDF never raises); story 5 proves no values in errors | defer (medium, unverified) |
| 13 | blind, phi | End-to-end test does not check written gold schema, key format, gold==silver count, or exact silver drop count | medium | `test_end_to_end.py:837-840` checks counts loosely and column names only | patch |
| 14 | blind | `test_bad_dates_dropped_and_counted` checks counts, not which rows survive | low | `test_silver.py:1089` asserts counts only | patch |
| 15 | blind | Job `__main__` entries untested for fail-fast before Spark | low | Code order verified correct (`load_conf` before `build_spark`); EMR uses these entries | patch |
| 16 | blind | Synth lacks non-padded date, discharge-before-encounter, duplicates, malformed rows; random MRNs may collide | low | Unit tests already cover non-padded dates; others are deferred rules; collisions only merge synthetic patients | reject |
| 17 | blind | `_EDGE_WS` duplicated in `silver.py` and `patient_key.py` | low | Two regex owners against AD-6; direct import fixes it | patch |
| 18 | blind | Identity normalized twice (silver, then inside `drop_invalid_identity`) | false | Normalization is idempotent; result identical; negligible cost at interview scale | reject |
| 19 | blind | Literal NBSP characters in source can be silently normalized by an editor | low | Invisible U+00A0 in synth and tests; `"\u00a0"` is a direct correction | patch |
| 20 | blind | No ruff lint rules (T20, BLE), so `noqa: BLE001` is dead | low | Adds config and noqa churn for prints that carry no PHI | reject |
| 21 | blind | `key_udf` parameters lack type hints | low | Cosmetic | reject |
| 22 | verification-gap | Silver path never tested for same key across formatting variants | medium | Pre-verified: `_keys` helper normalizes itself; removing silver normalization passes all tests | patch |
| 23 | verification-gap | CLI success path (`run --target local`) untested | medium | Pre-verified: no test calls `cli.main` with a valid env | patch |
| 24 | verification-gap | Trailing-slash strip on roots untested | low | Pre-verified: no test passes a trailing `/` | patch |

## Design Notes

- Regexes use `(?U)\s`, because Java's plain `\s` and Spark's `trim` are ASCII-only, so they miss NBSP.
- The key UDF uses `hmac.new(secret, f"{facility}:{mrn}", sha256).hexdigest()`. It is built on the driver, returns null on null input and never raises.
- The schema test compares `(name, dataType)` pairs, because `unionByName` and casts relax nullability.

## Verification

**Commands:**
- `.venv/bin/ruff format --check . && .venv/bin/ruff check .` -- expected: clean
- `.venv/bin/pytest -q` -- expected: all pass
- `.venv/bin/python -m synth.generate && .venv/bin/python -m pipeline run --target local` (with the env vars and a synthetic key) -- expected: exit 0, gold written
