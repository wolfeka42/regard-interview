# Project context: pipeline code rules

Moved from `CLAUDE.md` on 2026-10-12. Layers and patient-ID rules rewritten 2026-10-14 by course correction (`planning-artifacts/sprint-change-proposal-2026-10-14.md`). `AGENTS.md` points here before any `.py` change.

## Running
- Tests: `pytest -q`   Format: `ruff format .` (a hook also runs `ruff format` on each edited `.py` file)
- Runtime: Python 3.11, JDK 17 (`JAVA_HOME`), `pyspark==3.5.8`, matching EMR 7.14.0. Spark 3.5 does not run on Java 21.
- Architecture rules AD-1 to AD-11: `planning-artifacts/architecture/architecture-interview-test-2026-10-12/ARCHITECTURE-SPINE.md`.

## PySpark
- PySpark only for data manipulation; no pandas, no Python UDFs unless unavoidable. HMAC-SHA256 counts as unavoidable, because Spark 3.5 has no built-in HMAC. Keep that UDF small and isolated.
- Explicit StructType schemas; never inferSchema. CSV reads use `header=true`, `enforceSchema=false`, `multiLine=true`, `mode=PERMISSIVE`.
- Each transform is a pure function: DataFrame in, DataFrame out.

## Layers
- bronze (CSV files: `encounters.csv`, `labs.csv`, `providers.csv`, raw) -> silver (`clean_encounters`, `clean_labs`, `clean_providers`) -> gold (`provider_daily_activity`). Every layer is CSV with a header and is treated as PHI.
- There is no bronze job: `synth/generate.py` (or the supplied data) writes bronze; the pipeline runs silver then gold.
- Schemas, cleaning rules and gold metrics: `specs/spec-patient-history-pipeline/data-model.md`. All schemas and the constants `TIMESTAMP_FORMAT`, `DEFAULT_SALT`, `SENTINELS` live in `pipeline/schemas.py`.
- Put `customer` in every join, grouping and dedupe key: IDs collide across customers.
- Gold excludes cancelled encounters (`cancelled`/`canceled`) and retracted labs; labs count on their encounter's day; gold holds no patient-level field.

## Patient ID
- `hashedpatientid` = HMAC-SHA256 over `customer:patient_id`, both normalized with `(?U)\s` trim and lowercase (Spark's `trim` only removes ASCII spaces). 64 lowercase hex; null for a null ID.
- Salt: `PATIENT_ID_SALT` env var if set, else `DEFAULT_SALT = "regard-demo-salt"`. That constant is the only allowed default secret (interview requirement); never add another.
- Silver sets `patient_id` to null; the raw ID never leaves bronze. No crosswalk table.
- Never put the salt value in EMR Step args, Spark conf, cluster configurations or logs.

## Safety
- Never log, print or put PHI values in exception messages.
- Test data is always synthetic.
- Tests build chained dummy datasets per layer (bronze -> expected clean_* -> expected gold) and check every output with `assert_schema` and `assert_content` from `tests/assertions.py`; they also assert the hash is 64 hex and customer-scoped, `patient_id` is null in silver, and a missing `DATA_ROOT` fails fast.
