# interview-test: provider daily activity

PySpark pipeline that answers "How busy was each provider on each day?" across
encounters, labs and providers from several hospital customers. Runs locally and is
built for AWS EMR 7.14 (Spark 3.5.8, Python 3.11, Java 17).

```
{DATA_ROOT}/bronze/{encounters,labs,providers}.csv        raw, written by synth/generate.py
  -> silver: {DATA_ROOT}/silver/clean_{encounters,labs,providers}.csv
  -> gold:   {DATA_ROOT}/gold/provider_daily_activity.csv
```

Every layer is treated as PHI. Silver replaces `patient_id` with
`hashedpatientid` = HMAC-SHA256(salt, `customer:patient_id`); the salt comes from
`PATIENT_ID_SALT`, else the interview's `DEFAULT_SALT`. All data in this repo is
synthetic. Schemas and rules: `_bmad-output/specs/spec-patient-history-pipeline/data-model.md`.

## Results

- [Test results](reports/test-results.md): run time, commit, pass/fail totals, one row per test.
- [Pipeline output](reports/pipeline-output.md): row counts per layer and every silver and gold table.

Regenerate both (runs the tests and the pipeline on the synthetic data):

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17 DATA_ROOT=data/lake
.venv/bin/python -m synth.generate
.venv/bin/python -m report
```

## Commands

```bash
.venv/bin/ruff format --check . && .venv/bin/ruff check .
JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/pytest -q
export JAVA_HOME=/opt/homebrew/opt/openjdk@17 DATA_ROOT=data/lake
.venv/bin/python -m synth.generate && .venv/bin/python -m pipeline run --target local
```

`DATA_ROOT` is required; without it the pipeline exits with code 2 before starting Spark.
