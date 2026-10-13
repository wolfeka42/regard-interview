# Currency Review: Architecture Spine, Patient History Pipeline

- **Reviewed:** `../ARCHITECTURE-SPINE.md` (draft, 2026-10-12) and `../.memlog.md`
- **Review date:** 2026-10-12
- **Lens:** were the committed decisions web-researched or checked against reality, or asserted from training data? Are versions current? Do the named technologies still exist and fit?
- **Method:** web checks against AWS EMR release guides, PyPI JSON, Apache Spark docs and the Spark 4.2 migration guide; inspection of the local `.venv`, the JDK and `project-context.md`.

## Verdict

**Pass with changes.** Every named technology exists and fits, and the EMR 7.13 facts in the spine are accurate. But the version research in the memlog is stale. EMR 7.14.0 (Spark 3.5.8) shipped on 2026-09-08 and is now the current release, and PySpark 4.2.0 and 3.5.9 both shipped in July 2026. The memlog records 4.1.2 / 3.5.8 as the latest versions. The local environment also contradicts AD-10 on all three runtime axes, and the spine gives no step to fix that. Re-pin and add a parity step before story 1.

## Findings

### F1 — HIGH — Stack / AD-10: EMR 7.13.0 is no longer the latest release; 7.14.0 is current

- **Where:** Stack table (`Amazon EMR on EC2 | 7.13.0`), AD-10 title and rule, Structural Seed diagram, memlog line 15.
- **Evidence:** the [EMR 7.14.0 release guide](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-7140-release.html) shows:
  - initial release 2026-09-08
  - Spark **3.5.8-amzn-0** (7.13.0 has 3.5.6-amzn-2)
  - Python 3.9 and 3.11
  - Corretto 17 as the default
  - "no known issues"
- AWS's own CLI examples now use `--release-label emr-7.14.0`.
- The memlog's EMR entry was researched against a release that was superseded five weeks before this run. Nothing records the choice of 7.13 over 7.14.
- **Impact:** low technical risk, because both releases are on the 3.5 line with the same Python and Java. But the spine claims "runtime parity" with a release that is not the current one, and an interviewer can spot that. A new cluster would normally get 7.14.
- **Fix:**
  - Pin `emr-7.14.0` in Stack, AD-10 and the seed diagram, and log a `(version)` memlog entry with the source.
  - Or keep 7.13 and record why (for example, "matches an org-approved AMI").
  - Either way, make the release label a single constant in `pipeline/emr.py`.

### F2 — HIGH — Stack / memlog: PySpark version research is stale, and the local venv contradicts the pin

- **Where:** Stack row `pyspark==3.5.*`, memlog line 14 ("latest stable 4.1.2 (2026-05-21); 3.5.8 on 3.5 line").
- **Evidence:** [PyPI](https://pypi.org/pypi/pyspark/json) shows:
  - **4.2.0** released 2026-07-14, which is the latest. 4.1.3 and 4.0.4 shipped 2026-07-15.
  - **3.5.9** released 2026-07-16, which is the latest 3.5.x. It needs Python >=3.8 and `py4j>=0.10.9.7,<0.10.9.10`.
- The local `.venv` already has **pyspark 4.2.0** installed, so the memlog's "latest = 4.1.2" was never checked against the project either.
- The `3.5.*` pin itself is correct (EMR ships 3.5.x). The finding is that the research it rests on is out of date, and the environment does not follow the pin.
- **Spark 4.2 trap that matters here:** the [PySpark 4.2 migration guide](https://spark.apache.org/docs/4.2.0/api/python/migration_guide/pyspark_upgrade.html) says that in 4.2 `spark.sql.execution.pythonUDF.arrow.enabled` defaults to **true**, so plain `@udf` functions are Arrow-optimized. The venv has no `pyarrow` or `pandas`, and the project bans pandas. If tests run on the existing 4.2.0 venv, the HMAC UDF (AD-6) goes down an Arrow path that EMR's 3.5 never uses: it can fail on a missing dependency or behave differently. This is exactly the drift AD-10 is meant to prevent.
- **Fix:**
  - Pin `pyspark==3.5.8` to match EMR 7.14's 3.5.8-amzn-0, or at least `>=3.5.6,<3.6`.
  - Record PySpark 3.5.9 / 4.2.0 as the checked current versions.
  - Rebuild the venv. A `requirements.txt` or `pyproject.toml` must exist before story 1; none exists today.

### F3 — HIGH — AD-10: local runtime parity is asserted, but nothing gets the environment there

- **Where:** AD-10, Stack (Python 3.11, Java 17); memlog lines 18, 26 and 32.
- **Evidence from the actual machine:**
  - The `.venv` Python is **3.10.22**, not 3.11.
  - `java -version` is **Homebrew OpenJDK 21.0.12**. `/usr/libexec/java_home -V` finds no registered JDK, so no JDK 17 is installed.
  - PySpark is **4.2.0**, not 3.5.x.
- The [Spark 3.5.8 docs](https://spark.apache.org/docs/3.5.8/) confirm that Spark 3.5 supports Java 8/11/17 and Python 3.8+. Java 21 is not supported on 3.5. The memlog notes the JDK mismatch but the spine never turns it into an action.
- **Impact:** the first `pytest -q` will either run against the wrong Spark/Java (giving false confidence) or fail at JVM start. In a 25-minute live build, that costs the interview.
- **Fix:** add a "Local runtime" line to the Stack, or a short setup rule in `project-context.md`, covering:
  - creating the venv with `python3.11`
  - installing Corretto/Temurin 17 and setting `JAVA_HOME` for test runs (for example, a `conftest.py` guard that fails fast if `java -version` is not 17 or `pyspark.__version__` is not 3.5)
  - setting ruff `target-version = "py311"` so ruff flags syntax newer than the cluster supports

### F4 — MEDIUM — AD-6 / AD-7: the HMAC Python UDF has three Spark 3.5 gotchas the spine doesn't address

- **Is the UDF still necessary?** Yes. Open-source Spark has no native HMAC: the local PySpark 4.2.0 exposes `sha`, `sha1`, `sha2`, `aes_*` but no `hmac`. The `hmac()` function you find on the web is [Databricks-only](https://docs.databricks.com/aws/en/pyspark/reference/functions/hmac). Keeping the UDF is correct, but the spine should guard these:
  1. **Executors don't inherit the driver's environment on YARN.** AD-7 reads `PATIENT_KEY_HMAC_SECRET` from the environment on the driver. If the UDF body calls `os.environ` on the executor, it works in local mode (one process) and gets `None` on EMR. That is a local-passes/EMR-fails bug that AD-10 doesn't catch. Rule: read the key once from `conf` on the driver and capture the bytes in the UDF closure (or a broadcast).
     - Corollary: the secret then travels in the pickled task payload. That ties it to AD's "encryption in transit" deferral (`spark.authenticate`, `spark.network.crypto.enabled`, `spark.io.encryption.enabled` on EMR).
  2. **Null inputs.** The UDF receives `None` for a null `facility` or `mrn`, so the rule should require returning `None` rather than raising. A Python exception inside a UDF surfaces as a `PythonException` carrying the worker traceback in driver and executor logs. If the code builds an error message from the value, that is PHI in logs, which breaks AD-8. State that the UDF must never raise on data.
  3. **Arrow path difference.** Spark 3.5 runs plain `@udf` pickled by default. Spark 4.2 runs it Arrow-optimized by default (see F2). Declare the return type explicitly (`StringType()`), and do not pass `useArrow=True`, because that needs pyarrow and pandas.
- **Fix:** add these three lines to AD-6.

### F5 — MEDIUM — AD-9 / AD-1: spark-submit `--py-files` on EMR Steps is plausible but unverified in three details

- **What was confirmed:**
  - The [EMR transient-cluster docs](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-longrunning-transient.html) confirm terminate-after-last-step: `KeepJobFlowAliveWhenNoSteps=false` in `RunJobFlow`, which is the API default, or `--auto-terminate` in the CLI.
  - Steps run sequentially, and `ActionOnFailure=TERMINATE_CLUSTER` is a valid value.
  - The [EMR Spark step docs](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-submit-step.html) confirm `command-runner.jar` + `spark-submit` with local or S3 application paths and a client/cluster deploy-mode choice.
  - [Spark 3.5 docs](https://spark.apache.org/docs/3.5.8/submitting-applications.html) confirm that `--py-files` accepts `.zip`, `.egg` and `.py`.
  - **So the AD-9 design is supported.**
- **Not stated, and worth fixing in the rule:**
  - **Deploy mode.** The spine doesn't say which. Use `--deploy-mode cluster` so the driver runs in YARN. Otherwise the driver runs on the primary node, and logs land in the step logs, where any accidental PHI is more exposed.
  - **Entry point inside the package.** Submitting `pipeline/jobs/bronze.py` as the main file runs it as `__main__` outside its package. It must use absolute imports (`from pipeline import config`) resolved from the `--py-files` zip, never relative imports. A tiny `jobs_entry.py` shim avoids the problem.
  - **The zip is a build artifact.** `emr.py` must zip `pipeline/` and upload it plus the entry file to S3 before `RunJobFlow`. AD-9 implies this but doesn't name it.
  - **Multiple primary nodes.** EMR turns termination protection on automatically for clusters with multiple primary nodes, which overrides auto-terminate. Keep a single primary node.
- **Secret delivery (deferred to story 6).** EMR Steps have no per-step environment, so delivery will go through `--conf spark.yarn.appMasterEnv.*` / `spark.executorEnv.*`. Never pass the value as a step argument, because step args are visible in the console and in `DescribeStep`. Spark's default `spark.redaction.regex` matches "secret", so the variable name is redacted in the Spark UI. Record this constraint now, so story 6 doesn't pick the step-args route.

### F6 — LOW — Stack omits boto3 (or the AWS CLI), which `pipeline/emr.py` needs

- `emr.py` "submits a transient EMR cluster", which needs boto3 (current [1.43.112, Python >=3.10](https://pypi.org/pypi/boto3/json)). It isn't in the Stack, isn't in the venv, and has no pin. Add it, ideally as an optional `[emr]` extra so local runs and tests don't need it.

### F7 — INFO — Confirmed current (no action)

| Item | Spine | Checked | Status |
| --- | --- | --- | --- |
| EMR 7.13.0 Spark | 3.5.6-amzn-2 | [AWS 7.13.0 guide](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-7130-release.html) | Correct (but see F1) |
| EMR 7.13 PySpark Python | 3.11 | 7.13 "What's new": Python 3.11 default for PySpark; 3.9 also present | Correct |
| EMR 7.13 Java | Corretto 17 | "7.13 and higher ships with Amazon Corretto 17 by default" | Correct |
| Spark 3.5 on Python 3.11 / Java 17 | supported | Spark 3.5.8 docs: Java 8/11/17, Python 3.8+ | Correct |
| pytest | 9.1.1 | PyPI latest 9.1.1, Python >=3.10 | Current; matches venv |
| ruff | 0.17.0 | PyPI latest 0.17.0 | Current; matches venv |
| Plain Parquet, no Delta/Iceberg | — | Built into Spark 3.5; no extra jars | Fits |
| Transient cluster + Steps + auto-terminate | AD-9 | AWS management guide | Supported as described |

## Provenance gaps in the memlog

- Line 14 (PySpark latest) is wrong for the run date: the local venv itself has 4.2.0.
- Line 15 doesn't record that a newer EMR release (7.14.0) exists.
- Line 23 says "(web, not all confirmed for MWAA)". That is fine, because MWAA was rejected anyway.
- Lines 31–32 are "user did not object" assumptions that were never checked against the local machine (F3).
- There are no `(version)` entries for pytest, ruff, boto3 or the `--py-files` packaging approach, yet the Stack table states the pytest and ruff versions. They happen to be correct, but they appear to come from the venv rather than from research. Log them with a source.

## Recommended edits (smallest set)

1. Stack: set EMR to `7.14.0` (Spark 3.5.8-amzn-0), and `pyspark==3.5.8`. Add `boto3` (emr extra). Add a "Local runtime" note covering Python 3.11, JDK 17, `JAVA_HOME` and ruff `target-version = "py311"`.
2. AD-6: capture the key in the closure on the driver, never read the environment on executors; return `None` on null input, never raise on data; declare an explicit `StringType` and no Arrow.
3. AD-9: use `--deploy-mode cluster`; the package zip and entry file are uploaded to S3 by `emr.py`; use absolute imports in the job entry points; keep a single primary node; never put the secret in step args.
4. Memlog: add `(version)` entries for EMR 7.14.0, PySpark 3.5.9 / 4.2.0, pytest 9.1.1, ruff 0.17.0 and boto3 1.43.x, each with its source URL.
