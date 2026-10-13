# Rubric review: ARCHITECTURE-SPINE.md (Patient History Pipeline)

- Reviewed: 2026-10-12
- Spine: `../ARCHITECTURE-SPINE.md` (status: draft)
- Context read: `SPEC.md`, `data-model.md`, `stories.yaml`, `interview-runbook.md`, `_bmad-output/project-context.md`
- Checklist: good-spine rubric (divergence coverage, enforceable rules, safe deferrals, current tech, capability coverage, altitude dimensions and operational envelope, over-specification for a 25-minute build)

## Verdict

**Pass with fixes.** The spine is a strong consistency contract. AD-2, AD-3, AD-5 and AD-6 pin the main seams between stories, and the stack is verified current. Two things must change before story 1 starts. First, AD-4 contradicts story 1's "no joins" slice and leaves open where the key is computed until patients exist. Second, `conform_to_gold` does not say how missing columns are handled. The operational envelope for AWS is mostly unaddressed, and the EMR work (AD-9 plus per-job entry points) goes beyond the spec's "shown through spark-submit configuration", which costs time in a 25-minute build.

## Findings

### F1 [HIGH] AD-4 vs story 1: the steel thread breaks the "silver derives everything from patients" rule
- Story 1 says "one source, no joins" (encounters only). AD-4 says every silver event dataset is inner-joined to `silver/patients` and carries `patient_key_v1`, `age_at_encounter`, `sex`, `state` and `zip3` **from it**.
- If story 1 follows AD-4, it must build `patients` too, which goes beyond its scope and timebox. If it skips the join, story 1 computes the key directly on encounters, and story 2 has to rewrite silver encounters into the join form. The divergence point is where `patient_key_v1` is computed: on each event table, or once on patients and then carried through the join.
- Fix: add a transitional rule. Either (a) the key is always computed on each event dataset from its own `facility` and `mrn` via `patient_key.py`, and the patients join (story 2+) only adds `age_at_encounter`, `sex`, `state` and `zip3`; or (b) story 1 includes a minimal `patients` source. Option (a) is cheaper and keeps AD-6's single owner. Also state what an inner join does to the story 1 row counts once story 2 lands.

### F2 [HIGH] AD-5: `conform_to_gold` does not say how missing columns are handled
- The rule says it "selects `GOLD_SCHEMA`'s fields in order, casts each". The conventions say "Unmapped fields are null". In stories 1, 3 and 4, an encounter frame has no `event_value` or `event_unit`, and in story 1 there is no `age_at_encounter`, `sex`, `state` or `zip3`. A plain `select` raises on a missing column.
- Each story author will solve this differently: add `lit(None)` in each `gold_<source>`, or make `conform_to_gold` fill the gaps. That is exactly the drift AD-5 exists to prevent.
- Fix: in the Rule, have `conform_to_gold` add any missing `GOLD_SCHEMA` field as a typed null (`lit(None).cast(type)`), and say that each `gold_<source>` only renames to gold names and sets `event_type`.

### F3 [HIGH] Operational envelope for the AWS target is largely undecided and not deferred
AD-9 commits to `emr.py` creating a transient EMR on EC2 cluster, but the spine does not decide, defer or list as open questions the following, which story 6 cannot invent safely:
- **Code delivery:** how the package is zipped for `--py-files`, where it is uploaded (S3 code prefix), and who uploads it (`emr.py`?).
- **Infrastructure and IAM:** the S3 bucket(s) for `data_root`, `raw_root`, code and logs; the EMR service role and EC2 instance profile; VPC/subnet; region; instance types and count. There is no infrastructure-as-code decision, and no statement that the infrastructure is assumed to pre-exist.
- **Environments:** only "local" and "emr" exist. Nothing says whether there is one AWS account or environment, or whether `data_root` is per environment.
- **Logs:** EMR `LogUri` sends step stderr and YARN container logs to S3. These logs live alongside PHI, and Spark task-failure messages can include row content even if application code obeys AD-8. The spine needs a log destination plus an encryption/retention rule, or an explicit deferral.
- **Python on the cluster:** see F4.
- **Dependencies:** `emr.py` needs `boto3`, which is not in the Stack.
- **Failure/rerun:** covered for data by AD-2 (overwrite = idempotent) and for cluster by `TERMINATE_CLUSTER`. That is good enough.
- Fix: add one short Deferred entry ("AWS infrastructure (buckets, IAM roles, VPC, region, instance sizing, log bucket) is assumed pre-provisioned and passed to `emr.py` as env/config; not built") and one AD-level rule on the code artifact location and the log destination. Alternatively, cut AD-9's EMR half as described in F6.

### F4 [MEDIUM] AD-10 / Stack: parity claim is loose
- Verified (AWS EMR 7.13.0 release notes, released 2026-04-21): Spark 3.5.6-amzn-2, Corretto 17 default, Python 3.9 and 3.11 shipped. **Python 3.11 is the default for PySpark only from 7.13**, and other tools (including bootstrap `pip3`) still default to 3.9. A known re:Post issue describes bootstrap `pip3 install` landing in 3.9 site-packages while Spark runs 3.11. If anything is ever pip-installed on the cluster, it can break. Pin `PYSPARK_PYTHON=/usr/bin/python3.11` in the `spark-env` classification or the step conf to make the parity explicit.
- `pyspark==3.5.*` currently resolves to 3.5.9 on PyPI, while EMR runs 3.5.6. That is fine for APIs, but if AD-10 claims "same versions", pin `pyspark==3.5.6`, or soften the rule to "3.5 line".
- pytest 9.1.1 and ruff 0.17.0 are confirmed current on PyPI. Java 17 is fine for Spark 3.5 locally.

### F5 [MEDIUM] Raw-file contract between `synth/generate.py`, bronze and silver is not fixed
- AD-3 fixes column names and types but not the CSV dialect (header row, delimiter, quote/escape, encoding, reader `mode`) or the **date string format** that silver parses (`to_date(col, fmt)`). The generator (CAP-6) and silver typing (story 2+) are built independently and must agree. Changing the date format on the day is also the most likely swap.
- Add a convention row: "Raw CSV: header=true, comma, UTF-8, `mode=PERMISSIVE`; dates ISO `yyyy-MM-dd`; reader options live next to the `RAW_<SOURCE>` schema in `schemas.py`." Also state whether bronze adds lineage columns such as an ingest timestamp (currently implied "no", because the bronze schema equals the raw schema). Saying it explicitly prevents one story from adding them.

### F6 [MEDIUM] Over-specification for a 25-minute build
- **AD-9 emr half and AD-1 per-job `__main__` + `--py-files`:** The spec assumption is "EMR is shown through `spark-submit` configuration", and story 6 is cut first. A boto3 transient-cluster submitter (`emr.py`) is new scope. Recommend changing AD-9 to: `--target emr` prints, or writes, the three `aws emr add-steps`/`spark-submit` commands (or a steps JSON), and the transient-cluster submitter is deferred. The invariant to keep is "each job is runnable alone via `main(spark, conf)`".
- **AD-3 schema count:** separate `RAW_` and `BRONZE_` per source (identical, all strings) plus `SILVER_` for five sources gives up to 15 `StructType`s. Recommend that `RAW_<SOURCE>` doubles as the bronze schema (one constant), and that silver Parquet is read without a re-declared schema (Parquet is self-describing), keeping only `GOLD_SCHEMA` asserted. This cuts typing without losing the "no inferSchema" invariant (Parquet reads do not infer).
- **AD-8 `log.py` with a restricted signature** is fine and small. Keep it.
- **AD-7 "the key is required even for bronze"** is deliberate fail-fast. It is cheap, so keep it.

### F7 [MEDIUM] Open question on discharge/end dates blocks story 1's exact gold-schema test
- This is correctly raised and has a deadline ("before story 1"). However, `GOLD_SCHEMA` is asserted exactly from story 1 onward, and `project-context.md` (the file the coding agent is told to read) says gold keeps discharge dates. If the question is left open, story 1 and a later story will disagree. Recommend deciding now: add nullable `event_end_date` (date), map encounter=`discharge_date` and medication=`end_date`, and update `data-model.md` to match.

### F8 [LOW] AD-6: `normalize_mrn` implementation form and null/empty edge
- "This UDF is the only Python UDF" implies `normalize_mrn` is a Column expression (`trim` + `regexp_replace('^0+', '')`), but the rule does not say so. State "Column expression, not a UDF". Also define the result for an all-zero or empty MRN, and require the HMAC UDF to receive only non-null input (silver validity drops null MRN first). Otherwise two stories can disagree on whether `"000"` gives `""` or an invalid row.
- The UDF must take the key from `conf` (closure) and not from `os.environ` on executors. On EMR, executors do not see the driver's environment. Add one clause: "the key is passed into the UDF factory from `conf`; executors never read env".

### F9 [LOW] Deferred "how the secret reaches EMR" is safe for stories 1-5 but needs one guardrail
- AD-7 fixes what the job sees, so stories 1-5 cannot diverge. Story 6 should, however, be barred from passing the secret as a Step argument. Step args are visible in `DescribeStep` and the console. Spark conf/`appMasterEnv` values are redacted by the default `spark.redaction.regex`, because the name contains "secret", but a step arg is not redacted. Add: "never in Step args".

### F10 [LOW] Smaller items
- **Deferred "Per-source validity rules: set by each story".** This is safe for divergence, because each source owns its rules. But `patients` validity changes the inner-join drop counts of every event source (AD-4). Note that patient validity rules are owned by story 2.
- **Time order:** the spec says doctors need history "in time order". Parquet has no row order, so state that ordering is the reader's `ORDER BY event_date`, and that gold is not sorted. This prevents someone adding `orderBy` (a costly shuffle) or a test that asserts row order.
- **Gold schema location:** AD-3 puts `GOLD_SCHEMA` in `schemas.py`, and the capability map points CAP-4 there. That is consistent.
- **CAP-7** maps to `_bmad/custom/phi-review-lens.md`, which exists. That is fine; it is a process capability, not code.
- **Mermaid module graph:** `__main__` depends on jobs and emr, and `emr.py` imports nothing from jobs. Good. Consider stating "`emr.py` never imports pyspark", so `--target emr` does not need a local Spark.

## Checklist scorecard

| Criterion | Result | Notes |
| --- | --- | --- |
| Fixes the real divergence points, misses none | Partial | Missing: where the key is computed in story 1 (F1), filling missing gold columns (F2), raw dialect/date format (F5) |
| Every AD Rule enforceable and prevents its divergence | Mostly | AD-5 is under-specified (F2); AD-4 conflicts with story 1 (F1); AD-10 parity is loose (F4). Others are testable (schema asserts, import rules, grep for `print`/`show`) |
| Nothing deferred lets two stories diverge | Mostly | Secret delivery and validity rules are safe with the F9/F10 guardrails |
| Tech verified current | Pass | EMR 7.13.0 (2026-04-21): Spark 3.5.6-amzn-2, Corretto 17, Python 3.11 default for PySpark. pytest 9.1.1 and ruff 0.17.0 on PyPI. Pin nuance in F4 |
| Covers every spec capability | Pass | CAP-1..7 all mapped. CAP-2's "invalid rows removed and counted" is covered by AD-8 plus the Invalid-rows convention |
| Altitude dimensions decided/deferred/open (operational envelope) | Fail | Deployment is decided (AD-9). Environments, infrastructure/IAM, code artifact, logs and cluster Python are undecided (F3) |
| No over-specification for a 25-minute build | Partial | AD-9 EMR submitter and schema count (F6) |

## Sources (tech verification)
- AWS, Amazon EMR release 7.13.0: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-7130-release.html
- AWS re:Post, EMR 7.13.0 bootstrap pip3 targets Python 3.9 while Spark runs 3.11: https://repost.aws/questions/QUtGx2Iz0yTOG8OBD6GCrsIg
- PyPI JSON API (pytest 9.1.1, ruff 0.17.0, pyspark 3.5.9 latest 3.5.x), checked 2026-10-12
