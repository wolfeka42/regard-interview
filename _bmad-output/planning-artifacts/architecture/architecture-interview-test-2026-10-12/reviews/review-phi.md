# PHI / HIPAA review: ARCHITECTURE-SPINE.md

- **Target:** `../ARCHITECTURE-SPINE.md` (draft, 2026-10-12)
- **Lens:** `_bmad/custom/phi-review-lens.md`, applied to a design document rather than code
- **Basis:** `docs/hipaa-context.md`, `AGENTS.md`, `_bmad-output/project-context.md`, `specs/spec-patient-history-pipeline/data-model.md`
- **Note on severity:** the lens says not to rank findings. The caller asked for severity, so each finding has one. Treat it as a suggested order for fixing, not a risk rating.

## Verdict

The spine mostly holds up. It keeps identifiers out of gold through AD-4 and AD-5, keeps logs PHI-free through AD-8, and makes the key fail fast with no default through AD-7. It never calls gold de-identified. It still has two design gaps that break policy if built as written:

- `silver/patients` becomes a stored pseudonym-to-MRN crosswalk.
- The HMAC key's route to EMR is left open, and the obvious ways to build it put the key in plain sight.

Three smaller gaps cover Spark's own error text and log storage, how bronze and silver are stored, and supplied data on the interview day. Every fix is a one- or two-line rule change to the spine.

## Findings

### F1 [High] `silver/patients` stores a pseudonym-to-MRN crosswalk

- **Where:** AD-4 (lines 87-91), AD-6 (lines 103-107), first flowchart (line 35: the key feeds silver).
- **Gap:** AD-4 says event datasets "carry `patient_key_v1` ... from" `silver/patients`. That only works if `silver/patients` is written with `facility`, `mrn` and `patient_key_v1` side by side. The result is a stored Parquet table mapping each pseudonym back to its MRN. Lens check 5, `project-context.md` and `hipaa-context.md` all forbid a crosswalk. It also defeats the point of the key: anyone who can read silver can reverse every gold key.
- **Fix:** add this rule to AD-4 and AD-6: "`patient_key_v1` is never written next to `mrn` or `facility`. `silver/patients` keeps identifiers and no key. Each silver event function joins on `(facility, normalize_mrn(mrn))`, computes `patient_key_v1` from those join columns, then drops `mrn` and `facility` before writing." Add a test asserting that `SILVER_PATIENTS` has no `patient_key_v1` field.

### F2 [High] The route for the HMAC key to EMR is open, and the easy builds expose it

- **Where:** AD-7 (line 113), Structural Seed diagram (line 194), Deferred (line 217).
- **Gap:** "Secrets Manager → `PATIENT_KEY_HMAC_SECRET` on the steps" is deferred to story 6 with nothing ruled out. The quick ways to set an environment variable on an EMR Step all expose the key:
  - `spark-submit --conf spark.yarn.appMasterEnv.*` or `spark.executorEnv.*`: shows in the Spark UI Environment tab and the event logs.
  - The key placed in Step `Args`: returned by `DescribeStep`, shown in the console, echoed in step `stderr` and copied to the S3 `LogUri`.
  - Cluster `Configurations` with a `spark-env` export: returned by `DescribeCluster`.

  AD-7 also makes the orchestrator require the key even for `--target emr`. The laptop then holds the key just to submit a cluster, which invites passing it along in the step request. A `conf` object that holds the key also leaks it if it is ever logged or appears in an exception.
- **Fix:** add these rules to AD-7 and take them out of "Deferred":
  - "The key value never appears in `emr.py` request payloads, Step `Args`, Spark conf, cluster Configurations, or any log. On EMR, each Step fetches the key from Secrets Manager by secret **ID** into the process environment, for example a `bash -c` wrapper or `config.py` reading from Secrets Manager. The secret ID may travel in Step `Args`. The value may not."
  - "Executors get the key only through the HMAC UDF's closure, never through `spark.executorEnv`."
  - "`--target emr` checks that the secret ID is set, not the key."
  - "`conf.__repr__` masks the key."
  - Story 6 can keep deciding how to build it.

### F3 [Medium] Spark's own error text and the EMR log bucket are unguarded PHI paths

- **Where:** AD-8 (lines 117-123), AD-9 (line 131), Conventions "Invalid rows" (line 149).
- **Gap:** AD-8 only governs messages our code writes. Spark and Python can put row values in error text:
  - With ANSI mode on, a failed cast reports the value, for example `CAST_INVALID_INPUT ... 'value'`.
  - `FAILFAST` CSV parsing can report the bad record.
  - An exception inside the HMAC UDF returns a Python traceback.

  These messages end up in driver `stderr`, the EMR step logs and the S3 `LogUri`, and the spine names none of them. The Deferred Security Rule bullet covers gold, not the log bucket.
- **Fix:** add to AD-8:
  - Silver types values with `try_cast`, or pins `spark.sql.ansi.enabled=false`, so bad values become nulls that are counted and dropped.
  - CSV reads use `mode=PERMISSIVE`.
  - The HMAC UDF returns null for a null or empty input and never raises with its input in the message.
  - Add to Deferred: "the EMR `LogUri` bucket and Spark event logs are treated as PHI (same encryption and access as the data)."

### F4 [Medium] Storage expectations are named only for gold

- **Where:** Deferred, Security Rule controls (line 218); AD-2 (line 75).
- **Gap:** The Deferred bullet says the controls are "required for gold". `AGENTS.md` treats every layer as PHI. Bronze and `silver/patients` hold the most sensitive fields (SSN, name, DOB, MRN, full ZIP), so encryption at rest and access limits apply to them first. AD-2 also lets `data_root` be any local directory and says nothing about where it lives, so raw PHI output could end up in the repo and be committed.
- **Fix:** change the Deferred bullet to "required for every layer under `data_root` and `raw_root`, plus EMR logs. Bronze and `silver/patients` are the most sensitive." Add to AD-2: "a local `data_root` and `raw_root` sit outside the repo or under a git-ignored path. S3 prefixes use SSE-KMS (named, not built)."

### F5 [Low] The synthetic-only rule doesn't cover data supplied on the day

- **Where:** Conventions, Tests and Synthetic data rows (lines 151-152).
- **Gap:** The spine requires synthetic data for tests and `synth/`. The SPEC also allows "the supplied ... raw data" on the interview day (SPEC line 64). Nothing stops rows from that supplied data being pasted into test fixtures, or used as examples for schema-swap tests.
- **Fix:** add to the Tests convention: "Supplied raw data is read only through `raw_root` and is never copied into `tests/`, `synth/`, fixtures or docs. Fixtures for the day-of schema are generated synthetically."

## Checked, no gap

- **Identifiers kept out of gold:** AD-4 limits what event datasets carry. AD-5 `conform_to_gold` selects only `GOLD_SCHEMA` fields and drops the rest. Gold never imports `patient_key.py`.
- **ZIP3, the 90+ age cap, and `age_at_encounter` computed in silver:** stated in AD-4 and Conventions (lines 146 and 148).
- **HMAC-SHA256, not `sha2`:** AD-6 sets the `facility:normalized_mrn` input, 64 lowercase hex characters and the `_v1` column name. AD-7 has no default key and fails fast before Spark starts.
- **Observability:** AD-8 logs counts and column names only, bans `show()`, `collect()` and `print` for logging, and keeps values out of exception text. Invalid rows are counted, never quarantined with their values (line 149).
- **De-identification wording:** the spine never calls gold de-identified, and it lists gold under the required Security Rule controls.
- **Security Rule controls:** care-team access, read audit logging, KMS at rest and in transit, the AWS BAA and HIPAA-eligible EMR are all named in Deferred. Nothing required is missing except the scope issue in F4.

## Notes (no change required)

- **`encounter_id` in gold:** a source encounter or account number links straight back to the EHR, much as the MRN does. Gold is PHI for treatment, and `AGENTS.md` doesn't list it as forbidden, so keeping it is allowed. The spine should say it is kept on purpose so clinicians can cross-reference the EHR. Then a reviewer won't flag it, and nobody will later treat it as safe outside treatment use.
- **Quasi-identifiers:** `sex`, `state`, `zip3`, `age_at_encounter` and full `event_date` together narrow DOB to a few days and can single people out. That is acceptable because gold is PHI under Security Rule controls. Never cite these columns as a de-identification measure, and don't treat `zip3` as meeting Safe Harbor (Safe Harbor also requires `000` for ZIP3 areas under 20,000 people).
- **`event_value` (`result_value`, `dose`):** source free text can contain names or comments. AD-8 already keeps it out of logs. Silver validity rules shouldn't echo it.
- **Open Question on `event_end_date`:** adding a nullable discharge or end date matches `project-context.md` and raises no PHI concern beyond gold's existing PHI status.
