---
title: 'Sprint Change Proposal: provider daily activity replaces patient history'
date: '2026-10-14'
status: approved
approved: '2026-10-14'
mode: batch
scope: major
trigger: actual interview prompt received (step 3 of interview-runbook.md)
baseline_commit: '665ae5b'
---

# Sprint Change Proposal: provider daily activity

## 1. Issue summary

**Type:** a new requirement from stakeholders. The actual interview prompt replaced the prepared assumption.

The plan was built on a guessed product: one patient's history for doctors, drawn from patients, encounters, labs, diagnoses and medications, with `facility:mrn` keys. The real prompt asks a different question, **"How busy was each provider on each day?"** It covers three sources from several hospital customers and fixes the silver and gold schemas. This is the data review and course correction planned as step 3 of `interview-runbook.md`, with a budget of 15 to 20 minutes.

### Source requirements, as received

- Raw sources:
  - `encounters` (patient visits with a provider);
  - `labs` (results linked to encounters);
  - `providers`.
- Each source has a `customer` column naming the hospital system. ID formats differ by customer: some use integers, some UUIDs.
- **Silver** produces `clean_encounters`, `clean_labs` and `clean_providers`. It must:
  - deduplicate all tables;
  - clean sentinel values;
  - fix inconsistent formatting, for example `test_name` with stray spaces and mixed case;
  - keep missing optional fields as null, for example an empty `specialty`;
  - de-identify PHI. PHI fields are hashed between bronze and silver.
- Explicit schemas on CSV load, never `inferSchema`. All three sources have quality issues. Outputs are `.csv`.
- Constants, both used in silver:
  - `DEFAULT_SALT = "regard-demo-salt"` for hashing;
  - `TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX"` for parsing.
- An encounter can have more than one provider; it counts once for each provider.
- How busy a provider is: the encounters they saw and the labs run on the encounter day. Labs with `status = 'retracted'` are excluded.
- Test datasets: `encounters.csv` with columns `customer, encounter_id, patient_id, provider_id, start_time, end_time, updated_at, status`; `labs.csv`; and `providers.csv`.

Silver schemas:

| clean_encounters | | clean_labs | | clean_providers | |
|---|---|---|---|---|---|
| customer | string | customer | string | customer | string |
| encounter_id | string | lab_id | string | provider_id | string |
| patient_id | string | encounter_id | string | provider_name | string |
| provider_id | string | test_name | string | specialty | string |
| start_time | timestamp | result_value | double | active_flag | boolean |
| end_time | timestamp | result_unit | string | partition_timestamp | timestamp |
| status | string | observed_time | timestamp | | |
| duration_days | double | status | string | | |
| hashedpatientid | string | partition_timestamp | timestamp | | |
| partition_timestamp | timestamp | | | | |

Gold schema: `customer` string, `provider_id` string, `encounter_date` date, `provider_name` string, `specialty` string, `active_flag` boolean, `encounter_count` long, `unique_patients` long, `totaldurationdays` double, `lab_count` long.

### Decisions made in this session (Kailey)

| # | Decision |
|---|---|
| D1 | `clean_encounters.patient_id` keeps its column but is always null, so the raw patient ID never leaves bronze. |
| D2 | `lab_count` counts labs whose status is not `retracted`, null status included (`IS DISTINCT FROM`). The retracted filter applies only to `lab_count`. |
| D3 | A lab counts on its **encounter's** start date. It reaches a provider through `encounter_id`, because `clean_labs` has no provider column, and it never drops out because its own date differs. |
| D4 | An encounter with several providers arrives as **one raw row per provider**. The encounter identity is `(customer, encounter_id, provider_id)`. |
| D5 | `hashedpatientid` = HMAC-SHA256 over `customer:patient_id`, keyed by the salt. An env var can override the salt; `DEFAULT_SALT` is the fallback. Including `customer` stops integer ID 123 at two hospitals from sharing a hash. |
| D6 | Gold follows the pasted schema. `patients_seen` is dropped; the metrics are `encounter_count`, `unique_patients`, `totaldurationdays` and `lab_count`. |
| D7 | **Bronze is CSV.** The bronze layer is the three synthetic CSV files `encounters.csv`, `labs.csv` and `providers.csv` under `{DATA_ROOT}/bronze/`, written by `synth/generate.py`, or the supplied files dropped in their place. The separate raw-to-bronze job is retired. The pipeline runs silver, then gold. `RAW_ROOT` is dropped. |
| D8 | **Cancelled visits don't count.** Silver keeps them with their `status`. Gold leaves out encounters whose normalized status is `cancelled` or `canceled` from every metric, and their labs with them. |
| D9 | **Layer tests:**<br>• Dummy datasets for each layer are built in memory from the output schemas, chained so that one layer's expected output is the next layer's input: dummy bronze → expected `clean_*` → expected `provider_daily_activity`.<br>• Reusable check functions in `tests/assertions.py`: `assert_schema(df, schema)` checks the exact column names, types and order, and `assert_content(df, expected_rows)` is an exact, order-independent row match.<br>• Every silver and gold table is checked with both. |

### Defaults the prompt leaves open (override any of them)

| # | Default |
|---|---|
| F1 | `customer` is joined into every join, grouping and deduplication key, because IDs collide across customers. |
| F2 | IDs and `customer` are trimmed with the Unicode-aware `(?U)\s`. UUIDs and `customer` are lowercased; integer IDs keep their leading zeros. `patient_id` is normalized the same way before hashing. |
| F3 | Deduplication: (1) exact duplicate rows are dropped; (2) encounters keep the latest `updated_at` per `(customer, encounter_id, provider_id)`; (3) labs keep one row per `(customer, lab_id)` and providers one per `(customer, provider_id)`, picked by a deterministic ordering, because no `updated_at` column was specified for them. Raw `labs.csv` and `providers.csv` use the silver columns minus `partition_timestamp`. |
| F4 | One shared sentinel list in `schemas.py` (`""`, `NULL`, `N/A`, `NA`, `None`, `-`, `?`, `unknown`, case-insensitive) becomes null in every string field. It is extended after a look at the real files. |
| F5 | `test_name`: trimmed, inner whitespace collapsed to one space, lowercased. `status`: trimmed and lowercased, so `'Retracted '` matches. |
| F6 | `active_flag`: `true/false/t/f/1/0/y/n/yes/no`, case-insensitive; anything else is null. `result_value`: a non-numeric value is null and the row is kept. |
| F7 | Timestamps are parsed with `TIMESTAMP_FORMAT` in UTC; an unparseable value is null. `duration_days = (end_time - start_time) / 86400 s`; it is null when either end is null or when end is before start. |
| F8 | Required fields; rows missing one are dropped and counted:<br>• encounters: `customer`, `encounter_id`, `provider_id`, `start_time`;<br>• labs: `customer`, `lab_id`, `encounter_id`;<br>• providers: `customer`, `provider_id`.<br>A null `patient_id` keeps the encounter, with a null hash. |
| F9 | `partition_timestamp` is the job's run time, captured once per run, the same value on every row. |
| F10 | Gold metrics per `(customer, provider_id, encounter_date)`:<br>• `encounter_date = to_date(start_time)`;<br>• `encounter_count = count(DISTINCT encounter_id)`;<br>• `unique_patients = count(DISTINCT hashedpatientid)`;<br>• `totaldurationdays` = sum of `duration_days`, 0.0 when none;<br>• `lab_count` = labs that aren't retracted for those encounters, 0 when none.<br>Encounters are reduced to one row per `(customer, encounter_id, provider_id)` before the lab join, so labs aren't counted twice. Providers are left-joined, so activity for a provider missing from `providers.csv` keeps null provider fields instead of disappearing. |
| F11 | Superseded by D8. Every other encounter status counts. |
| F12 | Every layer is CSV with a header. Bronze is read under explicit all-string schemas. Silver and gold write timestamps in `TIMESTAMP_FORMAT`. Each read declares its schema. |
| F13 | Gold is sorted by `customer, provider_id, encounter_date`. |

## 2. Impact analysis

**Story impact:**
- **Story 1 (done, `665ae5b`)** is superseded but not reverted. Its scaffolding is reused:
  - the jobs with a storage handoff, `run --target local`, fail-fast config, counts-only logging and the Spark settings;
  - explicit-schema CSV reads, the HMAC function, the test fixture and the synth generator.
  - Replaced: the facility and MRN logic, the bronze job, the gold patient-history output and their tests.
- **Stories 2 (age and location), 3 (labs) and 4 (diagnoses and medications)** are obsolete as written. Labs comes back in a new form.
- **Stories 5 (bad input without PHI) and 6 (EMR)** carry over, renumbered as 3 and 4.

**Artifacts to change:**
- `SPEC.md`, `data-model.md` and `stories.yaml`;
- `ARCHITECTURE-SPINE.md`, AD-2 to AD-7 and AD-11, plus the conventions table;
- `project-context.md`, `phi-review-lens.md`, `AGENTS.md` and `docs/hipaa-context.md`;
- `deferred-work.md` and the runbook's never-cut line.
- There is no PRD, epics or UX file; the spec route stands in for them.

**Technical impact:**
- New and changed code: `schemas.py` (3 bronze schemas, 3 silver schemas, the gold schema, the sentinel list and both constants), `patient_key.py` (renamed to `hashing.py`), `config.py` (salt with a fallback, `RAW_ROOT` removed), `jobs/silver.py` and `jobs/gold.py`, `synth/generate.py` (writes the bronze CSVs), and the tests, including the new `tests/assertions.py` and the per-layer dummy datasets.
- Removed: `jobs/bronze.py`. Mostly unchanged: `log.py`, `spark.py`, and `__main__.py`, which now runs two jobs.

**Compliance conflicts this change creates:**
1. **A hardcoded salt conflicts with AD-7 and with check 4 of the PHI review checklist** ("no default key"). Anyone with the repo can brute-force small integer patient IDs from the hash.
   - Resolution: the interview mandates `DEFAULT_SALT`. It lives in one constant, and an env var overrides it.
   - The PHI review checklist (`phi-review-lens.md`) accepts that one fallback and still flags any other default or hardcoded secret.
   - Production uses Secrets Manager. The docs state the risk.
2. **Gold no longer holds patient-level data.** It's aggregates per provider and day. It stays treated as PHI, because dates and small counts can still single someone out, and `AGENTS.md` says every layer is PHI. Provider names are not patient PHI and are kept.

## 3. Recommended approach

**Direct adjustment on top of story 1's scaffold, with a rewritten plan. Story 1's commit is not reverted.**

| Option | Verdict |
|---|---|
| Direct adjustment only | Not enough: the product, sources and gold all change. |
| Roll back `665ae5b` | No: most of the scaffold carries over, and a revert throws away tested code. |
| Re-plan the MVP | Yes, together with the adjustment: rewrite the spec and stories, keep the code scaffold. |

- **Effort:** medium. The new story 1 is about the size of the old story 1, with one more source join.
- **Risk:** medium-high **on time**. The build window is 20 to 25 minutes, so the new story 1 is cut to produce the full gold schema first, and data quality comes in story 2.
- **Cut order:** 4 (EMR), 3, 2. Story 1 and the PHI review are never cut.

## 4. Detailed change proposals

### 4.1 `stories.yaml`, replacing the whole file

OLD: 6 stories. 1: steel thread, patient history. 2: age and location. 3: labs. 4: diagnoses and medications. 5: bad input without PHI. 6: EMR.

NEW:

```yaml
- id: "1"
  title: Steel thread - gold shows how busy each provider was each day
  description: >-
    synth/generate.py writes the bronze layer as CSV (encounters.csv, labs.csv, providers.csv
    under DATA_ROOT/bronze/); one command runs silver (clean_encounters, clean_labs,
    clean_providers) then gold (provider_daily_activity), both written as CSV. Silver parses
    TIMESTAMP_FORMAT, computes duration_days, nulls patient_id and adds
    hashedpatientid = HMAC(salt, customer:patient_id). Gold aggregates encounter_count,
    unique_patients, totaldurationdays and non-retracted lab_count per customer, provider and
    encounter day, excluding cancelled encounters, with provider fields joined. Tests build dummy
    datasets per layer from the output schemas (bronze -> expected clean_* -> expected gold) and
    check every output table with assert_schema and assert_content in tests/assertions.py. They
    also assert the hash is deterministic, 64-hex and customer-scoped, patient_id is null in
    silver, a multi-provider encounter counts for each provider, cancelled encounters and
    retracted labs are excluded, a lab counts on its encounter's day, and a missing DATA_ROOT
    fails at startup.
  spec_checkpoint: true
  done_checkpoint: true
  invoke_dev_with: >-
    Reuse the story 1 scaffold from 665ae5b (jobs, config, log, spark, CLI, HMAC, fixtures); retire
    the bronze job and RAW_ROOT; replace the facility/MRN and patient-history code. Keep all schemas and both constants in schemas.py.
    Reduce encounters to one row per (customer, encounter_id, provider_id) before the lab join.
- id: "2"
  title: Silver cleans every source's quality issues
  description: >-
    Dedupe all three sources (encounters by latest updated_at per customer, encounter_id,
    provider_id); turn shared sentinel values into null; normalize test_name and status; keep empty
    specialty as null; parse active_flag and result_value; drop and count rows missing required
    fields. Synthetic data gains each issue, and tests assert each rule and the dropped counts.
- id: "3"
  title: Bad input is handled without leaking PHI
  description: >-
    Malformed bronze CSV rows and forced Spark failures never put row values in logs or errors;
    malformed rows are counted. Carries over the old story 5.
- id: "4"
  title: The same pipeline is ready to run on AWS EMR
  description: >-
    Unchanged from the old story 6, except that the secret is the hashing salt: only a secret ID may
    appear in the printed command, never its value.
```

Rationale: the steel thread produces the full gold schema first, as the runbook requires; quality rules follow.

### 4.2 `SPEC.md`

**Title and Why.** OLD: "Patient History Pipeline"; doctors review an individual patient's history. NEW:

> # Provider Daily Activity Pipeline
> ## Why
> Regard's interview assignment: a PySpark pipeline that turns raw clinical data from several hospital customers into clean, aggregated metrics answering "How busy was each provider on each day?", using data engineering and HIPAA best practices, built live with AI assistance. Raw data has duplicates, sentinel values, inconsistent formatting and mixed ID formats per customer, and patient IDs must be de-identified before silver.

**Capabilities.** OLD: CAP-1 to CAP-7 (bronze ingest, silver and age, patient key, gold history, one command and EMR, synthetic data, PHI review). NEW:

- **CAP-1:** The bronze layer is `encounters.csv`, `labs.csv` and `providers.csv` under `{DATA_ROOT}/bronze/`, read by silver under explicit all-string schemas. *Success:* silver's `rows_in` equals each file's row count.
- **CAP-2:** Silver writes `clean_encounters`, `clean_labs` and `clean_providers` in the schemas from section 1, deduplicated, with sentinels nulled, formatting normalized and optional fields kept as null. *Success:* exact schemas, each cleaning rule tested, invalid rows dropped and counted.
- **CAP-3:** Patient IDs are de-identified between bronze and silver. *Success:* `hashedpatientid` is a deterministic 64-hex HMAC of `customer:patient_id`; the same ID at two customers hashes differently; `patient_id` is null in every silver row.
- **CAP-4:** Gold `provider_daily_activity` answers how busy each provider was each day. *Success:* exact gold schema; correct `encounter_count`, `unique_patients`, `totaldurationdays` and `lab_count`; cancelled encounters and retracted labs excluded; a multi-provider encounter counted for each provider; labs counted on their encounter's day.
- **CAP-5:** Unchanged. One command runs locally, and the EMR target prints the submission without the secret.
- **CAP-6:** `synth/generate.py` writes the bronze CSVs, with every quality issue above; unit tests use dummy datasets per layer built from the output schemas and checked by `assert_schema` and `assert_content`. No real records are used.
- **CAP-7:** Unchanged.

**Constraints.** The changed bullets:
- OLD: "The only Python UDF allowed is HMAC-SHA256". NEW: unchanged.
- OLD: "Gold is PHI for treatment use…". NEW: "Every layer is treated as PHI. Gold holds per-provider daily aggregates with no patient-level field; silver holds `hashedpatientid`, never a raw `patient_id`."
- OLD: "The HMAC key comes from an environment variable…; no default key". NEW: "The hashing salt comes from `PATIENT_ID_SALT` if set, else `DEFAULT_SALT = "regard-demo-salt"` as the prompt requires. This is a known demo weakness; production uses Secrets Manager."
- Added: "Timestamps are parsed with `TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX"`. Silver and gold are written as CSV."

**Non-goals.** OLD: lists a doctor-facing app and a pseudonym-to-MRN crosswalk. NEW: removes the doctor-facing app; keeps "no crosswalk from hash to patient_id", no UI, no streaming; adds "no cross-customer provider or patient matching".

**Success signal.** NEW: "Story 1 works: one command writes `provider_daily_activity` from synthetic data, `pytest -q` passes (schema and content checks for each layer, hash, cancelled, retracted, multi-provider, lab day, fail-fast), and the PHI review reports no gaps. Stories 2 to 4 are stretch."

**Assumptions and open questions.** Replaced with D1 to D6 and F1 to F13 from section 1. Old open question 1 (data format) is answered: CSV.

### 4.3 `data-model.md`, replacing the whole file

NEW content:
- **Layers:** bronze is CSV, raw strings (PHI); silver holds the `clean_*` tables, de-identified; gold holds `provider_daily_activity`.
- **Bronze CSV columns:**
  - `encounters`: as received;
  - `labs`: `customer, lab_id, encounter_id, test_name, result_value, result_unit, observed_time, status`;
  - `providers`: `customer, provider_id, provider_name, specialty, active_flag`.
- **Silver and gold schemas:** from section 1.
- **Hashing rule:** D5 and F2.
- **Metric rules:** F10.
- **Required tests:** the CAP-2 to CAP-4 success items.

### 4.4 `ARCHITECTURE-SPINE.md`

| AD | OLD | NEW |
|---|---|---|
| AD-1 | Three jobs: bronze, silver, gold | Two jobs, silver and gold, each with `main(spark, conf)` and a `__main__` entry. Bronze is files, not a job: `synth/generate.py` or the supplied data writes it. |
| AD-2 | Every layer is Parquet; gold at `gold/patient_history/` | Every layer is CSV with a header. Bronze is at `bronze/{source}.csv`, silver at `silver/clean_{source}/` and gold at `gold/provider_daily_activity/`. Silver and gold writes use overwrite. |
| AD-3 | `RAW_`, `SILVER_`, `GOLD_SCHEMA`, `DATE_FORMAT`; only dates and age change type | `RAW_*` and `CLEAN_*` for each of the 3 sources, `GOLD_SCHEMA`, `TIMESTAMP_FORMAT`, `DEFAULT_SALT` and `SENTINELS`. Silver types timestamps, `result_value`, `active_flag` and `duration_days`. |
| AD-4 | `silver/patients` alone holds identifiers; gold derives nothing | Silver derives `duration_days`, `hashedpatientid` and `partition_timestamp`, and nulls `patient_id`. Gold only joins, aggregates and casts. `patients.py` is removed. |
| AD-5 | Every source passes through `conform_to_gold` | `conform_to_gold` is kept as the single select-and-cast step onto `GOLD_SCHEMA`. A matching `conform_to(schema)` is added for each silver output. |
| AD-6 | `patient_key.py`: facility and MRN normalization, `patient_key_v1` | `hashing.py`: `normalize_id`, `make_hash_udf(salt)`, `with_hashed_patient(df)` over `customer:patient_id` → `hashedpatientid`. The other rules (built on the driver, null in null out, never raises, the only UDF) are kept. |
| AD-7 | Key env var required, no default | `DATA_ROOT` is required; `RAW_ROOT` is removed. `PATIENT_ID_SALT` is optional, falling back to `DEFAULT_SALT`. The secret is masked in `repr` and never logged or put in EMR args. |
| AD-11 | Sort by `patient_key_v1`, `event_date`, `event_type`, `event_code` | `GOLD_SORT_KEYS = customer, provider_id, encounter_date`; `order_gold` is kept. |
| Conventions | 5 sources; event types; event mapping; MRN tests | 3 sources. Silver tables are named `clean_{source}`. The event mapping is replaced by the F10 metric rules and D8. Tests: chained dummy datasets per layer (D9), and every output table is checked with `assert_schema` and `assert_content`. |

AD-8 and AD-10 are unchanged. AD-9 is unchanged except that local mode runs silver, then gold, and EMR prints two Steps.

### 4.5 Policy and review files

- **`AGENTS.md` Policy:**
  - OLD: "gold keeps full service dates for treatment use…" plus "Gold drops names, SSN, MRN…; use `age_at_encounter` and an HMAC-SHA256 patient key". NEW: "Treat every layer as PHI. Silver never holds a raw `patient_id`, only `hashedpatientid` (HMAC-SHA256 of `customer:patient_id`). Gold holds per-provider daily aggregates and no patient-level field."
  - The other bullets are unchanged.
- **`project-context.md`:**
  - The Layers and Patient ID sections are rewritten to match D1, D5, F2, AD-4 and AD-6.
  - "never fall back to a default key" becomes "the only fallback is `DEFAULT_SALT` in `schemas.py`".
  - The Safety test list is updated.
- **`phi-review-lens.md`:**
  - Checks 1 to 3 become: "gold has no patient-level column; silver `patient_id` is null; no raw patient ID after bronze".
  - Check 4 accepts the single `DEFAULT_SALT` fallback and flags any other default or hardcoded secret.
  - Check 5 requires `customer:` in the hash input and no crosswalk.
  - Check 8's test list is updated.
- **`docs/hipaa-context.md`:** a short section is added. Gold is aggregates per provider and day and is still handled as PHI, because of dates and small counts. A salted hash of small integer IDs with a public salt can be brute-forced, so production keeps the salt in Secrets Manager.
- **`deferred-work.md`:**
  - Removed as obsolete: facility whitespace, and the encounter validity item, which story 2's deduplication now covers.
  - Kept: non-atomic overwrite, uncounted malformed rows, Spark exception text.
- **`interview-runbook.md`:** the never-cut line becomes "story 1 (gold table; schema and content checks for each layer, hash, cancelled, retracted and multi-provider tests) and the PHI review".

## 5. Implementation handoff

**Scope: major.** The product, sources and gold all change.

| Step | Who / skill | Deliverable |
|---|---|---|
| 1 | This session, after approval | Apply 4.1 to 4.5 to the planning and policy files; commit the docs on `bob-the-builder`, no push |
| 2 | `bmad-build`, new story 1 | Steel-thread code and tests on the existing scaffold |
| 3 | `bmad-build`, stories 2 to 4 | In cut order, as time allows |

Running `bmad-spec` and `bmad-architecture` separately would cost time the session doesn't have, and the exact text is already above. Applying it directly in step 1 replaces them.

**Success criteria:**
- The planning files agree with D1 to D6 and F1 to F13.
- The new story 1 ships `provider_daily_activity` with its tests passing.
- The PHI review reports no gaps, accepting the documented `DEFAULT_SALT` fallback.
