# Review: reconcile spine against inputs

- **Spine:** `../ARCHITECTURE-SPINE.md` (draft, 2026-10-12)
- **Inputs checked:** SPEC.md, data-model.md, stories.yaml, interview-runbook.md, architecture-diagrams.md, spec `.memlog.md`, project-context.md, docs/hipaa-context.md, AGENTS.md, brief addendum.md, architecture `.memlog.md`
- **Out of scope (intentional, not flagged):** Security Rule controls, production orchestration, rationale prose.

## Verdict

**Needs revision before story 1.** The spine holds most hard rules (explicit schemas, a single HMAC UDF, fail-fast config, PHI-free logging, gold column set, 90-year cap, full dates). Two decisions conflict with the inputs: story 1 cannot be built under AD-4/AD-5 as written, and silver/patients becomes a stored MRN-to-key crosswalk. The spine also drops several quiet constraints: the 25-minute budget and cut order, no pandas, CSV or JSON input, and what changes in the day-of schema swap.

## Findings

Severity: **High** blocks a story or contradicts a policy rule. **Medium** is a dropped constraint that a builder would reasonably break. **Low** is drift or wording.

### F1 (High): Story 1 cannot be built under AD-4 and AD-5
- **Spine:** AD-4 (lines 85-91), AD-5 (93-97), Consistency Conventions "Gold column types" (146).
- **Input:** stories.yaml story 1 says "one source (encounters), no joins", with "exact gold schema" and `patient_key_v1` tests. The spec memlog says "story 1 ... meets the failure line alone". The runbook says story 1 is never cut.
- **Problem:**
  - AD-4 makes `silver/encounters` take `patient_key_v1` (and age, sex, state, zip3) from an inner join to `silver/patients`, and it drops events with no patient match. With no patients source in story 1, every encounter is dropped, or the key has nowhere to come from.
  - AD-5's `conform_to_gold` *selects* `GOLD_SCHEMA` fields, so it fails on the missing `age_at_encounter`, `sex`, `state` and `zip3` columns. The alternative is a story 1 `GOLD_SCHEMA` that differs from story 2's, which breaks the "exact gold schema" test once story 2 lands.
- **Fix:** add a rule for the steel thread. Silver events compute `patient_key_v1` themselves from their own `facility`/`mrn` through `patient_key.py`. `conform_to_gold` fills absent `GOLD_SCHEMA` fields with typed nulls, so the gold schema is final from story 1. The patients join (story 2) then only adds demographics. If the inner join should apply only once patients exist, say so.

### F2 (High): silver/patients is a stored pseudonym-to-MRN crosswalk
- **Spine:** AD-4 (88-89), AD-6 (103-107).
- **Input:** project-context.md line 23 says "Do not build a pseudonym-to-MRN crosswalk table". hipaa-context.md line 31 says "No crosswalk table is stored". The addendum (line 26) and the SPEC non-goals (line 57) say the same.
- **Problem:** AD-4 makes `silver/patients` the only identifier holder and has event tables take `patient_key_v1` *from* it, so `silver/patients` must persist both the MRN and `patient_key_v1` on one row. That stored Parquet dataset is exactly the forbidden MRN-to-pseudonym mapping, and the spine does not say silver is exempt.
- **Fix:** either (a) join events to patients on `facility` + `normalize_mrn(mrn)`, compute the key on the event side, and never write `patient_key_v1` into `silver/patients`, or (b) record an explicit decision with the user that a key-bearing silver patients table is acceptable because silver is already PHI. As written it silently contradicts policy.

### F3 (Medium): 25-minute budget, cut order and never-cut items are missing, and EMR scope exceeds story 6
- **Spine:** missing throughout. The closest text is AD-9 (125-132) and the Structural Seed (165-182).
- **Input:**
  - SPEC Constraints (line 53): ~25 minutes, and "no gold table from running code is a failure".
  - Runbook lines 13-17: cut order 6, 5, 4, 3, 2. Story 1 and the PHI review are never cut.
  - Architecture memlog line 13 (inherited constraint), and lines 16 and 25, where the user chose the three-job paradigm and per-source functions *because of* the 25-minute cost.
- **Problem:**
  - Nothing in the spine tells a builder that the AD-9 EMR path, `emr.py` and the per-job `__main__` entries belong to story 6 (cut first). Nothing says the minimum story 1 wiring is `config.py` + `schemas.py` + `patient_key.py` + `log.py` + the three jobs with `--target local`.
  - AD-9 has `emr.py` launching a transient EMR on EC2 cluster (boto3 RunJobFlow, Steps, auto-terminate). The inputs ask for less: SPEC Assumptions line 71 says "EMR is shown through `spark-submit` configuration", and story 6 says "Add spark-submit configuration". boto3 is not in Stack.
- **Fix:**
  - Add a short "Build order and cuts" note: story 1 needs the local path only; `--target emr` and `emr.py` are story 6 and cut first; the PHI review is never cut.
  - Either reduce AD-9's emr target to the spark-submit/Step configuration, or record that the user accepted the larger scope.

### F4 (Medium): Day-of schema swap boundary undefined, and the input format narrowed to CSV
- **Spine:** AD-3 (77-81, "Binds: the day-of schema swap"), Conventions "Source names" (144), "Synthetic data" (152), Paradigm diagram (29).
- **Input:**
  - SPEC Assumptions line 69: "patients, encounters, labs, diagnoses, medications as CSV or JSON; the real data replaces the schema and column mapping on the day".
  - architecture-diagrams.md line 7: "Raw files CSV / JSON".
  - Story 1 `invoke_dev_with`: one swappable StructType module.
- **Problem:**
  - The spine fixes `{raw_root}/{source}.csv` and five fixed source names. It never says JSON input is allowed or how the reader is chosen.
  - It does not say which spine elements are expected to change on the day and which stay fixed. Swappable: `RAW_*`/`BRONZE_*`/`SILVER_*` schemas, the source list, and the event-to-gold mapping table in Conventions. Fixed: `GOLD_SCHEMA`, AD-4, AD-6.
  - The event-to-gold mapping lives only in a conventions table, not in code (`schemas.py` or `gold.py`). The day-of course correction will therefore edit several places.
- **Fix:** state the raw format rule (CSV or JSON, chosen per source, read under the declared raw schema). Add one line to AD-3 naming what the swap may change and what stays invariant.

### F5 (Medium): "PySpark only, no pandas" dropped
- **Spine:** AD-6 (105) says the HMAC UDF is "the only Python UDF", and AD-10 covers APIs. Neither mentions pandas.
- **Input:** SPEC Constraints line 48, project-context.md line 9, addendum line 6, architecture memlog line 9 (inherited constraint).
- **Problem:** nothing forbids `toPandas()`, `pandas_udf`, or pandas in `synth/generate.py` and the tests. A pandas UDF is arguably not a "Python UDF" under AD-6's wording.
- **Fix:** add "no pandas anywhere (including `pandas_udf`, `toPandas`, and the generator)" to AD-6 or Conventions/Transforms.

### F6 (Medium): Spine status does not match memlog confirmation status
- **Spine:** AD-4 (83) is tagged [ADOPTED]. AD-3 (77) has no tag. Deferred (216).
- **Memlog:**
  - Line 35: the inner-join drop of unmatched events was "Proposed by architect", with no user confirmation recorded, yet AD-4 is ADOPTED.
  - Line 34: "Bronze/raw schemas all StringType ... not yet confirmed". AD-3 rightly lacks ADOPTED, but it also bundles the user-confirmed decision from line 22 (declared schemas per layer, gold test against the constant). That decision now reads as unadopted.
  - Line 24: the user said "no Step Functions/MWAA, **not even as deferred production plan**". Spine Deferred line 216 names "Step Functions, Airflow".
  - Lines 31-32: the local single-process run, TERMINATE_CLUSTER and Python 3.11/Java 17 were "user did not object" and are shown as ADOPTED (AD-9, AD-10). That is acceptable, but worth a confirmation pass.
- **Fix:** split AD-3 into an adopted part (declared schemas) and a proposed part (all-String bronze). Mark the AD-4 inner-join bullet as proposed, or confirm it with the user. Reword Deferred line 216 to drop the Step Functions/Airflow naming, per the user's decision.

### F7 (Low): Required test list not carried into the spine
- **Spine:** Conventions "Tests" (151) is generic.
- **Input:** data-model.md "Required tests" (lines 43-48), project-context.md line 28, story 5 ("no row values appear in logs or errors"), CAP-1 to CAP-4 success lines.
- **Problem:** the companions carry these tests, so this is not strictly lost. Still, the spine does not tie any AD to the specific tests: ZIP3 truncation, the 90 cap, the differs-by-facility key test, missing-key fail-fast, bronze count equals source, silver invalid count, and no PHI in logs or exceptions. In particular AD-8 has no test hook, so story 5's "no values in logs/errors" test has no named place to capture logs.
- **Fix:** add a "Verified by" line per AD, or one Conventions row that lists the required tests and points to data-model.md.

### F8 (Low): ZIP3 derivation rule is implicit
- **Spine:** Conventions (146) says only "zip3 string (keeps leading zeros)". AD-4 says silver derives it.
- **Input:** data-model.md line 31: "ZIP cut to 3 digits". CAP-4: "ZIP has 3 digits".
- **Problem:** there is no rule for ZIP+4, numeric-typed ZIPs that have already lost leading zeros (bronze all-String helps, but only if F6 is confirmed), or null/short ZIPs (invalid row, or null zip3?).
- **Fix:** add one line: the first 3 characters of a 5-digit string ZIP, a malformed ZIP gives a null `zip3` (or an invalid row), and silver owns it.

### F9 (Low): Local environment change not recorded as a prerequisite
- **Spine:** Stack (156-163), AD-10.
- **Memlog:** line 18 notes the local JDK is 21, and line 26 notes the local `.venv` is Python 3.10.
- **Problem:** AD-10 requires Java 17 and Python 3.11 locally, which means rebuilding the venv and switching the JDK before the session. If this is missed, Spark 3.5 fails on Java 21 during the live build.
- **Fix:** add a "pre-session setup" line (or a story 1 precondition): Python 3.11 venv, `JAVA_HOME` on 17.

## Checked and landed (no action)

- Explicit StructType and no inferSchema (AD-3). Pure transforms (Conventions).
- A single HMAC-SHA256 UDF over `facility:normalized mrn`, 64 hex characters, `patient_key_v1` (AD-6). Key rotation deferred to v2 (Deferred).
- Fail-fast with no default key, required at startup (AD-7).
- No PHI in logs, prints or exceptions; counts and column names only (AD-8).
- Gold forbidden columns: silver/patients is the only identifier holder, and conform_to_gold drops extras (AD-4, AD-5).
- `age_at_encounter` is computed in silver and capped at 90 (AD-4, Conventions).
- Full service dates are kept in gold, and gold is not described as de-identified anywhere.
- Synthetic-only test data and a fixed-seed generator (Conventions, CAP-6).
- One command, `pytest -q`, ruff (AD-9, Stack).
- Discharge and end dates are raised as an Open Question (consistent with memlog line 37).
- Security Rule controls are named and deferred, as intended.
