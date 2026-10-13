---
id: SPEC-patient-history-pipeline
companions:
  - ../../planning-artifacts/architecture/architecture-interview-test-2026-10-12/ARCHITECTURE-SPINE.md
  - data-model.md
  - architecture-diagrams.md
  - interview-runbook.md
  - ../../project-context.md
  - ../../../docs/hipaa-context.md
sources:
  - ../../planning-artifacts/briefs/brief-interview-test-2026-10-12/brief.md
  - ../../planning-artifacts/briefs/brief-interview-test-2026-10-12/addendum.md
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Patient History Pipeline

## Why

A mandate and a pain. Regard's interview assignment asks for a basic PySpark pipeline that turns raw healthcare data into an analysis-ready table using data engineering and HIPAA best practices, with AI assistance encouraged, built live in one hour. The table serves doctors who review an individual patient's history and need visits, labs, diagnoses and medications in one place, in time order, with exact dates. Raw data is inconsistent, over-exposes identifiers and has no stable patient key. The session is judged equally on a working pipeline and on visible, disciplined AI-assisted development.

## Capabilities

- **CAP-1**
  - **intent:** Raw source files land in bronze unchanged, read under an explicit schema that can be swapped for the data supplied on the day.
  - **success:** Bronze row count equals source row count, and the bronze schema equals the declared `StructType`.
- **CAP-2**
  - **intent:** Silver holds cleaned, typed records with `age_at_encounter` derived from date of birth and service date.
  - **success:** Tests show correct types, correct age at encounter, ages over 89 as 90+, and invalid rows removed and counted.
- **CAP-3**
  - **intent:** Each patient gets a stable pseudonymous key that keeps the MRN out of gold.
  - **success:** Tests show the same `facility:mrn` always yields the same 64-hex key, different facilities yield different keys, a missing key env var fails at startup, a blank or all-zero MRN is dropped and never shares a key, and no silver or gold dataset stores the MRN next to the key.
- **CAP-4**
  - **intent:** Doctors get one gold table holding each patient's clinical events with full service dates and no direct identifiers.
  - **success:** A test asserts the exact gold schema: no name, SSN, MRN, phone, email, street, city or DOB column; ZIP has 3 digits; service, discharge and end dates are full dates. A test asserts each patient's rows are in time order.
- **CAP-5**
  - **intent:** One command runs bronze through gold end to end; the same code runs on AWS EMR.
  - **success:** The command produces the gold table from synthetic data locally, `pytest -q` passes, and the EMR target emits a runnable EMR Steps submission without exposing the key.
- **CAP-6**
  - **intent:** Developers and tests have realistic synthetic raw data in the generic healthcare shape.
  - **success:** The generator writes deterministic synthetic files that the pipeline ingests without schema errors; no real records are used.
- **CAP-7**
  - **intent:** Every build and code review automatically checks changes for PHI leaks and HIPAA gaps.
  - **success:** `bmad-build` and `bmad-code-review` run the PHI/HIPAA reviewer and report findings with `file:line` (configured in `_bmad/custom/`).

## Constraints

- Architecture: `ARCHITECTURE-SPINE.md` decisions AD-1 to AD-10 bind every story; changing one goes through `bmad-architecture`, not a local override.
- PySpark only for data manipulation; no pandas. The only Python UDF allowed is HMAC-SHA256, because Spark's `sha2` is a plain hash.
- Explicit `StructType` schemas, never `inferSchema`; every transform is a pure function, DataFrame in and DataFrame out.
- Gold is PHI for treatment use (45 CFR 164.506), never presented as de-identified. Full rules: `data-model.md`, basis: `docs/hipaa-context.md`.
- The HMAC key comes from an environment variable (AWS Secrets Manager or KMS in production); the pipeline fails at startup without it and has no default key.
- No PHI in logs, prints or exception messages; log counts and column names only. Test data is synthetic only.
- The core build must fit about 25 minutes of a 60-minute live session; no gold table from running code is a failure.

## Non-goals

- A doctor-facing lookup app, or a pseudonym-to-MRN crosswalk table.
- Building production Security Rule controls (care-team access, read audit logging, encryption, AWS BAA); they are named, not built.
- A de-identified or Safe Harbor dataset, or population-level analytics.
- Any UI, streaming ingestion, or cross-facility patient matching (master patient index).

## Success signal

At the end of the 60-minute session, story 1 works: one command turns the supplied or synthetic raw data into the gold table, `pytest -q` passes including the gold-schema, patient-key, blank-MRN and time-order tests, and the PHI review reports no gaps. Stories 2 to 6 are stretch goals.

## Assumptions

- Gold has one row per clinical event (encounter, lab, diagnosis, medication).
- The prepared design uses a generic shape (patients, encounters, labs, diagnoses, medications; CSV by default, JSON allowed); the real data replaces the schema and column mapping on the day.
- Invalid rows are dropped in silver and only their count is logged.
- Spark runs in local mode during the interview; EMR is shown through the printed EMR Steps command, not launched.

## Open Questions

- What format and schema will the supplied raw data have?
- Is the prepared repo allowed into the interview? (Asking the recruiter.)
- Should story 1 (the steel thread) be built on synthetic data before the session, leaving the live hour to swap in the real schema and add stories 2 onward?
