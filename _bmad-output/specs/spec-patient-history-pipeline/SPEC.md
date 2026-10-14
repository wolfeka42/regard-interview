---
id: SPEC-provider-daily-activity-pipeline
companions:
  - ../../planning-artifacts/architecture/architecture-interview-test-2026-10-12/ARCHITECTURE-SPINE.md
  - data-model.md
  - architecture-diagrams.md
  - interview-runbook.md
  - ../../project-context.md
  - ../../../docs/hipaa-context.md
sources:
  - ../../planning-artifacts/sprint-change-proposal-2026-10-14.md
  - ../../planning-artifacts/briefs/brief-interview-test-2026-10-12/brief.md
  - ../../planning-artifacts/briefs/brief-interview-test-2026-10-12/addendum.md
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete contract for what to build, test, and validate. Rewritten 2026-10-14 from the actual interview prompt (`sprint-change-proposal-2026-10-14.md`, decisions D1 to D9 and F1 to F13); the folder keeps its original name so references stay valid.

# Provider Daily Activity Pipeline

## Why

Regard's interview assignment: a PySpark pipeline that turns raw clinical data from several hospital customers into clean, aggregated metrics answering "How busy was each provider on each day?", using data engineering and HIPAA best practices, built live with AI assistance. Raw data has duplicates, sentinel values, inconsistent formatting and mixed ID formats per customer (integers or UUIDs), and patient IDs must be de-identified before silver. The session is judged equally on a working pipeline and on visible, disciplined AI-assisted development.

## Capabilities

- **CAP-1**
  - **intent:** The bronze layer is `encounters.csv`, `labs.csv` and `providers.csv` under `{DATA_ROOT}/bronze/`, read by silver under explicit all-string schemas.
  - **success:** Silver's `rows_in` equals each file's row count; no read uses `inferSchema`.
- **CAP-2**
  - **intent:** Silver writes `clean_encounters`, `clean_labs` and `clean_providers` (schemas in `data-model.md`), deduplicated, with sentinel values nulled, formatting normalized and optional fields kept as null.
  - **success:** Exact schemas; each cleaning rule tested; rows missing a required field dropped and counted.
- **CAP-3**
  - **intent:** Patient IDs are de-identified between bronze and silver.
  - **success:** `hashedpatientid` is a deterministic 64-hex HMAC-SHA256 of `customer:patient_id`; the same ID at two customers hashes differently; `patient_id` is null in every silver row.
- **CAP-4**
  - **intent:** Gold `provider_daily_activity` answers how busy each provider was each day.
  - **success:** Exact gold schema; correct `encounter_count`, `unique_patients`, `totaldurationdays` and `lab_count`; cancelled encounters and retracted labs excluded; a multi-provider encounter counted for each provider; labs counted on their encounter's day.
- **CAP-5**
  - **intent:** One command runs silver then gold; the same code runs on AWS EMR.
  - **success:** The command produces the gold table from synthetic data locally, `pytest -q` passes, and the EMR target prints a runnable EMR Steps submission without exposing the salt.
- **CAP-6**
  - **intent:** Developers and tests have realistic synthetic data and per-layer dummy datasets.
  - **success:** `synth/generate.py` writes the bronze CSVs with every quality issue above; unit tests build chained dummy datasets per layer from the output schemas and check every output table with `assert_schema` and `assert_content`. No real records are used.
- **CAP-7**
  - **intent:** Every build and code review automatically checks changes for PHI leaks and HIPAA gaps.
  - **success:** `bmad-build` and `bmad-code-review` run the PHI/HIPAA reviewer and report findings with `file:line` (configured in `_bmad/custom/`).

## Constraints

- Architecture: `ARCHITECTURE-SPINE.md` decisions bind every story; changing one goes through course correction or `bmad-architecture`, not a local override.
- PySpark only for data manipulation; no pandas. The only Python UDF allowed is HMAC-SHA256.
- Explicit `StructType` schemas, never `inferSchema`; every transform is a pure function, DataFrame in and DataFrame out.
- Every layer is treated as PHI. Silver holds `hashedpatientid`, never a raw `patient_id`; gold holds per-provider daily aggregates and no patient-level field. Basis: `docs/hipaa-context.md`.
- The hashing salt comes from `PATIENT_ID_SALT` if set, else `DEFAULT_SALT = "regard-demo-salt"` as the prompt requires. This is a known demo weakness; production keeps the salt in AWS Secrets Manager.
- Timestamps are parsed with `TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX"`. Every layer is CSV with a header.
- No PHI in logs, prints or exception messages; log counts and column names only. Test data is synthetic only.
- The core build must fit about 20 to 25 minutes of the live session; no gold table from running code is a failure.

## Non-goals

- A crosswalk from `hashedpatientid` back to `patient_id`.
- Cross-customer provider or patient matching.
- Building production Security Rule controls (care-team access, read audit logging, encryption, AWS BAA); they are named, not built.
- Any UI or streaming ingestion.

## Success signal

Story 1 works: one command writes `provider_daily_activity` from synthetic data, `pytest -q` passes (schema and content checks for each layer, hash, cancelled, retracted, multi-provider, lab day, fail-fast), and the PHI review reports no gaps. Stories 2 to 4 are stretch.

## Assumptions

Decisions D1 to D9 and defaults F1 to F13 in `sprint-change-proposal-2026-10-14.md`; the data rules are restated in `data-model.md`.

## Open Questions

- Which sentinel values and quality issues does the supplied data actually contain? The shared sentinel list grows after a look at the files.
