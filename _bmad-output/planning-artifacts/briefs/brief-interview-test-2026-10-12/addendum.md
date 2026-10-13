# Addendum: PHI de-identification pipeline

Detail that belongs in the PRD or architecture rather than the brief.

## Technical constraints (from CLAUDE.md and discovery)
- Runtime: PySpark on AWS EMR. PySpark only for data manipulation; no pandas; no Python UDFs unless unavoidable.
- Explicit StructType schemas; never inferSchema.
- Each transform is a pure function (DataFrame in, DataFrame out).
- Layers: bronze (raw) -> silver (clean, PHI) -> gold (doctor-facing, PHI for treatment use).
- Gold excludes names, SSN, MRN, phone, email, street, city, full ZIP and full dates. **Superseded 2026-10-12:** gold keeps full dates and is PHI (see the HIPAA section below). CLAUDE.md still needs updating to match; the PHI review checklist now lives in `_bmad/custom/phi-review-lens.md` with full service dates allowed.
- IDs pseudonymized with HMAC-SHA256; key from an environment variable; never a plain hash.
- PHI never appears in logs, prints or exception messages.
- Test data is always synthetic.
- Commands: `pytest -q`, `ruff format .`

## HIPAA basis for full dates in gold
- Safe Harbor (45 CFR 164.514(b)(2)(i)(C)) treats every date element except the year as an identifier when the date relates to an individual. This covers admission, discharge, service and death dates as well as DOB. Dropping DOB alone does not de-identify the table.
- So gold with full dates is PHI. That is allowed: a covered entity may use PHI for treatment without patient authorization (164.506). Disclosures to a provider for treatment are exempt from the minimum-necessary standard (164.502(b)(2)(i)).
- A Limited Data Set (164.514(e)) may keep dates, but only for research, public health or health care operations under a data use agreement. It is the wrong legal basis for a doctor treating a patient.
- Obligations that follow from gold being PHI (Security Rule, 164.312): access control limited to the care team, audit logging of reads, integrity controls, and encryption at rest (S3/EMR) and in transit. AWS requires a signed BAA, and EMR must run as a HIPAA-eligible service under it.

## Patient ID and DOB design
- DOB dropped from gold; `age_at_encounter` computed in silver; ages over 89 become 90+.
- Patient key: HMAC-SHA256 over `facility:mrn` (trimmed, leading zeros normalized). Key from Secrets Manager/KMS via env var. Deterministic. Column named with the key version (`patient_key_v1`); rotation re-keys everything. Missing key fails at startup.
- Spark has no built-in HMAC (`sha2` is a plain hash), so a small Python UDF is the justified exception to the no-UDF rule.
- Doctor lookup: a key-holding app computes HMAC(MRN) at query time. Out of scope for the build; no crosswalk table.
