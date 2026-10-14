<!-- bmad:context -->
<!-- Verified 2026-10-12 against no commits (empty repo). Managed by bmad-project-context; edits inside this block are replaced on refresh. Keep anything you want preserved outside the markers. -->

## interview-test

PySpark pipeline that turns raw clinical data (encounters, labs, providers) from several hospital customers into gold metrics answering "How busy was each provider on each day?". Runs on AWS EMR. Layers: bronze (raw CSV) -> silver (cleaned, de-identified) -> gold (provider daily activity). Planning lives in `_bmad-output/planning-artifacts/`.

## Policy

- Treat every layer as PHI, gold included; never describe or test any layer as de-identified. Basis: `docs/hipaa-context.md`.
- Silver never holds a raw `patient_id`, only `hashedpatientid` (HMAC-SHA256 of `customer:patient_id`). Gold holds per-provider daily aggregates and no patient-level field.
- Never log, print or put PHI values in exception messages; log row counts and column names instead.
- Use only synthetic test data; never copy real records into tests or fixtures.
- Any design touching data access must assume Security Rule controls: care-team-only access, read audit logging, encryption at rest (S3/EMR) and in transit, AWS BAA.

## Where things are

- Before writing or changing any `.py` file, read `_bmad-output/project-context.md`: commands, PySpark rules, schemas, patient-ID hashing and test requirements.
- PHI review checklist used by `bmad-review`, `bmad-code-review` and `bmad-build`: `_bmad/custom/phi-review-lens.md`.

<!-- /bmad:context -->
