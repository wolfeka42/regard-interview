<!-- bmad:context -->
<!-- Verified 2026-10-12 against no commits (empty repo). Managed by bmad-project-context; edits inside this block are replaced on refresh. Keep anything you want preserved outside the markers. -->

## interview-test

PySpark pipeline that ingests raw healthcare data and builds gold tables doctors use to review an individual patient's health history. Runs on AWS EMR. Layers: bronze (raw) -> silver (cleaned) -> gold (doctor-facing). Planning lives in `_bmad-output/planning-artifacts/`.

## Policy

- Treat every layer as PHI, gold included: gold keeps full service dates for treatment use, so never describe or test it as de-identified. Basis: `docs/hipaa-context.md`.
- Gold drops names, SSN, MRN, phone, email, street, city and date of birth; use `age_at_encounter` and an HMAC-SHA256 patient key instead.
- Never log, print or put PHI values in exception messages; log row counts and column names instead.
- Use only synthetic test data; never copy real records into tests or fixtures.
- Any design touching gold access must assume Security Rule controls: care-team-only access, read audit logging, encryption at rest (S3/EMR) and in transit, AWS BAA.

## Where things are

- Before writing or changing any `.py` file, read `_bmad-output/project-context.md`: commands, PySpark rules, schemas, patient-ID hashing and test requirements.
- PHI review checklist used by `bmad-review`, `bmad-code-review` and `bmad-build`: `_bmad/custom/phi-review-lens.md`.

<!-- /bmad:context -->
