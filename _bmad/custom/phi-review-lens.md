# PHI / HIPAA Lens

Migrated from `.claude/skills/phi-review`. The date rule is updated to the current policy in `docs/hipaa-context.md`.

**Goal:** Find PHI leaks and HIPAA gaps in PySpark pipeline changes. Gold is doctor-facing for treatment use. It keeps full service dates and is PHI, so never judge it as de-identified data.

## Checks

1. List every column in each gold DataFrame. Flag any name, SSN, MRN, phone, email, street address, city or date of birth. A patient ID must appear only as an HMAC-SHA256 pseudonym.
2. Service dates (visit, lab, admission, discharge) may stay as full dates in gold. Age must come from `age_at_encounter`, computed in silver; ages over 89 must be grouped as 90+.
3. Check that ZIP codes are cut to 3 digits.
4. Pseudonymization must use HMAC-SHA256 with a key read from an environment variable. Flag a plain hash (including Spark `sha2`), a hardcoded or default key, or a key written to output. The pipeline must fail at startup if the key is missing.
5. Before hashing, the MRN must be trimmed, have its leading zeros normalized, and be prefixed with the source facility (`facility:mrn`). The ID column name must carry the key version (e.g. `patient_key_v1`). Flag any pseudonym-to-MRN crosswalk table.
6. Flag any secret in code, config, notebooks or tests: AWS access keys, tokens, passwords or connection strings. Secrets come from environment variables or AWS Secrets Manager; they are never logged, printed or committed.
7. Search for print, log and raise statements that could include row values or PHI columns.
8. Confirm tests assert: the exact gold schema; no MRN or DOB column; the ID is 64 hex characters; a missing key fails fast. All test data must be synthetic.

## Reporting

- Report each finding with `file:line` and a suggested fix.
- Report only real gaps. An empty result is valid.
- Do not assign severity, priority or ranking.
