# PHI / HIPAA Lens

Migrated from `.claude/skills/phi-review`. The date rule is updated to the current policy in `docs/hipaa-context.md`.

**Goal:** Find PHI leaks and HIPAA gaps in PySpark pipeline changes. Every layer is treated as PHI. Gold holds per-provider daily aggregates; never judge any layer as de-identified data. Rules updated 2026-10-14 (provider daily activity).

## Checks

1. List every column in each silver and gold DataFrame. A patient ID may appear only as `hashedpatientid`; `clean_encounters.patient_id` must be null; gold must have no patient-level column. Flag any raw patient ID after bronze.
2. Pseudonymization must be HMAC-SHA256 (`pipeline/hashing.py`). Flag a plain hash, including Spark `sha2`.
3. The salt comes from `PATIENT_ID_SALT`, falling back only to `DEFAULT_SALT` in `pipeline/schemas.py` (accepted: interview requirement). Flag any other default or hardcoded secret, and any salt value written to output, logs, Spark conf or EMR args.
4. The hash input must be `customer:patient_id`, both normalized. Flag any hash-to-patient_id crosswalk table.
5. Flag any secret in code, config, notebooks or tests: AWS access keys, tokens, passwords or connection strings.
6. Search for print, log and raise statements that could include row values or PHI columns.
7. Confirm tests: `assert_schema` and `assert_content` on every silver and gold output; hash 64 hex and customer-scoped; `patient_id` null in silver; missing `DATA_ROOT` fails fast. All test data must be synthetic.

## Reporting

- Report each finding with `file:line` and a suggested fix.
- Report only real gaps. An empty result is valid.
- Do not assign severity, priority or ranking.
