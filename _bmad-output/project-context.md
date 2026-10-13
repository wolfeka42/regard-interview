# Project context: pipeline code rules

Moved from `CLAUDE.md` on 2026-10-12. Gold and patient-ID rules updated to the decisions in `docs/hipaa-context.md`. `AGENTS.md` points here before any `.py` change.

## Running
- Tests: `pytest -q`   Format: `ruff format .` (a hook also runs `ruff format` on each edited `.py` file)
- Runtime: Python 3.11, JDK 17 (`JAVA_HOME`), `pyspark==3.5.8`, matching EMR 7.14.0. Spark 3.5 does not run on Java 21.
- Architecture rules AD-1 to AD-10: `planning-artifacts/architecture/architecture-interview-test-2026-10-12/ARCHITECTURE-SPINE.md`.

## PySpark
- PySpark only for data manipulation; no pandas, no Python UDFs unless unavoidable. HMAC-SHA256 counts as unavoidable, because Spark's `sha2` is a plain hash. Keep that UDF small and isolated.
- Explicit StructType schemas; never inferSchema.
- Each transform is a pure function: DataFrame in, DataFrame out.

## Layers
- bronze (raw) -> silver (clean, PHI) -> gold (doctor-facing, PHI for treatment use).
- Gold never contains names, SSN, MRN, phone, email, street, city, full ZIP or date of birth. Gold keeps full service dates (visit, lab, admission, discharge).
- Compute `age_at_encounter` in silver from DOB and the service date, and carry it into gold. Ages over 89 become 90+.
- Gold is sorted by `patient_key_v1`, `event_date`, `event_type`, `event_code` (nulls last) in a pure `order_gold` function; readers still sort, because Spark doesn't guarantee read order.

## Patient ID
- Pseudonymize IDs with HMAC-SHA256 using a key from an env var; never a plain hash.
- Before hashing, strip whitespace around the facility and uppercase it; remove all whitespace from the MRN and strip leading zeros (use regex; Spark's `trim` only removes ASCII spaces). Join as `facility:mrn`, because MRNs are only unique per facility. Use the same normalization for the patients join.
- Drop rows whose normalized facility or MRN is empty, on patients and events, before the join; never hash them.
- The ID must be deterministic across tables and loads. Name the column with the key version (e.g. `patient_key_v1`), because rotating the key changes every ID.
- Fail at startup if the key env var is missing; never fall back to a default key.
- Doctor lookup by MRN happens in an app that holds the key and computes the HMAC at query time. Do not build a pseudonym-to-MRN crosswalk table.
- `silver/patients` never holds the key; compute it on silver event rows, then drop `facility` and `mrn`.
- Never put the key value in EMR Step args, Spark conf, cluster configurations or logs.

## Safety
- Never log, print or put PHI values in exception messages.
- Test data is always synthetic.
- Tests assert the exact gold schema, that no MRN or DOB column exists, that the ID is 64 hex characters, and that a missing key fails fast.
