# Data model

Rewritten 2026-10-14 from the actual interview prompt (`../../planning-artifacts/sprint-change-proposal-2026-10-14.md`).

## Layers

| Layer | Holds | PHI | Location |
|---|---|---|---|
| Bronze | Raw records as received, all strings, CSV | Yes | `{DATA_ROOT}/bronze/{encounters,labs,providers}.csv` |
| Silver | `clean_encounters`, `clean_labs`, `clean_providers`; typed, deduplicated, patient ID hashed | Yes | `{DATA_ROOT}/silver/clean_{encounters,labs,providers}.csv` |
| Gold | `provider_daily_activity`: one row per customer, provider and encounter day | Treated as PHI | `{DATA_ROOT}/gold/provider_daily_activity.csv` |

## Bronze CSV columns

| File | Columns |
|---|---|
| `encounters.csv` | customer, encounter_id, patient_id, provider_id, start_time, end_time, updated_at, status |
| `labs.csv` | customer, lab_id, encounter_id, test_name, result_value, result_unit, observed_time, status |
| `providers.csv` | customer, provider_id, provider_name, specialty, active_flag |

An encounter with several providers is one row per provider.

## Silver schemas

**clean_encounters:** customer string, encounter_id string, patient_id string (always null), provider_id string, start_time timestamp, end_time timestamp, status string, duration_days double, hashedpatientid string, partition_timestamp timestamp.

**clean_labs:** customer string, lab_id string, encounter_id string, test_name string, result_value double, result_unit string, observed_time timestamp, status string, partition_timestamp timestamp.

**clean_providers:** customer string, provider_id string, provider_name string, specialty string, active_flag boolean, partition_timestamp timestamp.

## Gold schema: provider_daily_activity

customer string, provider_id string, encounter_date date, provider_name string, specialty string, active_flag boolean, encounter_count long, unique_patients long, totaldurationdays double, lab_count long. Sorted by customer, provider_id, encounter_date.

## Silver rules

- **IDs:** `customer` and IDs trimmed with `(?U)\s`; UUIDs and `customer` lowercased; integer IDs keep leading zeros.
- **Sentinels:** one shared list (`""`, `NULL`, `N/A`, `NA`, `None`, `-`, `?`, `unknown`, case-insensitive) becomes null in every string field.
- **Formatting:** `test_name` trimmed, inner whitespace collapsed, lowercased; `status` trimmed and lowercased.
- **Types:** timestamps parsed with `TIMESTAMP_FORMAT` in UTC, unparseable → null; `result_value` non-numeric → null (row kept); `active_flag` from `true/false/t/f/1/0/y/n/yes/no`, else null; empty `specialty` is null and kept.
- **duration_days:** `(end_time - start_time) / 86400 s`; null if either end is null or end is before start.
- **Dedupe:** exact duplicates dropped; encounters keep the latest `updated_at` per `(customer, encounter_id, provider_id)`; labs one row per `(customer, lab_id)`, providers one per `(customer, provider_id)`, chosen deterministically.
- **Required fields (missing → dropped and counted):** encounters `customer, encounter_id, provider_id, start_time`; labs `customer, lab_id, encounter_id`; providers `customer, provider_id`. A null `patient_id` keeps the encounter with a null hash.
- **partition_timestamp:** the run time, captured once per run.

## Patient ID hashing

- `hashedpatientid = HMAC-SHA256(salt, customer + ":" + patient_id)`, 64 lowercase hex characters, null for a null ID. `customer` is included because integer IDs collide across customers.
- Salt: `PATIENT_ID_SALT` env var if set, else `DEFAULT_SALT = "regard-demo-salt"` (interview requirement; production uses Secrets Manager).
- `patient_id` is null in silver; no crosswalk is stored.

## Gold metrics

Per `(customer, provider_id, encounter_date = to_date(start_time))`, over encounters whose status is not `cancelled` or `canceled`:

- `encounter_count = count(DISTINCT encounter_id)`
- `unique_patients = count(DISTINCT hashedpatientid)`
- `totaldurationdays = sum(duration_days)`, 0.0 when none
- `lab_count` = labs whose status `IS DISTINCT FROM 'retracted'`, joined through `(customer, encounter_id)` and counted on the encounter's day; 0 when none

Encounters are reduced to one row per `(customer, encounter_id, provider_id)` before the lab join so labs are not counted twice. Providers are left-joined on `(customer, provider_id)`; activity for an unknown provider keeps null provider fields.

## Required tests

- Chained dummy datasets per layer: dummy bronze → expected `clean_*` → expected `provider_daily_activity`.
- `tests/assertions.py`: `assert_schema(df, schema)` (exact names, types, order) and `assert_content(df, expected_rows)` (exact, order-independent), applied to every silver and gold output.
- Hash: deterministic, 64 hex, customer-scoped; `patient_id` null in silver.
- Cancelled encounters excluded; retracted labs excluded, null-status labs counted; multi-provider encounter counted per provider; lab counted on its encounter's day.
- Each cleaning rule (story 2) and dropped counts.
- Missing `DATA_ROOT` fails at startup. All fixtures are synthetic.
