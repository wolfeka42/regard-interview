# Data model

## Layers

| Layer | Holds | PHI | Responsibilities |
|---|---|---|---|
| Bronze | Raw records as received | Yes | Read under an explicit all-text `StructType`; no transformation. |
| Silver | Cleaned, typed records | Yes | Type dates; drop and count invalid rows; normalize facility and MRN; compute `age_at_encounter`, `zip3` and the patient key on event rows. `silver/patients` holds identifiers but never the key. |
| Gold | Per-patient clinical event history | Yes (treatment use) | Select allowed columns only; one row per clinical event. |

## Generic input shape (prepared design, swapped on the day)

| Source | Key fields |
|---|---|
| patients | facility, mrn, name, dob, sex, ssn, phone, email, street, city, state, zip |
| encounters | facility, mrn, encounter_id, encounter_date, encounter_type, discharge_date |
| labs | facility, mrn, encounter_id, test_code, result_value, unit, result_date |
| diagnoses | facility, mrn, encounter_id, icd10_code, diagnosis_date |
| medications | facility, mrn, encounter_id, drug_code, dose, start_date, end_date |

## Gold columns

| Column | Rule |
|---|---|
| `patient_key_v1` | HMAC-SHA256 hex (64 characters) |
| `event_type` | encounter, lab, diagnosis or medication |
| `event_date` | Full service date |
| `event_end_date` | Full date, nullable: encounter `discharge_date`, medication `end_date` |
| `event_code`, `event_value`, `event_unit` | From the source event |
| `encounter_id` | Source encounter ID |
| `age_at_encounter` | Integer years; 90 for ages over 89 |
| `sex`, `state`, `zip3` | ZIP cut to 3 digits |

**Never in gold:** name, SSN, MRN, phone, email, street, city, date of birth, full ZIP.

## Patient key

- Input: `facility + ":" + mrn`. Facility: surrounding whitespace stripped, uppercased. MRN: all whitespace removed, leading zeros stripped. A row with an empty normalized facility or MRN is dropped, never hashed.
- HMAC-SHA256 with a key read from an environment variable. The pipeline fails at startup if the key is missing.
- Deterministic across tables and loads. Rotating the key re-keys everything and bumps the column version (`patient_key_v2`).
- Computed only on silver event rows; no dataset stores the MRN next to the key (no crosswalk).
- Doctor lookup by MRN belongs in an app that holds the key; it is not built.

## Required tests

- The exact gold schema; no forbidden column present.
- `silver/patients` schema has no key field.
- Gold rows are in time order per patient (`patient_key_v1`, `event_date`, `event_type`, `event_code`, nulls last), tested on `order_gold` output, not on Parquet read back.
- A blank, whitespace-only or all-zero MRN, or a blank facility, is dropped and counted, never hashed.
- Patient key: deterministic, 64 hex characters, differs by facility, missing key fails fast.
- `age_at_encounter` calculation and 90+ grouping; ZIP3 truncation.
- Bronze row count matches the source; invalid rows are counted in silver.
- All fixtures are synthetic.

Column types, per-source event mapping and layer rules: `ARCHITECTURE-SPINE.md` (conventions, AD-3 to AD-6).
