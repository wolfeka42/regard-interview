# Adversarial review, update 1: Patient History Pipeline architecture spine

- **Reviewed:** `ARCHITECTURE-SPINE.md` (status final, updated 2026-10-12), only the delta:
  - AD-6: rows whose normalized facility or MRN is empty are dropped and counted before hashing
  - new AD-11: gold sorted by `patient_key_v1, event_date, event_type, event_code` (nulls last), written in that order, readers sort too
  - Tests convention: time-order test (AD-11) and blank/all-zero MRN test
  - Deferred: drop-rate threshold and key scope
- **Units one level down:** stories 1-6 (`specs/spec-patient-history-pipeline/stories.yaml`), `data-model.md`, `project-context.md`
- **Lens:** build two units that each follow every AD exactly and still don't fit together. Sized for a 25-minute live build, so each fix is one sentence or one helper.
- **Reviewer date:** 2026-10-12

## Verdict

**Approve with three one-line tightenings.** The delta closes the hole it was aimed at (blank MRNs collapsing into one key). It also opens three small new seams:

1. The new drop has no single owner and no defined form. "Empty" isn't defined for null, and `silver_patients` isn't covered.
2. AD-11 doesn't say where the sort lives. That decides whether the time-order test can be written at all, because a Parquet read-back proves nothing.
3. The sort key isn't total, and Spark sorts nulls first by default. A test that compares exact row lists can flake on tied rows.

None of these is a blocker, and each fix fits inside an existing AD.

---

## F1. The blank-MRN drop has no owner, so stories write it differently (AD-6 vs AD-4 and AD-8) — HIGH

**Unit A: story 1, `silver_encounters`.** It follows AD-6 and adds its own `filter(col("mrn") != "")` after step 1 of AD-4, then calls `with_patient_key`.

**Unit B: story 3, `silver_labs`.** It also follows AD-6 and writes `filter(col("mrn").isNotNull())`, reasoning that the UDF already "returns null on null input".

**Why both comply and still clash:**

- `normalize_mrn("0000")` returns `""`, not null. Unit B's filter lets that row through, and the UDF hashes `"FAC:"`. Every all-zero MRN at a facility gets the same key, which is exactly what the new rule is meant to stop.
- `normalize_mrn(null)` stays null. Unit A's `!= ""` evaluates to null, so the row is dropped, but only by accident of SQL three-valued logic.
- AD-6 says the row is dropped before hashing, but AD-4's steps are normalize, then `attach_patient`, then `with_patient_key`. So the drop can sit before or after the join. If it comes after, blank-MRN events first join to whichever blank-MRN patient row survived the dedupe.
- **`silver_patients` isn't covered.** AD-6's rule says "before hashing", and patients are never hashed (AD-4). So `silver_patients` keeps blank-MRN rows, and AD-4's "one row per normalized `(facility, mrn)`" dedupe collapses every blank-MRN patient at a facility into one arbitrary row of PHI. Gold's output happens to be safe because the events are dropped anyway, but silver now holds a nonsense merged identity. Story 2 and story 5 could each reasonably decide differently whether that is "invalid".
- **Counting.** "Drops and counts" in AD-6 reads like a separate counter. A developer can comply by adding `df.filter(blank).count()` or an accumulator inside the transform. That breaks AD-8 ("transforms stay pure ... no counters") and also runs a second Spark action.

**Minimal fix (one helper and one wording change in AD-6):**

- `patient_key.py` owns `drop_invalid_identity(df)`. It keeps only rows where both `normalize_facility` and `normalize_mrn` are non-null and non-empty.
- AD-4 step 1 becomes "normalize **and call `drop_invalid_identity`**". That one function is applied to patients **and** events, before the join.
- Replace "drops and counts it" with "drops it. The drop is counted only through AD-8's `dropped`, never by a separate counter."
- The blank-MRN test then targets `drop_invalid_identity` with `""`, `"0000"`, `"  "` and null. It also checks one silver event function, to show that two all-zero MRNs at one facility produce no rows rather than one key.

## F2. AD-11 doesn't say where the sort lives, so the time-order test can't be written soundly — HIGH

**Unit A: the gold job author.** AD-11 says "the gold job sorts ... and writes", and the Transforms convention says I/O happens only in `main`. So they put `orderBy(...)` in `gold.main`, right before `.write.mode("overwrite").parquet(...)`. The pure `gold_*` functions and the union stay unsorted.

**Unit B: the test author.** The Tests convention requires "gold rows in time order (AD-11)" and says transforms are "tested on in-memory synthetic DataFrames". They have two options, and both fail:

- They test the union of conformed frames. It isn't sorted, so the test fails, or they sort inside the test and prove nothing.
- They run `main` against a temporary directory and `spark.read.parquet(...).collect()`. Spark writes a range-partitioned sort as many part files (up to `spark.sql.shuffle.partitions` = 200). A read-back has no order guarantee across files or splits, as AD-11 itself admits. The test passes locally by luck, which is a false green, and can flip with file listing order or split size.

**Why both comply and still clash:** AD-11 places the sort in "the job", the conventions place I/O in `main`, and the test convention needs the behavior on an in-memory DataFrame. No function exists for the test to call.

**Minimal fix (one sentence in AD-11):** "The sort is the pure function `order_gold(df)` in `jobs/gold.py`. `main` calls it on the unioned, conformed frame as the last step before writing. The time-order test calls `order_gold` on a shuffled synthetic frame and checks `.collect()`. It never asserts order on a Parquet read-back."

`collect()` on a sorted DataFrame does keep the order, and AD-8 forbids `collect` only for logging, so this is legal. The demo reader keeps AD-11's "readers sort too", ideally by calling the same `order_gold`.

## F3. The sort key isn't total, and "nulls last" is not Spark's default (AD-11) — MEDIUM

- **Ties are real.** These rows all match on `(patient_key_v1, event_date, event_type, event_code)`:
  - two glucose labs on one day, with different `result_value` (repeat draws)
  - two medications with the same `drug_code` and start date but different doses
  - two same-type encounters on one day with different `encounter_id`

  With ties, Spark's order between the tied rows depends on partitioning, so it is not deterministic.
  - **Unit A** writes the time-order test as `assert rows == expected_rows`, comparing full rows. The test is flaky as soon as its fixture contains a tie.
  - **Unit B**, the synthetic data generator (story 1 / CAP-6), naturally produces same-day repeat labs.
- **Nulls.** `orderBy("event_code")` sorts nulls first in Spark. One developer writes `F.col(c).asc_nulls_last()`, and another writes `orderBy(*cols)` and believes "nulls last" is the default. Today only `event_code` can be null:
  - `patient_key_v1` is non-null after F1's drop.
  - `event_date` is required by the Invalid-rows convention.
  - `event_type` is a literal.

  Even so, the two versions produce different files and fail each other's tests.
- **Alphabetical `event_type`** puts `diagnosis` before `encounter` on the same day. That is deterministic and fine, but a test author who expects encounter first will "fix" the code. Name it explicitly.

**Minimal fix (extend AD-11's key and pin the test style):**

- Sort by `patient_key_v1, event_date, event_type, event_code, encounter_id, event_end_date, event_value, event_unit`. Every column uses `asc_nulls_last()`, and the order of `event_type` is lexical.
- Every gold column except the four demographic ones, which are constant per patient and date, is now in the key. So the only rows left tied are identical rows, and their order can't be observed.
- The test asserts that the projected key tuples are non-decreasing. It doesn't compare exact rows.

If the 25-minute budget can't take the longer key, keep the four-column key and make only the test change (a monotonicity check on the key projection). The demo order may then shuffle tied rows, but no test will flake.

## F4. Drop-rate threshold vs AD-8 counting (Deferred) — LOW, note only

The Deferred item is reasonable. If it is ever adopted, it must use AD-8's `rows_in`/`rows_out` from `main`. A per-cause breakdown (blank identity, orphan, duplicate, bad date) would need more actions or columns and would reopen F1's counter problem. Add five words to the Deferred bullet: "using AD-8 counts only".

AD-11 also lacks the `[ADOPTED]` tag that AD-1 through AD-10 carry. That is cosmetic, but the tag is how readers tell final decisions apart.

---

## Checked and found closed

- **The blank-MRN rule against AD-4's "one row per normalized `(facility, mrn)`".** This is consistent for events once F1 lands, because drop-before-join means no join on an empty MRN.
- **The blank-MRN rule against AD-8's `dropped`.** It is consistent, because the drop is just one more cause inside `rows_in - rows_out`, which AD-8 already says covers "invalid rows".
- **AD-11 against AD-5.** The sort comes after `unionByName` of conformed frames, so it can't reintroduce a column. Sort columns are all in `GOLD_SCHEMA`.
- **AD-11 against AD-2's overwrite and idempotency.** A rerun overwrites the folder. The order is not part of the storage contract (readers sort), so reruns remain equivalent.
- **The key-scope Deferred item against AD-7.** No conflict: AD-7 stays strict until it is decided.

## Proposed edits, in order of value

1. AD-6 and AD-4 step 1: add `drop_invalid_identity(df)` in `patient_key.py` (null or empty after normalizing), applied to patients and events before the join, counted only via AD-8. (F1)
2. AD-11: the sort is `order_gold(df)`, a pure function called last in `gold.main`. The test calls it on a shuffled in-memory frame and never reads Parquet back for order. (F2)
3. AD-11: make the key total with `asc_nulls_last()` on every column, or keep the key short and make the test check key-projection monotonicity. (F3)
4. Tag AD-11 `[ADOPTED]`. The drop-rate item uses AD-8 counts only. (F4)
