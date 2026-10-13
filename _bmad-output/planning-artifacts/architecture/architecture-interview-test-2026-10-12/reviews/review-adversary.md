# Adversarial review: Patient History Pipeline architecture spine

- **Reviewed:** `ARCHITECTURE-SPINE.md` (draft, 2026-10-12)
- **Units one level down:** stories 1-6 in `specs/spec-patient-history-pipeline/stories.yaml`, plus `data-model.md`
- **Lens:** find two stories that each follow every AD exactly and still produce code that doesn't fit together. Each pair below is a hole, and each comes with the smallest AD change that closes it, sized for a ~25-minute build window.
- **Reviewer date:** 2026-10-12

## Verdict

The spine is solid on the layer and storage boundaries (AD-1, AD-2, AD-5). Its weak point is inside silver. Stories 1-4 each write their own `silver_<source>` function, and the spine fixes what those functions output but not who owns the shared steps they all repeat:

- computing the patient key
- joining to patients
- computing age
- typing fields
- counting dropped rows

Five real holes come from that. All of them can be closed with one shared helper or one sentence added to an existing AD. **Status: approve after the five ADs below are tightened, and resolve the open question about end dates before story 1.**

---

## H1. Two owners of `patient_key_v1`, and facility is not normalized (stories 1 and 2) — HIGH

**Story 1** ("one source, no joins") must produce `patient_key_v1` on encounters with no patients dataset available. It follows AD-6 by calling the HMAC UDF on `encounters.facility` and `normalize_mrn(encounters.mrn)` inside `silver_encounters`.

**Story 2** follows AD-4 literally. It treats `silver/patients` as the source of the key: `silver_patients` computes the key, and the event joins to patients on `(facility, normalize_mrn(mrn))` and takes the key from the patients side.

**Why both comply yet clash:**

- After story 2 there are two places that compute the key. The joined frame either has two `patient_key_v1` columns (an ambiguity error) or the story 2 developer quietly drops one. Which one wins is not decided anywhere.
- Story 1 also contradicts AD-4 as written: an event dataset with no join to patients is not allowed by AD-4.
- **Facility is never normalized.** AD-6 normalizes only the MRN. One story can trim or upper-case `facility` for the join while the hash input uses raw `facility`, or the reverse. `"GEN "` and `"GEN"` then match in the join but hash to different keys, which splits one patient into two. This is exactly the failure AD-6 says it prevents.

**Minimal fix (tighten AD-6 and AD-4):**

- `patient_key.py` exposes one helper, `with_patient_key(df, secret) -> df`. It normalizes `facility` (trim, upper) and `mrn` (trim, strip leading zeros) and adds `patient_key_v1`. Every silver dataset, patients and events alike, calls it on its own `facility`/`mrn`.
- Events join to patients **on `patient_key_v1`**. They never join on raw or normalized facility/MRN.
- The patients side contributes only demographics.

Joining on the key means the join key and the hash input are the same value by construction. Story 1 stays valid with no join, and story 2 only adds the join.

Add to AD-4: "Until the patients join exists (story 1), the demographic columns are null in gold."

## H2. Each event story writes its own join, age and ZIP logic, and patients can fan out events (stories 2, 3 and 4) — HIGH

**Story 2** writes the join and the `age_at_encounter`/`zip3` logic inside `silver_encounters`. It computes age as `year(encounter_date) - year(dob)`.

**Stories 3 and 4** follow AD-4 too, but each writes its own copy for labs, diagnoses and medications. They compute age as `floor(months_between(date, dob) / 12)`, and one copy caps at 90 with `> 89` while another uses `>= 90` after a different floor.

**Why both comply yet clash:**

- AD-4 says each event dataset carries `age_at_encounter`, and the conventions say "whole years". Neither names one implementation. The same patient on the same day can show age 45 on an encounter row and 44 on a lab row. AD-4's stated goal ("age ... computed in two places") is not met: it is computed in four places.
- **Nobody owns patient uniqueness.** AD-4 requires an inner join to `silver/patients`, but no AD says that dataset has one row per patient. If the raw patients file holds two rows for one `facility:mrn` (an updated address, a duplicate load), every event for that patient is duplicated in gold. Story 2 assumes the data is unique. Story 5's validity rules are per-source and could reasonably leave duplicates in, since a duplicate is not an "invalid" row. Both stories comply, and the doctor sees doubled labs.

**Minimal fix (tighten AD-4):**

- Add one function in `jobs/silver.py`, `attach_patient(events, patients, date_col) -> df`, as the **only** place that:
  - joins events to patients (inner, on `patient_key_v1`)
  - computes `age_at_encounter = least(floor(months_between(date_col, dob) / 12), 90)`
  - computes `zip3 = substring(zip, 1, 3)`
  - selects the allowed demographic columns
- Every `silver_<event>` calls it.
- `silver_patients` guarantees one row per `patient_key_v1`. It applies `dropDuplicates(["patient_key_v1"])` and counts the extra rows as `dropped`.

## H3. Silver typing is unconstrained, so values, ZIPs and dates can be typed differently (stories 2, 3 and 4 against gold) — HIGH

AD-3 says "typing happens only in silver" but does not say which fields get typed, or how.

- **Story 3** types `result_value` as `double` in `SILVER_LABS`, because a lab value looks numeric. Rows like `"<5"` or `"positive"` fail the cast. Under the invalid-rows convention they are dropped and counted, so real clinical results quietly disappear. The values that survive reach gold as `"5.0"` through AD-5's string cast.
- **Story 4** keeps `dose` as a string (`"10 mg"`). Gold `event_value` then mixes `"5.0"`-style values from the double cast with free text.
- **Story 2** may type `zip` as int in `SILVER_PATIENTS`, which loses the leading zero (`02134` becomes `2134`, so ZIP3 becomes `213`) before ZIP3 is computed. This still follows the rules, because the string-ZIP rule exists only for **gold** columns.
- **Dates:** each `silver_<source>` picks its own `to_date` format. On the day of the schema swap, `dob` in patients and `result_date` in labs can be parsed with different patterns. One story turns a bad date into null and keeps the row. Another drops it.

**Minimal fix (add one rule to AD-3):**

- In silver, only dates (`DateType`) and `age_at_encounter` (int) change type. Codes, values, units, IDs, MRN, ZIP, sex and state stay `StringType` and are only trimmed.
- All dates are parsed through one `parse_date(col)` in `schemas.py`, using one declared format constant.
- A row whose required date is null or cannot be parsed is invalid: it is dropped and counted.

## H4. No agreed way to return the dropped count (story 5 against stories 2, 3 and 4) — MEDIUM

AD-8 requires one `dropped` count per source. The conventions require `silver_<source>` to be a pure `DataFrame -> DataFrame`.

- **Stories 2-4** build pure functions. Rows lost in the inner join disappear with no count.
- **Story 5** must log `dropped`. It can follow the rules in either of two incompatible ways:
  - change `silver_<source>` to return `(df, dropped)`, or add a `_valid` flag column, which breaks every existing caller and test
  - compute `dropped` in `main` as `bronze_count - silver_count`

Another developer might count only validity failures inside the function and miss the join orphans that AD-4 says must also be "dropped and counted".

**Minimal fix (tighten AD-8):**

- `silver_<source>` stays `DataFrame -> DataFrame`.
- `silver.main` logs `dropped = rows_in - rows_out` per source.
- That one count includes invalid rows, duplicate patients (H2) and orphan events (AD-4).
- Tests for story 5 assert this count, using the row counts of the synthetic input and output.

## H5. The secret can reach EMR in ways that work locally and fail or leak on the cluster (stories 1 and 6) — MEDIUM

**Story 1** writes the HMAC UDF so that it calls `os.environ["PATIENT_KEY_HMAC_SECRET"]` inside the function body. This works in local mode, where everything runs in one process. A reader could argue it complies, because AD-7 says "only silver reads the key" and `patient_key.py` is silver's dependency.

**Story 6** sets the variable for the EMR Step through `spark.yarn.appMasterEnv`, which makes it visible only on the driver. Executors run the UDF and raise `KeyError`.

The tempting fix is worse. A developer passes the secret value in the Step arguments (`--conf ...SECRET=<value>`), so the HMAC key shows up in the EMR console and API. AD-7 also requires the orchestrator itself to hold the secret, even for `--target emr`. That pushes story 6 toward carrying the value from the laptop into the Step definition.

**Minimal fix (tighten AD-6 and AD-7):**

- Only `config.py` reads the environment.
- The UDF is built by a factory, `make_patient_key_udf(secret: bytes)`, from `conf`, and the closure captures the secret, so executors never read the environment.
- For `--target emr`, the orchestrator requires a `PATIENT_KEY_SECRET_ARN` instead of the value. The secret value never appears in Step arguments or the cluster configuration.
- Each Step's job `main` resolves the ARN through Secrets Manager inside `config.py` and still fails fast if it can't.

---

## Lesser findings

- **L1. A gold schema change is waiting at story 4 (stories 1 and 4).** The open question about `discharge_date` and `end_date` is a real clash and needs a decision now:
  - Story 1 fixes `GOLD_SCHEMA` without `event_end_date`.
  - Story 4 (medications, with `end_date`) and `project-context.md` (which keeps discharge dates) push to add the column.
  - Every earlier gold test then changes, and AD-5's `conform_to_gold` must tolerate a column that some sources never set.

  **Fix:** add a nullable `event_end_date` to `GOLD_SCHEMA` in story 1 (encounter: `discharge_date`, medication: `end_date`, others null). That costs one line now and avoids rewriting the gold tests later.
- **L2. Sex and state are not normalized.** `sex`/`state` case and value sets (`F`/`female`/`f`) are never normalized. Low risk, because only story 2 touches them. It becomes a clash only if the data swap brings a second patients-like source. Note it in the conventions if time allows.
- **L3. Gold has no time order.** The spec says doctors see events "in time order", but Parquet does not keep row order and nothing in the spine assigns ordering. It belongs in the consuming query or app, not in gold. State that so no story tries to `orderBy` before the write and claims it as a guarantee.
