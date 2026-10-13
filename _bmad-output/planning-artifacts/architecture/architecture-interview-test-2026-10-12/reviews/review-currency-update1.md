# Currency Review, Update 1: AD-6 amendment and new AD-11

- **Reviewed:** `../ARCHITECTURE-SPINE.md` (final, updated 2026-10-12), delta only: the AD-6 empty-key drop rule and the new AD-11 (gold sorted before the Parquet write). Context: `../.memlog.md` lines 54-55, `specs/spec-patient-history-pipeline/{SPEC.md,data-model.md,stories.yaml}`.
- **Review date:** 2026-10-12
- **Lens:** was each committed decision in the delta researched or checked against reality, rather than asserted from training data? Checked against Spark 3.5 docs and behavior.
- **Method:**
  - Read the Apache Spark **v3.5.8** source at the release tag: `DataSourceScanExec.scala`, `FilePartition.scala`, `FileFormatWriter.scala`, `UTF8String.java` and `stringExpressions.scala`.
  - Read the PySpark 3.5.8 API reference for `Column.asc_nulls_last`.
  - Ran a local experiment that sorts, writes Parquet and reads it back. The local venv has PySpark 4.2.0 on JDK 21, not the pinned 3.5.8 (see the earlier `review-currency.md` F2/F3). The code path involved is the same in v3.5.8, v4.0.0 and v4.2.0 (verified below).
- **Research trail:** the memlog records both delta decisions as coming from a party-mode review and a user request. No source is cited for the claims about Parquet or Spark ordering, or for how `trim` behaves. The claims below were checked here for the first time.

## Verdict

**Pass with changes.** Both decisions are sound and implementable on Spark 3.5. But AD-11's one factual claim names the wrong cause, and two details need tightening before story 1:

- The "nulls last" scope is ambiguous.
- AD-6's "blank" depends on `trim`, which in Spark removes only ASCII spaces.

## Answers to the specific questions

### Q1. Does `orderBy` before a Parquet write in Spark 3.5 give files whose rows read back in order? Is that guaranteed?

**Each file is written in order, but reading the dataset back does not return that order, and Spark guarantees nothing here.**

- **Write side (verified in source):**
  - `orderBy` plans a global sort: a range-partitioning shuffle, then a sort inside each partition.
  - With no `partitionBy` and no bucketing, `FileFormatWriter` (v3.5.8, lines 144-193) has an empty `requiredOrdering`, so it adds no re-sort.
  - Each write task streams its sorted range into its own `part-NNNNN` file. So rows are in order inside each file, and the files in part-number order make up the global order.
  - This is how Spark is implemented, not a documented contract.
  - AQE partition coalescing in 3.5 merges only adjacent post-shuffle partitions, so it keeps this order.
  - Anything placed between the sort and the write breaks it: `repartition`, `union`, a join, `dropDuplicates`, or setting `maxRecordsPerFile` (which splits a range across files but keeps order inside each file).
- **Read side (verified in source and by experiment):**
  - In `FileSourceScanExec.createReadRDD` (v3.5.8 `DataSourceScanExec.scala` line 709), file splits are sorted **by size, largest first**: `.sortBy(_.length)(implicitly[Ordering[Long]].reverse)`. They are not sorted by file name.
  - `FilePartition.getFilePartitions` then packs the splits into read partitions in that order.
  - The same line is in v4.0.0 (line 810) and v4.2.0 (line 892).
  - So `spark.read.parquet(gold).collect()` returns files in size order. A file over `maxPartitionBytes` (128 MB) is also split, and its pieces are reordered by size too.
- **Experiment** (`local[4]`, AQE off, 4 shuffle partitions, keys `k00000`..`k03999`, payload growing with the key so later files are larger):
  - The in-memory `collect()` after `orderBy` came back sorted.
  - The re-read came back **not sorted**. Its first row was `k02997`, and the files were read in the order `part-00003, 00002, 00001, 00000`.
  - In a first run with files of nearly equal size and skewed keys, the re-read happened to come back sorted. The re-read order depends on file sizes, so a test of it can pass or fail by chance.

### Q2. Can "nulls last" be expressed as stated?

**Yes, as long as it is applied to each key.**

- `Column.asc_nulls_last()` and `pyspark.sql.functions.asc_nulls_last(col)` both exist in PySpark 3.5.8 (both since 2.4).
- Spark's default ascending sort puts nulls **first**, so a bare `orderBy("event_code")` would break the rule.
- Null placement is set per sort key. There is no global "nulls last" setting on `orderBy`.
- The spine's wording, "sorts ... by A, B, C, D (nulls last)", doesn't say which keys this applies to (see F2).

### Q3. Does any Spark 3.5 behavior make a time-order unit test unreliable on the in-memory DataFrame vs the re-read Parquet?

- **In-memory, asserted on `df.collect()` right after `orderBy`:** reliable. `collect` concatenates result partitions in partition order, and range partitions are ordered.
  - Ties are not reliable. Two rows with the same `(patient_key_v1, event_date, event_type, event_code)`, such as two labs with the same test code on the same day, can come back in either order across runs.
  - The test must assert non-decreasing order, not a fixed list of rows.
- **In-memory, but sorted before a later `unionByName` or `select` with a shuffle:** unreliable. The sort must be the last row-order-changing step of the gold transform, after the union.
- **On re-read Parquet:** unreliable, as shown under Q1. The test can only pass by chance: tiny test data usually produces one or a few similar-sized files, so it passes locally and has no meaning at scale.
- **Asserting on `.show()` or `.take()` output:** don't. `take` on a sorted DataFrame is fine, but AD-8 bans `show`/`collect` for logging, and tests should use `collect()` on synthetic data only.

### Q4. Is "Parquet keeps no guaranteed row order" accurately phrased?

**No. The conclusion is right, but the stated cause is wrong.**

- The Parquet format does keep row order: row groups and pages are stored and read sequentially, so a single file read with any Parquet reader returns rows in the order written.
- What is not guaranteed is the order of a **Spark DataFrame read from a multi-file dataset**. Spark makes no promise about DataFrame row order without an `orderBy`, and in practice the 3.5 file scan reorders splits by size.
- An interviewer who knows Parquet could challenge the sentence as written. See F1 for the suggested wording.

## Findings

### F1 — MEDIUM — AD-11: the ordering caveat blames Parquet, not Spark's read path

- **Where:** AD-11 rule, sentence 2. Also memlog line 55: "since Parquet has no row order".
- **Evidence:** Q1 and Q4 above. The cause is Spark 3.5.8 `DataSourceScanExec.scala:709`, which sorts splits by size, largest first. The experiment reproduced out-of-order reads.
- **Fix:** replace the sentence with:

  > Each Parquet file holds its rows in that order, but Spark does not guarantee row order when it reads a multi-file dataset (it schedules file splits by size), so any reader that needs the order, the demo included, sorts by the same keys.

  Add a memlog `(source)` line that cites the Spark source line.

### F2 — MEDIUM — AD-11: "nulls last" scope is ambiguous, and the test level isn't fixed

- **Where:** AD-11 rule; the Tests convention ("gold rows in time order (AD-11)"); `data-model.md:48`, which leaves out "nulls last" entirely, so the companions have drifted.
- **Evidence:** Q2 and Q3 above.
  - Under the silver validity rules, `patient_key_v1`, `event_date` and `event_type` can never be null. Only `event_code` can be.
  - Spark's default would put null codes first.
  - Without a stated test level, someone writing a story may test the re-read output, which passes or fails by chance.
- **Fix:**
  - Name the expression in AD-11: `orderBy(*[F.col(c).asc_nulls_last() for c in GOLD_SORT_KEYS])`, with `GOLD_SORT_KEYS` defined once in `schemas.py` and used by the job, the demo and the test.
  - Say the sort is the last step of the gold transform, after `unionByName`, and that nothing reorders rows between it and the write.
  - In the Tests convention, say the test checks the **gold transform's output via `collect()`**, not re-read Parquet, and checks non-decreasing order on the keys, not tie order.
  - Add "nulls last" to `data-model.md:48`.

### F3 — LOW — AD-6: Spark `trim` removes only ASCII spaces, so "blank" can let whitespace through

- **Where:** AD-6, `normalize_facility`/`normalize_mrn` ("trim") and the new empty-key rule.
- **Evidence:**
  - Spark 3.5.8 `UTF8String.trim()` "Trims space characters (ASCII 32) from both ends".
  - The SQL docs for `trim(str)` say it "Removes the leading and trailing space characters" (`stringExpressions.scala:1067`).
  - So an MRN of `"\t"`, `" "` or `"0\r"` stays non-empty after normalization. Several such patients then merge under one key, which is the failure this amendment exists to prevent.
  - Separately, the rule doesn't say that a **null** normalized value counts as empty. The UDF returns null for null input, so a missing MRN would produce a null key, not one shared key. But the drop and its count should still cover null explicitly.
- **Fix:**
  - Define "empty" as `F.col(x).isNull() | (F.col(x) == "")` after normalization.
  - Normalize whitespace with `regexp_replace(x, r"^\s+|\s+$", "")`, or use `trim` but test the tab and CR cases.
  - Make the leading-zero strip `regexp_replace(x, "^0+", "")`, so `"0000"` really becomes `""` as the rule's example says.
  - Also state that `silver_patients` applies the same drop. Otherwise its de-duplication keeps one arbitrary blank-MRN patient row.

## Checked and fine

- An `orderBy` on four keys at interview scale is cheap. A range-partitioning global sort is the right primitive, and `sortWithinPartitions` would not give a global order.
- Without `partitionBy`, Spark 3.4+ planned writes (V1Writes) do not override the user's sort, so the SPARK-40885-style issue with `partitionBy` doesn't apply.
- Putting `patient_key_v1` first gives per-patient time order, which is what CAP-4 needs. `event_type` as a same-day tiebreaker sorts alphabetically (diagnosis, encounter, lab, medication). That isn't a clinical order, but it is deterministic. Note it if doctors expect the encounter first.
- AD-6's drop counts flow into AD-8's `dropped`, and the rule is placed before hashing. Both are consistent with the existing ADs.
- PHI: no values appear in the delta, and the tests use synthetic data.
