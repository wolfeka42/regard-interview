- source_spec: `_bmad-output/specs/spec-patient-history-pipeline/stories/1-steel-thread-doctor-sees-encounters-in-gold.md`
  summary: Facility normalization does not collapse inner whitespace, so "FAC 1" and "FAC  1" key differently.
  evidence: AD-6 defines facility normalization as strip-surrounding plus uppercase; changing it needs a bmad-architecture update after the day-of data review.
- source_spec: `_bmad-output/specs/spec-patient-history-pipeline/stories/1-steel-thread-doctor-sees-encounters-in-gold.md`
  summary: Encounter validity rules for discharge_date before encounter_date, duplicate encounter_id and blank encounter_id are undecided.
  evidence: All three reach gold today; spine Deferred leaves per-source validity rules beyond required dates to the day-of data review.
- source_spec: `_bmad-output/specs/spec-patient-history-pipeline/stories/1-steel-thread-doctor-sees-encounters-in-gold.md`
  summary: A layer write that fails partway after overwrite leaves bronze, silver or gold partial or empty.
  evidence: AD-2 prescribes plain mode("overwrite") on each layer folder; atomic staging and promotion would need an architecture change.
- source_spec: `_bmad-output/specs/spec-patient-history-pipeline/stories/1-steel-thread-doctor-sees-encounters-in-gold.md`
  summary: Malformed CSV rows (wrong field count) are padded or truncated under PERMISSIVE and never counted.
  evidence: Bronze dropped is always 0 by construction; counting bad input without values belongs to story 5.
- source_spec: `_bmad-output/specs/spec-patient-history-pipeline/stories/1-steel-thread-doctor-sees-encounters-in-gold.md`
  summary: Unhandled Spark exceptions print full tracebacks; unverified whether any path can carry row values (medium if true).
  evidence: Would be settled by story 5's test that no row values appear in logs or errors, run against forced Spark failures.
