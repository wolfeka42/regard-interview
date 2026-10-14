- source_spec: `_bmad-output/specs/spec-patient-history-pipeline/stories/1-steel-thread-doctor-sees-encounters-in-gold.md`
  summary: A layer write that fails partway after overwrite leaves bronze, silver or gold partial or empty.
  evidence: AD-2 prescribes plain mode("overwrite") on each layer folder; atomic staging and promotion would need an architecture change.
- source_spec: `_bmad-output/specs/spec-patient-history-pipeline/stories/1-steel-thread-doctor-sees-encounters-in-gold.md`
  summary: Malformed CSV rows (wrong field count) are padded or truncated under PERMISSIVE and never counted.
  evidence: Bronze dropped is always 0 by construction; counting bad input without values belongs to story 5.
- source_spec: `_bmad-output/specs/spec-patient-history-pipeline/stories/1-steel-thread-doctor-sees-encounters-in-gold.md`
  summary: Unhandled Spark exceptions print full tracebacks; unverified whether any path can carry row values (medium if true).
  evidence: Would be settled by story 5's test that no row values appear in logs or errors, run against forced Spark failures.

Updated 2026-10-14 (sprint-change-proposal-2026-10-14.md): removed the facility-whitespace and encounter-validity entries as obsolete; the remaining entries now apply to the provider daily activity pipeline (silver and gold writes; malformed bronze CSV rows belong to story 3).
- source_spec: `_bmad-output/implementation-artifacts/spec-1-steel-thread-provider-daily-activity.md`
  summary: Labs with no matching silver encounter and encounters with an unsupported timestamp offset (+05:30, fractional seconds, no offset) disappear without a per-reason count.
  evidence: Gold's inner lab join and parse_ts nulls drop them; only silver's total dropped is logged. Per-reason bad-input counts belong to story 3.
- source_spec: `_bmad-output/implementation-artifacts/spec-1-steel-thread-provider-daily-activity.md`
  summary: A missing bronze file stops silver with a Spark traceback after earlier sources were already rewritten.
  evidence: silver.main reads and writes each source in turn; the error names the path only. Pre-checking inputs is bad-input handling for story 3.
- source_spec: `_bmad-output/implementation-artifacts/spec-1-steel-thread-provider-daily-activity.md`
  summary: docs/hipaa-context.md line 15 reads "ld now falls under the Security Rule" instead of "Gold now ...".
  evidence: Uncommitted edit not made by this build; surfaced to the human to restore.
