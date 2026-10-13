---
title: "Product Brief: Patient History Pipeline"
status: final
created: 2026-10-12
updated: 2026-10-12
---

# Product Brief: Patient History Pipeline

## Executive Summary

A basic PySpark pipeline that turns raw healthcare data into an analysis-ready gold table that doctors use to review an individual patient's health history. It runs on AWS EMR and moves data through three layers: bronze (raw), silver (cleaned) and gold (doctor-facing).

It is built live in a one-hour interview for Regard. The brief to the candidate is to follow data engineering and HIPAA best practices, with AI assistance encouraged. The build therefore has two goals of equal weight: a correct, defensible pipeline, and a visible demonstration of AI-assisted development using BMad skills, project instructions and an automated PHI review.

## The Problem

Doctors reviewing a patient need that patient's history in one place and in time order: visits, labs, diagnoses and medications, with exact dates so they can see what changed and when. Raw source data is not fit for this. It is inconsistently formatted, carries identifiers that need not be exposed, and has no stable key that joins one patient's records across sources.

## Who This Serves

- **Doctors** reviewing their own patients' histories. They need the full timeline and the patient's age at each encounter. They do not need the MRN, address, contact details or birthday in the table itself.
- **The interviewer**, who judges data engineering quality, HIPAA judgment and how the AI is used.

## The Solution

- **Bronze:** raw data landed as-is with an explicit schema; no inference.
- **Silver:** cleaned and typed; still holds full PHI. Computes `age_at_encounter` from DOB and the service date.
- **Gold:** one table keyed by `patient_key_v1` with full service dates, ready for per-patient history review. `[ASSUMPTION]` one row per clinical event (encounter, lab, diagnosis, medication); the real grain depends on the raw data supplied on the day.

Every transform is a pure function (DataFrame in, DataFrame out) with an explicit `StructType` schema.

## HIPAA Position

- **Gold is PHI, on purpose.** Full service dates are identifiers under Safe Harbor, so gold is not de-identified. That is permitted: doctors reviewing their own patients is treatment use (45 CFR 164.506). Basis: `docs/hipaa-context.md`.
- **Gold drops** names, SSN, MRN, phone, email, street, city and date of birth. ZIP is cut to 3 digits; ages over 89 become 90+. Dropping DOB costs doctors nothing, since they need age, and it removes the strongest re-identifier.
- **Patient key:** HMAC-SHA256 over `facility:mrn` (normalized). The key comes from an environment variable, backed by AWS Secrets Manager or KMS in production. The key is deterministic and versioned in the column name, and the pipeline fails at startup without it. Doctor lookup by MRN belongs in an app that holds the key; it is mentioned, not built.
- **No PHI in logs**, prints or exception messages. **Test data is synthetic.**
- **Production controls, named but not built:** care-team-only access, read audit logging, encryption at rest and in transit, and an AWS BAA with EMR used as a HIPAA-eligible service.

## Success Criteria

`[ASSUMPTION]` drawn from the assignment's own wording:

- **Data engineering:** the pipeline runs end to end on synthetic data and produces the gold table. Schemas are explicit and transforms pure. Tests pass.
- **HIPAA:** tests prove the gold schema has no MRN or DOB, the patient key is valid, and a missing key fails fast. The PHI review finds no gaps.
- **AI use:** the interviewer sees the AI working under project rules, and sees the automated PHI review catch or confirm issues.
- **Defensible:** every design choice above can be explained in one sentence.

## 60-Minute Scope

**Failure line:** no gold table produced by running PySpark code at 60 minutes. A design without code does not meet the assignment.

**Prepared before the session:** this brief, a spec (`bmad-spec`, no PRD), the architecture, and stories with acceptance criteria. The live hour adapts that design to the real data and builds it.

| Step | Minutes | What happens |
|---|---|---|
| 1. Introduce BMad | 5 | What BMad is; show the installed skills. |
| 2. Review the prepared plan | 5 `[ASSUMPTION]` | Walk through the brief, spec and acceptance criteria. |
| 3. Data review and course correction | 15–20 | Review the supplied data with BMad skills plus manual review; adjust the plan via `bmad-help` and course correction. |
| 4. Build | 20–25 `[ASSUMPTION]` | Via `bmad-help`: implement the stories with `bmad-build`, including tests and the PHI review. |
| 5. Demo | 5 `[ASSUMPTION]` | Run the pipeline and the tests on synthetic data; show the gold table and the review result. |

**Input design** `[ASSUMPTION]`: source-agnostic. Bronze reads whatever arrives behind a swappable `StructType` schema. The prepared design uses a generic shape (patients, encounters, labs, diagnoses, medications as CSV or JSON), so on the day mostly the schema and column mapping change.

**Cut first when behind** `[ASSUMPTION]`: extra source tables (keep one source through to gold), then non-PHI tests, then talk about production controls. Never cut the gold table, the patient-key and no-PHI tests, or the PHI review.

## AI-Assisted Workflow

- **BMad installed in the repo** (`.claude/skills/`, `_bmad/`). The plan was built with BMad skills: brief, then `bmad-spec`, then architecture and stories. `bmad-help` picks each next step.
- **Layered project instructions**, so the AI follows the HIPAA rules without being told each session: `CLAUDE.md` loads `AGENTS.md` (policy), which points to `_bmad-output/project-context.md` (code rules) before any `.py` change.
- **Automated PHI review**: a custom check (`_bmad/custom/phi-review-lens.md`) built into `bmad-build`, `bmad-code-review` and `bmad-review`. It runs as an independent reviewer with no shared context.
- **Change handling live**: when the real data differs from the plan, the plan changes through `bmad-help` and course correction, not ad hoc.
- **Audit trail**: decisions are logged as they are made (`.memlog.md`), so every design choice has a recorded reason.
- **Formatting hook**: `ruff format` runs automatically on every edited Python file.

## If the Prepared Repo Is Not Allowed

`[ASSUMPTION]` pending the recruiter's answer:

- Bring the planning documents as plain text: brief, spec, architecture, stories and `docs/hipaa-context.md`.
- Reinstall BMad at the start with its installer (`npx bmad-method install`), then recreate `AGENTS.md` and the PHI review check from the saved copies. Budget this time out of step 1.
- If installing is not possible, paste the policy and code rules into the session as the first prompt and run the PHI checklist as a manual review prompt.

## Open Questions

- The raw input schema and format are unknown until the interview.
- Whether the prepared repo may be brought in is unconfirmed; the recruiter is being asked.
- Session minutes for steps 2, 4 and 5 and the cut list are assumptions, not decisions.
