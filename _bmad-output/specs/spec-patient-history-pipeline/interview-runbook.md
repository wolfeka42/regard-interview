# Interview runbook

## Session plan (60 minutes)

| Step | Minutes | What happens |
|---|---|---|
| 1. Introduce BMad | 5 | What BMad is; show the installed skills. |
| 2. Review the prepared plan | 5 (assumed) | Walk through the brief, this spec and the stories. |
| 3. Data review and course correction | 15–20 | Review the supplied data with BMad skills plus manual review; adjust the schema and plan via `bmad-help` and course correction. |
| 4. Build | 20–25 (assumed) | Implement the stories with `bmad-build`, including tests and the PHI review. |
| 5. Demo | 5 (assumed) | Run the pipeline and tests; show the gold table and the review result. |

## Cut order when behind

Stories form a steel thread (`stories.yaml`): story 1 alone produces a working gold table, and finishing it counts as success. Stories 2 to 4 are stretch; cut from the last story upward: 4, then 3, 2.

Never cut: story 1 (gold table; schema and content checks for each layer, hash, cancelled, retracted and multi-provider tests) and the PHI review.

## AI workflow to show

- BMad skills installed in the repo; the plan came from brief, spec and stories.
- Layered instructions: `CLAUDE.md` loads `AGENTS.md` (policy), which points to `_bmad-output/project-context.md` before any `.py` change.
- PHI/HIPAA review built into `bmad-build`, `bmad-code-review` and `bmad-review`, run by an independent reviewer.
- Changes after the data review go through `bmad-help` and course correction.
- Decisions logged in `.memlog.md` files; `ruff format` runs on every edited Python file.

## If the prepared repo is not allowed (assumed)

- Bring the brief, this spec folder and `docs/hipaa-context.md` as plain text.
- Reinstall BMad with `npx bmad-method install`; recreate `AGENTS.md` and the PHI review check from the saved copies, within step 1.
- If installing is impossible, paste the policy and code rules as the first prompt and run the PHI checklist as a manual review prompt.
