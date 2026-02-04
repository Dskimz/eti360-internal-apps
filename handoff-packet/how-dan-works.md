# How Dan Works (for AI agents)

Dan is building internal apps quickly and iterating in production-like environments. The best outcomes happen when the agent is explicit, step-by-step, and avoids surprises.

## High-signal collaboration style

- Prefer **short, executable instructions** over long explanations.
- When Dan asks “what do I paste”, respond with **one copy/paste terminal block**.
- Use **absolute URLs** and **exact endpoint paths** (no “click around”).
- When something is time-sensitive or ambiguous (e.g., “today”, “latest”), use **concrete dates**.
- Assume Dan is comfortable with outcomes, not implementation details. Explain only what’s necessary to unblock the next action.

## Guardrails

- Never ask Dan to paste secrets (API keys). Use Render env vars for secrets.
- If Dan pastes a key by accident, instruct to rotate it and remove it from local files/history.
- Avoid destructive commands unless explicitly requested.

## Repo + deployment workflow (important)

Dan often has two copies:

- **Working copy (non-git):** `/Users/danskimin/ETI360-Branding/eti360-internal-apps` (where Codex edits happen)
- **Git repo (deploy source):** `~/Desktop/ETI360/eti360-internal-apps` (the clone that gets committed/pushed to trigger Render deploys)

So the usual flow is:

1) Copy updated files from ETI360-Branding into the Desktop git repo (`cp ...`).
2) `git add -A && git commit -m "..." && git push`
3) Render auto-deploys (or manual deploy).

If Dan says “I pushed but nothing changed”, verify he committed/pushed from the Desktop clone, not the ETI360-Branding folder.

## Render workspace confusion

Dan may have multiple Render workspaces (e.g., ETI360 vs TripRisk360). If services “disappear”, it’s usually the wrong workspace selected.

## Preferred debugging inputs

- For UI: browser DevTools console + Network tab screenshots, plus the URL.
- For backend: the last ~30 lines of Render logs around the error.
- For DB: use the built-in schema browser (`/db/ui`) rather than raw psql unless necessary.

## When proposing changes

- Make sure changes are consistent with the internal UI style guide (calm, neutral, whitespace; borders `#F2F2F2`, background `#F5F6F7`).
- Keep endpoints stable; prefer adding new endpoints over breaking existing ones.
- Add DB tables in schemas:
  - app-specific: `weather.*`, etc.
  - shared: `ops.*` for auth/usage/prompts
- When a workflow uses LLMs, always ensure it logs:
  - run timestamp (UTC) in `ops.llm_runs`
  - per-provider/prompt tokens in/out/total + cost in `ops.llm_usage`
  - attribution via `prompt_key` so `/prompts/ui` can compare prompt cost over time

## Global header codemod (only when explicitly requested)

Dan may provide a repository-wide header standard in `handoff-packet/Global Header.md`.

- Treat this as a **codemod** task (header insertion only), not feature work.
- Do it on an isolated branch and avoid behavior changes, refactors, or formatting.
