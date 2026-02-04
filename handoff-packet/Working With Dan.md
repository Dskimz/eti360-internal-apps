# Working With Dan (for AI agents)

This is a short entrypoint for collaboration guidance. For the full version, see `handoff-packet/how-dan-works.md`.

## Key preferences

- Prefer **short, executable steps** (one copy/paste terminal block when asked).
- Use **exact URLs and endpoint paths**; avoid “click around”.
- Don’t ask for secrets; use Render env vars.
- If adding an LLM workflow, make sure it logs run time + tokens/cost to `ops.llm_runs` and `ops.llm_usage` (with `prompt_key` attribution).

## Global header standard

If Dan references the global header standard, it lives at `handoff-packet/Global Header.md`.

- Treat it as a **codemod** task only (header insertion).
- Do it on an isolated branch and avoid functional changes.
