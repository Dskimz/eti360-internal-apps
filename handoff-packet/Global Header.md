# Global File Header Standard (AI Instruction)

## Task
Review all code files in the repository and add a standardized file header comment where missing.

---

## Header Format (Mandatory)

Add this at the **very top** of each non-trivial file, using the idiomatic comment style for the language.

TypeScript/JS example:

```ts
/**
 * Purpose: <one-sentence description of what this file does>
 * Scope: <app name or core/shared boundary>
 * Dependencies: <non-obvious imports only>
 * Notes: <constraints, side-effects, or invariants>
 */
```

Python example:

```py
# Purpose: <one-sentence description of what this file does>
# Scope: <app name or core/shared boundary>
# Dependencies: <non-obvious imports only>
# Notes: <constraints, side-effects, or invariants>
```
---

## Rules

- Max **3–5 lines** total.
- **Purpose** describes *what*, not how.
- **Scope** must clearly state ownership boundary.
- **Dependencies** list only important or cross-layer imports.
- **Notes** used only for mutability, side effects, or warnings.

---

## Do NOT Add Headers To

- `index.ts` files that only re-export.
- Auto-generated files.
- Config files under 20 lines (eslint, prettier, env).

---

## Editing Constraints

- Do **not** change runtime behavior.
- Do **not** refactor or rename code.
- Do **not** add inline comments elsewhere.
- Only add headers where missing.

---

## Output Requirements

- Preserve existing formatting.
- Preserve imports and spacing.
- One commit / PR per app folder.

---

## Safe Agent Execution (Important)

This task **may be run repo-wide** by a single AI agent **only** under the following conditions:

- The agent’s sole responsibility is header insertion.
- No feature work, refactors, formatting, or lint fixes are permitted.
- Changes are limited strictly to file header comment blocks.
- The work is performed on an isolated branch (e.g. `agent/global-headers`).
- The branch is reviewed and merged immediately after completion.

This task should be treated as a **codemod**, not collaborative development.

---

## Success Criteria

- Every eligible file has a header.
- Headers are consistent in structure.
- No functional diffs.

---

## Usage Tip

Run this instruction **per app folder** when possible.
Repo-wide execution is acceptable **only** for header insertion.
