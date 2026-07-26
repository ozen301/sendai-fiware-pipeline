# Sendai FIWARE Repository Guide

This guide is for contributors (human and AI) working on the Sendai
FIWARE pipeline. For the public-facing project description and
operator-facing docs, start at [README.md](README.md), which carries
the full documentation index.

Do not commit **excluded content**: credentials, private hostnames,
restricted reference material, `ref_docs/`, runtime output, logs,
state, metadata snapshots, token caches, or `.env` / `*.env` files.
Private local reference material may exist under `ref_docs/`
(gitignored, possibly absent in another checkout); ask the user for
specific documents if you need one.

## Repository Conventions

- Base conclusions on the current repository, its tests, and command
  output rather than memory or planning discussions.
- Preserve unrelated work and operational safeguards.
- AI agents must not commit without explicit authorization.
- Manage Python with [`uv`](https://docs.astral.sh/uv/) (`uv run`,
  `uv sync`, `uv add`, etc.); do not activate `.venv` directly.
- Keep dependencies in `pyproject.toml`.
- Put production code under `sendai_pipeline/` and operator CLI shims
  under `scripts/`.
- Keep implementation aligned with
  [docs/pipeline_spec.md](docs/pipeline_spec.md), the canonical data
  contract. The spec defines the contract; the code defines how it is
  implemented. If they disagree about the contract, resolve the defect
  deliberately and update both sides in the same change.

## Workflow

For new features and behavior changes, follow Spec → Tests →
Implementation → Validation → Drift prevention:

1. **Spec:** Establish the intended behavioral or architectural
   contract and obtain human review before implementation. If the user
   supplies a reviewed spec or plan, treat it as the starting contract.
2. **Tests:** Express the reviewed behavior in tests, including
   important boundaries and failure modes. Tests are the implementation
   contract.
3. **Implementation:** Implement against the reviewed spec and tests
   in a fresh context. Obtain human review before committing.
4. **Validation:** Run the relevant automated checks and inspect the
   resulting diff.
5. **Drift prevention:** When a change touches a documented contract
   (a column→attribute mapping, payload shape, filter rule, or
   entity-id convention), update `docs/pipeline_spec.md` in the same
   change.

### Validation

After changing code, tests, or configuration, run:

```sh
uv run ruff format .
uv run ruff check .
uv run pyright
uv run pytest -q
git diff --check
```

If the environment prevents a check, report the exact command and
reason instead of describing the change as fully validated. For a
documentation-only change, `git diff --check` plus verification of the
changed claims and references is sufficient unless the documentation
affects executable tooling.

### Cross-Agent Validation

Use cross-agent validation actively for complicated or high-stakes
work, not only for code. A plan or design for a critical or complex
change, a spec, a non-trivial refactor, or a hard diagnosis can go
through review as readily as an implementation. Documentation
explaining subtle behavior counts too: a read-only check of docstrings
and comments against the code catches inaccuracy and drift. The more
consequential or non-obvious the work is, the stronger the case for a
second model's review before committing to a direction. Prefer
cross-model validation when both Codex and Claude Code are available.
The user may waive cross-agent validation for a particular task.

The process uses three roles:

- **Author:** produces the artifact under review.
- **Reviewer:** critiques the artifact and returns findings.
- **Commander:** holds authority over the final hand-off to the human.
  This is usually the Author or Reviewer, not a separate third agent.

Two guardrails apply: never ask an agent to review its own artifact,
and avoid recursive agent delegation. A delegated Author or Reviewer
should not call another agent unless explicitly asked.

Review is **iterative, not single-pass**. Each round:

1. The Commander sends the artifact to the Reviewer; the Reviewer
   returns findings.
2. The Commander accepts, applies, or deliberately declines each
   finding, recording why when declining; the Author makes the edits.
3. The Commander sends the updated artifact back for the next round.

Continue the existing Reviewer session across rounds when one is
available, so it retains prior context instead of re-exploring the
repository. Keep re-review read-only.

Repeat until the Reviewer raises no new substantive issues. Keep the
number of rounds bounded to avoid trivial back-and-forth, but do not
set aside a material concern merely to make the review converge. If
findings do not settle, surface the disagreement to the user. The goal
is a robust artifact, not fast convergence.

Whenever sending diffs or files to another agent, send only the minimum
relevant source code, tests, documentation, and configuration. Never
send excluded content. If a diff mixes ordinary code with excluded
content, send only a minimized subset.

When working with Codex:

- Choose reasoning effort explicitly: **medium** for routine,
  well-understood tasks; **high** for non-obvious logic, debugging, API
  integration, or extra-care reviews; and **xhigh** for architecture,
  multi-step planning, or work with significant consequences if wrong.
- A Codex thread's sandbox is fixed when the thread is created
  (reasoning effort can be chosen per call): a read-only review thread
  that later needs to write cannot be resumed into write mode. Start a
  fresh write-capable thread.
- To continue a review across rounds, pass `--resume` in the request to
  the `codex:codex-rescue` subagent, or run `/codex:rescue --resume`.
  Do not resume by sending `SendMessage` to the wrapper: it cold-starts
  a new Codex session and discards the Reviewer's context. Invoking
  `codex:rescue` through the Skill tool hangs the session.

### Implementation Delegation

When delegating implementation to another agent, pass only the
reviewed test files (the implementation contract for this delegation),
relevant constraints (style, logging, configuration, library choices),
and the permitted edit area. Do not include your implementation
approach: independent authoring is the goal.

## Code Style

### Typing

- Annotate all function signatures on public APIs and module-level
  functions.
- Annotate local variables only when the type is not inferable.
- Do not add `from __future__ import annotations`; Python 3.13 supports
  built-in generics at runtime. Use string literals for rare forward
  refs.

### Docstrings and Comments

Write for a developer who has only the committed repository, not the
planning discussion. These principles also apply to documentation
files, apart from the docstring mechanics:

- Use Google-style docstrings for public modules, classes, and
  functions. Begin with what the code does; add the reason only when it
  helps the reader.
- Keep `Args` and `Returns` focused on the caller's contract. Document
  non-obvious collection shapes and private helpers, but omit internal
  steps that do not affect use.
- Explain subtle mechanisms in execution order; use a short concrete
  example when it clarifies the behavior.
- Verify claims about ordering, mappings, failure handling, and
  "always" or "never" against the implementation and tests. Describe
  current behavior, not change history. Re-check each claim when
  copying a docstring between parallel functions.
- Use precise, unambiguous, ESL-friendly prose. Read the whole passage
  once cold and remove context, repetition, or detail the reader does
  not need.

### Configuration

Environment-backed settings use the `XSettings` dataclass +
`from_env()` pattern from `sendai_pipeline/auth.py`: typed fields,
sensible defaults, and injected mapping support for tests. Do not
scatter bare `os.getenv()` calls. When you add a new env var, also
document it in [docs/configuration.md](docs/configuration.md) and
list it in `.env.example`.

### Logging

Library code uses module-level loggers:

```python
logger = logging.getLogger(__name__)
```

Never use `print`, `logging.basicConfig`, or library-installed
handlers. Entry points configure handlers via
`sendai_pipeline/logging_setup.py`.

Emit structured records with `extra={"event": "<name>", ...}` using
a stable event name. To find existing event names, grep the code for
`extra={"event":`; reuse one if it fits, and follow the same naming
style (`snake_case`, action-oriented, e.g. `post_succeeded`,
`window_partial`, `token_refresh_failed`) when introducing a new one.

Pick a level by severity: **DEBUG** for per-row diagnostics and no-op
decisions, **INFO** for successful lifecycle and post events,
**WARNING** for abnormal-but-recoverable states, **ERROR** for
terminal failures and exhausted retries. Grep `logger.<level>(`
near a similar existing event if you're unsure.

Use `logger.exception` inside `except` blocks. Never log secrets
(`Authorization` headers, bearer tokens, consumer key/secret, DB
password). `logging_setup.SecretsFilter` redacts known secret keys
and bearer tokens as defense-in-depth. See that module for the
current key/regex list. The contract is that secrets never reach a
log call in the first place; the filter is a backstop, not a
permission.

### Tests

- Use `pytest`, not `unittest.TestCase`.
- Test names: `test_<verb>_<condition>_<outcome>`.
- Use fake objects (`FakeSession`, `FakeAuth`, `FakeResponse`) instead
  of mocks.

The default suite must run without a live MySQL server, a live FIWARE
backend, or a `.env` file, so it is safe to run repeatedly. Committed
fixtures must be small and sanitized: no credentials, private
hostnames, or restricted reference content.

### Commit Messages

Use Conventional Commits format:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

Common types: `feat`, `fix`, `refactor`, `docs`, `style`, `test`, `chore`.
Add a body only when the reason is not obvious from the diff;
add footers for issue references (`Closes #123`), breaking change notices
(`BREAKING CHANGE: ...`), or other metadata. Simple commits need only the
subject line.
