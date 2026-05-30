# Sendai FIWARE Repository Guide

This guide is for contributors (human and AI) working on the Sendai
FIWARE pipeline. For the public-facing project description and
operator-facing docs, start at [README.md](README.md), which carries
the full documentation index.

Do not commit credentials, private hostnames, restricted reference
material, `ref_docs/`, runtime output, logs, state, metadata
snapshots, token caches, or `.env` / `*.env` files. Private local
reference material may exist under `ref_docs/` (gitignored, possibly
absent in another checkout); ask the user for specific documents if
you need one.

## Development Rules

- Python is managed with [`uv`](https://docs.astral.sh/uv/) (`uv run`,
  `uv sync`, `uv add`, etc.). Don't activate `.venv` directly.
- Dependencies live in `pyproject.toml`.
- Prefer production code under `sendai_pipeline/`; operator CLI shims
  go under `scripts/`.
- Keep implementation aligned with [docs/pipeline_spec.md](docs/pipeline_spec.md).
  When the spec and code drift, fix one or the other deliberately —
  don't let the drift persist.
- AI agents should not commit without explicit authorization. Leave
  changes for the user to review and commit manually.

## Workflow

New features follow Spec → Tests → Implementation → CI validation →
drift detection:

1. **Spec:** Write or update the behavioral/architectural contract.
   Human reviews before implementation.
2. **Tests:** Write tests from the spec. Tests are the implementation
   contract.
3. **Implementation:** Implement against reviewed tests in a fresh
   context. Human reviews before committing.
4. **CI validation:** Automated checks confirm the contract still
   holds.
5. **Drift detection:** Keep code and spec in sync as either changes.

### Cross-Agent Validation

Prefer cross-model validation when both Codex and Claude Code are
available: one agent authors (the **Author**), the other reviews
(the **Reviewer**), and a **Commander** accepts findings and hands
the post-review artifact to the human. The Commander is typically
the Author or the Reviewer — the role is about authority over the
final hand-off, not a third agent. Do not ask an agent to review
its own artifact, and avoid recursive review loops — a delegated
author or reviewer should not call another agent unless explicitly
asked.

When sending diffs or files between agents for review, send only
ordinary source code, tests, documentation, and configuration. Do
**not** send `.env` / `*.env`, credentials, private hostnames,
restricted reference material, `ref_docs/` content, runtime output,
logs, state, metadata snapshots, or token caches. If a diff mixes
ordinary code with excluded content, send only a minimized subset.

If Claude invokes Codex through `codex:rescue`, choose reasoning
effort explicitly:

- **medium:** routine tasks, straightforward edits, well-understood
  problems.
- **high:** non-obvious logic, debugging, API integration, or
  extra-care reviews.
- **xhigh:** architecture decisions, multi-step planning, or work
  with significant consequences if wrong.

When a Codex thread starts read-only (e.g. a review pass) and you
later need write access, use `--fresh --write`, not `--resume --write`.
The sandbox is fixed at thread birth — `--resume` inherits the
read-only sandbox of the dead thread and `apply_patch` will be
rejected. `--fresh` opens a new thread with the correct sandbox from
the start.

### Implementation Delegation

When delegating implementation to another agent, pass only the
reviewed test files (as the authoritative spec), relevant constraints
(style, logging, configuration, library choices), and the permitted
edit area. Do not include your implementation approach — independent
authoring is the goal.

## Code Style

### Formatting, Linting, and Type Checking

Use `ruff` for formatting and linting, and `pyright` for static type
checking.

```sh
uv run ruff format .
uv run ruff check .
uv run pyright
```

Configuration is in `pyproject.toml`. Run all three before reporting an
implementation complete; they must be clean.

### Typing

- Annotate all function signatures on public APIs and module-level
  functions.
- Annotate local variables only when the type is not inferable.
- Do not add `from __future__ import annotations`; Python 3.13 supports
  built-in generics at runtime. Use string literals for rare forward
  refs.

### Docstrings and Comments

- Use Google-style docstrings for public modules, classes, and
  functions.
- Add private-helper docstrings when purpose or behavior is not
  obvious.
- Do not reference `docs/` paths or section labels from production
  code, docstrings, comments, or runtime strings. Inline the actual
  invariant or rationale instead — docs paths drift.

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
and bearer tokens as defense-in-depth — see that module for the
current key/regex list. The contract is that secrets never reach a
log call in the first place; the filter is a backstop, not a
permission.

### Tests

- Use `pytest`, not `unittest.TestCase`.
- Test names: `test_<verb>_<condition>_<outcome>`.
- Use fake objects (`FakeSession`, `FakeAuth`, `FakeResponse`) instead
  of mocks.

The default suite must run with no live MySQL, FIWARE, or `.env`.
Gate any test that needs a real backend with
`@pytest.mark.integration` + `RUN_INTEGRATION_TESTS=1`. Committed
fixtures must be small and sanitized — no credentials, private
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
(`BREAKING CHANGE: ...`), or other metadata. Simple commits need only the subject line.
