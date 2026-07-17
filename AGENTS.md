# Sendai FIWARE Repository Guide

This guide is for contributors (human and AI) working on the Sendai
FIWARE pipeline. For the public-facing project description and
operator-facing docs, start at [README.md](README.md), which carries
the full documentation index.

Do not commit **excluded content**: credentials, private hostnames,
restricted reference material, `ref_docs/`, runtime output, logs,
state, metadata snapshots, token caches, or `.env` / `*.env` files. Private local
reference material may exist under `ref_docs/` (gitignored, possibly
absent in another checkout); ask the user for specific documents if
you need one.

## Development Rules

- Python is managed with [`uv`](https://docs.astral.sh/uv/) (`uv run`,
  `uv sync`, `uv add`, etc.). Don't activate `.venv` directly.
- Dependencies live in `pyproject.toml`.
- Prefer production code under `sendai_pipeline/`; operator CLI shims
  go under `scripts/`.
- Keep implementation aligned with [docs/pipeline_spec.md](docs/pipeline_spec.md),
  the canonical data contract. The spec is authoritative for that data
  contract; the code is authoritative for *how* it is implemented. If
  they disagree on the contract itself, treat it as a defect: decide
  deliberately which side is right and fix the other, rather than
  letting the drift persist.
- AI agents should not commit without explicit authorization. Leave
  changes for the user to review and commit manually.

## Workflow

New features follow Spec → Tests → Implementation → CI validation →
drift prevention:

1. **Spec:** Write or update the behavioral/architectural contract.
   Human reviews before implementation.
2. **Tests:** Write tests from the spec. Tests are the implementation
   contract.
3. **Implementation:** Implement against reviewed tests in a fresh
   context. Human reviews before committing.
4. **CI validation:** Automated checks confirm the contract still
   holds.
5. **Drift prevention:** When a change touches a documented contract
   (a column→attribute mapping, payload shape, filter rule, or
   entity-id convention), update `docs/pipeline_spec.md` in the same
   change so code and spec do not silently diverge. Human or cross-agent
   review should verify they still agree.

### Cross-Agent Validation

Use cross-agent validation actively for complicated or high-stakes
work, not only for code. A plan or design for a critical or complex
change, a spec, a non-trivial refactor, or a hard diagnosis can go
through review as readily as an implementation. Documentation
explaining subtle behavior counts too: a read-only check of docstrings
and comments against the code catches inaccuracy and drift. The more
consequential or non-obvious the work is, the stronger the case for a
second model's review before you commit to a direction. Prefer cross-model
validation when both Codex and Claude Code are available.

The process uses three roles:

- **Author:** produces the artifact under review.
- **Reviewer:** critiques the artifact and returns findings.
- **Commander:** holds authority over the final hand-off to the human.
  This is usually the Author or the Reviewer, not a separate third
  agent.

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
available, so it keeps prior context instead of re-exploring the repo;
re-review stays read-only, so resuming is safe.

Repeat this process until the Reviewer raises no new substantive
issues. Converge in a bounded number of rounds (to cut trivial
back-and-forth, not real concerns); if findings are not settling, stop
and surface the disagreement to the human rather than looping
indefinitely. The goal is a robust artifact, not fast convergence.
Never set aside a material concern such as a better alternative or an
unresolved risk to make the review settle sooner, even if it arrives
late or reopens a settled question.

Whenever you send diffs or files to another agent, send only ordinary
source code, tests, documentation, and configuration. Do **not** send
any of the excluded content listed above. If a diff mixes ordinary
code with excluded content, send only a minimized subset.

When working with Codex:

- Choose reasoning effort explicitly: **medium** for routine,
  well-understood tasks; **high** for non-obvious logic, debugging,
  API integration, or extra-care reviews; **xhigh** for architecture
  decisions, multi-step planning, or work with significant
  consequences if wrong.
- A Codex thread's sandbox is fixed when the thread is created
  (reasoning effort can be chosen per call): a read-only thread (e.g. a
  review pass) that later needs to write can't be resumed into write
  mode — start a fresh write thread.
- To continue a review across rounds, invoke `codex:codex-rescue` with
  `--resume` (not `SendMessage` to the wrapper, which cold-starts a fresh
  Codex session each round).

### Implementation Delegation

When delegating implementation to another agent, pass only the
reviewed test files (the implementation contract for this delegation),
relevant constraints (style, logging, configuration, library choices),
and the permitted edit area. Do not include your implementation
approach: independent authoring is the goal.

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

Write for a developer seeing this code for the first time, who does
not hold the design in their head and has only the committed
repository. Avoid terms and context that come from planning documents
or discussions the reader cannot see (e.g. agent sessions); the
goal is not to record all you know but to move that reader to
understanding by the shortest clear path.
Before calling a docstring or comment done, read it once cold: does it
state what the code does up front, follow a logical order, and say only
what the reader needs? If not, revise, judging the whole docstring or
comment rather than a single phrase.

These principles, apart from the code-specific mechanics below, apply
to documentation files as well.

- Use Google-style docstrings for public modules, classes, and
  functions. State what the code does first; let the "why", if
  necessary, follow as a short note. Do not dwell on what the code must
  *not* do.
- For subtle logic, explain the mechanism step by step, not just
  the conclusion; a concrete example often helps. Stay within this
  code's behavior; stating unrelated paths or another product's behavior
  pulls the reader off track.
- Match the level of detail to the reader: `Args`/`Returns` are the
  caller's contract, not the internal step sequence — cut anything that
  doesn't affect how the code is used. Document a `dict`/`list`/tuple
  shape (with a short example) or a private helper when it is not already
  obvious.
- Keep claims accurate: details such as execution order, key order,
  input→output mappings, and any "always/never" must match the code.
  Verify, don't recall. Re-check each claim when copying a
  docstring between parallel functions. Avoid absolutes unless literally
  true, and describe current behavior, not change history.
- Keep references and terms unambiguous: the reader should be able to
  tell what every name points to; no word should carry a second reading
  in context; metaphors should not need decoding; and pronouns and
  connectives ("because", "so") should resolve and hold.
- Keep prose ESL-friendly (clear structure, accessible wording) for a
  technically fluent reader (terms used directly, no padding); a longer
  well-structured sentence is fine, and precise terms beat vaguer ones.
  Reference a `docs/` file when it genuinely helps.

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

The default suite must run with no live MySQL, FIWARE, or `.env`.
Gate any test that needs a real backend with
`@pytest.mark.integration` + `RUN_INTEGRATION_TESTS=1`. Committed
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
