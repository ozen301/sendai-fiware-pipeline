# Operator Data Tools Spec

This spec covers four operator-facing capabilities requested for the
Sendai FIWARE pipeline:

1. **Resend** data of a specified date range and/or place.
2. **Show** data of a specified date range and/or place.
3. **Delete Comet** history.
4. **Delete Orion** entities.

All four are operator-driven (never cron). All four follow the same
project conventions: `uv run python scripts/<tool>.py`, dry-run by
default, `--send` to perform live writes, structured logs to
`logs/<tool>.log`, no shared mutable global state, secrets never logged.

For the wider context (pipeline workflow, existing tools), see
[overview.md](overview.md) and
[tools_and_troubleshooting.md](tools_and_troubleshooting.md).

## Naming and source-of-truth conventions

These conventions are shared by all four tools below.

**Entity spec.** Same shape as `create_entities.py`:
`ENTITY_ID[:ENTITY_TYPE]`. Canonical Sendai ids such as
`jp.sendai.Blesensor.per3600.105` infer `ENTITY_TYPE` from the id.
When inference is unavailable, the operator must pass `:ENTITY_TYPE`;
tools that expose `--type` also accept it as the default/override.

**Place shorthand.** Several tools accept `--place N` (e.g. `105`)
and/or `--interval-min {5,60}`. The tool resolves the place number via
`metadata/sensors.csv` using the same `load_metadata` +
`index_by_place_interval` path the runners use. Tools that can safely
show both intervals may make `--interval-min` optional; tools that
publish or delete one interval still require a resolved interval. If
the operator supplies both `--place` and explicit entity ids,
`--place` is rejected with a config error (to prevent ambiguity).

**Date range.** `--from` and `--to` accept either a source window key
(`YYYYMMDD_HHMM`, JST) or an ISO-8601 timestamp. Bounds are inclusive
on both ends. Each tool documents which form it expects.

**Dry-run.** Default. No live writes, no live deletes. Prints the
planned action(s) and exits 0. `--send` flips to live mode.

**Idempotency.** Every tool must be safe to re-run. `409`/`422` (already
exists) on create, `404` on delete, and "no rows touched" on backfill
are all treated as no-ops, logged at INFO, and counted in the run
summary.

---

## 1. Resend: `scripts/resend.py`

### Goal

Re-publish source-windows in a specified date range, optionally
narrowed to a specific place or set of places, by replaying the
runner-internal `_process_send_window` code path for windows with
source rows.

### Note

`resend.py` can be used for resending one or more windows.
Pass the same value to `--from` and `--to` to resend exactly one window:

```sh
uv run python scripts/resend.py flow \
    --interval-min 60 \
    --from 20260524_1000 --to 20260524_1000 \
    --reason "payload fix" --send
```

### CLI

```
uv run python scripts/resend.py {flow|direction}
    [--interval-min {5|60}]
    --from YYYYMMDD_HHMM
    --to   YYYYMMDD_HHMM
    [--place N [--place N ...]]
    [--entity-id ID [--entity-id ID ...]]
    --reason "..."
    [--force]
    [--send]
```

| Flag | Required | Purpose |
|---|---|---|
| `flow` / `direction` | yes | Which product. |
| `--interval-min` | conditional | `5` or `60`. Required for `--place` or unfiltered runs. Optional with canonical `--entity-id`, where all ids must infer the same interval. A single invocation publishes one interval. |
| `--from`, `--to` | yes | Source-window keys, JST, inclusive. |
| `--place` | no | Place number filter. Repeatable. Resolved via metadata. |
| `--entity-id` | no | Explicit entity id. Repeatable. Mutually exclusive with `--place`. |
| `--reason` | yes | Audit string; written to every log record this run emits. |
| `--force` | no | Bypass the per-target skip. Default: skip any target already `ok` in state (whether its payload matches or has drifted); see the `--force` Safety note for why a uniform backfill needs it. |
| `--send` | no | Live writes. Default: dry-run. |

### Behavior

1. Resolve the interval before window validation: explicit
   `--interval-min` wins, canonical `--entity-id` can infer it, and
   `--place`/unfiltered runs require the flag. Then validate args
   (range is non-empty and aligned to interval; place/entity-id mutual
   exclusion; `--reason` non-empty). There is no age cap on the range.
2. Enumerate every source window between `--from` and `--to` at
   `--interval-min` step.
3. In `--send` mode, prepare the run and then process each window:
   - **Dead-letter pre-flight (before opening the MySQL connection).** Load
     the state store and check the enumerated windows: if any is already
     `dead_letter` in state, abort the whole run (exit 2), listing every
     offending window key, before any source-row select or Orion POST. A
     dead-letter records a deliberate operator decision that a window is
     unrecoverable; refusing up front (rather than aborting mid-run when
     `begin_window_attempt` first reaches such a window) avoids wasted work
     and names all blockers at once (clear them with `state_repair.py`, or
     narrow the range, then re-run). The scan covers every enumerated key
     regardless of source rows, so a dead-letter window with no source rows
     (which the per-window step below would otherwise skip silently) is
     still reported.
   - **No-source-row skip.** For each enumerated window, select its source
     rows; if a window has no source rows, skip it before calling
     `_process_send_window`: no `begin_window_attempt`, no state entry, no
     save, and no cadence-counter advance. This matches the live runners,
     which only process windows that have source rows. For direction this
     prevents permanent `partial` growth from empty windows; for flow it
     avoids re-touching an existing window that the live path would never
     revisit.
   - **Dry-run is unchanged:** it runs no pre-flight, makes no MySQL query,
     cannot know which windows are empty, and keeps printing the planned
     range.
4. For each window with source rows, call the same
   `_process_send_window` the cron runners use, but with
   **`interval_metadata` filtered to the
   place/entity-id selection** so the transform step builds payloads
   only for the requested targets. For Product A, completion state still
   follows the normal observed-target rule (`stored ∪ observed`) and a
   filtered resend never shrinks existing `expected_target_ids`. For
   Product B, a new resend-created window uses the filtered fixed-target
   set; an existing window keeps its stored first-attempt snapshot. (The
   transforms iterate `interval_metadata`/the metadata index when
   constructing payloads; filtering state bookkeeping alone would still
   POST to every active target, which is the opposite of what `--place`
   means to an operator.)
5. `--force` flips the per-target skip: pass a flag through
   `_process_send_window` that causes the hash-skip check to be
   bypassed for this run. (Implementation detail: add a
   `force_resend: bool = False` kwarg to `_process_send_window` in both
   `run_flow.py` and `run_direction.py`; default `False` preserves
   today's behavior.)
6. Dry-run prints, per window, the planned `(window_key, target_count,
   skipped_by_hash, would_post)` tuple and exits **before any MySQL
   query, Orion auth token fetch, or Orion HTTP call** (no live side
   effects, no credentials required in dry-run).
7. **State persistence cadence (`--send`).** Resend does **not** persist
   state after every target POST. The shared `_process_send_window`
   rewrites the whole state file on each target by default. This gives
   durability for the cron runners' small rolling lookback, where the
   cost is negligible. Across a multi-day resend that same per-target save
   rewrites a large (production: ~16 MB) state file once per target:
   O(windows × targets) full rewrites, which dominate runtime. Resend
   instead suppresses the per-target save and flushes on a coarse
   per-window cadence: it persists once every `_RESEND_SAVE_EVERY`
   processed windows (module constant, default `100`) and once more at
   the end of the run when any processed window or final GC removal
   remains unflushed. The cadence counter
   advances once per processed window with source rows (including a
   window whose source rows are filtered to zero POST targets, which is
   still processed and can still become `partial`), so any in-memory
   state change such a window makes is flushed by the cadence rather than
   left stranded until a later window's save. A no-source-row window is
   skipped before processing and does not advance the cadence counter.
   (Implementation detail: add a `persist_each_target: bool = True` kwarg
   to `_process_send_window` in both `run_flow.py` and `run_direction.py`,
   guarding the in-loop `state_store.save()`; default `True` preserves
   the cron runners' per-target durability. Resend passes `False` and
   owns the cross-window flush cadence. Suppressing the per-target save
   changes only *when* the store is written to disk, not *what* it
   contains: the same `record_target` / `recompute_status` mutations the
   per-target-save path performs still occur, so the final on-disk state
   is identical to that path for a completed run.) `_RESEND_SAVE_EVERY` is an internal I/O-batching knob,
   not deployment-tuned configuration, so it is a module constant rather
   than an env var.
8. **State GC (`--send`).** Resend reclaims complete windows older than
   the same horizon the live runners use:
   `run_started_at − 2×max(MAX_LOOKBACK_HOURS_PER300,
   MAX_LOOKBACK_HOURS_PER3600)`. Capture `run_started_at` once near the
   top of the run and reuse it for every GC call; do not recompute a
   moving `datetime.now()`. Compute the horizon over both intervals
   because the shared state file holds both `per300` and `per3600`
   windows, and apply the same non-negative validation the live settings
   objects apply to both lookback values. Run
   `gc_complete_before(cutoff)` before each cadence `store.save()` and
   once before the final flush. Because `gc_complete_before` mutates only
   the in-memory store, the final flush runs when the cadence counter is
   greater than zero or when the final GC removed any entries. GC is
   unconditional in `--send` mode, has no separate `--gc` flag, and does
   not run in dry-run. It removes only complete windows older than the
   cutoff; partial failed deliveries and any window inside the horizon
   remain in state. Resent windows older than the horizon are reclaimed
   on exit, so they do not linger in `state_doctor`/`show` output.
9. **DB reconnect on the per-window select (`--send`).** Resend opens one
   MySQL connection at run start and reuses it for every per-window
   select. Over a multi-hour run that connection can be dropped
   server-side (an idle `wait_timeout`, a brief network partition, or a
   server failover). When a per-window select fails because the
   connection was lost, resend reopens the connection and retries the
   select, rather than aborting the whole run on the first transient
   blip. Specifically:
   - **Only a genuine connection loss triggers a reconnect.** The retry
     fires for `pymysql.err.InterfaceError` (socket already closed) or
     `pymysql.err.OperationalError` whose MySQL error code is `2006`
     (`CR_SERVER_GONE_ERROR`) or `2013` (`CR_SERVER_LOST`). Every other
     database error (a bad column, a programming error, a data error)
     re-raises immediately and aborts the run; a bare `OperationalError`
     catch would wrongly mask those. (Implementation detail: the
     predicate lives in `db.py` as `is_connection_lost_error(exc)`,
     beside the 2006/2013 codes the selector tests already pin, so
     `scripts/resend.py` does not import `pymysql` directly.)
   - **Reconnect means a fresh connection, not `ping(reconnect=True)`**
     (deprecated in PyMySQL). On a caught connection-loss error resend
     closes the dead handle defensively (a close that itself raises on a
     dead socket is swallowed, so it never replaces the real error),
     opens a new connection through the same path used at run start, and
     retries the select. A fresh connection is safe because each select
     is self-contained: `connect` uses `autocommit`, there is no open
     transaction or session state to carry across, and the new
     connection re-applies all connection settings.
   - **Retries are bounded.** Resend reconnects at most
     `_RESEND_DB_RECONNECT_ATTEMPTS` times per select (module constant,
     default `2` → 3 total tries) with a 1 s fixed backoff between
     attempts. When the attempts are exhausted, resend re-raises, so a
     **sustained** outage still aborts the run with exit 2, the same
     terminal behavior as before this guard. `_RESEND_DB_RECONNECT_ATTEMPTS`
     and the backoff are internal reliability knobs, not deployment-tuned
     configuration, so they are module constants rather than env vars.
   - **Scope is the select only.** The reconnect wraps the per-window
     source-row select, not the Orion POST path; POST failures keep their
     existing handling. The live cron runners (`run_direction` /
     `run_flow`) are unaffected: each cron process opens its own
     connection, does its work in a few minutes, and exits, so a dropped
     connection simply fails one tick and the next tick reconnects; they
     do not need and must not get this in-run retry, which is why it lives
     in resend rather than in the shared `db.select_*` helpers.

### Safety

- **A long `--send` run starves the live cron like downtime.** The same
  per-product lock the cron runners take is held (blocking `LOCK_EX`) for
  the whole invocation. While it is held, every live 5-minute tick for
  that product takes the lock non-blocking, fails, and no-ops, so a long
  resend is operationally **equivalent to live-cron downtime** for its
  duration. Live windows that elapse during the hold are **permanently
  unpublished** once they age past the live runner's lookback. With no
  older open window to widen it, that lookback is the **reprocess floor**
  (`REPROCESS_HOURS_PER300` / `REPROCESS_HOURS_PER3600`, default 2h / 12h),
  **not** the 72h `MAX_LOOKBACK_HOURS_*`; dynamic lookback only widens for
  windows already in state, and these were never seen. Before a long run,
  follow "I need to resend a large range without dropping live data" in
  [tools_and_troubleshooting.md](tools_and_troubleshooting.md) (chunk with
  gaps, or run as a maintenance window and apply the downtime recovery).
  The lock is released on exit (incl. exception).
- **STH-Comet caveat:** re-POSTing a value creates a new Comet history
  row even when the Orion value is unchanged. `--force` makes this
  explicit; without it, targets whose stored payload hash matches the
  new payload are skipped.
- **`--force` is required for a *uniform* backfill.** Without it, the
  per-target skip is applied by each target's stored status: a target that
  is already `ok` is skipped (whether its payload matches or has drifted),
  while a target with no stored record (for example one whose window was
  already reclaimed by GC) is posted. Across a range wider than the live
  GC horizon this is **non-uniform**: recent in-state windows are skipped
  (no new Comet row) while older reclaimed windows are re-posted (a new
  Comet row), yet the run still exits 0 and looks uniform. Pass `--force`
  whenever the intent is a uniform re-publish of the whole range.
- **Exit 0 does not imply every window is `complete`.** The run exits 1
  only when a POST fails. Two no-failed-POST cases still leave a window
  short of `complete`, and neither is a delivery failure (both signal a
  configuration or source-data gap a bare re-run will not fix), so neither
  changes the exit code:
  - An expected target with source rows but **no payload** (its rows were
    filtered out, or it had no data this window): the window finishes
    `partial`, emits a per-window `window_partial` WARNING, and is counted
    in the `resend_summary` `windows_partial` field.
  - A **new flow window whose effective target set is empty**: it returns
    before any state update, so it posts nothing, emits no `window_partial`
    WARNING, and is counted as neither `windows_partial` nor
    `windows_complete`. Watch for it via the per-window
    `resend_window_processed` records (zero posts) rather than the
    aggregate counts.
- **A dead-letter window in range aborts the run (exit 2).** The dead-letter
  pre-flight (Behavior step 3) refuses the run up front, before any POST,
  and lists every offending key. Clear them with `state_repair.py` or
  narrow the range, then re-run.
- **No age cap on the range.** Resend publishes exactly the `--from` /
  `--to` range you name; there is no `MAX_LOOKBACK_HOURS_*` ceiling on how
  far back it may reach. The guardrail against an over-wide replay is
  dry-run (the default): it prints one line per planned window (each with
  its target count and would-post count) before any live write, so you can
  gauge how large the range is before adding `--send`.
- Empty source-row windows are skipped before state mutation. This
  prevents permanent `partial` accumulation for windows the live runners
  would never create.
- **Durability tradeoff of the coarse save cadence.** Because state is
  flushed every `_RESEND_SAVE_EVERY` processed windows rather than after
  each target (see Behavior step 7), a true crash or hard kill
  (SIGKILL, power loss) can lose at most `_RESEND_SAVE_EVERY` windows of
  *recorded* delivery progress. A handled abort, such as exhausted DB
  reconnects, best-effort flushes completed windows before exit; if that
  flush fails, the original error still decides the exit code. A re-run
  re-POSTs only progress that did not reach disk, creating extra STH-Comet
  rows. This is already the expected cost of a resend (see the STH-Comet
  caveat above), and is bounded by the cadence and by chunked operation. Because
  resend suppresses the per-target save, every cadence flush follows a
  full `recompute_status`, so a hard kill cannot leave a flushed window
  with recorded targets but a stale aggregate status on disk; it only drops
  the unflushed in-memory windows, which the re-run reprocesses. (This
  applies to resend; the live runners keep per-target saves and are
  unaffected by this reasoning.)
- **A hard kill can orphan a `.tmp` file.** `save()` writes a uniquely
  named temporary file and renames it over the state file atomically
  (`os.replace`), so after any normal process failure the on-disk state is
  intact. A `SIGKILL` between the write and the rename can leave a
  `state/.<product>.json.*.tmp` file behind; it is never read and is safe
  to delete.
- In-resend GC (see Behavior step 8) bounds mid-run state growth and
  returns complete old windows to the live runner's baseline on exit.
- **A transient DB blip no longer aborts a long run.** A dropped
  connection on a per-window select is recovered by a bounded reconnect
  (see Behavior step 9), so a single network or server hiccup mid-resend
  does not throw away the chunk. Only a **sustained** outage (reconnects
  exhausted) still aborts with exit 2; the recovery is to re-run the
  chunk with `--force` (duplicate STH-Comet rows are the expected resend
  cost). A repeated `resend_db_reconnect` storm in the log signals the DB
  itself is unhealthy, not a one-off blip.

### Log events

- `resend_requested` (run start; carries args + resolved entity ids).
- `resend_window_processed` (per processed source-row window; status
  counts).
- `resend_window_empty` (DEBUG; per skipped no-source-row window).
- `resend_gc` (INFO; carries product, interval, reason, GC cutoff, and
  reclaimed-window count).
- `resend_db_reconnect` (WARNING; per reconnect attempt on a dropped
  connection; carries product, interval, reason, the window key, attempt
  number, and the error class, never the DSN or password).
- `resend_db_reconnect_exhausted` (ERROR; `logger.exception` when the
  bounded reconnect attempts are exhausted, just before the run aborts;
  carries product, interval, reason, the window key, and attempt count so
  the operator knows which chunk to re-run).
- `resend_summary` (run end; includes `windows_empty` for skipped
  no-source-row windows, `windows_gc` for complete windows reclaimed by
  resend GC, and `windows_partial` / `windows_complete` for the windows
  whose aggregate status was recomputed. A new flow window with an empty
  effective target set returns before recomputation and is in neither count;
  see the exit-code note under Safety).

### Tests

- `test_resend_dry_run_prints_planned_windows_no_orion_calls`
- `test_resend_range_loops_window_keys_inclusive_bounds`
- `test_resend_place_filter_resolves_via_metadata`
- `test_resend_force_bypasses_hash_skip`
- `test_resend_rejects_place_and_entity_id_together`
- `test_resend_requires_reason`
- `test_resend_accepts_old_range`
- `test_resend_skips_window_with_no_source_rows`
- `test_resend_empty_window_not_persisted_as_partial`
- `test_resend_summary_reports_empty_window_count`
- `test_resend_filtered_to_empty_window_still_creates_partial`
- `test_resend_persists_on_window_cadence_not_per_target`
- `test_resend_final_state_matches_per_target_baseline`
- `test_resend_gc_removes_complete_windows_older_than_horizon`
- `test_resend_gc_keeps_complete_windows_inside_horizon`
- `test_resend_gc_keeps_partial_windows`
- `test_resend_gc_cutoff_uses_max_of_both_intervals`
- `test_resend_gc_runs_periodically_and_finally`
- `test_resend_dry_run_does_not_gc`
- `test_resend_reconnects_after_dropped_connection_and_continues`
- `test_resend_reconnect_retries_are_bounded_then_aborts`
- `test_resend_does_not_reconnect_on_non_connection_operational_error`
- `test_resend_does_not_reconnect_on_non_connection_db_error`
- `test_resend_reconnect_preserves_progress`
- `test_resend_final_close_on_dead_handle_does_not_raise`
- `test_resend_dry_run_unaffected_by_reconnect_logic`
- `test_resend_summary_reports_partial_and_complete_window_counts`
- `test_resend_partial_window_keeps_exit_code_zero`
- `test_resend_dry_run_summary_reports_zero_partial_complete`
- `test_resend_aborts_before_posting_when_range_contains_dead_letter`
- `test_resend_preflight_lists_all_dead_letter_windows`
- `test_resend_preflight_aborts_on_dead_letter_with_no_source_rows`
- `test_resend_preflight_allows_range_with_no_dead_letter`

The shared kwarg also carries its own contract in the runner test
modules: `_process_send_window` saves once per posted target by default
and skips the in-loop save when `persist_each_target=False`, for both
`run_flow` and `run_direction`.

---

## 2. Show: `scripts/show_data.py`

### Goal

Read recorded data for a date range and/or set of places, either from
Orion (current values) or STH-Comet (history), in one CLI. Default
output is raw JSON (the same shape `check_entity.py` /
`check_history.py` already return); `--pretty` renders a human-readable
table.

### CLI

```
uv run python scripts/show_data.py
    --source {orion|comet}
    [--type TYPE]
    [--attrs LIST | --flow-attrs | --direction-attrs]
    [--place N [--place N ...]]
    [--entity-id ID [--entity-id ID ...]]
    [--from ISO_OR_WINDOW] [--to ISO_OR_WINDOW]
    [--last-n N]
    [--interval-min {5|60}]
    [--pretty]
```

| Flag | Purpose |
|---|---|
| `--source orion` | One GET per entity to `/orion/v2.0/entities/<id>`. Current values only. `--from/--to/--last-n` rejected. |
| `--source comet` | One GET per (entity, attr) to STH-Comet. Historical values. `--from/--to/--last-n` honored. |
| `--type TYPE` | Default entity type for specs that omit one, or an override for inferred canonical ids. |
| `--attrs LIST` | Comma-separated attribute selector. |
| `--flow-attrs` / `--direction-attrs` | Shortcut for the Product A / Product B attribute sets. Mutually exclusive with `--attrs`. |
| `--place N` | Place number (repeatable). With `--interval-min`, selects that interval; without it, resolves every active interval for the place. Mutually exclusive with `--entity-id`. |
| `--entity-id ID` | Explicit entity id (repeatable). Canonical ids infer entity type. Mutually exclusive with `--place`. |
| `--from`, `--to` | ISO-8601 or `YYYYMMDD_HHMM`. Comet-only. |
| `--last-n N` | Comet-only; defaults to `10` if no `--from`/`--to`. |
| `--interval-min` | Optional interval filter when expanding `--place`. |
| `--pretty` | Render a compact text table instead of JSON. |

### Replaces `check_entity.py` and `check_history.py`

`scripts/check_entity.py` and `scripts/check_history.py` are removed.
Their functionality moves under `show_data.py --source orion` and
`--source comet` respectively.
[tools_and_troubleshooting.md](tools_and_troubleshooting.md), the
README's "Common commands" snippet, `docs/overview.md`, `docs/deployment.md`,
and any incident playbooks that reference the old scripts must be
updated in the same change.

**Flag-surface parity:** `show_data.py --source comet` must accept
**all** the passthrough flags `check_history.py` had:
`--h-limit`, `--h-offset`, `--aggr-method`, `--aggr-period`,
in addition to the new `--place` / `--from` / `--to` / `--last-n` /
`--pretty` flags listed above. Operators relying on STH-Comet
aggregation passthrough should see no regression.

### Output shapes

**Default (JSON):**

The legacy scripts both emit pretty-printed JSON
(`json.dumps(..., indent=2, sort_keys=True)`) per record. `show_data.py`
matches that exact formatter call on the default path, so existing
operator workflows that visually inspect output keep working unchanged.

**`--pretty`:**

```
entity                                attr                       value       time
jp.sendai.Blesensor.per3600.101       peopleCount_immedate       42          2026-05-24T10:00:00+09:00
jp.sendai.Blesensor.per3600.101       peopleCount_near           18          2026-05-24T10:00:00+09:00
jp.sendai.Blesensor.per3600.102       peopleCount_immedate       33          2026-05-24T10:00:00+09:00
```

For Comet output, multiple history rows per attribute render as
multiple table rows. Rows are grouped per `(entity, recvTime)`: rows
arriving in one Orion → Comet notification share a `recvTime`, so the
group corresponds to one POST. Groups are ordered by their measurement
window: the value of the co-arriving `dateObservedFrom` attribute when
present in the group, falling back to `recvTime` otherwise. Within a
group, rows are sorted alphabetically by attribute name. This makes
attributes of the same window adjacent, and duplicate-POST groups
(e.g. from a stale subscription) appear as separate consecutive groups
under the same window value rather than being interleaved by arrival
time.

Null values render as `null` (preserving Product B's null/0
distinction); missing entities render with a `(not found)` placeholder
row and the run still exits 0.

### Behavior

- Read-only. Never writes to Orion or Comet.
- Auth client is shared across requests (single token fetch).
- HTTP errors (other than 404) propagate as a non-zero exit code.

### Tests

- `test_show_orion_emits_one_json_per_entity`
- `test_show_comet_emits_one_json_per_entity_attr_pair`
- `test_show_orion_rejects_from_to_last_n`
- `test_show_pretty_renders_table_with_known_columns`
- `test_show_place_without_interval_min_resolves_both_intervals`
- `test_show_missing_entity_renders_not_found_row_pretty`
- `test_show_attrs_and_flow_attrs_mutually_exclusive`

---

## 3. Delete Comet: `scripts/delete_history.py`

### Goal

Operator tool that wipes STH-Comet history for specified entities or
attributes, using the Comet DELETE endpoints documented in
`scratch/swagger.json`:

- `DELETE /comet/v1.0/contextEntities/type/<type>/id/<id>/attributes/<attr>`
  wipes one attribute on one entity.
- `DELETE /comet/v1.0/contextEntities/type/<type>/id/<id>`
  wipes all attributes on one entity.

**The service-wide `DELETE /contextEntities` shape is intentionally not
exposed.** A single operator typo would erase all history under the
configured `Fiware-Service`. If a service-wide wipe is ever truly
needed, do it by curl with explicit operator sign-off.

**Date range is not supported.** The swagger
(`scratch/swagger.json`) lists no `dateFrom`/`dateTo` parameters on any
DELETE shape. The CLI therefore does not accept range flags; operators
must accept whole-attribute or whole-entity granularity. (The earlier
`scripts/dev/probe_sth_delete_range.py` probe is removed in this
change because its result was inconclusive (only 2 history rows
landed, below its 3-row gate) and keeping it would imply range delete
might work, which the swagger contradicts. The non-range probe
`scripts/dev/probe_sth_delete.py` stays as the authoritative
live-platform check that DELETE itself is reachable.)

### CLI

```
uv run python scripts/delete_history.py
    [--type TYPE]
    [--attrs LIST | --flow-attrs | --direction-attrs]
    --reason "..."
    [--send]
    [--i-know-this-is-production]
    ENTITY_SPEC [ENTITY_SPEC ...]
```

Where `ENTITY_SPEC` is `ENTITY_ID[:ENTITY_TYPE]`.

| Flag | Purpose |
|---|---|
| `ENTITY_SPEC` | One or more entities to operate on. |
| `--type TYPE` | Default entity type for specs that omit one, or an override for inferred canonical ids. |
| `--attrs LIST` | Comma-separated attribute names. If present, the tool deletes per-attribute (one DELETE per (entity, attr)). |
| `--flow-attrs` / `--direction-attrs` | Shortcuts. Mutually exclusive with `--attrs`. |
| (no `--attrs`) | Per-entity delete (one DELETE per entity). |
| `--reason` | Required. Audit string written to every log record. |
| `--send` | Live deletes. Default: dry-run. |
| `--i-know-this-is-production` | Required for `--send` when `FIWARE_SERVICE=""` or `FIWARE_SERVICE_PATH="/"`. See Safety below. |

### Behavior

1. Validate args. Canonical Sendai entity ids infer the entity type;
   custom ids require `:ENTITY_TYPE` or `--type`.
2. For each (entity[, attr]) pair, dry-run prints the DELETE URL that
   would be sent and exits.
3. `--send` issues the actual DELETE. 204 is the success code per
   swagger. 404 is logged at INFO and treated as no-op (entity/attr
   already absent from Comet). Other non-2xx codes are terminal for
   that target and counted as failures, but the run continues to the
   next target.
4. No batching; one HTTP request per target. Comet has no bulk endpoint.

### Safety

- Hard-coded refusal of `*` or empty `ENTITY_ID` (defense against
  argument-expansion mistakes).
- Refuses to run live (`--send`) when `FIWARE_SERVICE` or
  `FIWARE_SERVICE_PATH` resolves to a production catch-all
  (`FIWARE_SERVICE=""` or `FIWARE_SERVICE_PATH="/"`) **unless** the
  operator also passes `--i-know-this-is-production`. Dry-run is
  always allowed.
- Dry-run performs **no auth/token/network calls**; it only prints the
  planned URLs.

### Log events

- `delete_history_requested` (run start).
- `delete_history_target` (per target; carries URL, status, outcome).
- `delete_history_summary` (run end).

### Tests

- `test_delete_history_dry_run_prints_urls_no_http`
- `test_delete_history_per_attribute_when_attrs_given`
- `test_delete_history_per_entity_when_attrs_omitted`
- `test_delete_history_404_treated_as_noop`
- `test_delete_history_non_204_failure_counted_continues`
- `test_delete_history_rejects_attrs_and_flow_attrs_together`
- `test_delete_history_requires_reason`

---

## 4. Delete Orion entities: `scripts/delete_entities.py`

### Goal

Sibling of `create_entities.py`. Deletes one or more Orion entities,
with an optional chained Comet purge for that entity's history.

### CLI

```
uv run python scripts/delete_entities.py
    [--purge-history]
    [--attrs LIST | --flow-attrs | --direction-attrs]
    --reason "..."
    [--send]
    ENTITY_ID[:ENTITY_TYPE] [...]
```

| Flag | Purpose |
|---|---|
| `ENTITY_ID[:ENTITY_TYPE]` | One or more entities. Canonical Sendai ids infer the type; custom ids require inline `:ENTITY_TYPE`. |
| `--purge-history` | After each successful Orion DELETE, also delete the entity's Comet history. |
| `--attrs` / `--flow-attrs` / `--direction-attrs` | With `--purge-history`, scope the Comet purge to specific attributes (per-attribute Comet DELETEs). Without, the Comet purge is per-entity. **Rejected at arg-parse time if `--purge-history` is not also passed**; silently ignored attrs flags are a footgun. |
| `--reason` | Required. |
| `--send` | Live deletes. Default: dry-run. |
| `--i-know-this-is-production` | Required for `--send` when `FIWARE_SERVICE=""` or `FIWARE_SERVICE_PATH="/"`. Same guard `delete_history.py` uses; applies to the chained Comet purge as well. |

### Behavior

1. Validate args. Canonical Sendai entity ids infer the entity type;
   custom ids require inline `:ENTITY_TYPE`.
2. Dry-run prints the planned `DELETE /orion/v2.0/entities/<id>?type=<type>`
   per entity, plus the planned Comet DELETE(s) if `--purge-history`,
   and exits.
3. `--send` issues `DELETE /orion/v2.0/entities/<id>?type=<type>` per
   entity. 204 = deleted. 404 = already absent, logged INFO, no-op. Other
   non-2xx = failure for that entity (continue to next).
4. If `--purge-history` and the Orion DELETE returned 204 *or* 404, run
   the corresponding Comet delete(s) via the same code path as
   `delete_history.py` (extract into a shared helper in
   `sendai_pipeline/comet_client.py`).
5. Skip the Comet purge step if the Orion DELETE failed (don't compound
   one error with another).
6. **Comet purge is best-effort.** A Comet purge failure on an entity
   whose Orion DELETE already succeeded does NOT make the overall run
   exit non-zero. The failure is logged at WARNING and surfaced in the
   summary, but the operator's primary intent (deleting the Orion
   entity) succeeded, and a flaky Comet endpoint shouldn't make this
   script noisy. Orion DELETE failures still drive the exit code.

### Ordering rationale

Orion first, then Comet. If Orion still has the entity, future pipeline
runs would re-publish to it and immediately re-populate Comet; wiping
Comet first would create a brief inconsistency for no benefit. Deleting
Orion first stops new publications; the Comet purge then has a stable
target.

### Log events

- `delete_entities_requested`
- `delete_entities_target` (per entity, both phases)
- `delete_entities_summary`

### Tests

- `test_delete_entities_dry_run_prints_orion_and_comet_plan`
- `test_delete_entities_orion_404_treated_as_noop`
- `test_delete_entities_purge_history_runs_only_on_orion_success_or_404`
- `test_delete_entities_purge_history_per_attribute_with_attrs_flag`
- `test_delete_entities_purge_history_per_entity_without_attrs_flag`
- `test_delete_entities_non_204_orion_failure_skips_purge`
- `test_delete_entities_requires_reason`

---

## 5. Delete Orion subscriptions: `scripts/delete_subscriptions.py`

### Goal

Operator tool that removes Orion subscriptions by id, using
`DELETE /orion/v2.0/subscriptions/<id>`. Created for the recurring
need to retire stale STH-Comet subscriptions whose triggers or
notification attributes no longer match current pipeline output.
General-purpose by id; it is not coupled to STH-Comet specifically.

### CLI

```
uv run python scripts/delete_subscriptions.py
    --reason "..."
    [--send]
    [--i-know-this-is-production]
    SUBSCRIPTION_ID [SUBSCRIPTION_ID ...]
```

| Flag | Purpose |
|---|---|
| `SUBSCRIPTION_ID` | One or more Orion subscription ids (24-char hex). |
| `--reason` | Required. Audit string written to every log record. |
| `--send` | Live deletes. Default: dry-run. |
| `--i-know-this-is-production` | Required for `--send` when `FIWARE_SERVICE=""` or `FIWARE_SERVICE_PATH="/"`. Same guard `delete_entities.py` uses. |

### Behavior

1. Validate args. Subscription ids must be 24 hex chars; reject `*`,
   empty strings, and anything else at parse time (defense against
   shell-expansion mistakes).
2. **Pre-fetch.** For every id, do
   `GET /orion/v2.0/subscriptions/<id>` first and print the returned
   `description` (or `<no description>` if absent). This confirms the
   operator is targeting what they think they are, and lets the tool
   refuse a 404 before issuing a DELETE.
3. **Dry-run** performs the GET (so the operator sees the description),
   prints the planned `DELETE /orion/v2.0/subscriptions/<id>`, and
   exits. No DELETE is issued.
4. **`--send`** issues `DELETE /orion/v2.0/subscriptions/<id>` per id.
   204 = deleted. 404 mid-run (e.g. another operator deleted it
   between the GET and the DELETE) is logged at INFO and treated as
   no-op. Other non-2xx = failure for that id, run continues to the
   next id.
5. **Pre-fetch 404 is terminal for that id.** If the GET returns 404,
   skip the DELETE for that id and count it as absent. This is the
   "wrong id" guard the pre-fetch exists to provide.
6. 401 on either GET or DELETE triggers one forced-refresh retry,
   matching the rest of the Orion client surface.

### Safety

- Hard-coded refusal of `*` or empty `SUBSCRIPTION_ID`.
- Hard-coded refusal of ids that aren't 24 hex chars (Orion's
  ObjectId shape). Stops typos and obviously-wrong inputs from ever
  reaching the network.
- Refuses live `--send` against a catch-all FIWARE service/path
  (`FIWARE_SERVICE=""` or `FIWARE_SERVICE_PATH="/"`) without
  `--i-know-this-is-production`. Dry-run is always allowed.
- Dry-run still performs the GET pre-fetch; operators want to see the
  description before authorizing the delete. Dry-run therefore needs
  auth/network access, unlike `delete_entities.py` whose dry-run is
  purely offline. (Trade-off: dry-run is no longer fully offline, but
  it is much more useful for the "is this the right subscription?"
  question that motivates having a dry-run at all here.)

### Log events

- `delete_subscriptions_requested` (run start).
- `delete_subscriptions_target` (per id; carries id, phase
  `prefetch`/`delete`, http_status, ok).
- `delete_subscriptions_summary` (run end).

### Tests

- `test_delete_subscriptions_requires_reason`
- `test_delete_subscriptions_requires_at_least_one_id`
- `test_delete_subscriptions_rejects_empty_id`
- `test_delete_subscriptions_rejects_wildcard_id`
- `test_delete_subscriptions_rejects_malformed_id`
- `test_delete_subscriptions_rejects_send_in_production_without_override`
- `test_delete_subscriptions_dry_run_does_not_issue_delete`
- `test_delete_subscriptions_dry_run_prints_description_from_prefetch`
- `test_delete_subscriptions_prefetch_404_skips_delete_counts_absent`
- `test_delete_subscriptions_send_issues_delete_per_id`
- `test_delete_subscriptions_delete_204_counted_as_deleted`
- `test_delete_subscriptions_delete_404_counted_as_absent_exit_zero`
- `test_delete_subscriptions_delete_500_counted_as_failure_exit_nonzero_continues`
- `test_delete_subscriptions_emits_requested_target_and_summary_log_events`

### Library helpers in `sendai_pipeline/sth_subscriptions.py`

Two thin functions colocated with the existing
create-subscription code (the create/delete pair belongs together
the same way `CometClient` carries both create and delete of entity
history):

- `get_subscription(subscription_id, *, settings, auth, session=None) -> dict | None`
  returns the parsed subscription, or `None` on 404. 401 triggers
  one forced-refresh retry; other non-2xx raises `requests.HTTPError`.
- `delete_subscription(subscription_id, *, settings, auth, session=None) -> int`
  returns the HTTP status (204 or 404). 401 triggers one
  forced-refresh retry; other non-2xx raises `requests.HTTPError`.

Both reuse `_headers(...)` already in the module and accept any
settings object exposing the fields `_headers` reads (`base_url`,
`service`, `service_path`, `verify_tls`, `timeout`), typed as
`settings: Any`. Both `StHSubscriptionSettings` and
`OrionSettings` qualify; the delete CLI uses `OrionSettings` since
it has no need for `COMET_NOTIFY_URL`.

---

## Shared implementation work

Two pieces of new pipeline-side code; both must be import-safe in
dry-run (no FIWARE creds required for dry-run is the established
pattern in `create_entities.py`).

### `sendai_pipeline/comet_client.py`: extend

Add two methods to `CometClient`:

- `delete_attribute_history(entity_id, entity_type, attr) -> int`
- `delete_entity_history(entity_id, entity_type) -> int`

Each returns the HTTP status; raises `requests.HTTPError` only on
non-204/404 results (404 is returned as `404` for the caller to count).
401 triggers one forced-refresh retry, matching `get_history`.

### `sendai_pipeline/run_flow.py` / `run_direction.py`: extend

Add `force_resend: bool = False` kwarg to `_process_send_window` (or
the equivalent skip-check helper). Default `False` preserves all
current behavior. The new `resend.py` script passes `True` when
`--force` is on.

### Logging

Each new script uses the same `LoggingSettings.from_env()` +
`configure_logging(..., product="<script-stem>")` pattern as the
existing operator scripts. Log file is `logs/<script-stem>.log` per the
existing convention.

---

## Documentation updates after implementation

The scripts described in this spec (`resend.py`, `show_data.py`,
`delete_history.py`, `delete_entities.py`) have been implemented and
the operator reference has moved to
[tools_and_troubleshooting.md](tools_and_troubleshooting.md). Refer to
that document for current usage; this spec preserves the design rationale.

---

## Decided design choices

1. **No age gate on resend (removed `--allow-old`).** An earlier design
   refused ranges older than `MAX_LOOKBACK_HOURS_*` unless `--allow-old`
   was passed. That gate was removed: the threshold borrowed a live-runner
   lookback knob unrelated to manual-resend safety, and it blocked even a
   dry-run preview of an old range. Dry-run-by-default is the guardrail.
2. **`--i-know-this-is-production` guardrail on `delete_history.py`.**
   Included.
3. **Four separate tools, not a unified `data.py`.** Matches the
   established one-script-one-job convention in `scripts/`. The cleanup
   value the operator was asking for comes from culling superseded
   scripts (see next section), not from collapsing the new ones into a
   multiplexer.

## Repo cleanup completed

Superseded scripts removed during this change:

- `scripts/resend.py` replaces the old single-window replay tool
  (`--from X --to X`).
- `show_data.py --source orion` replaces the old entity-check tool.
- `show_data.py --source comet` replaces the old history-check tool.
- `scripts/dev/probe_sth_delete_range.py` removed: inconclusive
  probe; swagger is authoritative.

Net change to `scripts/`: −4 files, +4 files. Files in the directory
stay the same in count, but each remaining file's purpose is
non-overlapping.
