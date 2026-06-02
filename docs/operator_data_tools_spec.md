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
`ENTITY_ID[:ENTITY_TYPE]`. When `:ENTITY_TYPE` is omitted, the operator
must pass a default via `--type`.

**Place shorthand.** Several tools accept `--place N` (e.g. `105`)
and/or `--interval-min {5,60}`. The tool resolves the place number to
the matching entity id via `metadata/sensors.csv` using the same
`load_metadata` + `index_by_place_interval` path the runners use. If
the operator supplies both `--place` and explicit entity ids, the
explicit ids win and `--place` is rejected with a config error (to
prevent ambiguity).

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

## 1. Resend — `scripts/resend.py`

### Goal

Re-publish source-windows in a specified date range, optionally
narrowed to a specific place or set of places, by replaying the
runner-internal `_process_send_window` code path once per window.

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
    --interval-min {5|60}
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
| `--interval-min` | yes | `5` or `60`. A single invocation publishes one interval. |
| `--from`, `--to` | yes | Source-window keys, JST, inclusive. |
| `--place` | no | Place number filter. Repeatable. Resolved via metadata. |
| `--entity-id` | no | Explicit entity id. Repeatable. Mutually exclusive with `--place`. |
| `--reason` | yes | Audit string; written to every log record this run emits. |
| `--force` | no | Bypass the per-target payload-hash skip. Default: skip targets whose last `ok` payload hash matches the new payload. |
| `--allow-old` | no | Allow `--from` to predate `now − MAX_LOOKBACK_HOURS_*` for `--interval-min`. Default: refuse. |
| `--send` | no | Live writes. Default: dry-run. |

### Behavior

1. Validate args (range is non-empty and aligned to interval; place/
   entity-id mutual exclusion; `--reason` non-empty; range age vs
   `MAX_LOOKBACK_HOURS_*` checked against `--allow-old`).
2. Enumerate every source window between `--from` and `--to` at
   `--interval-min` step.
3. For each window, call the same `_process_send_window` the cron
   runners use, but with **`interval_metadata` filtered to the
   place/entity-id selection** so the transform step builds payloads
   only for the requested targets. For Product A, completion state still
   follows the normal observed-target rule (`stored ∪ observed`) and a
   filtered resend never shrinks existing `expected_target_ids`. For
   Product B, a new resend-created window uses the filtered fixed-target
   set; an existing window keeps its stored first-attempt snapshot. (The
   transforms iterate `interval_metadata`/the metadata index when
   constructing payloads; filtering state bookkeeping alone would still
   POST to every active target — that's the opposite of what `--place`
   means to an operator.)
4. `--force` flips the per-target skip: pass a flag through
   `_process_send_window` that causes the hash-skip check to be
   bypassed for this run. (Implementation detail: add a
   `force_resend: bool = False` kwarg to `_process_send_window` in both
   `run_flow.py` and `run_direction.py`; default `False` preserves
   today's behavior.)
5. Dry-run prints, per window, the planned `(window_key, target_count,
   skipped_by_hash, would_post)` tuple and exits **before any MySQL
   query, Orion auth token fetch, or Orion HTTP call** (no live side
   effects, no credentials required in dry-run).

### Safety

- The same per-product lock the cron runners take is acquired for the
  whole invocation. A long range can hold the lock long enough to push
  back the next cron tick; that's expected. The lock is released on
  exit (incl. exception).
- **STH-Comet caveat:** re-POSTing a value creates a new Comet history
  row even when the Orion value is unchanged. `--force` makes this
  explicit; without it, targets whose stored payload hash matches the
  new payload are skipped.
- Range is capped at `MAX_LOOKBACK_HOURS_*` by default; pass
  `--allow-old` to go further back. Operator error here would be
  expensive to undo.

### Log events

- `resend_requested` (run start; carries args + resolved entity ids).
- `resend_window_processed` (per window; status counts).
- `resend_summary` (run end).

### Tests

- `test_resend_dry_run_prints_planned_windows_no_orion_calls`
- `test_resend_range_loops_window_keys_inclusive_bounds`
- `test_resend_place_filter_resolves_via_metadata`
- `test_resend_force_bypasses_hash_skip`
- `test_resend_rejects_place_and_entity_id_together`
- `test_resend_requires_reason`
- `test_resend_old_window_rejected_without_allow_old`

---

## 2. Show — `scripts/show_data.py`

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
| `--type TYPE` | Default entity type for specs that omit one. |
| `--attrs LIST` | Comma-separated attribute selector. |
| `--flow-attrs` / `--direction-attrs` | Shortcut for the Product A / Product B attribute sets. Mutually exclusive with `--attrs`. |
| `--place N` | Place number (repeatable). Requires `--interval-min` if no explicit `--entity-id`. Resolved via metadata. |
| `--entity-id ID` | Explicit entity id (repeatable). Mutually exclusive with `--place`. |
| `--from`, `--to` | ISO-8601 or `YYYYMMDD_HHMM`. Comet-only. |
| `--last-n N` | Comet-only; defaults to `10` if no `--from`/`--to`. |
| `--interval-min` | Pick interval when expanding `--place`. |
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
multiple table rows. Rows are grouped per `(entity, recvTime)` — rows
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
- `test_show_place_requires_interval_min_when_no_entity_id`
- `test_show_missing_entity_renders_not_found_row_pretty`
- `test_show_attrs_and_flow_attrs_mutually_exclusive`

---

## 3. Delete Comet — `scripts/delete_history.py`

### Goal

Operator tool that wipes STH-Comet history for specified entities or
attributes, using the Comet DELETE endpoints documented in
`scratch/swagger.json`:

- `DELETE /comet/v1.0/contextEntities/type/<type>/id/<id>/attributes/<attr>`
  — wipe one attribute on one entity.
- `DELETE /comet/v1.0/contextEntities/type/<type>/id/<id>`
  — wipe all attributes on one entity.

**The service-wide `DELETE /contextEntities` shape is intentionally not
exposed.** A single operator typo would erase all history under the
configured `Fiware-Service`. If a service-wide wipe is ever truly
needed, do it by curl with explicit operator sign-off.

**Date range is not supported.** The swagger
(`scratch/swagger.json`) lists no `dateFrom`/`dateTo` parameters on any
DELETE shape. The CLI therefore does not accept range flags; operators
must accept whole-attribute or whole-entity granularity. (The earlier
`scripts/dev/probe_sth_delete_range.py` probe is removed in this
change because its result was inconclusive — only 2 history rows
landed, below its 3-row gate — and keeping it would imply range delete
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
| `--type TYPE` | Default entity type for specs that omit one. |
| `--attrs LIST` | Comma-separated attribute names. If present, the tool deletes per-attribute (one DELETE per (entity, attr)). |
| `--flow-attrs` / `--direction-attrs` | Shortcuts. Mutually exclusive with `--attrs`. |
| (no `--attrs`) | Per-entity delete (one DELETE per entity). |
| `--reason` | Required. Audit string written to every log record. |
| `--send` | Live deletes. Default: dry-run. |
| `--i-know-this-is-production` | Required for `--send` when `FIWARE_SERVICE=""` or `FIWARE_SERVICE_PATH="/"`. See Safety below. |

### Behavior

1. Validate args.
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
- Dry-run performs **no auth/token/network calls** — it only prints the
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

## 4. Delete Orion entities — `scripts/delete_entities.py`

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
    ENTITY_ID:ENTITY_TYPE [...]
```

| Flag | Purpose |
|---|---|
| `ENTITY_ID:ENTITY_TYPE` | One or more entities. Same shape as `create_entities.py`. |
| `--purge-history` | After each successful Orion DELETE, also delete the entity's Comet history. |
| `--attrs` / `--flow-attrs` / `--direction-attrs` | With `--purge-history`, scope the Comet purge to specific attributes (per-attribute Comet DELETEs). Without, the Comet purge is per-entity. **Rejected at arg-parse time if `--purge-history` is not also passed** — silently ignored attrs flags are a footgun. |
| `--reason` | Required. |
| `--send` | Live deletes. Default: dry-run. |
| `--i-know-this-is-production` | Required for `--send` when `FIWARE_SERVICE=""` or `FIWARE_SERVICE_PATH="/"`. Same guard `delete_history.py` uses; applies to the chained Comet purge as well. |

### Behavior

1. Dry-run prints the planned `DELETE /orion/v2.0/entities/<id>?type=<type>`
   per entity, plus the planned Comet DELETE(s) if `--purge-history`,
   and exits.
2. `--send` issues `DELETE /orion/v2.0/entities/<id>?type=<type>` per
   entity. 204 = deleted. 404 = already absent, logged INFO, no-op. Other
   non-2xx = failure for that entity (continue to next).
3. If `--purge-history` and the Orion DELETE returned 204 *or* 404, run
   the corresponding Comet delete(s) via the same code path as
   `delete_history.py` (extract into a shared helper in
   `sendai_pipeline/comet_client.py`).
4. Skip the Comet purge step if the Orion DELETE failed (don't compound
   one error with another).
5. **Comet purge is best-effort.** A Comet purge failure on an entity
   whose Orion DELETE already succeeded does NOT make the overall run
   exit non-zero. The failure is logged at WARNING and surfaced in the
   summary, but the operator's primary intent (deleting the Orion
   entity) succeeded, and a flaky Comet endpoint shouldn't make this
   script noisy. Orion DELETE failures still drive the exit code.

### Ordering rationale

Orion first, then Comet. If Orion still has the entity, future pipeline
runs would re-publish to it and immediately re-populate Comet — wiping
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

## 5. Delete Orion subscriptions — `scripts/delete_subscriptions.py`

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
- Dry-run still performs the GET pre-fetch — operators want to see the
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
  — returns the parsed subscription, or `None` on 404. 401 triggers
  one forced-refresh retry; other non-2xx raises `requests.HTTPError`.
- `delete_subscription(subscription_id, *, settings, auth, session=None) -> int`
  — returns the HTTP status (204 or 404). 401 triggers one
  forced-refresh retry; other non-2xx raises `requests.HTTPError`.

Both reuse `_headers(...)` already in the module and accept any
settings object exposing the fields `_headers` reads (`base_url`,
`service`, `service_path`, `verify_tls`, `timeout`) — typed as
`settings: Any`. Both `StHSubscriptionSettings` and
`OrionSettings` qualify; the delete CLI uses `OrionSettings` since
it has no need for `COMET_NOTIFY_URL`.

---

## Shared implementation work

Two pieces of new pipeline-side code; both must be import-safe in
dry-run (no FIWARE creds required for dry-run is the established
pattern in `create_entities.py`).

### `sendai_pipeline/comet_client.py` — extend

Add two methods to `CometClient`:

- `delete_attribute_history(entity_id, entity_type, attr) -> int`
- `delete_entity_history(entity_id, entity_type) -> int`

Each returns the HTTP status; raises `requests.HTTPError` only on
non-204/404 results (404 is returned as `404` for the caller to count).
401 triggers one forced-refresh retry, matching `get_history`.

### `sendai_pipeline/run_flow.py` / `run_direction.py` — extend

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

1. **`--allow-old` for resend.** Tool refuses ranges older than
   `MAX_LOOKBACK_HOURS_*` by default; operator must pass `--allow-old`
   to override.
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
- `scripts/dev/probe_sth_delete_range.py` removed — inconclusive
  probe; swagger is authoritative.

Net change to `scripts/`: −4 files, +4 files. Files in the directory
stay the same in count, but each remaining file's purpose is
non-overlapping.
