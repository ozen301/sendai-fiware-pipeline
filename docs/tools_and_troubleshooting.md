# Operator Tools and Troubleshooting

Every CLI under `scripts/` that an operator might invoke: what it
does, when to use it, and how, plus a symptom-keyed troubleshooting
playbook at the bottom. For the deployment flow that uses some of
these, see [deployment.md](deployment.md). For environment variables
referenced below, see [configuration.md](configuration.md).

Every command runs through `uv run`. None of these tools should be
invoked from cron except the runners themselves (covered in
deployment.md §8) and `refresh_metadata.py` (optional, in deployment.md
§8 too).

Tools fall into six groups:

| Group | Tools |
|---|---|
| **Bootstrap** | [`create_entities.py`](#create_entitiespy), [`create_sth_subscriptions.py`](#create_sth_subscriptionspy) |
| **Metadata** | [`refresh_metadata.py`](#refresh_metadatapy) |
| **Inspect Orion / Comet** | [`show_data.py`](#show_datapy), [`show_subscriptions.py`](#show_subscriptionspy) |
| **Delete Orion / Comet** | [`delete_entities.py`](#delete_entitiespy), [`delete_history.py`](#delete_historypy), [`delete_subscriptions.py`](#delete_subscriptionspy) |
| **Inspect / repair state** | [`state_doctor.py`](#state_doctorpy), [`state_repair.py`](#state_repairpy), [`migrate_flow_state.py`](#migrate_flow_statepy) |
| **Replay** | [`resend.py`](#resendpy) |

A symptom-keyed troubleshooting index is at the [bottom of this
doc](#troubleshooting).

---

## Bootstrap tools

### `create_entities.py`

Create one or more Orion entities. Use before publishing to any
entity for the first time. NGSI `POST .../attrs` updates existing
entities and never creates them.

```
uv run python scripts/create_entities.py [--send] ENTITY_ID:ENTITY_TYPE [...]
```

| Flag / arg | Purpose |
|---|---|
| `ENTITY_ID:ENTITY_TYPE` (positional, ≥ 1) | One entity to create. Repeat for multiple entities in one invocation. |
| `--send` | Perform live creates. Omit for dry-run (default). Dry-run requires no FIWARE credentials. |

The plan and outcome go to `logs/create_entities.log` at INFO. The
script posts to `/orion/v2.0/entities` with no Orion `options` query
parameter; in particular, it does **not** set `options=upsert`, which
would tell Orion to overwrite any existing entity. Instead, the script
treats `201` as created and `409`/`422` as already-existing-on-platform
and skips them, so an existing entity is never replaced. Safe to
re-run.

Example: create the per-3600 and per-300 entities for places 101-102:

```sh
uv run python scripts/create_entities.py --send \
  jp.sendai.Blesensor.per3600.101:Blesensor.per3600 \
  jp.sendai.Blesensor.per300.101:Blesensor.per300 \
  jp.sendai.Blesensor.per3600.102:Blesensor.per3600 \
  jp.sendai.Blesensor.per300.102:Blesensor.per300
```

### `create_sth_subscriptions.py`

Create the Orion subscriptions that drive STH-Comet history. See
[pipeline_spec.md §5](pipeline_spec.md) for what the subscription
bodies contain.

```
uv run python scripts/create_sth_subscriptions.py [--product a|b|all] [--send] [--no-show-body]
```

| Flag | Purpose |
|---|---|
| `--product a` / `b` / `all` | Which product to create. Defaults to `all`. |
| `--send` | Perform live creation. Omit for dry-run (default). |
| `--no-show-body` | In dry-run, suppress printing the redacted body. |

Reads `COMET_NOTIFY_URL` from `.env`. Dry-run prints the proposed bodies without
contacting FIWARE. In live send mode, the creator inventories existing
subscriptions and skips only an exact current shape. A description prefix alone
is not an idempotency match. Orion's omitted and literal-`false`
`notification.onlyChangedAttrs` / `notification.covered` representations are
equivalent; `true` remains behavior-changing drift.

> **Ordering matters.** Create subscriptions before the first live
> attribute-update POST (see deployment.md §6). `skipInitialNotification`
> is on by default, so updates that happened before the subscription
> existed are not replayed into Comet.

Example: typical operator flow (inspect both products, then create
both for real):

```sh
# Dry-run: print the redacted subscription bodies for both products.
uv run python scripts/create_sth_subscriptions.py

# Looks right; create both subscriptions on the platform.
uv run python scripts/create_sth_subscriptions.py --send
```

**Replacing a stale Product A subscription.** A recognized Product A
subscription that does not match the current exact contract makes creation fail
safely. The creator reports the stale id; it does not delete the subscription,
replace it, or create a second Product A subscription. During an approved
operator maintenance window, remove that exact id and then create the current
shape:

```sh
# If needed, find the subscription id by listing what is on the broker:
uv run python scripts/show_subscriptions.py

# Preview deletion of the exact stale id.
uv run python scripts/delete_subscriptions.py \
  --reason "replace stale Product A STH-Comet subscription" \
  "$STALE_PRODUCT_A_SUBSCRIPTION_ID"

# Repeat the same reviewed target in live mode.
uv run python scripts/delete_subscriptions.py \
  --reason "replace stale Product A STH-Comet subscription" \
  --send \
  "$STALE_PRODUCT_A_SUBSCRIPTION_ID"

# Install Product A's current exact shape.
uv run python scripts/create_sth_subscriptions.py --product a --send
```

For a catch-all FIWARE service or service path, add
`--i-know-this-is-production` to the live deletion command. The tool fetches and
prints the subscription description before deletion; confirm it matches the
reviewed target.

The creator also fails fast if a peer-product subscription has an unsafe
overlapping trigger. The exact current Product A and Product B selectors are
disjoint, so their shared `dateRetrieved` name is safe. This separation depends
on exact selector canonicality, not the peer's notification behavior. A peer
with a broad, malformed, typeless, or otherwise non-canonical selector cannot
prove that separation and must be removed before creation proceeds.

---

## Metadata tools

### `refresh_metadata.py`

Atomically rebuild `metadata/sensors.csv` from the stable seed plus
the staged refresh file. Use only if you adopt the refresh workflow
(deployment.md §4); skip if you maintain `sensors.csv` by hand.

```
uv run python scripts/refresh_metadata.py
```

Takes no arguments. Reads the three `SENSOR_METADATA_*` paths from
`.env` (see [configuration.md](configuration.md)) and writes the
merged file atomically (writes `<SENSOR_METADATA_PATH>.new` first, then
`os.replace()` over the destination). Renames the staged `ID` column
to `identifcation` so the output matches the canonical runtime schema.

The script also diffs the merged result against the previous
`metadata/sensors.csv` and emits per-row `metadata_row_added`,
`metadata_row_removed`, and `metadata_row_changed` log events for the
audit trail. If a row in the *stable seed* changed (it normally
shouldn't), a `WARNING`-level `stable_seed_changed` event fires.
Review those before assuming the refresh is correct.

> **Staged-file schema requirement.** The staged file (currently the
> 2026 batch) **must** carry an `ID` column, not `identifcation`. If
> the staged file is missing `ID`, or has `ID` and `identifcation` both
> present, the script exits with a validation error and does not write
> `metadata/sensors.csv`. If you have a complete file that already uses
> `identifcation`, either rename its column to `ID` before staging, or
> skip the refresh workflow entirely and maintain `metadata/sensors.csv`
> by hand.

Safe to run while the pipeline is idle. Avoid running it in parallel
with a cron-driven runner to keep the metadata read consistent.

---

## Orion / Comet inspection

### `show_data.py`

Read current Orion values or STH-Comet history for one or more
entities. Read-only. Use Orion when you need the latest stored value;
use Comet when you need historical rows.

```
uv run python scripts/show_data.py
    --source {orion|comet}
    [--type TYPE]
    [--attrs LIST | --flow-attrs]
    [--place N [--place N ...]]
    [--entity-id ID [--entity-id ID ...]]
    [--from ISO_OR_WINDOW] [--to ISO_OR_WINDOW]
    [--last-n N]
    [--h-limit N] [--h-offset N]
    [--aggr-method M] [--aggr-period P]
    [--interval-min {5|60}]
    [--pretty]
    [ENTITY_ID[:ENTITY_TYPE] ...]
```

| Flag / arg | Purpose |
|---|---|
| `--source orion` / `comet` | Read current Orion entity JSON or STH-Comet history. |
| `ENTITY_ID[:ENTITY_TYPE]`, `--entity-id` | Explicit entity target. Canonical `jp.sendai.Blesensor.per300/per3600.<N>` ids infer the type; append `:ENTITY_TYPE` per id or supply `--type` for custom ids or overrides. The `:ENTITY_TYPE` suffix splits on the *last* colon. So a colon-bearing entity id (e.g. a URN-style id) must be given *with* a colon-free inline type, like `urn:ngsi-ld:Foo:Bar:SomeType`; it cannot be passed bare, and `--type` alone will not fix it (the trailing segment is taken as the type first). The configured Product B aggregate id is exempt: it is matched as a whole before any split and works bare. A colon-bearing type cannot be given inline; pass it with `--type`, which works only for a colon-free id or the aggregate id. |
| `--place N` | Resolve place numbers through metadata. With `--interval-min`, selects that interval; without it, selects every active interval for the place. Mutually exclusive with explicit entity ids. |
| `--attrs LIST` | Comma-separated attributes. Required for non-aggregate Comet reads. On `--source comet`, for the configured Product B aggregate id, omitting it enumerates the contract scalars plus `peopleCount_flow_<place_number>` for every active interval-60 metadata row. Use an explicit list to inspect inactive or historical dynamic attributes. |
| `--flow-attrs` | Shortcut for all ten Product A attributes: `dateObservedFrom`, `dateObservedTo`, `dateRetrieved`, `identifcation`, `peopleCount_immedate`, `peopleCount_near`, `peopleCount_far`, `peopleOccupancy_immedate`, `peopleOccupancy_near`, and `peopleOccupancy_far`. |
| `--from` / `--to` / `--last-n` | Comet-only history bounds. `--last-n` defaults to `10` when no bounds are provided. |
| `--h-limit` / `--h-offset` | STH-Comet pagination passthrough. |
| `--aggr-method` / `--aggr-period` | STH-Comet aggregation passthrough. |
| `--pretty` | Render a compact `entity / attr / value / time` table instead of raw JSON. |

Default output is the raw Orion or STH-Comet JSON response, pretty
printed once per entity or entity/attribute pair. A 404 prints a
not-found record and exits 0; other HTTP errors are failures. Pretty
mode renders null values as `null`.

When an explicit id equals `PRODUCT_B_AGGREGATE_ENTITY_ID`, the tool
uses `PRODUCT_B_AGGREGATE_ENTITY_TYPE` if no inline type or `--type` is
given. Orion current state for this aggregate is the last window
written, which may be an older window after a resend; use
`dateObservedFrom` and `dateObservedTo` to identify the represented
source window.

Examples:

```sh
uv run python scripts/show_data.py --source orion --flow-attrs \
  jp.sendai.Blesensor.per3600.101

uv run python scripts/show_data.py --source comet \
  --attrs peopleCount_immedate --last-n 20 \
  jp.sendai.Blesensor.per300.101 jp.sendai.Blesensor.per300.102

uv run python scripts/show_data.py --source comet \
  --entity-id "$PRODUCT_B_AGGREGATE_ENTITY_ID" --pretty

uv run python scripts/show_data.py --source orion --place 101 --pretty
```

### `show_subscriptions.py`

List the Orion subscriptions currently on the broker — each one's id,
selectors, trigger attributes, notification shape, and server-managed
delivery telemetry (`timesSent`, `failsCounter`, last success/failure).
Read-only: it never creates, edits, or deletes. Use it to see what is
subscribed, confirm exactly one current Product A subscription after a
cutover, and read the exact id to retire with
[`delete_subscriptions.py`](#delete_subscriptionspy). To recreate a
product's current shape, run
[`create_sth_subscriptions.py`](#create_sth_subscriptionspy) (it takes a
product, not an id); use this tool to confirm the result.

```
uv run python scripts/show_subscriptions.py [SUBSCRIPTION_ID ...] [--json]
```

With no ids it lists every subscription; with ids it shows only those
exact ones (24-char lowercase hex). Output is unredacted — endpoint
URLs, custom headers, and broker credentials are all shown, since the
tool runs on internal hosts for authorized operators; the one rule is
that credentials are never written to the log file. The default is a
human-readable summary; `--json` emits the raw broker objects as a JSON
array on stdout (diagnostics and the count line go to stderr, so the
array pipes cleanly into `jq`).

Exit codes: `0` read succeeded (including an empty broker); `1` an
operational read failure — a named id was absent or its fetch errored,
an inventory read failed, or a runtime auth/token failure prevented the
read; `2` an argument or configuration error — a bad or duplicate id, or
a settings/auth construction failure before any request.

```sh
# Summary of every subscription.
uv run python scripts/show_subscriptions.py

# Raw JSON for two specific ids, piped into jq.
uv run python scripts/show_subscriptions.py --json \
  65e87f5c20bd0c390e057c62 65e87f5c20bd0c390e057c63 \
  | jq '.[] | {id, description}'
```

### `delete_entities.py`

Delete one or more Orion entities. Dry-run is the default; pass
`--send` for live deletion. `--purge-history` also deletes the
entity's Comet history after Orion returns deleted or already absent.

```
uv run python scripts/delete_entities.py
    [--purge-history]
    [--attrs LIST | --flow-attrs]
    --reason "..."
    [--send]
    [--i-know-this-is-production]
    ENTITY_ID[:ENTITY_TYPE] [...]
```

Live deletion refuses catch-all FIWARE scopes unless
`--i-know-this-is-production` is present. Attribute flags are only
valid with `--purge-history`. Canonical Sendai entity ids infer the
entity type; append `:ENTITY_TYPE` for custom ids or when overriding
the inferred type.

For the dedicated Product B aggregate entity, omit the attribute
selector to purge the whole Comet entity history. `--attrs` remains
available for an explicitly reviewed partial purge; deleting a contract
scalar can make historical rows harder to interpret.

### `delete_history.py`

Delete STH-Comet history for selected entities or attributes. Dry-run
prints the DELETE URLs and performs no auth or network calls.

```
uv run python scripts/delete_history.py
    [--type TYPE]
    [--attrs LIST | --flow-attrs]
    --reason "..."
    [--send]
    [--i-know-this-is-production]
    ENTITY_ID[:ENTITY_TYPE] [...]
```

With no attribute selector, the tool deletes all history for each
entity. With `--attrs` or a shortcut, it deletes one attribute series
per entity. Live deletion uses the same catch-all scope guard as
`delete_entities.py`. Canonical Sendai entity ids infer the entity
type; append `:ENTITY_TYPE` or pass `--type` for custom ids or
overrides.

For the dedicated Product B aggregate entity, whole-entity deletion is
the normal operation. An explicit `--attrs` list is still accepted when
the intended scope is narrower.

### `delete_subscriptions.py`

Delete one or more Orion subscriptions by id. Use to retire stale
subscriptions whose trigger or notification attributes no longer match
current pipeline output (for example, an old STH-Comet subscription
left behind after a schema change).

```
uv run python scripts/delete_subscriptions.py
    --reason "..."
    [--send]
    [--i-know-this-is-production]
    SUBSCRIPTION_ID [...]
```

For every id, the tool first does a `GET
/orion/v2.0/subscriptions/<id>` and prints the subscription's
description so you can confirm you are targeting the right one. Dry-run
stops there. `--send` then issues the actual `DELETE`. 204 = deleted,
404 = already absent (logged at INFO, no-op). Live deletion uses the
same catch-all scope guard as `delete_entities.py`. Ids must be 24-char
hex (Orion's ObjectId shape); anything else is rejected at parse time.

Find subscription ids with [`show_subscriptions.py`](#show_subscriptionspy):

```bash
# Human-readable summary of every subscription (id, trigger, notification).
uv run python scripts/show_subscriptions.py

# Or pull the raw objects and pick fields with jq.
uv run python scripts/show_subscriptions.py --json \
  | jq '.[] | {id, description, condition_attrs: .subject.condition.attrs}'
```

---

## State inspection and repair

The runners persist a per-window state file under `state/` so they
know which targets have already received a successful Orion write.
When a window sticks in `pending` or `partial` and the normal retry
isn't clearing it, use these tools, in order, to inspect, repair, and
(only as a last resort) replay.

### Revision sweep cursor

Each product state file also has a top-level `last_aggregated_at`
cursor. This is the revision-sweep watermark: source rows with
`aggregated_at` before that value have already been scanned for that
product. The cursor is separate for `state/flow.json` and
`state/direction.json`; a failed write (Product A POST or Product B
PUT) does not hold it back because the per-window state keeps the
failed window for retry.

Product A (flow) seeds a missing cursor from `run_flow.py`'s code-level
`REVISION_CURSOR_SEED` and drains forward automatically. Product B
(direction) instead initializes a missing cursor, in send mode only, to that
run's start time, truncated to whole seconds, and begins scanning forward from
there on later runs. (A dry-run neither initializes nor persists the cursor.)
For an existing cursor, both runners keep it forward-only.

Revision discovery is paced by two controls:
`REVISION_SWEEP_DISCOVERY_SPAN` bounds each MySQL discovery scan, and
`REVISION_SWEEP_MAX_WINDOWS` sets a soft cap on how many discovered or
old-open windows are processed in one run; a run may exceed it to keep one
`aggregated_at` second together. These controls drain Product A's initial
backlog and bound later scans for both products. Once a cursor is current,
each cron tick scans only revisions since the prior run.

Because `aggregated_at` is a MySQL `timestamp`, the sweep cursor must be
compared in a JST (`+09:00`) MySQL session. Check the runtime connection
with the same environment the runners use:

```sql
SELECT
  @@global.time_zone AS global_time_zone,
  @@session.time_zone AS session_time_zone,
  @@system_time_zone AS system_time_zone,
  NOW() AS session_now,
  UTC_TIMESTAMP() AS utc_now,
  TIMEDIFF(NOW(), UTC_TIMESTAMP()) AS session_utc_offset;
```

The required result is a session offset of `09:00:00`. A
`session_time_zone` of `SYSTEM` is acceptable only when
`system_time_zone` is `JST`; otherwise the connection should be corrected
before relying on revision-sweep cursor ranges.

To inspect the cursor, read the top-level field:

```sh
jq '.last_aggregated_at' state/flow.json
jq '.last_aggregated_at' state/direction.json
```

To re-sweep from an earlier point, stop that product's runner, back up
the state file, set the top-level `last_aggregated_at` to the desired
JST ISO timestamp, and restart the runner. Resetting earlier can
repeat matching Orion writes and append duplicate STH-Comet history rows.

### `state_doctor.py`

Read-only diagnostic. Lists every open window (`pending` or
`partial`) for one product and explains why each is open.

```
uv run python scripts/state_doctor.py {flow|direction}
uv run python scripts/state_doctor.py {flow|direction} --pretty
```

| Arg | Purpose |
|---|---|
| `flow` / `direction` (positional, required) | Which product's state to inspect. |
| `--pretty` | Render a human-readable dashboard with status bars and tables instead of JSON. |
| `--top N` | Limit ranked failed target tables in `--pretty` output. Default: `20`. |
| `--window-limit N` | Limit open-window rows in `--pretty` output. Default: `30`. |
| `--ascii` | Keep `--pretty` output ASCII-only. |
| `--all` | Show all `--pretty` dashboard table rows. |

Default output is JSON for scripts. It includes retained-window status
counts, open-window diagnostics, ranked failed target summaries,
and failed HTTP status counts. Each open-window row includes fields
including:

- `window`: the window key, e.g. `per3600/20260524_1000`.
- `status`: `pending` or `partial`.
- `interval_min`, `first_seen`, `source_window_start`, `source_window_end`: window timing context.
- `target_status_category`: `all_failed`, `all_ok`, or `mixed`.
- `expected_target_source`: `stored` when the v2 expected-target snapshot is present, `derived` for legacy rows that fell back to currently-recorded target keys (treat `derived` as diagnostic only).
- `target_count` / `ok_count` / `failed_count`: aggregate counts across the window's expected targets.
- `failed_http_statuses`: distinct HTTP status codes seen on `failed` targets.
- `failed_target_ids`: expected target IDs whose retained target record is `failed`.
- `retry_reachable`: whether the window is still inside the configured retry horizon (`MAX_LOOKBACK_HOURS_*`).

In `--pretty` mode, the status overview is one stacked bar using
distinct symbols: `█` = `complete`, `▒` = `partial`, `◆` = `pending`,
`×` = `dead_letter`, and `?` = `unknown`. The doctor also tries to load
current sensor metadata from `SENSOR_METADATA_PATH` (default
`metadata/sensors.csv`) so tables can show place number, batch, and
interval. Missing or stale metadata does not fail the command; the
output falls back to entity IDs. Large tables are truncated by default
and print a hint to rerun with `--all` when rows are hidden.
Direction reports instead label the section `Aggregate target failures`
and omit per-place columns because each Product B window has one
configured aggregate target.

The doctor never mutates state and emits a warning to stderr if the
state file changes during the read (race with a concurrent runner).

### `state_repair.py`

Apply targeted repairs to specific windows. Dry-run is the default;
`--apply` mutates state under the same per-product lock the cron
runners use, writes a timestamped `.bak` backup under `state/`, and
verifies the result.

```
uv run python scripts/state_repair.py {flow|direction}
    --window WINDOW_KEY [--window ...]
    --action {recompute_complete|dead_letter}
    [--expected-target-id ID [--expected-target-id ID ...]]
    [--reason "..."]
    [--apply]
```

| Flag | Purpose |
|---|---|
| `flow` / `direction` (positional) | Which product's state to repair. |
| `--window WINDOW_KEY` (≥ 1) | Window to repair, e.g. `per3600/20260523_2200`. Repeat for multiple windows. |
| `--action recompute_complete` | Recompute the aggregate status from per-target records and store the result. Use when the doctor reports a stale `partial` whose stored expected targets are all already `ok`. |
| `--action dead_letter` | Mark the window unrecoverable so the runner never retries it. Use when the source data is no longer recoverable or a reviewed non-transient failure should stop normal retry. |
| `--expected-target-id ID` | Provide expected entity IDs explicitly, required for legacy rows where the doctor reports `expected_target_source=derived`. Repeat. |
| `--reason "..."` | Required for `dead_letter`. Recorded as audit context. |
| `--apply` | Actually mutate state. Omit for dry-run (default). |

Examples:

```sh
# Stale partial whose expected targets are all already ok: dry-run then apply.
uv run python scripts/state_repair.py flow \
  --window per3600/20260523_2200 \
  --action recompute_complete
uv run python scripts/state_repair.py flow \
  --window per3600/20260523_2200 \
  --action recompute_complete --apply

# Legacy row, explicit expected targets.
uv run python scripts/state_repair.py flow \
  --window per3600/20260523_2200 \
  --action recompute_complete \
  --expected-target-id jp.sendai.Blesensor.per3600.101 \
  --expected-target-id jp.sendai.Blesensor.per3600.102 \
  --apply

# Unrecoverable window: dead-letter with audit reason.
uv run python scripts/state_repair.py direction \
  --window per300/20260525_0640 \
  --action dead_letter \
  --reason "source row no longer retained" \
  --apply
```

Do not edit window records in `state/*.json` by hand. Always go
through this tool so the backup, lock acquisition, and
reload-and-verify steps happen. The top-level revision cursor is the
one controlled-edit exception; see [Revision sweep cursor](#revision-sweep-cursor).

### `migrate_flow_state.py`

One-off migration that converts **flow** state written under the old
full-roster completion model to the observed-target model: it re-derives
each window's `expected_target_ids` from the targets actually recorded,
recomputes the aggregate status, and drops windows that recorded no
targets. `dead_letter` windows are left untouched. Dry-run is the
default; `--apply` mutates state under the flow product lock, writes a
timestamped `.bak` backup under `state/`, and reloads-and-verifies the
result. Flow-only: there is no `product` argument and direction state
is never touched.

```
uv run python scripts/migrate_flow_state.py [--apply]
```

| Flag | Purpose |
|---|---|
| `--apply` | Actually mutate `state/flow.json`. Omit for dry-run (default). |

```sh
# Preview what would change, then apply.
uv run python scripts/migrate_flow_state.py
uv run python scripts/migrate_flow_state.py --apply
```

Run this once, after deploying the observed-target completion change, to
clear flow windows that were stuck `partial` only because a never-seen
target sat in their old expected roster. It is idempotent and safe to
re-run.

---

## Replay

### `resend.py`

Re-run one or more source windows end-to-end: fetch source rows from
MySQL, build payloads, and write them to Orion. Product A POSTs its
per-place entities; Product B replaces all attributes on its one
aggregate entity with `PUT /attrs`. Use **only** when the source data is
still available in MySQL and replaying the write is correct and
meaningful.

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
    [--max-imputation-tier N]  # flow only
```

| Flag | Purpose |
|---|---|
| `flow` / `direction` (positional) | Which product to resend. |
| `--interval-min 5` / `60` | Product A source interval. Required for Product A `--place` or unfiltered runs; optional with canonical `--entity-id`, where it is inferred. Product B supports only `60` and defaults to it when omitted. |
| `--from YYYYMMDD_HHMM` | First source window start, JST. Inclusive. |
| `--to YYYYMMDD_HHMM` | Last source window start, JST. Inclusive. Equal to `--from` replays exactly one window. |
| `--place N` | Product A place filter; repeatable and resolved through metadata. Mutually exclusive with `--entity-id`. Product B rejects this selector before taking the product lock or sending. |
| `--entity-id ID` | Product A entity filter; repeatable and mutually exclusive with `--place`. Product B rejects this selector because a filtered aggregate PUT would remove other places. |
| `--reason "..."` | Required. Recorded as audit context in the run log. |
| `--force` | Bypass the unchanged-payload skip. By default, a prior-`ok` target is skipped only when its payload hash is unchanged; drift is written again. `--force` also rewrites unchanged prior-`ok` targets. |
| `--send` | Perform live Orion writes. Omit for dry-run: prints the planned per-window plan and exits before any MySQL query, Orion token fetch, or Orion HTTP call. |
| `--max-imputation-tier N` | Override the Product A source imputation ceiling. Product B rejects this flag. |

Resend writes to the same `window_key` as the original publication
because it's a retry of the original business window, not a synthetic
replacement. By default, unchanged prior-`ok` targets are skipped, while
drifted prior-`ok` targets are written through the same shared
`_process_send_window` path used by the live runners; `scripts/resend.py`
does not carry separate drift logic. Pass `--force` when the intent is to
redeliver unchanged targets too. For a wide range this matters: without
`--force`, unchanged recent in-state windows are skipped while drifted
or GC-reclaimed windows are written, so the result can be non-uniform
even though the run exits 0 (see ["I need to resend a large range without
dropping live data"](#i-need-to-resend-a-large-range-without-dropping-live-data)).

Product B uses the same shared direction path, but each non-empty
60-minute source window produces either one clean or degraded aggregate PUT, a
no-payload skip, or an all-excluded source-invalid result. Empty, no-payload,
and all-excluded windows create no window state. An attempted degraded PUT or
an all-excluded window makes a live resend exit `1`; a failed PUT also makes it
exit `1`. The `resend_summary` record reports `puts_ok`, `puts_failed`,
`windows_degraded`, `windows_no_payload`, and `windows_source_invalid`.
An unchanged prior-`ok` degraded package that is not forced is a DEBUG no-op:
it produces no warning, does not increment `windows_degraded`, and does not
change the exit code.

For Product A, `--place` / `--entity-id` limits which flow payloads are
built and POSTed, but it does not shrink a retained window's
`expected_target_ids`; completion remains `stored ∪ observed`. Product B
does not accept target filters; every attempted write uses the
configured aggregate id and type.

Examples:

```sh
# Single window, dry-run.
uv run python scripts/resend.py direction \
  --from 20260525_0600 --to 20260525_0600 \
  --reason "payload shape fix"

# Same single window, live.
uv run python scripts/resend.py direction \
  --from 20260525_0600 --to 20260525_0600 \
  --reason "payload shape fix" \
  --send

# Range of windows narrowed to one place, forcing re-POST.
uv run python scripts/resend.py flow \
  --interval-min 60 \
  --from 20260524_0000 --to 20260524_2300 \
  --place 105 \
  --reason "metadata drift" \
  --force --send
```

> **STH-Comet caveat.** Once subscriptions are active, repeating an Orion
> value creates a new Comet history row even when the Orion value is
> unchanged (the subscription fires on metadata changes, including the
> `TimeInstant` re-emission). Use resend judiciously and prefer
> `state_repair.py recompute_complete` whenever the failure was a
> bookkeeping bug rather than a missed Orion write.

> **Lock / live-cron caveat.** `--send` holds the per-product lock for
> the whole run, no-opping every live tick meanwhile, which is the same
> effect as live-cron downtime. For ranges longer than the reprocess floor (≈2h for
> 5-min data), read ["I need to resend a large range without dropping live
> data"](#i-need-to-resend-a-large-range-without-dropping-live-data) under
> Troubleshooting *before* you run.

> **Transient DB errors.** A long `--send` run reuses one MySQL
> connection across all windows and reconnects automatically on the
> per-window data fetch if the connection is lost mid-run (an idle
> timeout, a brief network blip, a failover), so a one-off hiccup no
> longer aborts the resend. If the run still exits `2` with a connection
> error, a `--force` re-run of the same chunk is the recovery (duplicate
> Comet rows are the expected resend cost). Repeated `resend_db_reconnect`
> warnings in `logs/resend.log` mean the database is persistently
> unhealthy; fix the DB or network before re-running, rather than
> retrying into the same outage.

---

## Troubleshooting

For each symptom, the recommended first tool comes first; later tools
are escalations only if the earlier one rules out the simpler cause.

### "Product B reports degraded data, source-invalid data, or a failed PUT"

These signals mean different things:

1. `direction_window_degraded` is a WARNING and `windows_degraded` increases
   when Product B attempts a PUT for a **degraded package**. At least one
   **emitted place** had both totals and received its own
   `peopleCount_flow_<N>` attribute, while at least one **excluded place** was
   missing a required total and received no attribute of its own. A live run
   exits `1` even if the PUT succeeded. Check `sourceQuality` for the excluded
   places and missing totals, then ask the source-data administrator to repair
   them. Publishing the emitted places limits how much data is suppressed; it
   does not fix or accept the upstream defect.
2. `direction_window_source_invalid` is a WARNING and
   `windows_source_invalid` increases when every candidate is an excluded
   place. There is no emitted place, so Product B writes nothing and a live run
   exits `1`.
3. `puts_failed` increases when Orion did not accept an attempted aggregate
   PUT. This is a delivery failure. Check nearby Orion authentication, timeout,
   and response-error records; do not treat it as evidence about source-data
   quality.

For a degraded package, each emitted place's `from` and `to` dictionaries
contain all emitted places. If the source has no movement row between two
emitted places, the value is `0`. An excluded place appears in an emitted
place's dictionary only when the source recorded that movement; the recorded
value is kept exactly, including `0`. With emitted place 3 and excluded place
5, a recorded `5 → 3` count is kept as
`peopleCount_flow_3.value.from["5"]`. If that row is absent, the `"5"` entry is
absent too, and `peopleCount_flow_5` is not published.

An old window can produce `direction_window_degraded` during the automatic
revision sweep. A newly discovered revision is a forced resend, so it attempts
the PUT and reports degradation again even when the package has not changed.
An ordinary sweep retry uses the normal prior-status check. Outside the forced
path, an unchanged prior-`ok` degraded package is skipped with
`direction_window_degraded_unchanged` at DEBUG level: there is no warning,
counter increase, or nonzero-exit effect.

### "A window is stuck `partial` and not clearing"

1. `state_doctor.py {flow|direction}`: read which targets failed
   and check `target_status_category` /
   `expected_target_source` / `retry_reachable`.
2. If `target_status_category=all_ok` and `expected_target_source=stored`:
   the doctor is telling you the aggregate status is stale.
   `state_repair.py … --action recompute_complete --apply`.
3. If `expected_target_source=derived`: legacy row. Provide
   `--expected-target-id` flags from the metadata and then
   `recompute_complete --apply`.
4. If `retry_reachable=false` and the failure is genuinely
   unrecoverable: `state_repair.py … --action dead_letter --reason "…" --apply`.
5. If the source row is still available in MySQL and repeating the write makes
   sense: `resend.py … --from W --to W --send`.

### "Cron fired but `logs/{product}.log` looks empty"

The flow runner short-circuits when `TARGET_FLOW_BATCHES` is empty, and
the direction runner short-circuits when `TARGET_DIRECTION_BATCHES` is
empty. In either case, only a `run_started` / `run_summary` pair is
emitted. Set the product-specific target batch variable in `.env` (see
[configuration.md](configuration.md)).

### "Orion returns 401 / `token_refresh_failed`"

Either the consumer key/secret is wrong, the token endpoint is
unreachable, or the cached token at `state/token.json` is corrupt.
Check the credentials in `.env`, then remove
`state/token.json` and re-run.

### "STH-Comet has a gap for windows around a cutover"

Subscriptions ship with `skipInitialNotification`, so any attribute
updates that landed *before* `create_sth_subscriptions.py --send`
are not replayed. If those windows are still in MySQL, replay them
with `resend.py … --send`. If not, the gap is permanent.

### "Resuming after a planned downtime longer than `REPROCESS_HOURS_*`"

The runner's normal rolling lookback is `REPROCESS_HOURS_*` (default
2h for 5-min windows, 12h for 60-min windows). After a longer
shutdown, that floor isn't wide enough to discover the windows that
elapsed during downtime; dynamic lookback only widens for windows
the state file already knows about, not for never-seen ones.

For an outage of `D` hours, temporarily widen the reprocess floor
before restarting:

1. Set `REPROCESS_HOURS_PER300=D + 1` (add a small margin for
   schedule jitter) in `.env`.
2. If the outage also exceeded the 60-min floor (12h), set
   `REPROCESS_HOURS_PER3600=D + 1` too.
3. Let one or two cron cycles complete so the catch-up SQL range
   covers every window in the gap.
4. Restore the steady-state values once `state_doctor.py` shows the
   gap windows tracked as `pending` / `partial` / `complete`.

The 72h `MAX_LOOKBACK_HOURS_*` ceilings already permit this without
changes. Outages longer than 72h need either a higher ceiling for the
duration of the recovery, or explicit `resend.py … --send` runs for
each lost window.

### "I need to resend a large range without dropping live data"

`resend.py --send` holds the per-product lock (`state/<product>.lock`)
for the whole run. While it runs, every live 5-minute tick for that
product no-ops: it takes the lock non-blocking and fails. This is
**operationally identical to live-cron downtime**: live windows that
elapse during the run are permanently unpublished once they age past the
reprocess floor (`REPROCESS_HOURS_PER300` / `REPROCESS_HOURS_PER3600`,
default 2h / 12h). See "Resuming after a planned downtime longer than
`REPROCESS_HOURS_*`" above for why these never-seen windows are not
auto-recovered: the danger
threshold is the reprocess floor, *not* the 72h `MAX_LOOKBACK_HOURS_*`.
The same caution applies to any long-running tool that holds the
per-product lock, such as `state_repair.py … --apply`.

Before you run a large backfill, set the required flags and know the
gotchas:

1. **`--force`**: required for a *uniform* re-publish. Without it, an
   unchanged target already `ok` in state is skipped while a drifted
   target or a target with no stored record is posted, so a wide range can
   come out non-uniform even though the run reports success. See the
   `--force` row in the `resend.py` reference above.
2. **Intervals differ by product**: Product A needs separate 5-minute and
   60-minute invocations to cover both series. Product B has only the
   60-minute aggregate path and rejects 5-minute requests.
3. **Clear dead-letter windows first.** If the range contains a window an
   operator previously dead-lettered, the run aborts up front (exit 2),
   listing the offending keys, before posting anything. GC never reclaims
   dead-letters, so an *old* range can still hold them. Resolve with
   `state_repair.py` (see its reference above) or narrow the range, then
   re-run.
4. **Read the product-specific summary.** Product A reports POST and
   partial-window counters. Product B reports PUT, degraded, no-payload, and
   source-invalid counters. A failed PUT, an attempted degraded PUT, or an
   all-excluded source-invalid window exits `1`.

Then keep the live cron from starving: pick one of two approaches:

- **Chunk with gaps** (preferred for an unattended range). Split the
  range and run it in pieces, leaving at least one cron interval (≥5 min)
  idle between chunks so a live tick can take the lock and publish current
  windows. Keep each chunk's *expected hold* under the reprocess floor
  (≈2h for 5-min data) so no live window ages out before the next live
  tick fires. Window count is only a rough proxy for hold time; watch
  the actual run duration, not just the range size.
- **Maintenance window** (preferred for one big backfill). Run the resend
  as planned downtime, then republish whatever it shadowed with the
  floor-widening recovery in "Resuming after a planned downtime longer
  than `REPROCESS_HOURS_*`": temporarily raise `REPROCESS_HOURS_*` to
  cover the resend's wall-clock
  duration, let one or two cron cycles catch up, then restore the
  steady-state values.

A skipped live tick prints a one-line contention notice to stderr
(captured by cron), so you can confirm after the fact which ticks a
resend displaced.

If a resend is hard-killed (`SIGKILL` or an OOM kill), it can leave a
`state/.<product>.json.*.tmp` file behind. State is written to that temp
file and then renamed over the real file atomically (`os.replace`), so
after any normal process failure the real state file is intact and the
leftover `.tmp` is never read, so it is safe to delete before re-running.

### "An old `partial` window keeps showing up in `state_doctor.py` after I deactivated a place"

Pre-existing open windows retain their original `expected_target_ids`
snapshot from their first attempt. Deactivating a place in metadata
does not retroactively shrink that set. Resolve with
`state_repair.py recompute_complete --apply` (after confirming the
remaining targets are actually `ok`) or `dead_letter --apply` if the
window is genuinely unrecoverable.
