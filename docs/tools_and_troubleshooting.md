# Operator Tools and Troubleshooting

Every CLI under `scripts/` that an operator might invoke — what it
does, when to use it, and how — plus a symptom-keyed troubleshooting
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
| **Inspect Orion / Comet** | [`show_data.py`](#show_datapy) |
| **Delete Orion / Comet** | [`delete_entities.py`](#delete_entitiespy), [`delete_history.py`](#delete_historypy), [`delete_subscriptions.py`](#delete_subscriptionspy) |
| **Inspect / repair state** | [`state_doctor.py`](#state_doctorpy), [`state_repair.py`](#state_repairpy) |
| **Replay** | [`resend.py`](#resendpy) |

A symptom-keyed troubleshooting index is at the [bottom of this
doc](#troubleshooting).

---

## Bootstrap tools

### `create_entities.py`

Create one or more Orion entities. Use before publishing to any
entity for the first time — NGSI `POST .../attrs` updates existing
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
parameter — in particular, it does **not** set `options=upsert`, which
would tell Orion to overwrite any existing entity. Instead, the script
treats `201` as created and `409`/`422` as already-existing-on-platform
and skips them, so an existing entity is never replaced. Safe to
re-run.

Example — create the per-3600 and per-300 entities for places 101–102:

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

Reads `COMET_NOTIFY_URL` from `.env`. The creator is idempotent — it
skips existing subscriptions matched on either the per-product
description prefix or the subscription's structural shape.

> **Ordering matters.** Create subscriptions before the first live
> attribute-update POST (see deployment.md §6). `skipInitialNotification`
> is on by default, so updates that happened before the subscription
> existed are not replayed into Comet.

Example — typical operator flow (inspect both products, then create
both for real):

```sh
# Dry-run: print the redacted subscription bodies for both products.
uv run python scripts/create_sth_subscriptions.py

# Looks right — create both subscriptions on the platform.
uv run python scripts/create_sth_subscriptions.py --send
```

**Changing a subscription's trigger attribute or shape.** Because the
creator is idempotent on the description prefix, re-running it after
you change a trigger attribute in code will **not** replace a live
subscription whose shape has drifted — it will see the old one as
"exists" and skip. You have to delete the live subscription first:

```sh
# Find the subscription id, e.g. via:
curl -H "Authorization: Bearer ${TOKEN}" \
  "${FIWARE_BASE_URL}/orion/v2.0/subscriptions?limit=100"

# Then delete it via the operator tool (dry-run first; --send to act).
uv run python scripts/delete_subscriptions.py \
  --reason "replace stale subscription before re-creating with new shape" \
  <id>

# Now re-run the creator to install the new shape.
uv run python scripts/create_sth_subscriptions.py --send
```

The creator also fails fast if it detects a peer-product subscription
whose trigger overlaps this product's history attributes (e.g. Product
B creation aborts when Product A is still on a stale
`dateObservedFrom` trigger), so the correct order is: delete stale
Product A subscription → create new Product A subscription → create
Product B subscription.

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
shouldn't), a `WARNING`-level `stable_seed_changed` event fires —
review those before assuming the refresh is correct.

> **Staged-file schema requirement.** The staged file (currently the
> 2026 batch) **must** carry an `ID` column — not `identifcation`. If
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
    [--attrs LIST | --flow-attrs | --direction-attrs]
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
| `ENTITY_ID[:ENTITY_TYPE]`, `--entity-id` | Explicit entity target. Append `:ENTITY_TYPE` per id or supply `--type`. |
| `--place N` | Resolve place numbers through metadata. Requires `--interval-min`; mutually exclusive with explicit entity ids. |
| `--attrs LIST` | Comma-separated attributes. Required for Comet unless a shortcut is used. |
| `--flow-attrs` | Shortcut for the seven Product A attributes. |
| `--direction-attrs` | Shortcut for Product B attributes (`dateObservedFrom`, `dateObservedTo`, `peopleCount_flow`). |
| `--from` / `--to` / `--last-n` | Comet-only history bounds. `--last-n` defaults to `10` when no bounds are provided. |
| `--h-limit` / `--h-offset` | STH-Comet pagination passthrough. |
| `--aggr-method` / `--aggr-period` | STH-Comet aggregation passthrough. |
| `--pretty` | Render a compact `entity / attr / value / time` table instead of raw JSON. |

Default output is the raw Orion or STH-Comet JSON response, pretty
printed once per entity or entity/attribute pair. A 404 prints a
not-found record and exits 0; other HTTP errors are failures. Pretty
mode renders null values as `null`.

Examples:

```sh
uv run python scripts/show_data.py --source orion --flow-attrs \
  jp.sendai.Blesensor.per3600.101:Blesensor.per3600

uv run python scripts/show_data.py --source comet --type Blesensor.per300 \
  --attrs peopleCount_immedate --last-n 20 \
  jp.sendai.Blesensor.per300.101 jp.sendai.Blesensor.per300.102

uv run python scripts/show_data.py --source orion --place 101 \
  --interval-min 60 --pretty
```

### `delete_entities.py`

Delete one or more Orion entities. Dry-run is the default; pass
`--send` for live deletion. `--purge-history` also deletes the
entity's Comet history after Orion returns deleted or already absent.

```
uv run python scripts/delete_entities.py
    [--purge-history]
    [--attrs LIST | --flow-attrs | --direction-attrs]
    --reason "..."
    [--send]
    [--i-know-this-is-production]
    ENTITY_ID:ENTITY_TYPE [...]
```

Live deletion refuses catch-all FIWARE scopes unless
`--i-know-this-is-production` is present. Attribute flags are only
valid with `--purge-history`.

### `delete_history.py`

Delete STH-Comet history for selected entities or attributes. Dry-run
prints the DELETE URLs and performs no auth or network calls.

```
uv run python scripts/delete_history.py
    [--type TYPE]
    [--attrs LIST | --flow-attrs | --direction-attrs]
    --reason "..."
    [--send]
    [--i-know-this-is-production]
    ENTITY_ID[:ENTITY_TYPE] [...]
```

With no attribute selector, the tool deletes all history for each
entity. With `--attrs` or a shortcut, it deletes one attribute series
per entity. Live deletion uses the same catch-all scope guard as
`delete_entities.py`.

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

Find subscription ids with:

```bash
curl -sS -H "Authorization: Bearer ${TOKEN}" \
  -H "Fiware-Service: ${FIWARE_SERVICE}" \
  -H "Fiware-ServicePath: ${FIWARE_SERVICE_PATH:-/}" \
  "${FIWARE_BASE_URL}/orion/v2.0/subscriptions?limit=100" \
  | jq '.[] | {id, description, condition_attrs: .subject.condition.attrs}'
```

---

## State inspection and repair

The runners persist a per-window state file under `state/` so they
know which targets have already received an `ok` POST. When a window
sticks in `pending` or `partial` and the normal retry isn't clearing
it, use these tools — in order — to inspect, repair, and (only as a
last resort) replay.

### `state_doctor.py`

Read-only diagnostic. Lists every open window (`pending` or
`partial`) for one product and explains why each is open.

```
uv run python scripts/state_doctor.py {flow|direction}
```

| Arg | Purpose |
|---|---|
| `flow` / `direction` (positional, required) | Which product's state to inspect. |

Output is a JSON array, one entry per open window, with fields
including:

- `window` — the window key, e.g. `per3600/20260524_1000`.
- `status` — `pending` or `partial`.
- `interval_min`, `first_seen`, `source_window_start`, `source_window_end` — window timing context.
- `target_status_category` — `all_failed`, `all_ok`, `mixed`, or `missing_targets`.
- `expected_target_source` — `stored` when the v2 expected-target snapshot is present, `derived` for legacy rows that fell back to currently-recorded target keys (treat `derived` as diagnostic only).
- `target_count` / `ok_count` / `failed_count` / `missing_count` — aggregate counts across the window's expected targets.
- `failed_http_statuses` — distinct HTTP status codes seen on `failed` targets.
- `retry_reachable` — whether the window is still inside the configured retry horizon (`MAX_LOOKBACK_HOURS_*`).

The doctor reports aggregate counts rather than per-target records;
inspect `state/{flow,direction}.json` directly (read-only) if you
need the per-target details.

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
# Stale partial whose expected targets are all already ok — dry-run then apply.
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

# Unrecoverable window — dead-letter with audit reason.
uv run python scripts/state_repair.py direction \
  --window per300/20260525_0640 \
  --action dead_letter \
  --reason "source row no longer retained" \
  --apply
```

Do not edit `state/*.json` by hand. Always go through this tool so
the backup, lock acquisition, and reload-and-verify steps happen.

---

## Replay

### `resend.py`

Re-run one or more source windows end-to-end: fetch source rows from
MySQL, build payloads, and POST them to Orion. Use **only** when the
source data is still available in MySQL and a payload/data bug has
been fixed — i.e. when re-POSTing is correct and meaningful.

```
uv run python scripts/resend.py {flow|direction}
    --interval-min {5|60}
    --from YYYYMMDD_HHMM
    --to   YYYYMMDD_HHMM
    [--place N [--place N ...]]
    [--entity-id ID [--entity-id ID ...]]
    --reason "..."
    [--force]
    [--allow-old]
    [--send]
```

| Flag | Purpose |
|---|---|
| `flow` / `direction` (positional) | Which product to resend. |
| `--interval-min 5` / `60` | Source aggregation interval of the windows. |
| `--from YYYYMMDD_HHMM` | First source window start, JST. Inclusive. |
| `--to YYYYMMDD_HHMM` | Last source window start, JST. Inclusive. Equal to `--from` replays exactly one window. |
| `--place N` | Place number filter; repeatable. Resolved through metadata. Mutually exclusive with `--entity-id`. |
| `--entity-id ID` | Explicit entity id; repeatable. Mutually exclusive with `--place`. |
| `--reason "..."` | Required. Recorded as audit context in the run log. |
| `--force` | Bypass the per-target payload-hash skip. By default, targets whose last `ok` payload hash matches the new payload are skipped. |
| `--allow-old` | Allow `--from` to predate `now − MAX_LOOKBACK_HOURS_*` for the chosen interval. Default refuses old ranges to prevent surprise wide replays. |
| `--send` | Perform live Orion writes. Omit for dry-run: prints the planned per-window plan and exits before any MySQL query, Orion token fetch, or Orion HTTP call. |

Resend writes to the same `window_key` as the original publication
because it's a retry of the original business window, not a synthetic
replacement. By default, existing per-target `ok` records with
unchanged payload hashes are skipped (the same code path as normal
send-mode retries) — so resend is safe to use even when a window is
already partly complete. Pass `--force` when the intent is to
redeliver values that haven't changed.

Examples:

```sh
# Single window, dry-run.
uv run python scripts/resend.py direction \
  --interval-min 5 \
  --from 20260525_0640 --to 20260525_0640 \
  --reason "payload shape fix"

# Same single window, live.
uv run python scripts/resend.py direction \
  --interval-min 5 \
  --from 20260525_0640 --to 20260525_0640 \
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

> **STH-Comet caveat.** Once subscriptions are active, re-POSTing a
> value creates a new Comet history row even when the Orion value is
> unchanged (the subscription fires on metadata changes, including the
> `TimeInstant` re-emission). Use resend judiciously and prefer
> `state_repair.py recompute_complete` whenever the failure was a
> bookkeeping bug rather than a missed POST.

---

## Troubleshooting

For each symptom, the recommended first tool comes first; later tools
are escalations only if the earlier one rules out the simpler cause.

### "A window is stuck `partial` and not clearing"

1. `state_doctor.py {flow|direction}` — read which targets are
   missing or failed and check `target_status_category` /
   `expected_target_source` / `retry_reachable`.
2. If `target_status_category=all_ok` and `expected_target_source=stored`:
   the doctor is telling you the aggregate status is stale.
   `state_repair.py … --action recompute_complete --apply`.
3. If `expected_target_source=derived`: legacy row. Provide
   `--expected-target-id` flags from the metadata and then
   `recompute_complete --apply`.
4. If `retry_reachable=false` and the failure is genuinely
   unrecoverable: `state_repair.py … --action dead_letter --reason "…" --apply`.
5. If the source row is still available in MySQL and re-POSTing makes
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
elapsed during downtime — dynamic lookback only widens for windows
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
duration of the recovery, or explicit `resend.py … --allow-old` runs for
each lost window.

### "An old `partial` window keeps showing up in `state_doctor.py` after I deactivated a place"

Pre-existing open windows retain their original `expected_target_ids`
snapshot from their first attempt. Deactivating a place in metadata
does not retroactively shrink that set. Resolve with
`state_repair.py recompute_complete --apply` (after confirming the
remaining targets are actually `ok`) or `dead_letter --apply` if the
window is genuinely unrecoverable.
