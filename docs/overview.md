# Pipeline Overview

This is the maintainer-facing overview for the Sendai FIWARE pipeline.
Read this first; the other docs assume the vocabulary defined here.

For the canonical data contract (exact column → attribute mapping,
filtering rules, payload shapes) see [pipeline_spec.md](pipeline_spec.md).
This document is the narrative walk-through; the spec is the rulebook.

## What the pipeline does

A scheduled Python job reads aggregated BLE sensor metrics from a private
MySQL database and publishes them as NGSI v2 attributes to the Sendai FIWARE
Orion broker. Two independent jobs use different target models:

- **Product A (`run_flow`)** publishes ten per-place attributes: source-window
  and retrieval timestamps, exact entity identity,
  `peopleCount_immedate/near/far`, and
  `peopleOccupancy_immedate/near/far`.
- **Product B (`run_direction`)** bundles one 60-minute inter-place flow
  ("回遊性") window into dynamic `peopleCount_flow_<N>` attributes and
  replaces one aggregate entity's full attribute set.

Product A writes per-place `Blesensor.per3600.<N>` and
`Blesensor.per300.<N>` entities with update-only `POST /attrs`. Product B
writes the exclusively owned aggregate entity `jp.sendai.Blesensor.flow` of
type `Blesensor.flow` by default with replace-all `PUT /attrs`. The Product B
target is configurable, but it remains one entity and must not contain
unrelated attributes or receive writes from another owner.
Product A exclusively owns product data on the per-sensor entities.
Descriptive attributes such as `latitude`, `longitude`, `locationName`, and
`status` may coexist there; they are sensor metadata, not Product B data.

Downstream, Sendai's STH-Comet history service subscribes to Orion and
stores every published value as a time series. The pipeline tags each
attribute with a `TimeInstant` NGSI metadata field carrying the source
window's start time; Comet uses that tag as the history timestamp instead
of the wall-clock time the write arrived. That way the history record
reflects "this measurement is for 10:00-11:00 JST" even if the write
actually landed at, for instance, 13:25.

---

## Architecture at a glance

Two independent pipelines read different tables in the same MySQL database.
Product A updates individual sensor entities. Product B replaces one aggregate
direction entity for each sendable 60-minute window. Both run on the five-minute
schedule, but Product B never sends 5-minute direction rows.

```
MySQL (bleData2025d)
  ├── flow_metrics2_per_place2_agg_imputed ──► Product A (run_flow.py)
  │                                              publishes peopleCount_immedate/near/far
  │                                              and peopleOccupancy_immedate/near/far,
  │                                              with window/retrieval/identity attrs
  │
  └── direction_metrics2_per_place2_agg    ──► Product B (run_direction.py)
                                                 publishes one 60-min aggregate
                                                 peopleCount_flow_<N> package

Product A ──► per-place Orion entities (jp.sendai.Blesensor.per{300,3600}.<N>)
Product B ──► one aggregate Orion entity (jp.sendai.Blesensor.flow by default)
Both      ──► STH-Comet history (via separate Orion subscriptions)
```

Each product is coordinated across cron runs by its own JSON state
file (`state/flow.json` / `state/direction.json`). Product A records the
per-place targets for each window. Product B records the single aggregate
target for each sendable 60-minute window. Operator scripts build only on the
public `publish_*_window` / `replay_*_window` seam; the automatic supplemental-
and revision-recovery paths are internal runner policy, not public APIs.

Both runners are staggered in cron to spread load. Product A publishes both
intervals; Product B publishes only 60-minute windows:

```
:00  :02  :04  :05  :06  :07  :09  :10  :12  :14  :15 ...
      ^         ^         ^         ^         ^
      A         B         A         B         A         (repeats)
```

Key timing concepts (see the Vocabulary section for exact definitions):

```
Timeline for one window (example: 60-min window covering 10:00-11:00 JST)

   10:00 ──── source window starts
   11:00 ──── source window closes (start + 60 min)
   13:00 ──── earliest eligible publish (window start + SOURCE_STABILITY_DELAY_HOURS (defaults to 3h))
   13:02 ──── cron fires → pipeline reads MySQL, publishes to Orion, records state
   (retry) ── partial/failed targets retried every 5 min
   10:00+72h ─ retry horizon (MAX_LOOKBACK_HOURS_PER3600 = 72h): last chance
                for the normal run to pick up this window
```

The **rolling lookback** (`REPROCESS_HOURS_PER300` / `REPROCESS_HOURS_PER3600`)
is the normal-run lookback floor for discovering new windows. The
**retry horizon** (`MAX_LOOKBACK_HOURS_PER300` / `MAX_LOOKBACK_HOURS_PER3600`,
both 72h by default) is the outer limit for that fresh path. At steady state,
the revision sweep can also retry older open windows whose revisions are
already behind its cursor.

## Vocabulary

These terms appear throughout this doc and the codebase. Pinning them
down once up front:

**NGSI v2.** The REST API spec FIWARE Orion exposes. Entities have an
`id`, a `type`, and named attributes shaped as
`{"type": …, "value": …, "metadata": …}`. Product A uses
`POST /orion/v2.0/entities/<id>/attrs` to update selected attributes. Product B
uses `PUT` on the same `/attrs` resource to replace the aggregate entity's
complete attribute set.

**Entity.** A single addressable thing in Orion. Product A uses one per
(place, interval); for example, `jp.sendai.Blesensor.per3600.105` is place 105's
60-minute aggregate. Product B uses one aggregate entity for all places included
in a source-window package.

**Attribute.** A named field on an entity (e.g. `peopleCount_immedate`).
Each attribute carries an NGSI type, a value, and optional metadata.

**Source row.** One row in a MySQL aggregation table. Each row already
represents one place (or place-pair) for one time window at one
aggregation interval.

**Aggregation interval.** How wide the source window is, in minutes. Product A
publishes `5` and `60`; Product B publishes only `60`. The source also stores
`1` for internal use, which both products skip.

**Source aggregation window (or just "window").** A specific time slice
identified by its start time and interval. For example,
`per3600/20260524_1000` is the 60-minute window starting at 2026-05-24
10:00 JST and ending at 11:00 JST. Product A produces one update per
observed place; Product B builds one aggregate replacement per sendable
window. Exactly which places a Product B window includes, and when it
writes nothing, is the source-quality contract in
[pipeline_spec.md §4.3](pipeline_spec.md#43-attribute-mapping-request-body).

**`TimeInstant`.** An NGSI metadata field attached to every attribute the
pipeline writes, set to the window's start time. Comet indexes history by this
value rather than the wall-clock arrival time (see the intro above and
[pipeline_spec.md §2.6](pipeline_spec.md#26-timeinstant-metadata)).

**Target.** A single `(entity_id, window)` delivery. Product A can have many
per-place targets in one window. A sendable Product B window has exactly one:
the configured aggregate entity.

**Window status.** The aggregate state of one window across all its
targets:
- `pending`: a fresh attempt is in flight. Set at the start of every
  retry, so prior `ok` or `failed` target records may still be present
  underneath; the aggregate is only re-derived after the run finishes.
- `partial`: the run finished with at least one expected target not yet
  `ok` (still missing, `pending`, or `failed`). The window is still
  eligible for the next run's retry.
- `complete`: every entity id in `expected_target_ids` has a recorded `ok`.
  Product A uses its observed-target union; Product B uses the single aggregate
  id. Product B creates no state record when no candidate row survives, or when
  candidates survive but every one is missing a required total. If Product B
  successfully writes the complete places while omitting other candidates with
  missing totals, that delivery is still `complete`.
- `dead_letter`: operator-marked unrecoverable. Never retried.

**Target status.** Per-target outcome: `ok` / `failed` / `pending`. An unchanged
prior-`ok` payload is skipped. A revised payload is written again and appends a
corrective STH-Comet row under the original window `TimeInstant`.

**Send mode.** Per-product gate (`FLOW_SEND_MODE`, `DIRECTION_SEND_MODE`).
`dry-run` (default) builds the full payload and writes it to the log but does
not mutate Orion; `send` performs Product A updates or Product B replacements.
Either way, the runner still makes read-only
`GET` calls to Orion for entity-map validation (so OAuth credentials
must be valid in dry-run too). Flipping to `send` is an explicit
operator action.

**Batch.** Sensor install cohort: `2023` (`Pixel3aUT` devices) or `2026`
(`M5Stack` devices) in this deployment. Used
for filtering, because the source table stores parallel rows under both device
types and the pipeline must pick the correct one. `TARGET_FLOW_BATCHES` selects
Product A publish targets. `TARGET_DIRECTION_BATCHES` gates Product B source
inclusion and oldest-device-type selection; it does not select the aggregate
Orion target.

**`identifcation` (misspelled).** An NGSI attribute name kept for platform
compatibility. Product A writes the exact metadata-selected `entity_id` used
in the request URL (not the legacy CSV column of the same name); Product B
writes its configured aggregate entity id. See
[pipeline_spec.md Metadata governance](pipeline_spec.md#metadata-governance).

**State file.** A per-product JSON file under `state/`
(`state/flow.json`, `state/direction.json`) recording every window's status and
every target's last write outcome. In `send` mode the file is rewritten
atomically after each target's write and once more near the
end of the run (for retention GC); in `dry-run` mode it is not
touched.

The state file is the pipeline's memory between cron invocations. It skips
unchanged successful deliveries while allowing changed source windows to append
corrective history rows.

**`null` vs `0`.** For all numeric attributes, `null` means "no
observation was recorded," and `0` means "observed zero." The two are
semantically different and the pipeline preserves both: nullable source
values are sent as JSON `null`, never coerced to `0`. Product B's rule for
when a missing pairwise movement becomes `0` versus no key at all is in
[pipeline_spec.md §2.7](pipeline_spec.md#27-null-vs-0) and
[§4.3](pipeline_spec.md#43-attribute-mapping-request-body).

**Source stability delay.** `SOURCE_STABILITY_DELAY_HOURS`. A window is
eligible to publish only once its source `startdate` is at or before
`run_started_at − delay`, floored to the interval boundary, so the source
aggregator has time to finish computing it. Example: at 13:25 JST with a 3h
delay the cutoff floors to 10:00, so the latest 60-minute window touched is
the one starting at 10:00; 11:00-12:00 waits until at least 14:00.

**Rolling lookback.** `REPROCESS_HOURS_PER300` / `REPROCESS_HOURS_PER3600`.
The minimum lookback floor the normal run uses to discover new windows. An
open window already in state is picked up regardless of this floor, until it
ages past the retry horizon.

**Retry horizon.** `MAX_LOOKBACK_HOURS_PER300` / `MAX_LOOKBACK_HOURS_PER3600`.
The maximum age at which an open (`pending`/`partial`) window is still retried
by the fresh path. At steady state, the revision sweep can retry older open
windows whose revisions are already behind its cursor; otherwise they need an
explicit `scripts/resend.py … --send`. Defaults for all three settings are in
[configuration.md](configuration.md), and the recovery conditions are in
[pipeline_spec.md §2.10](pipeline_spec.md#210-scheduling-and-retry).

## End-to-end data flow

For every scheduled run, each product walks the same five-stage pipeline.
The state file is read at stage 1 to know what's already done, and
written at stage 5 to record what just happened:

```
       cron (every 5 minutes)
                 │
                 ▼
   ┌──────────────────────────────┐
   │ 1. Window selection          │ ◄── reads state/{flow,direction}.json
   │    - run_{flow,direction}.py │     (which windows are still open?)
   │    - state.py                │
   └─────────────┬────────────────┘
                 │
                 ▼
   ┌──────────────────────────────┐
   │ 2. Source read               │ ◄── reads MySQL (bleData2025d)
   │    - db.py                   │     - flow_metrics2_per_place2_agg_imputed     
   └─────────────┬────────────────┘     - direction_metrics2_per_place2_agg
                 │
                 ▼
   ┌──────────────────────────────┐
   │ 3. Filter & map              │ ◄── reads metadata/sensors.csv
   │    - metadata.py             │     (place → entity_id, batch, device)
   │    - transform_flow.py       │
   │    - transform_direction.py  │
   └─────────────┬────────────────┘
                 │
                 ▼
   ┌──────────────────────────────┐
   │ 4. Write Orion attributes    │ ──► FIWARE Orion (NGSI v2)
   │    - auth.py                 │     Product A: update per-place attrs
   │    - orion_client.py         │     Product B: replace aggregate attrs
   └─────────────┬────────────────┘
                 │
                 ▼
   ┌──────────────────────────────┐
   │ 5. Record outcome            │ ──► writes state/{flow,direction}.json
   │    - state.py                │     (mark each target ok/failed,
   └─────────────┬────────────────┘      recompute window status)
                 │
                 ▼
        Orion notifies STH-Comet
        via subscription; Comet
        stores history indexed
        by TimeInstant
```

All files in the boxes are under `sendai_pipeline/`.

The five stages in plain words:

1. **Window selection.** The run identifies which source aggregation
   windows are eligible to publish *this run*. A window is eligible when
   its `startdate` is at or before `run_started_at −
   SOURCE_STABILITY_DELAY_HOURS` (default 3h), floored to the interval.
   Otherwise the source aggregator may still be revising it. The SQL
   query fetches rows for every eligible window in the lookback range,
   not just the open ones; per-target skipping happens later in stage 4,
   right before the write. `pending` and `partial`
   windows remain in scope as long as they are still inside the retry
   horizon (`MAX_LOOKBACK_HOURS_PER300` / `MAX_LOOKBACK_HOURS_PER3600`,
   both 72h by default, since source rows can arrive at MySQL up to 3 days
   late).

2. **Source read.** One MySQL query per interval fetches all rows for
   the eligible windows. The pipeline uses each table's natural composite
   key: `(startdate, group_place_id, device_type, interval_min)` for
   Product A; the from/to/device-type variants for Product B. Product A
   also applies the source-quality gate in SQL: only rows with
   `imputation_tier <= SOURCE_MAX_IMPUTATION_TIER` are fetched, defaulting
   to tiers `0`, `1`, and `2`. Each fetched row carries
   `flow_gt_m60/m80/m120` for counts and nullable
   `stay_gt_m60/m80/m120` for occupancy.

3. **Filter & map.** The run entry point loads `metadata/sensors.csv`
   once via `metadata.load_metadata()`; the transforms receive the
   already-built index. Both drop noise-prefix rows, resolve each place
   through metadata, and keep only the interval's expected device type (the
   source stores parallel `M5Stack` and `Pixel3aUT` rows; picking one avoids
   double-counting). Product A (`transform_flow.py`) maps each surviving row
   to one per-place entity. Product B (`transform_direction.py`) resolves both
   the `from` and `to` side of each row, treats the literal `ALL` as a real
   aggregation key, and selects one city-wide device type from the oldest
   targeted active batch. The exact per-row filter order is
   [pipeline_spec.md §3.2](pipeline_spec.md#32-filtering-rules) (Product A) and
   [§4.2](pipeline_spec.md#42-filtering-rules) (Product B); which places a
   Product B window then emits or excludes is
   [§4.3](pipeline_spec.md#43-attribute-mapping-request-body).

4. **Write Orion attributes.** Product A sends one update-only `POST /attrs`
   per `(entity, window)`; Product B sends one replace-all `PUT /attrs` to the
   configured aggregate entity per sendable 60-minute window. Writes retry
   transient failures (`5xx`, `429`, network errors) with exponential backoff
   and force one token refresh on a `401`, while other `4xx` responses are
   fatal for that write; the exact backoff schedule and `429` / `401` handling
   are [pipeline_spec.md §2.10](pipeline_spec.md#210-scheduling-and-retry).
   Product B dry-run shows the `PUT /attrs` target and full aggregate payload
   without mutating Orion.

5. **Record outcome.** Each write result is written back to the
   per-product state file with the entity id, HTTP status, and a SHA-256
   of the payload's stable attributes. The hash answers "would this re-send
   the same semantic value?" during retries; an unchanged prior-`ok` target
   is skipped. The window status (`pending`, `partial`, `complete`,
   `dead_letter`) is then recomputed against its `expected_target_ids`, a set
   the two products define differently: **Product A** unions the previously
   stored targets with those that produced a payload this run, so a sensor
   that saw nobody never blocks completion; **Product B** expects only the one
   configured aggregate entity. A window is `complete` once every expected id
   has a recorded `ok`. The exact rules — supplemental discovery, the hash
   scope, and Product B's degraded/source-invalid outcomes — are in
   [pipeline_spec.md §2.9](pipeline_spec.md#29-idempotency),
   [§3.2.1](pipeline_spec.md#321-window-completion-observed-target), and
   [§4.3](pipeline_spec.md#43-attribute-mapping-request-body).

### A row's life: worked example (Product A)

A single 60-minute row in `flow_metrics2_per_place2_agg_imputed`:

```
startdate         = "20260524_1000"        # JST window 10:00-11:00
group_place_id    = "sendai202603.105"
device_type       = "M5Stack"
interval_min      = 60
flow_gt_m60       = 42                     # peopleCount_immedate
flow_gt_m80       = 18                     # peopleCount_near
flow_gt_m120      = 7                      # peopleCount_far
stay_gt_m60       = 12.5                   # peopleOccupancy_immedate
stay_gt_m80       = 3.1                    # peopleOccupancy_near
stay_gt_m120      = 0.0                    # peopleOccupancy_far
```

After filter & map:

- Prefix check passes (`sendai202603.` is not in `IGNORED_PLACE_PREFIXES`).
- Interval `60` is allowed.
- `place_number = 105` (parsed from the final `.105` suffix of
  `group_place_id`).
- Metadata lookup `(place_number=105, interval_min=60)` returns
  `entity_id = jp.sendai.Blesensor.per3600.105`,
  `entity_type = Blesensor.per3600`, `batch = 2026`,
  `expected_device_type = M5Stack`. Product A writes that exact `entity_id` as
  the `identifcation` attribute; it does not use the legacy metadata CSV column
  with the same misspelled name.
- Device type matches (`M5Stack == M5Stack`).

Ten-attribute POST body sent to Orion (the `TimeInstant` metadata on each attribute
tells STH-Comet to index this history record at the window's start time,
2026-05-24 10:00 JST):

```json
POST /orion/v2.0/entities/jp.sendai.Blesensor.per3600.105/attrs?type=Blesensor.per3600
{
  "dateObservedFrom": {"type": "DateTime", "value": "2026-05-24T10:00:00+09:00",
                       "metadata": {"TimeInstant": {"type": "DateTime", "value": "2026-05-24T10:00:00+09:00"}}},
  "dateObservedTo":   {"type": "DateTime", "value": "2026-05-24T11:00:00+09:00", "metadata": {…}},
  "dateRetrieved":    {"type": "DateTime", "value": "2026-07-23T09:12:34+09:00", "metadata": {…}},
  "identifcation":    {"type": "Text", "value": "jp.sendai.Blesensor.per3600.105", "metadata": {…}},
  "peopleCount_immedate":    {"type": "number", "value": 42,   "metadata": {…}},
  "peopleCount_near":        {"type": "number", "value": 18,   "metadata": {…}},
  "peopleCount_far":         {"type": "number", "value": 7,    "metadata": {…}},
  "peopleOccupancy_immedate":{"type": "number", "value": 12.5, "metadata": {…}},
  "peopleOccupancy_near":    {"type": "number", "value": 3.1,  "metadata": {…}},
  "peopleOccupancy_far":     {"type": "number", "value": 0.0,  "metadata": {…}}
}
```

State record after this single POST returned 204, assuming every other
observed 60-minute target in the same window also got an `ok` earlier in
the run (truncated for readability; the real `targets` map contains
one entry per id in `expected_target_ids`):

```json
"per3600/20260524_1000": {
  "status": "complete",
  "expected_target_ids": [
    "jp.sendai.Blesensor.per3600.1",
    …,
    "jp.sendai.Blesensor.per3600.105",
    …
  ],
  "targets": {
    "jp.sendai.Blesensor.per3600.1":   {"status": "ok", "last_http_status": 204, "last_payload_sha256": "…"},
    "…":                                {"…": "…"},
    "jp.sendai.Blesensor.per3600.105": {"status": "ok", "last_http_status": 204, "last_payload_sha256": "…"}
  }
}
```

For Product A, `expected_target_ids` is the union of any
previously-stored expected set and the targets this window produced a
valid payload for this run, *not* the full active-metadata roster. A
per-place sensor that saw nobody produces no source row, so its entity
is simply absent from this set and does not hold the window in
`partial`. The window reaches `complete` once every id in the set has a
`targets[…].status = "ok"` record; if one of the targets had returned
500 instead, the window would stay `partial` until the next run retries
the failed ones (and the `105` target would not be re-POSTed because
it's already `ok`). If a late source row later appears for an entity the
window never saw, supplemental discovery re-queries that exact
`startdate`, adds the entity to `expected_target_ids`, and POSTs it
without re-sending the targets that are already `ok`.

### Product B differences

Product B reads `direction_metrics2_per_place2_agg` and builds one aggregate
package per sendable 60-minute source window. The shape a consumer sees, and
the exact emit/exclude and `from`/`to` rules, are
[pipeline_spec.md §4.3](pipeline_spec.md#43-attribute-mapping-request-body);
the differences worth carrying as intuition are:

- **Each row is a movement** with a `from` and a `to` place, so both sides
  must resolve through metadata. The literal `ALL` is a real total key, not
  noise: a place needs both `ALL → N` (arrivals) and `N → ALL` (departures)
  before it gets its own `peopleCount_flow_<N>` attribute, and the pipeline
  uses those source totals verbatim rather than summing the pairwise movements.
- **One city-wide device type** (the oldest targeted active batch's) is
  applied to both `ALL` and pairwise rows, so all counts come from the same
  population; this disambiguation is required, not optional.
- **The attribute roster is dynamic.** The package carries
  `peopleCount_flow_<N>` only for places with both totals, plus a
  `sourceQuality` attribute naming any place dropped for a missing total.
  There is no fixed range and no sentinel for absent places.
- **One aggregate target, last-written wins.** A sendable window expects only
  the configured aggregate entity. Because a revision write for an older
  window can become current, consumers read `dateObservedFrom` /
  `dateObservedTo` to know which window is shown.

Here is the concrete degraded case. Place 3 has both hourly totals, so Product B
publishes `peopleCount_flow_3`. Place 5 has `ALL → 5` but is missing `5 → ALL`,
so Product B does not publish `peopleCount_flow_5`. The source did record 12
people moving `5 → 3`, so that count is kept on place 3 as
`peopleCount_flow_3.value.from["5"]`. If the `5 → 3` row had been absent, the
`"5"` key would also be absent; Product B would not invent `"5": 0`.

Place 3's `from.all` stays at the source value 85. Product B does not recompute
it from the visible place-number keys under `from`, which sum to 12 in this
abbreviated example.
`sourceQuality` tells consumers that place 5 was omitted because its outgoing
total was missing. The example omits `dateObservedFrom` and `dateObservedTo`
and elides repeated metadata:

```json
{
  "dateRetrieved": {"type": "DateTime", "value": "2026-05-24T13:25:43+09:00", "metadata": {…}},
  "identifcation": {"type": "Text", "value": "jp.sendai.Blesensor.flow", "metadata": {…}},
  "sourceQuality": {
    "type": "StructuredValue",
    "value": {
      "status": "degraded",
      "evaluatedAt": "2026-05-24T13:25:43+09:00",
      "excludedPlaceNumbers": [5],
      "missingFromAllPlaceNumbers": [],
      "missingToAllPlaceNumbers": [5]
    },
    "metadata": {"TimeInstant": {"type": "DateTime", "value": "2026-05-24T10:00:00+09:00"}}
  },
  "peopleCount_flow_3": {
    "type": "StructuredValue",
    "value": {"from": {"all": 85, "3": 0, "5": 12}, "to": {"all": 82, "3": 0}},
    "metadata": {…}
  }
}
```

This package does not contain place 5's own attribute, its surviving
`ALL → 5` total, or any movement whose two places are both excluded. A later
full-window rebuild restores those values if the source supplies `5 → ALL` and
place 5 becomes publishable.

## Repo map

```
sendai-fiware-pipeline/
├── sendai_pipeline/         # production package (only this runs from cron)
│   ├── auth.py              # OAuth2 client-credentials: fetch/cache/refresh
│   ├── db.py                # MySQL connection + the two source queries
│   ├── metadata.py          # load + validate metadata/sensors.csv
│   ├── entity_map.py        # query Orion to validate metadata targets exist
│   ├── filter_settings.py   # rollout/source-filter env parsing
│   ├── transform_flow.py    # Product A: rows → per-entity attr payloads
│   ├── transform_direction.py  # Product B source transform
│   ├── orion_client.py      # NGSI v2 HTTP client with retry/backoff
│   ├── comet_client.py      # STH-Comet read and delete helper
│   ├── state.py             # per-window status store (JSON file)
│   ├── state_tools.py       # state inspection/repair primitives
│   ├── state_report.py      # state-doctor JSON and dashboard rendering
│   ├── sth_subscriptions.py # STH-Comet subscription logic (called by the CLI)
│   ├── create_entities.py   # Orion entity bootstrap logic (called by the CLI)
│   ├── refresh.py           # metadata refresh helpers
│   ├── run_flow.py          # cron entry: Product A, both intervals
│   ├── run_direction.py     # cron entry: Product B direction
│   ├── revision_sweep.py    # shared revision work selection
│   ├── settings_validation.py # shared environment-value handling
│   ├── windowing.py         # shared source-window and datetime helpers
│   └── logging_setup.py     # rotating JSON-line logger
│
├── scripts/                 # thin operator-facing CLI shims over sendai_pipeline/
│   ├── create_entities.py        # → sendai_pipeline.create_entities (bootstrap Orion entities)
│   ├── create_sth_subscriptions.py  # → sendai_pipeline.sth_subscriptions (create Comet subscriptions)
│   ├── show_data.py              # read current Orion values or STH-Comet history
│   ├── delete_entities.py        # delete Orion entities, optionally purging Comet history
│   ├── delete_history.py         # delete STH-Comet history
│   ├── refresh_metadata.py       # rebuild metadata/sensors.csv from inputs
│   ├── state_doctor.py           # read-only state inspection
│   ├── state_repair.py           # repair aggregate status / dead-letter
│   ├── migrate_flow_state.py     # one-off: migrate flow state to observed-target completion
│   ├── resend.py                 # replay one or a range of source windows
│   └── dev/                      # REPL/notebook probes, not production
│
# Note: `create_entities` and `sth_subscriptions` each have two files:
# the module under `sendai_pipeline/` does the work, the file under
# `scripts/` is just the CLI entry point. See
# [tools_and_troubleshooting.md](tools_and_troubleshooting.md) for
# which one to invoke (always the one under scripts/).
│
├── docs/                    # tracked docs (this dir)
│   ├── overview.md          # ← you are here
│   ├── configuration.md     # env-var reference
│   ├── deployment.md        # first-time install + cutover + cron
│   ├── tools_and_troubleshooting.md  # per-script details + troubleshooting playbooks
│   ├── pipeline_spec.md     # canonical data contract
│   ├── source_schema_reference.md  # MySQL DESC snapshots
│
├── tests/                   # pytest suite (one file per module)
├── metadata/                # runtime sensor metadata (gitignored)
├── state/                   # window state + token cache (gitignored)
├── logs/                    # rotating runtime logs (gitignored)
├── ref_docs/                # local-only reference material (gitignored)
│
├── .env / .env.example      # runtime configuration
└── README.md                # pitch + basic deploy + tool index
```

The gitignored directories (`metadata/`, `state/`, `logs/`, `ref_docs/`,
`.env`) carry production credentials, runtime data, and restricted
reference material. Do not commit them.

## Where to go next

- **Setting up a new host:** [deployment.md](deployment.md)
- **Day-to-day operations and troubleshooting:** [tools_and_troubleshooting.md](tools_and_troubleshooting.md)
- **Configuration reference:** [configuration.md](configuration.md)
- **Authoritative data semantics:** [pipeline_spec.md](pipeline_spec.md)
