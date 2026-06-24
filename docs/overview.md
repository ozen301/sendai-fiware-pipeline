# Pipeline Overview

This is the maintainer-facing overview for the Sendai FIWARE pipeline.
Read this first; the other docs assume the vocabulary defined here.

For the canonical data contract (exact column → attribute mapping,
filtering rules, payload shapes) see [pipeline_spec.md](pipeline_spec.md).
This document is the narrative walk-through; the spec is the rulebook.

## What the pipeline does

A scheduled Python job reads aggregated BLE sensor metrics from a private
MySQL database and republishes them as NGSI v2 attribute updates on the
Sendai FIWARE Orion broker. Two independent jobs share the same target
entities:

- **Product A (`run_flow`)** publishes per-place pedestrian counts and stay
  times (the `peopleCount_immedate/near/far` and
  `peopleOccupancy_immedate/near` attributes).
- **Product B (`run_direction`)** publishes inter-place flow ("回遊性") as a
  single `peopleCount_flow` structured attribute carrying each place's
  in/out movement counts.

Both products write to the same per-place entities
(`Blesensor.per3600.<N>` for 60-minute windows, `Blesensor.per300.<N>` for
5-minute windows). They coexist safely because their measurement
attributes are disjoint: Product A writes `peopleCount_immedate/near/far` and
`peopleOccupancy_immedate/near`, Product B writes `peopleCount_flow` (and
also `identifcation` and `dateRetrieved`), and the few shared envelope
attributes (`dateObservedFrom`, `dateObservedTo`) are computed identically
from the same source window, so when both products write them for the
same window, the value is the same either way.

Downstream, Sendai's STH-Comet history service subscribes to Orion and
stores every published value as a time series. The pipeline tags each
attribute with a `TimeInstant` NGSI metadata field carrying the source
window's start time; Comet uses that tag as the history timestamp instead
of the wall-clock time the POST arrived. That way the history record
reflects "this measurement is for 10:00-11:00 JST" even if the POST
actually landed at, for instance, 13:25.

---

## Architecture at a glance

Two independent pipelines, **Product A** (the flow runner) and
**Product B** (the direction runner), read different tables in the
same MySQL database and write to the same per-place FIWARE Orion
entities every 5 minutes. They own disjoint measurement attributes on
those entities (Product A the `peopleCount_*` / `peopleOccupancy_*`
counts, Product B `peopleCount_flow`); both also stamp the shared
`dateObservedFrom` / `dateObservedTo` timestamps, which they compute
identically from the same source window, so those writes agree. As there is no
other overlap, the two products coexist without coordination beyond
their separate state files.

```
MySQL (bleData2025d)
  ├── flow_metrics2_per_place2_agg_imputed ──► Product A (run_flow.py)
  │                                              writes peopleCount_immedate/near/far
  │                                              and peopleOccupancy_immedate/near
  │
  └── direction_metrics2_per_place2_agg    ──► Product B (run_direction.py)
                                                 writes peopleCount_flow

Both products ──► same Orion entities (jp.sendai.Blesensor.per{300,3600}.<N>)
                  ──► STH-Comet history (via Orion subscription)
```

Each product is coordinated across cron runs by its own JSON state
file (`state/flow.json` / `state/direction.json`). The state file
records which `(window, entity)` targets have already received a
successful POST; without it every run would re-POST the same windows
and create duplicate STH-Comet history rows.

Both runners publish two aggregation intervals (5-minute and
60-minute) and are staggered by 2 minutes in cron to spread load:

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
   13:02 ──── cron fires → pipeline reads MySQL, POSTs to Orion, writes state
   (retry) ── partial/failed targets retried every 5 min
   10:00+72h ─ retry horizon (MAX_LOOKBACK_HOURS_PER3600 = 72h): last chance
                for the normal run to pick up this window
```

The **rolling lookback** (`REPROCESS_HOURS_PER300` / `REPROCESS_HOURS_PER3600`)
is the normal-run look-back floor for discovering new windows. The
**retry horizon** (`MAX_LOOKBACK_HOURS_PER300` / `MAX_LOOKBACK_HOURS_PER3600`,
both 72h by default) is the outer limit. Open windows (i.e., windows that are not fully complete) older than this
are not retried automatically.

## Vocabulary

These terms appear throughout this doc and the codebase. Pinning them
down once up front:

**NGSI v2.** The REST API spec FIWARE Orion exposes. Entities have an
`id`, a `type`, and named attributes shaped as
`{"type": …, "value": …, "metadata": …}`. The pipeline talks to Orion
exclusively through `POST /orion/v2.0/entities/<id>/attrs`, which appends or
updates attributes on an existing entity without touching the others.

**Entity.** A single addressable thing in Orion. For us, one per
(place, interval). For example, `jp.sendai.Blesensor.per3600.105` is "place
105, 60-minute aggregates."

**Attribute.** A named field on an entity (e.g. `peopleCount_immedate`).
Each attribute carries an NGSI type, a value, and optional metadata.

**Source row.** One row in a MySQL aggregation table. Each row already
represents one place (or place-pair) for one time window at one
aggregation interval.

**Aggregation interval.** How wide the source window is, in minutes.
The pipeline only publishes `5` and `60`; the source also stores `1` for
internal use, which we skip.

**Source aggregation window (or just "window").** A specific time slice
identified by its start time and interval. For example,
`per3600/20260524_1000` is the 60-minute window starting at
2026-05-24 10:00 JST and ending at 11:00 JST. A window can produce up
to one POST per active target entity in the matching interval; Product
B always emits one payload per active target (using a sentinel `null`
`peopleCount_flow` when there are no observations), while Product A
only emits payloads for targets whose source row survived filtering.

**`TimeInstant`.** An NGSI metadata field attached to every attribute we
write. In this pipeline, we set its value to the window's start time. STH-Comet, when configured
with `metadata: ["TimeInstant"]` on the subscription, uses this as the
history timestamp instead of the wall-clock arrival time. Result: Comet's
history is indexed by *when the measurement is for* rather than *when we
happened to send it*.

**Target.** A single `(entity_id, window)` pair, i.e. "what we're going
to POST." Each window has many targets, one per active entity in the
matching interval.

**Window status.** The aggregate state of one window across all its
targets:
- `pending`: a fresh attempt is in flight. Set at the start of every
  retry, so prior `ok` or `failed` target records may still be present
  underneath; the aggregate is only re-derived after the run finishes.
- `partial`: the run finished with at least one expected target not yet
  `ok` (still missing, `pending`, or `failed`). The window is still
  eligible for the next run's retry.
- `complete`: every entity id in `expected_target_ids` has a recorded
  `ok`. What goes *into* `expected_target_ids` differs by product
  (observed-union for Product A, fixed roster for Product B); see "Record
  outcome" below.
- `dead_letter`: operator-marked unrecoverable. Never retried.

**Target status.** Per-target outcome: `ok` / `failed` / `pending`. Once
`ok`, that target is terminal for that window. Normal retries never
re-POST it, even if the source aggregate later drifts. This is what
makes the pipeline history-idempotent under STH-Comet subscriptions
(re-POSTing a value would create a duplicate Comet row).

**Send mode.** Per-product gate (`FLOW_SEND_MODE`, `DIRECTION_SEND_MODE`).
`dry-run` (default) builds the full payload and writes it to the log
but does not POST attribute updates to Orion; `send` performs the live
attribute-update POSTs. Either way, the runner still makes read-only
`GET` calls to Orion for entity-map validation (so OAuth credentials
must be valid in dry-run too). Flipping to `send` is an explicit
operator action.

**Batch.** Sensor install cohort: In our case, `2023` (`Pixel3aUT`
devices) or `2026` (`M5Stack` devices). Used
for filtering, because the source table stores parallel rows under both
device types and we have to pick the right one. `TARGET_FLOW_BATCHES`
and `TARGET_DIRECTION_BATCHES` select which batches each product
publishes for.

**`identifcation` (misspelled).** A metadata column and NGSI attribute
name. The misspelling matches what the live Sendai broker expects, so do
not "fix" it. Its value is the place number as a string: `"1"`, `"2"`,
… for the 2023 batch and `"101"`, `"102"`, … for the 2026 batch (i.e.
the final dot-separated component of the entity id). The metadata CSV is
authoritative; the pipeline reads the value verbatim from the
`identifcation` column rather than reconstructing it.

**State file.** A per-product JSON file under `state/`
(`state/flow.json`, `state/direction.json`) recording every window's
status and every target's last POST outcome. In `send` mode the file is
rewritten atomically after each target's POST and once more near the
end of the run (for retention GC); in `dry-run` mode it is not
touched.

The state file is the pipeline's memory between cron invocations and
the mechanism that makes it history-idempotent: because STH-Comet
records a new time-series row every time a value is re-POSTed, any
target that has already received an `ok` is permanently skipped on
subsequent runs, even if the source aggregate changes later. Without
the state file, every run would re-POST every window in its rolling
lookback range and fill Comet history with duplicates.

**`null` vs `0`.** For all numeric attributes, `null` means "no
observation was recorded," and `0` means "observed zero." The two are
semantically different and the pipeline preserves both: 
nullable source values are sent as JSON `null`, never coerced to `0`.
This matters most for `peopleCount_flow` (Product B), where a missing
value and a confirmed empty movement are distinct cases.

**Source stability delay.** `SOURCE_STABILITY_DELAY_HOURS` (default 3h).
A window is eligible to publish only if its source `startdate` is at or
before `run_started_at − delay`, floored to the interval boundary. The
aim is to wait until the raw BLE sensor data has actually reached MySQL
and the source aggregator has finished computing the window.
Publishing earlier risks sending an incomplete or inaccurate snapshot
that will silently disagree with the value a later re-aggregation would
have produced. Example: at 13:25 JST with a 3h delay, the cutoff floors
to 10:00, so the latest 60-minute window the pipeline will touch is the
one *starting* at 10:00 (i.e. 10:00-11:00); the 11:00-12:00 window is
held back until at least 14:00.

**Rolling lookback.** `REPROCESS_HOURS_PER300` / `REPROCESS_HOURS_PER3600`
(default 2h for 5-min windows, 12h for 60-min windows). The minimum
look-back floor the normal run uses when discovering new windows. A
window that the state file already knows about (i.e. open) is picked up
regardless of this floor, until it ages past the retry horizon.

**Retry horizon.** `MAX_LOOKBACK_HOURS_PER300` /
`MAX_LOOKBACK_HOURS_PER3600` (both 72h by default). The maximum age at
which an *open window*, one whose state is still `pending` or `partial`
because some target hasn't reported `ok` yet, is still retried by the
normal run. In our case, source rows can arrive at MySQL up to 3 days
late, so both intervals default to 72h regardless of aggregation type.
Windows older than the horizon are not picked up by the normal run; they
require an explicit operator resend (`scripts/resend.py … --send`).

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
   │ 4. POST to Orion             │ ──► FIWARE Orion (NGSI v2)
   │    - auth.py                 │     POST /orion/v2.0/entities/<id>/attrs
   │    - orion_client.py         │
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
   right before the POST (any target already recorded `ok` for that
   window in the state file is not re-POSTed). `pending` and `partial`
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
   to tiers `0`, `1`, and `2`.

3. **Filter & map.** The run entry point loads `metadata/sensors.csv`
   once via `metadata.load_metadata()`; the transforms receive the
   already-built index. The two transforms apply slightly different
   filter orders.

   *Product A* (`transform_flow.py`), per row:
   1. drop if `interval_min ∉ {5, 60}`;
   2. drop if `group_place_id` matches an `IGNORED_PLACE_PREFIXES`
      entry (silent DEBUG; defaults `quick.`, `test`);
   3. drop if no metadata row matches `(place_number, interval_min)`;
   4. drop if the row's `device_type` doesn't match the metadata's
      `expected_device_type` (the source stores parallel `M5Stack` and
      `Pixel3aUT` rows; this picks the right one: `M5Stack` for the
      2026 batch, `Pixel3aUT` for the 2023 batch).

   *Product B* (`transform_direction.py`), per row, against both
   `from_group_place_id` and `to_group_place_id`:
   1. drop if `interval_min ∉ {5, 60}`;
   2. drop if either side matches an `IGNORED_PLACE_PREFIXES` entry
      (the literal `ALL` is exempt; it's a real aggregation key);
   3. resolve each non-`ALL` side via metadata, with the source-side
      batch (derived from the `sendai2023.` / `sendai202603.` prefix)
      required to match the metadata batch, dropping if either side fails
      to resolve;
   4. drop self-loops (`from == to`), while keeping cross-batch pairs
      when both sides resolve;
   5. choose the Product B device type from the oldest active batch for
      the interval, then drop if either device-type field disagrees with
      that selected type. With active 2023 and 2026 targets, the
      selected type is `Pixel3aUT`; with only 2026 active targets, it is
      `M5Stack`.

   Survivors are paired with their metadata row, which provides the
   destination entity id, entity type, and `identifcation` text (the
   misspelled place identifier, just the place number as a string,
   e.g. `"105"`). Product B additionally aggregates surviving rows by
   destination place to build the nested `peopleCount_flow` structure,
   and emits a sentinel `{"from": {"all": null}, "to": {"all": null}}`
   payload for any active target that received no observations.

4. **POST to Orion.** One POST per `(entity, window)` to
   `/orion/v2.0/entities/<entity_id>/attrs?type=<entity_type>` with
   OAuth2 client-credentials auth. Retries use exponential backoff on
   `5xx`, `429`, and network errors: the client sleeps 1s, then 2s, 4s,
   8s, 16s between successive attempts and then gives up. (`429`
   honors the server's `Retry-After` header if present, otherwise uses
   the standard backoff.) A single `401` triggers a forced token
   refresh and one extra retry. Other `4xx` responses are fatal for
   that POST (no retry, because it's a payload/config bug, not a transient
   failure).

5. **Record outcome.** Each POST result is written back to the
   per-product state file with the entity id, HTTP status, and a SHA-256
   of the payload bytes (used to detect "would this re-POST send the
   same value?" during retries). The window aggregate status (`pending`,
   `partial`, `complete`, `dead_letter`) is then recomputed against the
   window's `expected_target_ids`. The two products define that set
   differently:

   - **Product A (per-place counts):** expected = the previously-stored
     set **unioned with** the targets that produced a valid payload this
     run. A per-place sensor that legitimately saw nobody emits no source
     row, so that entity is simply never part of the window's expected
     set, so it does not block completion. The window therefore completes
     against the targets it actually saw. A late row for a target that
     was not seen yet is picked up by send-mode targeted supplemental
     discovery (re-query the retained complete windows by exact
     `startdate`) and expands the expected set on a later run.
   - **Product B (inter-place flow):** every active target receives a
     payload (a sentinel `null` when the place was silent), so expected
     is the fixed roster snapshotted from active metadata on the window's
     *first* attempt and is not expanded from current metadata on retry.
     Snapshotting up front prevents a mid-flight metadata change from
     silently redefining what "complete" means for an in-progress window.

   In both cases the window goes to `complete` once every id in its
   expected set has a recorded `ok`; otherwise it stays `partial`.

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
```

After filter & map:

- Prefix check passes (`sendai202603.` is not in `IGNORED_PLACE_PREFIXES`).
- Interval `60` is allowed.
- `place_number = 105` (parsed from the final `.105` suffix of
  `group_place_id`).
- Metadata lookup `(place_number=105, interval_min=60)` returns
  `entity_id = jp.sendai.Blesensor.per3600.105`,
  `entity_type = Blesensor.per3600`, `batch = 2026`,
  `expected_device_type = M5Stack`,
  `identifcation = "105"` (the place number as a string, which Product B
  posts and Product A does not).
- Device type matches (`M5Stack == M5Stack`).

POST body sent to Orion (the `TimeInstant` metadata on each attribute
tells STH-Comet to index this history record at the window's start time,
2026-05-24 10:00 JST):

```json
POST /orion/v2.0/entities/jp.sendai.Blesensor.per3600.105/attrs?type=Blesensor.per3600
{
  "dateObservedFrom": {"type": "DateTime", "value": "2026-05-24T10:00:00+09:00",
                       "metadata": {"TimeInstant": {"type": "DateTime", "value": "2026-05-24T10:00:00+09:00"}}},
  "dateObservedTo":   {"type": "DateTime", "value": "2026-05-24T11:00:00+09:00", "metadata": {…}},
  "peopleCount_immedate":    {"type": "number", "value": 42,   "metadata": {…}},
  "peopleCount_near":        {"type": "number", "value": 18,   "metadata": {…}},
  "peopleCount_far":         {"type": "number", "value": 7,    "metadata": {…}},
  "peopleOccupancy_immedate":{"type": "number", "value": 12.5, "metadata": {…}},
  "peopleOccupancy_near":    {"type": "number", "value": 3.1,  "metadata": {…}}
}
```

State record after this single POST returned 204, assuming every other
active 60-minute target in the same window also got an `ok` earlier in
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

Product B reads `direction_metrics2_per_place2_agg` and aggregates many
rows into per-place `peopleCount_flow` structures before posting. Key
differences from Product A:

- **Each row has two place keys** (`from_group_place_id`,
  `to_group_place_id`). A movement is from one place to another. Both
  sides must pass metadata resolution. Cross-batch pairs (a 2023 place
  paired with a 2026 place) are sent when both sides resolve.
- **The literal string `ALL` is a real aggregation key**, not a noise
  prefix. Rows with `from_group_place_id = 'ALL'` populate the
  destination entity's `peopleCount_flow.from.all` (meaning "total
  unique BLEIDs that came into this place from anywhere"), and vice
  versa for `to.all`. The pipeline never sums pairwise rows to
  approximate `all`. The source already provides the deduplicated
  unique-BLEID total.
- **The source table stores parallel rows** under both
  `(Pixel3aUT, Pixel3aUT)` and `(M5Stack, M5Stack)` for every per-place
  target. Product B selects one device type per interval from the oldest
  active target batch and applies it to both `ALL` and pairwise rows, so
  the nested inter-place values and the deduplicated totals use the same
  source population. The device-type filter is therefore a required
  disambiguator; omitting it would double-count every movement.
- **Targets with no observations still receive a payload** with sentinel
  `peopleCount_flow = {"from": {"all": null}, "to": {"all": null}}`.
  This keeps Comet history continuous even when a place was silent for
  the window.
- **Completion is fixed-target, not observed-union.** Because every
  active target always gets a payload, Product B's `expected_target_ids`
  is the full roster snapshotted from active metadata on the window's
  first attempt and is never expanded from current metadata on retry (a
  metadata change mid-flight logs `window_expected_targets_changed` and
  keeps the original snapshot). This is the opposite of Product A, whose
  expected set grows from the targets actually observed. Product B has no
  supplemental discovery pass.
- **`null` vs `0`** matters and is preserved end-to-end: `null` means
  no observation, `0` means an observed zero. The two are semantically
  different and the pipeline never collapses one into the other.

## Repo map

```
sendai-fiware-pipeline-dev/
├── sendai_pipeline/         # production package (only this runs from cron)
│   ├── auth.py              # OAuth2 client-credentials: fetch/cache/refresh
│   ├── db.py                # MySQL connection + the two source queries
│   ├── metadata.py          # load + validate metadata/sensors.csv
│   ├── entity_map.py        # query Orion to validate metadata targets exist
│   ├── filter_settings.py   # rollout/source-filter env parsing
│   ├── transform_flow.py    # Product A: rows → per-entity attr payloads
│   ├── transform_direction.py  # Product B: rows → per-entity flow payloads
│   ├── orion_client.py      # NGSI v2 client with retry/backoff
│   ├── comet_client.py      # STH-Comet read and delete helper
│   ├── state.py             # per-window status store (JSON file)
│   ├── state_tools.py       # state inspection/repair primitives
│   ├── sth_subscriptions.py # STH-Comet subscription logic (called by the CLI)
│   ├── create_entities.py   # Orion entity bootstrap logic (called by the CLI)
│   ├── refresh.py           # metadata refresh helpers
│   ├── run_flow.py          # cron entry: Product A, both intervals
│   ├── run_direction.py     # cron entry: Product B, both intervals
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
