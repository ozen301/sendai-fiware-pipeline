# Sendai FIWARE Pipeline Spec

Canonical data contract for the pipeline: exact column → attribute
mappings, filter rules, payload shapes, and the operational rules that
both products must obey. For the narrative walk-through and operator
vocabulary, see [overview.md](overview.md).

The pipeline runs **two independent jobs** against Sendai's FIWARE
platform:

| # | Source MySQL table | What gets sent | Target Orion entity |
|---|---|---|---|
| **A** | `bleData2025d.flow_metrics2_per_place2_agg_imputed` | Per-place pedestrian counts and stay times (gap-filled superset of `flow_metrics2_per_place2_agg`) | `jp.sendai.Blesensor.per3600.<N>` (60-min) and `jp.sendai.Blesensor.per300.<N>` (5-min), **one entity per place** |
| **B** | `bleData2025d.direction_metrics2_per_place2_agg` | Place-to-place flow ("回遊性") | One exclusively owned aggregate entity, `jp.sendai.Blesensor.flow` by default (see §4) |

## Contents

- §1 Infrastructure
- §2 Common rules (apply to both products)
- §3 Product A: per-place counts
- §4 Product B: inter-place flow
- §5 Orion endpoints and STH-Comet subscriptions

---

## 1. Infrastructure

### MySQL source server

The source MySQL server lives on a private network and is reachable only
from authorised hosts. Connection host, database name,
and the read account are kept out of this committed spec; see `.env` or
the team's secrets store. The pipeline is expected to run on a host that
is already on the same network.

### FIWARE side: common headers for every request

Every Orion attribute write carries:

```
Accept:              application/json
Content-Type:        application/json
Authorization:       Bearer <accessToken>     # OAuth2 client-credentials, auto-refresh
Fiware-Service:      ${FIWARE_SERVICE}        # omitted when env var is empty
Fiware-ServicePath:  ${FIWARE_SERVICE_PATH}   # always emitted; defaults to "/"
```

`Fiware-Service` is emitted only when configured; production runs set it
to the platform-assigned tenant. `Fiware-ServicePath` is always emitted
and defaults to `/` when `FIWARE_SERVICE_PATH` is unset or empty.

The access token has an expiry; the pipeline refreshes it automatically
via the OAuth2 client-credentials grant against the WSO2 API Manager that
fronts Sendai's FIWARE deployment (see [overview.md](overview.md) for
where this lives in the code).

### Metadata governance

Product A entity IDs, entity types, install batches, expected device types, and
the required `identifcation` column are loaded from `metadata/sensors.csv`.
Product A writes an `identifcation` attribute whose value is the exact
metadata-selected `entity_id` used in that payload's Orion request URL, for
example `jp.sendai.Blesensor.per3600.1000`. It does not use the CSV column also
named `identifcation`; that legacy column's per-place values may be bare place
numbers. Product B sets the aggregate `identifcation` value to
`PRODUCT_B_AGGREGATE_ENTITY_ID`. The pipeline reads this file at startup and
does not reconstruct Product A entity ids or types from a pattern. Product B
also uses the metadata for source eligibility and device-type selection, but
its single aggregate target comes from `PRODUCT_B_AGGREGATE_ENTITY_ID` and
`PRODUCT_B_AGGREGATE_ENTITY_TYPE` (§4).

Product A exclusively owns product data on the per-sensor `Blesensor.per300`
and `Blesensor.per3600` entities, while Product B exclusively owns product data
on its configured aggregate entity. Descriptive sensor attributes such as
`latitude`, `longitude`, `locationName`, and `status` may coexist with Product A
data on sensor entities. Bare legacy Product B `peopleCount_flow` may not
coexist there.

Orion entity queries only validate non-blockingly that configured targets
exist. A missing entity is logged, but the configured target remains
authoritative and the write result determines the window outcome.

`metadata/sensors.csv` is produced from a stable manually seeded
metadata file (currently the 2023 batch) plus the latest refreshable
metadata sheet (currently the 2026 batch). 

---

## 2. Common rules

These rules apply to **both** Product A and Product B.

### 2.1 Allowed intervals

Product A publishes `interval_min ∈ {5, 60}` rows. Product B publishes only
`interval_min = 60`; it ignores 5-minute direction rows in fresh sends and
revision sweeps. `interval_min = 1` rows exist for further source aggregation
and are never sent by either product.

### 2.2 Noise prefixes: dropped silently

Some `group_place_id` rows are internal artifacts that the operator
wants neither emitted nor flagged. The configured prefixes are:

| Prefix | Examples | 
|---|---|
| `quick.` | `quick.1` |
| `test`   | `test01.1` |

Matching rows are filtered out **before** the metadata lookup and
produce a `DEBUG`-level `ignored_place_prefix` log event. Rows that
slip past the prefix filter but find no matching metadata entry produce
a separate `DEBUG`-level `unknown_place_interval` event. Keeping the
two event names distinct preserves a structured signal for genuine
metadata gaps (e.g. a `sendai2023.99` row with no metadata entry)
without forcing them through a WARN channel. Operators can filter on
the event name when triaging.

The prefix list is operator-configurable via `IGNORED_PLACE_PREFIXES`
(see [`.env.example`](/.env.example)). Matching is `startswith` against
`group_place_id`. For Product B, the literal aggregation key `'ALL'` is
**not** a noise prefix; it is exempt from this filter (see §4).

### 2.3 Metadata-driven Product A entity mapping

Mapping is **metadata-driven**, not pattern-reconstructed:

```
group_place_id     →  the part after the last '.'        = place_number
(place_number, interval_min)
                   →  look up row in metadata/sensors.csv = metadata row
metadata.entity_id, metadata.entity_type                  ↓
                  POST URL = .../entities/{metadata.entity_id}/attrs
                  query    = ?type={metadata.entity_type}
```

The metadata CSV's `entity_id` and `entity_type` columns are the authoritative
Product A target source. The pipeline **must not** reconstruct an entity id
from `interval_min × 60`, even if today's metadata follows that pattern in
100% of rows. Product B resolves source places through this metadata but writes
the aggregate target configured in §4 instead of these per-place targets.

`group_place_id` prefix per install batch (informational, used only to
derive `place_number`):

| Prefix | Install batch |
|---|---|
| `sendai2023.` | 2023設置 (places 1-28) |
| `sendai202603.` | 2026年3月設置 (places 101-112, 201-210) |

### 2.4 Device-type filtering (batch disambiguation)

The source tables store parallel rows under both `Pixel3aUT` and `M5Stack`.
Product A selects the expected type from each place's metadata:

| Place number range | Install batch | Use rows where `device_type =` |
|---|---|---|
| 1-28 | 2023設置 | `Pixel3aUT` |
| 101-112, 201-210 | 2026年3月設置 | `M5Stack` |

Product B instead selects one city-wide device type from the oldest targeted
active batch for interval 60 and applies it to both places in every included
row (§4.2). Device-type filtering is a required disambiguator for both products;
omitting it would double-count.

### 2.5 NGSI attribute types

The Sendai broker carries the following types on Product A per-place entities
and historical Product B per-place attributes. Product A matches the existing
convention exactly; writing
the canonical NGSI `Integer` / `Number` would change the stored type
and create mixed-type history in STH-Comet.

| Attribute | NGSI type |
|---|---|
| `dateObservedFrom`, `dateObservedTo`, `dateRetrieved` | `DateTime` |
| `identifcation` | `Text` |
| `peopleCount_immedate`, `peopleCount_near`, `peopleCount_far` | `"number"` (lowercase string) |
| `peopleOccupancy_immedate`, `peopleOccupancy_near`, `peopleOccupancy_far` | `"number"` (lowercase string) |
| historical bare `peopleCount_flow` | `StructuredValue` |

The `identifcation` and `immedate` spellings are intentional. The live Sendai
entities already used these names before this pipeline, so the pipeline keeps
them for compatibility instead of renaming them.

> Verified 2026-05-23 against the live broker for `Blesensor.per3600.*`
> and `Blesensor.per300.*` entities. Their `peopleOccupancy_far` attribute
> uses `"number"`; Product A preserves that existing type when mapping
> `stay_gt_m120`.

### 2.6 TimeInstant metadata

Every attribute either product writes carries the exact NGSI metadata object
below, where `<dateObservedFrom>` is the source-window start:

```json
{"TimeInstant": {"type": "DateTime", "value": "<dateObservedFrom>"}}
```

When the STH-Comet subscription
requests `metadata: ["TimeInstant"]`, Comet uses this value as the
stored history timestamp instead of the wall-clock receive time. This
aligns Comet `recvTime` with the logical aggregate window start.

### 2.7 `null` vs `0`

For all numeric attributes: `null` means "no observation," `0` means
"observed zero." The two are semantically different and the pipeline preserves
both. Product A sends nullable source values as JSON `null`.

For Product B, the representation depends on which places have both required
hourly totals (§4.3). Between two places whose totals are complete, a missing
pairwise source row is written as `0` because both places' totals show that they
were measuring. If a place is missing one of its totals, Product B keeps a
pairwise movement between it and a place whose totals are complete only when
the source contains that exact row. A recorded count of `0` is kept as `0`; if
the row is absent, the place-number key is absent. Product B never uses a null
matrix or invents a `0` for an unrecorded movement involving a place whose
totals are incomplete.

### 2.8 Time zone and retrieval timestamp

All `DateTime` values are explicit JST (`+09:00`). NTP sync is required
on the pipeline host. Orion v2 rejects sub-second precision in
DateTime values; the pipeline truncates to whole seconds before
emitting.

Each top-level Product A run or resend captures one retrieval timestamp at its
invocation boundary, normalizes it to JST, and truncates it to whole seconds.
That timestamp becomes `dateRetrieved` and is reused across every Product A
payload and every HTTP retry in the invocation.

### 2.9 Idempotency

In normal send mode, a prior successful target result is terminal for
`(product, interval, source window, entity_id)` only while the payload hash is
unchanged. Failed or missing targets are retried. A prior-`ok` target with an
unchanged payload hash is skipped as a real no-op; a prior-`ok` target whose
payload hash differs is written again by its owning product.

A window is `complete` once every entity id in its expected target set has a
recorded `ok`. Product A unions that set from the targets actually observed
(§3.2). Product B has exactly one expected target, the configured aggregate
entity (§4). A Product B attempt that produces no aggregate payload records no
window state.

Product A's semantic hash excludes only the top-level `dateRetrieved` attribute
and includes the other nine §3.3 attributes. The stable `identifcation` and
`peopleOccupancy_far` values remain in the hash, so a new retrieval timestamp
alone does not cause another POST.

Once STH-Comet subscriptions are enabled, a drift or revision write appends a
corrective history row instead of replacing the old row. For Product B, the
corrective row retains the source window's original `TimeInstant` and carries a
new `dateRetrieved` send time. Existing Comet history is not deleted, rewritten,
or purged during the aggregate cutover.

### 2.10 Scheduling and retry

| Topic | Rule |
|---|---|
| Schedule | Cron (or systemd timer), every 5 minutes. |
| Source stability delay | Process windows whose `startdate` is at or before `now − SOURCE_STABILITY_DELAY_HOURS` (default 3h). Separate from the 72h retry horizon. |
| Fresh-path catch-up | Each run reprocesses a rolling `startdate` lookback against the per-window state store. Missed or failed targets are picked up on the next run while their window is still inside the lookback. The lookback widens to cover open windows already in state, but only up to `MAX_LOOKBACK_HOURS_*` (72h by default). The fresh path cannot reach a window it never saw once that window ages past the reprocess floor (`REPROCESS_HOURS_*`). |
| Revision-sweep catch-up | A second catch-up path is independent of the normal `startdate` lookback. It scans each source table by `aggregated_at` to discover older `(interval_min, startdate)` windows whose source rows were inserted or revised at or after the sweep cursor, then sends the current payload for those windows. Because it selects by `aggregated_at` rather than by recency, the sweep also recovers such missed windows: their `aggregated_at` is at or after the cursor, so the sweep discovers and resends them. When the sweep is disabled (`REVISION_SWEEP_ENABLED=false`) it does not run at all — no scan, no resend — and even when enabled it cannot reach windows whose `aggregated_at` predates the cursor's starting point. In those cases, republish the affected windows per "Resuming after a planned downtime longer than `REPROCESS_HOURS_*`" in [tools_and_troubleshooting.md](tools_and_troubleshooting.md). |
| Revision cursor | `last_aggregated_at` is a per-product, forward-only watermark: automatic discovery considers revisions at or after this value, while revisions below it are outside the sweep. It lives at the top of that product's state JSON (`state/flow.json` or `state/direction.json`) and advances forward once per run. A failed send does not hold it back; failures are remembered in the per-window state store and retried from there. To re-sweep from an earlier point, edit that product's cursor in its state file. |
| Retry | Exponential backoff on `5xx` and network errors (1s, 2s, 4s, 8s, 16s). On `429`, the client honors a `Retry-After` header when present and otherwise falls back to the same backoff. Single `401` triggers a forced token refresh and one extra retry within the retry budget (a `401` arriving on the final attempt is not retried further). Other `4xx` is fatal for that write. |
| Token refresh | OAuth2 client-credentials; proactive on expiry and on `401`. |
| Logging | One structured line per Orion write in `logs/{product}.log` (rotating). The line carries the target entity id, HTTP status, and a payload hash + byte count. Whether the full request/response bodies are also logged is controlled by `LOG_PAYLOAD_MODE`: `hash` (always hash only), `failure` (default: hash on success, body excerpt on failure), or `full` (always body). |

Revision cursor comparisons depend on the MySQL session time zone.
`last_aggregated_at` is stored as a JST ISO timestamp, then formatted
as a second-resolution `YYYY-MM-DD HH:MM:SS` SQL bound. The source
`aggregated_at` columns are MySQL `timestamp` columns, so MySQL compares
and returns them in the current session time zone. Runtime DB sessions
used by the runners must therefore resolve to JST (`+09:00`), for
example `@@session.time_zone = 'SYSTEM'` only when
`@@system_time_zone = 'JST'`. A non-JST session can shift revision
cursor ranges and cause the sweep to skip or repeat revised windows.

The fresh-path and revision-sweep catch-ups split work by window age, at
the lookback's lower bound (i.e., the oldest `startdate` the fresh path
reprocesses this run). The fresh path owns windows whose `startdate` is
at or after that bound (the more recent windows, inside the lookback);
the sweep owns windows whose `startdate` is strictly before it (the
older windows). No window is processed by both paths in one run.
§2.9's payload-hash check collapses redundant writes on the fresh path
and on sweep *retry* items, but a sweep *discovery* force-sends the
current payload without a hash-skip, so during an initial cursor drain a
window the fresh path already re-sent can be re-sent once more as an
additional corrective history row.

When a send-mode Product B revision sweep has no stored cursor, it initializes
`last_aggregated_at` to the run's start time, truncated to whole seconds. The
cursor is forward-only thereafter, so the automatic sweep discovers revisions
at or after that starting point. Use `scripts/resend.py` for deliberate replay
of older source windows.

### 2.11 Rollout gates

Use product-specific batch gates to control deployment scope. For Product A,
`TARGET_FLOW_BATCHES` selects per-place publish targets. For Product B,
`TARGET_DIRECTION_BATCHES` is a source inclusion and rollout gate: it controls
which source places can contribute and which included batch supplies the
oldest targeted device type. It does not select Orion targets; Product B always
writes the single configured aggregate entity. Empty or unset Product B batches
remain a safe no-op.

---

## 3. Data Product A: Per-place counts

Target entity types: `Blesensor.per3600.<N>` (60-min), `Blesensor.per300.<N>` (5-min).

### 3.1 Source schema: `flow_metrics2_per_place2_agg_imputed`

A gap-filled superset of `flow_metrics2_per_place2_agg`. Original and
imputed rows are eligible only up to the configured imputation tier:
the pipeline reads rows where `imputation_tier <= SOURCE_MAX_IMPUTATION_TIER`
(`2` by default, equivalent to "smaller than 3"). `imputed_flag` is
preserved for possible future metadata or filtering but does not affect
current behavior.

Load-bearing columns:

| Column | Use |
|---|---|
| `startdate` | Source window, format `YYYYMMDD_HHMM`. |
| `group_place_id` | Source place key; final numeric suffix maps to metadata `place_number`. |
| `device_type` | Batch filter (see §2.4). |
| `interval_min` | See §2.1. |
| `aggregated_at` | DB-stamped timestamp (`DEFAULT` / `ON UPDATE CURRENT_TIMESTAMP`) that moves on every insert or revision. The revision sweep uses it to discover changed `(interval_min, startdate)` windows. |
| `imputation_tier` | Product A source-quality gate; only rows at or below `SOURCE_MAX_IMPUTATION_TIER` are read. |
| `flow_gt_m60`, `flow_gt_m80`, `flow_gt_m120` | Count attributes. |
| `stay_gt_m60`, `stay_gt_m80`, `stay_gt_m120` | Nullable occupancy attributes (`decimal(10,1)`). |

The source uniquely identifies rows by
`(startdate, group_place_id, device_type, interval_min)`.

Full `DESC` output and a representative sample row are kept in
[source_schema_reference.md](source_schema_reference.md).

### 3.2 Filtering rules

Product A first applies the SQL-level source-quality gate:
`imputation_tier <= SOURCE_MAX_IMPUTATION_TIER` (`2` by default). Rows
above that tier are not fetched and do not reach transform-layer
filtering.

Fetched rows then apply the §2 common rules. Per-row order (matches
`transform_flow.py`):

1. Drop if `interval_min ∉ {5, 60}` (§2.1).
2. Drop if `group_place_id` matches an `IGNORED_PLACE_PREFIXES` entry (§2.2).
3. Drop if no metadata row matches `(place_number, interval_min)`.
4. Drop if `device_type` ≠ metadata `expected_device_type` (§2.4).

Each surviving row produces one payload for the resolved entity.

### 3.2.1 Window completion (observed-target)

A Product A window's `expected_target_ids` is the stored set **unioned
with** the entities that produced a valid payload on the current run. It
is *not* the full active-metadata roster: a per-place sensor that saw
nobody emits no source row, so that entity is never required and does
not hold the window in `partial`. The window completes against the
targets it actually observed.

Late source rows can still arrive for a window that already completed
and aged past the normal rolling lookback. In send mode the pipeline
runs **targeted supplemental discovery**: it re-queries the retained
`complete` windows by exact `startdate` (one `startdate IN (…)` query
per interval, bounded to windows whose source start falls between the
max-lookback horizon and the normal lookback's lower bound). A row for a
not-yet-seen entity expands that window's expected set and is POSTed
without re-sending the targets already `ok`; a re-query that reveals no
new entity is a no-op. Dry-run performs no supplemental discovery.

Old on-disk flow state written under the previous full-roster model is
converted with the one-off `scripts/migrate_flow_state.py` tool
(dry-run by default; `--apply` to write, after a timestamped backup),
which re-derives each window's expected set from its recorded targets,
recomputes status, and drops windows that recorded no targets.

### 3.3 Attribute mapping (request body)

| Attribute | NGSI type | Value source |
|---|---|---|
| `dateObservedFrom` | `DateTime` | `startdate` parsed as JST → ISO 8601 (`YYYY-MM-DDTHH:MM:00+09:00`) |
| `dateObservedTo` | `DateTime` | `dateObservedFrom + interval_min` minutes |
| `dateRetrieved` | `DateTime` | Run-level retrieval timestamp from §2.8 |
| `identifcation` | `Text` | Exact metadata-selected `entity_id` used in the request URL |
| `peopleCount_immedate` | `number` | `flow_gt_m60` |
| `peopleCount_near` | `number` | `flow_gt_m80` |
| `peopleCount_far` | `number` | `flow_gt_m120` |
| `peopleOccupancy_immedate` | `number` | Nullable `stay_gt_m60` converted to `float` |
| `peopleOccupancy_near` | `number` | Nullable `stay_gt_m80` converted to `float` |
| `peopleOccupancy_far` | `number` | Nullable `stay_gt_m120` converted to `float` |

The same column names (`flow_gt_mXX`, `stay_gt_mXX`) are used for both
5-min and 60-min rows; the interval is carried by the row's
`interval_min` column, not the column name.

For the occupancy attributes, source `null` becomes JSON `null`, while source
zero becomes `0.0`; no observation and observed zero remain distinct. Every
attribute in the ten-attribute payload carries `TimeInstant` metadata whose
value is `dateObservedFrom`, as specified in §2.6.

For example, a 60-minute row for the metadata-selected entity
`jp.sendai.Blesensor.per3600.1000` produces this request body. The retrieval
timestamp is shared by all Product A payloads and retries in the invocation;
the `TimeInstant` on every attribute remains the source-window start.

```json
{
  "dateObservedFrom": {
    "type": "DateTime",
    "value": "2026-03-15T15:00:00+09:00",
    "metadata": {"TimeInstant": {"type": "DateTime", "value": "2026-03-15T15:00:00+09:00"}}
  },
  "dateObservedTo": {
    "type": "DateTime",
    "value": "2026-03-15T16:00:00+09:00",
    "metadata": {"TimeInstant": {"type": "DateTime", "value": "2026-03-15T15:00:00+09:00"}}
  },
  "dateRetrieved": {
    "type": "DateTime",
    "value": "2026-07-23T09:12:34+09:00",
    "metadata": {"TimeInstant": {"type": "DateTime", "value": "2026-03-15T15:00:00+09:00"}}
  },
  "identifcation": {
    "type": "Text",
    "value": "jp.sendai.Blesensor.per3600.1000",
    "metadata": {"TimeInstant": {"type": "DateTime", "value": "2026-03-15T15:00:00+09:00"}}
  },
  "peopleCount_immedate": {
    "type": "number",
    "value": 6,
    "metadata": {"TimeInstant": {"type": "DateTime", "value": "2026-03-15T15:00:00+09:00"}}
  },
  "peopleCount_near": {
    "type": "number",
    "value": 237,
    "metadata": {"TimeInstant": {"type": "DateTime", "value": "2026-03-15T15:00:00+09:00"}}
  },
  "peopleCount_far": {
    "type": "number",
    "value": 430,
    "metadata": {"TimeInstant": {"type": "DateTime", "value": "2026-03-15T15:00:00+09:00"}}
  },
  "peopleOccupancy_immedate": {
    "type": "number",
    "value": 0.2,
    "metadata": {"TimeInstant": {"type": "DateTime", "value": "2026-03-15T15:00:00+09:00"}}
  },
  "peopleOccupancy_near": {
    "type": "number",
    "value": null,
    "metadata": {"TimeInstant": {"type": "DateTime", "value": "2026-03-15T15:00:00+09:00"}}
  },
  "peopleOccupancy_far": {
    "type": "number",
    "value": 0.0,
    "metadata": {"TimeInstant": {"type": "DateTime", "value": "2026-03-15T15:00:00+09:00"}}
  }
}
```

---

## 4. Data Product B: Inter-place flow

Product B writes one aggregate package per sendable 60-minute source window to
one exclusively owned Orion entity. The confirmed default target is:

| Setting | Confirmed default |
|---|---|
| `PRODUCT_B_AGGREGATE_ENTITY_ID` | `jp.sendai.Blesensor.flow` |
| `PRODUCT_B_AGGREGATE_ENTITY_TYPE` | `Blesensor.flow` |

Each write replaces the entity's complete attribute set with `PUT /attrs`.
Operators must not attach unrelated attributes or other writers to this entity,
because a Product B write intentionally removes attributes omitted from the new
source-window package. Product A remains a per-place `POST /attrs` writer and
does not use this entity.

The aggregate entity must exist before Product B writes it. Validation is
non-blocking: a missing entity is logged, Product B still attempts the write,
and the `PUT /attrs` result is authoritative. A missing aggregate entity makes
that write fail with 404, so fresh environments must bootstrap it explicitly
with both id and type before enabling Product B.

### 4.1 Source schema: `direction_metrics2_per_place2_agg`

Load-bearing columns:

| Column | Use |
|---|---|
| `startdate` | Source window, format `YYYYMMDD_HHMM`. |
| `from_group_place_id`, `to_group_place_id` | Source place keys; may also be literal `'ALL'` for aggregate rows. |
| `from_device_type`, `to_device_type` | Product B device-population filter; both sides must match the selected device type for the interval. |
| `interval_min` | Product B accepts exactly `60`. |
| `aggregated_at` | DB-stamped timestamp (`DEFAULT` / `ON UPDATE CURRENT_TIMESTAMP`) that moves on every insert or revision. The revision sweep uses it to discover changed `(interval_min, startdate)` windows, then re-fetches the full window's rows to build and send the complete aggregate payload. It never builds a payload from a partial revised-row set. |
| `count` | Movement count used inside dynamic `peopleCount_flow_<N>` attributes. |

### 4.2 Filtering rules

Apply these rules before constructing the aggregate package:

1. Keep only `interval_min = 60`. Five-minute direction rows never produce a
   Product B write, even when `REPROCESS_HOURS_PER300` or
   `MAX_LOOKBACK_HOURS_PER300` is configured.
2. Drop a row if either place id matches `IGNORED_PLACE_PREFIXES`. The literal
   `'ALL'` is exempt because it is a source total key.
3. Resolve every non-`ALL` endpoint through active metadata. Its source batch
   must be included by `TARGET_DIRECTION_BATCHES`, and the source-side batch
   prefix must agree with the metadata batch.
4. Select the expected device type from the oldest targeted active batch for
   interval 60. Keep a row only when both device-type fields match it.

Self-loop rows are retained. A surviving `N→N` row is a real movement route,
not a duplicate to discard. Cross-batch pairs are also valid when both places
resolve and the row passes the selected device-type filter.

Cross-batch direction pairs are valid Product B observations. The same
selected device type is used for pairwise rows and `'ALL'` rows so the
inter-place values and the pre-computed deduplicated totals come from
the same source device population.

The source table stores parallel pairwise and `'ALL'` rows under both
`(Pixel3aUT, Pixel3aUT)` and `(M5Stack, M5Stack)` for every place. Mixed
`(Pixel3aUT, M5Stack)` pairings do not occur. The device-type filter is a
required disambiguator, not just a cross-batch exclusion. Adding a batch to
`TARGET_DIRECTION_BATCHES` can change the oldest selected device type for all
included places; this city-wide filter is part of the contract.

### 4.3 Attribute mapping (request body)

One replace-all write is built for each sendable source window. The body is:

| Attribute | NGSI type | Value source |
|---|---|---|
| `dateObservedFrom` | `DateTime` | `startdate` parsed as JST |
| `dateObservedTo` | `DateTime` | `dateObservedFrom + 60 minutes` |
| `dateRetrieved` | `DateTime` | now in JST (truncated to whole seconds) |
| `identifcation` | `Text` | exactly `PRODUCT_B_AGGREGATE_ENTITY_ID` |
| `sourceQuality` | `StructuredValue` | source-integrity status and excluded-place lists for this package |
| `peopleCount_flow_<N>` | `StructuredValue` | totals and pairwise movements for emitted place `N`, as defined below |

Each attribute has the metadata object defined in §2.6. There is exactly one
`identifcation` attribute, its value always equals the configured aggregate
entity id, and it is included in every package.

#### Which places receive a flow attribute

A place needs two source totals for its own `peopleCount_flow_<N>` attribute:

- `ALL → N` is the number of people who arrived at place `N` from everywhere;
  it supplies `peopleCount_flow_<N>.value.from.all`.
- `N → ALL` is the number of people who left place `N` for everywhere; it
  supplies `peopleCount_flow_<N>.value.to.all`.

The pipeline uses the source totals verbatim. It does not reconstruct a missing
total by summing pairwise movements.

After the §4.2 filters, the spec calls every non-`ALL` place in a surviving row
a **candidate place**. A candidate with both totals is an **emitted place**:
Product B publishes its `peopleCount_flow_<N>` attribute. A candidate missing
one or both totals is an **excluded place**: Product B drops that place's own
attribute and records the reason in `sourceQuality`. The emitted-place roster is
the sorted list of candidates with both totals.

For example, suppose place 3 has both totals, while place 5 has `ALL → 5` but is
missing `5 → ALL`. Product B publishes `peopleCount_flow_3`, does not publish
`peopleCount_flow_5`, and reports place 5 in `excludedPlaceNumbers` and
`missingToAllPlaceNumbers`. This is a **degraded package**: at least one place
can be published, but at least one other candidate was excluded. A package with
no excluded candidates is **clean**. If every candidate is excluded, the whole
window is source-invalid and Product B writes nothing.

A place with no surviving source row is not a candidate and is simply absent.
Product B does not create sentinel or null attributes to fill a fixed roster.

#### What the `from` and `to` dictionaries contain

For every emitted place `N`, `peopleCount_flow_<N>.value` has this shape. For
example, with an emitted roster of places 3, 5, and 10,
`peopleCount_flow_10.value` is:

```json
{
  "from": { "all": 26, "3": 24, "5": 2, "10": 0 },
  "to":   { "all": 24, "3": 22, "5": 2, "10": 0 }
}
```

Each `from` and `to` dictionary contains every emitted place number exactly
once, plus `"all"`. If a pairwise row between two emitted places is missing,
the corresponding value is `0`. Both places have complete totals, so this zero
means that they were measuring and no movement was observed between them.

For a surviving `M→N` row, the count populates `from.<M>` of place `N` and
`to.<N>` of place `M`. A surviving self-loop `N→N` row populates both
`from.<N>` and `to.<N>` of place `N`. Each dictionary includes the emitted
place's own key even when that self-loop count is `0`.

Product B also keeps an actual recorded movement between an emitted place and
an excluded place. In the place 3 / place 5 example above:

- a surviving `5 → 3` row is stored as
  `peopleCount_flow_3.value.from["5"]`;
- a surviving `3 → 5` row is stored as
  `peopleCount_flow_3.value.to["5"]`.

The source count is copied exactly, including a recorded `0`. If the source has
no `5 → 3` row, Product B does not create `from["5"]`; if it has no `3 → 5`
row, Product B does not create `to["5"]`. These extra keys may therefore differ
between emitted attributes and between one attribute's `from` and `to`
dictionaries. Seeing a place 5 key inside `peopleCount_flow_3` does not mean
that `peopleCount_flow_5` exists.

Dropping place 5 still loses information from this package: its own
`peopleCount_flow_5` attribute is absent, including its surviving `ALL → 5`
total; a movement between two excluded places is absent; and an excluded
place's self-loop is absent. If a later source correction supplies the missing
total, a full-window rebuild publishes the place's attribute and restores these
movements.

An emitted place's `from.all` and `to.all` are the verbatim deduplicated source
totals. They can include movement involving excluded places or other places
outside the emitted roster, whether or not the matching pairwise source row
survived. The pipeline does not reduce or recompute `all` to match the matrix,
and consumers must not expect the published pairwise entries to sum to `all`.

#### How consumers identify a clean or degraded package

Every written package contains `sourceQuality`, so a consumer can see whether
any candidate was excluded without reading pipeline logs or knowing the
expected place roster. Its shape is exactly:

```json
"sourceQuality": {
  "type": "StructuredValue",
  "value": {
    "status": "clean",
    "evaluatedAt": "<dateRetrieved>",
    "excludedPlaceNumbers": [],
    "missingFromAllPlaceNumbers": [],
    "missingToAllPlaceNumbers": []
  },
  "metadata": {
    "TimeInstant": {
      "type": "DateTime",
      "value": "<dateObservedFrom>"
    }
  }
}
```

For a degraded package, `status` is `"degraded"`,
`missingFromAllPlaceNumbers` is the ascending list of candidates without
`ALL → N`, `missingToAllPlaceNumbers` is the ascending list without `N → ALL`,
and `excludedPlaceNumbers` is the ascending union of those lists. In the place
3 / place 5 example, these lists are `[]`, `[5]`, and `[5]`, respectively.
`excludedPlaceNumbers` includes every excluded candidate, even when no
pairwise movement between it and an emitted place was recorded.

`evaluatedAt` exactly equals the package's scalar `dateRetrieved.value`.
`sourceQuality` is always present, including on clean revision writes. It is an
additive attribute: a clean package preserves the names, NGSI types, value
shapes, and semantics of all pre-existing attributes.

The builder and aggregate-history enumeration place `sourceQuality` after
`identifcation` and before dynamic flow attributes for deterministic output.
NGSI consumers must not treat attribute ordering as part of the contract.

All attributes use `TimeInstant = dateObservedFrom`, so degraded and corrected
quality samples for one source window have the same Comet `recvTime`. When raw
history contains more than one `sourceQuality` sample at that `recvTime`, a
consumer selects the sample with the greatest parseable `evaluatedAt`; response
order is not a correction-order signal. Because emitted timestamps have whole-
second precision, two writes in the same second can also tie on `evaluatedAt`.
Raw history cannot resolve that tie; Orion current state is the authoritative
last-written package.

The attribute roster is dynamic. Historical names
`peopleCount_flow_1` through `peopleCount_flow_28` do not define a fixed range;
any valid emitted place number becomes an attribute suffix.

#### No-write outcomes and state

| Candidates | Complete candidates | Write decision | Resulting state | Signal | Send-mode exit effect |
|---|---:|---|---|---|---:|
| none | none | no payload; no PUT | no record | `direction_window_no_payload` at `DEBUG`; `windows_no_payload += 1` | unchanged |
| all complete | one or more | ordinary clean-payload PUT/skip rules | `complete` after success or unchanged prior `ok`; ordinary failure state otherwise | ordinary PUT/window counters | existing policy |
| some complete and some excluded; PUT required | one or more | attempt one degraded PUT | `complete` after success; ordinary `partial`/`dead_letter` failure state otherwise | `direction_window_degraded` at `WARNING`; `windows_degraded += 1`; normal PUT counters also apply | 1 |
| some complete and some excluded; prior-`ok` payload hash unchanged and not forced | one or more | skip PUT | `complete` | `direction_window_degraded_unchanged` at `DEBUG`; no degraded counter | unchanged |
| all excluded | none | source-invalid; no PUT | no record | `direction_window_source_invalid` at `WARNING`; `windows_source_invalid += 1` | 1 |

A sendable window has one expected target id, the configured aggregate entity
id, and uses the state key `per3600/<startdate>`. Each attempt replaces any
stored Product B expected-target snapshot with that one id. A 204 response to
the aggregate `PUT /attrs` marks it `ok` without a follow-up GET.

Both degraded events carry the window key and the sorted
`excluded_place_numbers`, `missing_from_all_place_numbers`, and
`missing_to_all_place_numbers`. `windows_degraded` counts degraded payloads
whose PUT was attempted in send mode or previewed in dry-run. It does not count
every degraded window transformed during rolling lookback. A successful
degraded PUT increments `puts_ok` and `windows_complete`; a failed one increments
`puts_failed` and the applicable `windows_partial` or `windows_dead_letter`
counter. Either attempted send increments `windows_degraded` and makes send mode
exit 1. A successful degraded write is delivery-complete and is not retried
solely because it is degraded.

An unchanged prior-`ok` degraded package is a DEBUG no-op: it does not increment
`windows_degraded` or affect exit status. A forced resend is an attempted PUT
and signals degradation again. Newly discovered revision-sweep work uses that
forced path even when the semantic hash is unchanged; old sweep retry items use
the ordinary retry and prior-status path. Dry-run remains non-mutating and exits
0; it can preview `windows_degraded > 0` while `windows_complete == 0`.

The semantic hash excludes top-level `dateRetrieved` and nested
`sourceQuality.value.evaluatedAt`, but includes the quality status and lists and
all other emitted attributes. Hash canonicalization does not mutate the outgoing
payload. Replace-all writes remove an excluded place from Orion current state.
If a later full-window rebuild finds repaired totals and `aggregated_at`
advances, the clean package restores the place and its routes. Its changed
quality status/lists remain inside the hash and trigger the ordinary prior-`ok`
drift PUT. If a repair does not advance `aggregated_at`, an explicit forced
direction resend is required.

### 4.4 Full body example

This clean package represents 2024-07-20 10:00-11:00 JST. Places 3 and 5 both
have the two required totals, so both receive a flow attribute and
`sourceQuality` reports no excluded places. Every attribute carries the same
`TimeInstant` metadata value; it is shown in full on the first attribute and
elided as `{…}` on the rest for readability:

```json
{
  "dateObservedFrom": {
    "type": "DateTime",
    "value": "2024-07-20T10:00:00+09:00",
    "metadata": {"TimeInstant": {"type": "DateTime", "value": "2024-07-20T10:00:00+09:00"}}
  },
  "dateObservedTo":   {"type": "DateTime", "value": "2024-07-20T11:00:00+09:00", "metadata": {…}},
  "dateRetrieved":    {"type": "DateTime", "value": "2024-07-22T13:25:43+09:00", "metadata": {…}},
  "identifcation":    {"type": "Text", "value": "jp.sendai.Blesensor.flow", "metadata": {…}},
  "sourceQuality": {
    "type": "StructuredValue",
    "value": {
      "status": "clean",
      "evaluatedAt": "2024-07-22T13:25:43+09:00",
      "excludedPlaceNumbers": [],
      "missingFromAllPlaceNumbers": [],
      "missingToAllPlaceNumbers": []
    },
    "metadata": {…}
  },
  "peopleCount_flow_3": {
    "type": "StructuredValue",
    "value": {
      "from": {"all": 85, "3": 0, "5": 12},
      "to":   {"all": 82, "3": 0, "5": 14}
    },
    "metadata": {…}
  },
  "peopleCount_flow_5": {
    "type": "StructuredValue",
    "value": {
      "from": {"all": 79, "3": 14, "5": 2},
      "to":   {"all": 77, "3": 12, "5": 2}
    },
    "metadata": {…}
  }
}
```

The `"5": 2` values show the confirmed self-loop rule. No 5-minute Product B
variant exists.

---

## 5. Orion endpoints and STH-Comet subscriptions

### Attribute-write endpoints

Product A updates one metadata-selected per-place entity at a time:

```text
POST {url_orion_v20_entities}/{metadata.entity_id}/attrs
     ?type={metadata.entity_type}
```

Product B replaces the configured aggregate entity's complete attribute set:

```text
PUT {url_orion_v20_entities}/{PRODUCT_B_AGGREGATE_ENTITY_ID}/attrs
    ?type={PRODUCT_B_AGGREGATE_ENTITY_TYPE}
```

When a Product A sensor entity or the Product B aggregate entity must be
created, its creation body is exactly the bare identity:

```json
{"id": "<entity_id>", "type": "<entity_type>"}
```

The creation body seeds no attributes. The first successful publication by the
owning product establishes that entity's product-owned attributes.

Product B dry-run output shows this `PUT /attrs` operation and its full
aggregate body without mutating Orion.

Live Product B writes log `put_succeeded` or `put_failed`. Dry-run logs
`put_succeeded` with `dry_run=true`, so logs never describe a Product B
replacement as an attribute-update post.

Orion current state on the Product B aggregate entity is **last written**, not
necessarily the newest source window. A revision write for an older window can
remain current until another write occurs. Consumers must use
`dateObservedFrom` and `dateObservedTo` to identify the represented window.

### Product A STH-Comet subscription

Product A keeps one subscription selecting `idPattern: ".*"` for
`Blesensor.per300` and `Blesensor.per3600`. Both its projection and trigger use
the same ten-attribute list from §3.3:

```json
{
  "subject": {
    "entities": [
      {"idPattern": ".*", "type": "Blesensor.per300"},
      {"idPattern": ".*", "type": "Blesensor.per3600"}
    ],
    "condition": {
      "attrs": [
        "dateObservedFrom",
        "dateObservedTo",
        "dateRetrieved",
        "identifcation",
        "peopleCount_immedate",
        "peopleCount_near",
        "peopleCount_far",
        "peopleOccupancy_immedate",
        "peopleOccupancy_near",
        "peopleOccupancy_far"
      ],
      "notifyOnMetadataChange": true
    }
  },
  "notification": {
    "http": {"url": "<COMET_NOTIFY_URL>"},
    "attrs": [
      "dateObservedFrom",
      "dateObservedTo",
      "dateRetrieved",
      "identifcation",
      "peopleCount_immedate",
      "peopleCount_near",
      "peopleCount_far",
      "peopleOccupancy_immedate",
      "peopleOccupancy_near",
      "peopleOccupancy_far"
    ],
    "attrsFormat": "legacy",
    "metadata": ["TimeInstant"]
  }
}
```

Subscription creation uses `options=skipInitialNotification` when
`STH_SUBSCRIPTION_SKIP_INITIAL=true`. Because `condition.attrs` lists all ten
projected attributes, a semantic correction to any one of them — for example a
same-window change to only `peopleOccupancy_near` — triggers a Comet
notification. Because all ten attributes carry the source-window `TimeInstant`,
Comet indexes every projected attribute at that window's start.

The subscription body does not set `alterationTypes: ["entityUpdate"]`, and
Product A write requests do not use `options=forcedUpdate`. The contract is one
notification for each semantic Product A payload change, not one notification
for each wire attempt. An identical retry does not append duplicate history.

### Product B STH-Comet subscription

The Product B subscription targets only the configured aggregate entity and
notifies the full current attribute set. Its contract is:

```json
{
  "description": "Product B aggregate STH-Comet history",
  "subject": {
    "entities": [{
      "id": "<PRODUCT_B_AGGREGATE_ENTITY_ID>",
      "type": "<PRODUCT_B_AGGREGATE_ENTITY_TYPE>"
    }],
    "condition": {
      "attrs": ["dateRetrieved"],
      "notifyOnMetadataChange": true
    }
  },
  "notification": {
    "http": {"url": "<COMET_NOTIFY_URL>"},
    "attrsFormat": "legacy",
    "metadata": ["TimeInstant"]
  }
}
```

`subject.entities` uses exact `id` and `type`, not `idPattern`.
`notification.attrs` is omitted, not set to an empty list, so new dynamic
`peopleCount_flow_<N>` names can be forwarded without editing the subscription.
For idempotency, the matcher also accepts an existing subscription whose
`notification.attrs` is `[]`, because Orion treats that as the same
all-attribute notification shape.
Shape matching also requires the new description prefix and the configured
notification URL; an old per-place Product B subscription or a subscription
routed to a stale URL is not a match.

For both products, Orion may materialize omitted
`notification.onlyChangedAttrs` and `notification.covered` defaults as
`false` in a GET response. The matchers treat omission and literal `false` as
equivalent, while `true` or a non-Boolean value remains behavior-changing
drift. Server-managed delivery timestamps and counters are likewise excluded
from behavior matching.

The cross-product guard treats a shared trigger attribute as safe only when the
peer's entity selectors exactly equal that product's configured current
selectors and those selectors are disjoint from the product being created.
This proof depends only on the selectors: unrelated notification drift does
not make disjoint entities reachable. A missing type, type pattern, malformed
selector, or other non-canonical selector fails closed.

Subscription creation keeps `options=skipInitialNotification` when
`STH_SUBSCRIPTION_SKIP_INITIAL=true`. The body contains no `throttling` field.

The all-attribute subscription is safe only because Product B exclusively owns
the entity and replaces all attributes on every write.

Neither product's subscription sets `throttling`; per-subscription throttling
can silently discard burst notifications, including Product A bursts and
Product B backlog or revision writes.

Revision writes rebuild the complete Product B package from the full source
window and use `PUT /attrs` with the original `TimeInstant` and a new
`dateRetrieved`. STH-Comet therefore appends corrective rows instead of
rewriting prior history.
