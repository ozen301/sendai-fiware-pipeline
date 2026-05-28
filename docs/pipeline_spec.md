# Sendai FIWARE — MySQL → Orion Pipeline Spec

Canonical data contract for the pipeline: exact column → attribute
mappings, filter rules, payload shapes, and the operational rules that
both products must obey. For the narrative walk-through and operator
vocabulary, see [overview.md](overview.md).

The pipeline runs **two independent jobs** against Sendai's FIWARE
platform:

| # | Source MySQL table | What gets sent | Target Orion entity |
|---|---|---|---|
| **A** | `bleData2025d.flow_metrics2_per_place2_agg_imputed` | Per-place pedestrian counts and stay times (gap-filled superset of `flow_metrics2_per_place2_agg`) | `jp.sendai.Blesensor.per3600.<N>` (60-min) and `jp.sendai.Blesensor.per300.<N>` (5-min) — **one entity per place** |
| **B** | `bleData2025d.direction_metrics2_per_place2_agg` | Place-to-place flow ("回遊性") | **Same per-place entities as Product A** — Product B adds the `peopleCount_flow` attribute on each (see §4.3) |

## Contents

- §1 Infrastructure
- §2 Common rules (apply to both products)
- §3 Product A — per-place counts
- §4 Product B — inter-place flow
- §5 Endpoint and example POST

---

## 1. Infrastructure

### MySQL source server

The source MySQL server lives on a private network and is reachable only
from authorised hosts. Connection host, database name,
and the read account are kept out of this committed spec — see `.env` or
the team's secrets store. The pipeline is expected to run on a host that
is already on the same network.

### FIWARE side — common headers for every POST

Every Orion POST carries:

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

Entity IDs, entity types, install batches, expected device types, and
the misspelled `identifcation` attribute value are all loaded from
`metadata/sensors.csv`. The pipeline reads this file at startup and does
**not** reconstruct entity ids or types from any pattern — even if
today's data follows one. Orion's `GET /v2.0/entities?type=…` is used
only to *validate* that metadata targets exist on the platform; missing
platform entities are logged, but the metadata file remains
authoritative.

`metadata/sensors.csv` is produced from a stable manually seeded
metadata file (currently the 2023 batch) plus the latest refreshable
metadata sheet (currently the 2026 batch). 

---

## 2. Common rules

These rules apply to **both** Product A and Product B.

### 2.1 Allowed intervals

Only `interval_min ∈ {5, 60}` rows are published. `interval_min = 1`
rows exist in the source tables for further aggregation but are never
sent.

### 2.2 Noise prefixes — dropped silently

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
without forcing them through a WARN channel — operators can filter on
the event name when triaging.

The prefix list is operator-configurable via `IGNORED_PLACE_PREFIXES`
(see [`.env.example`](/.env.example)). Matching is `startswith` against
`group_place_id`. For Product B, the literal aggregation key `'ALL'` is
**not** a noise prefix; it is exempt from this filter (see §4).

### 2.3 Metadata-driven entity mapping

Mapping is **metadata-driven**, not pattern-reconstructed:

```
group_place_id     →  the part after the last '.'        = place_number
(place_number, interval_min)
                   →  look up row in metadata/sensors.csv = metadata row
metadata.entity_id, metadata.entity_type                  ↓
                  POST URL = .../entities/{metadata.entity_id}/attrs
                  query    = ?type={metadata.entity_type}
```

The metadata CSV's `entity_id` and `entity_type` columns are the
authoritative source. The pipeline **must not** reconstruct an entity
id from `interval_min × 60`, even if today's metadata follows that
pattern in 100% of rows.

`group_place_id` prefix per install batch (informational, used only to
derive `place_number`):

| Prefix | Install batch |
|---|---|
| `sendai2023.` | 2023設置 (places 1–28) |
| `sendai202603.` | 2026年3月設置 (places 101–112, 201–210) |

### 2.4 Device-type filtering (batch disambiguation)

The source tables store parallel rows under both `Pixel3aUT` and
`M5Stack` device types for every per-place target. The filter picks the
right one:

| Place number range | Install batch | Use rows where `device_type =` |
|---|---|---|
| 1 – 28 | 2023設置 | `Pixel3aUT` |
| 101 – 112, 201 – 210 | 2026年3月設置 | `M5Stack` |

This filter is a **required disambiguator** for both products, not just
a cross-batch exclusion — omitting it would double-count.

### 2.5 NGSI attribute types

The Sendai broker carries the following types on every per-place
entity. The pipeline matches the existing convention exactly; writing
the canonical NGSI `Integer` / `Number` would change the stored type
and create mixed-type history in STH-Comet.

| Attribute | NGSI type |
|---|---|
| `dateObservedFrom`, `dateObservedTo`, `dateRetrieved` | `DateTime` |
| `identifcation` | `Text` |
| `peopleCount_immedate`, `peopleCount_near`, `peopleCount_far` | `"number"` (lowercase string) |
| `peopleOccupancy_immedate`, `peopleOccupancy_near` | `"number"` (lowercase string) |
| `peopleCount_flow` | `StructuredValue` |

> Verified 2026-05-23 against the live broker for `Blesensor.per3600.*`
> and `Blesensor.per300.*` entities. Live entities also carry a
> `peopleOccupancy_far` attribute (also `"number"`); Product A does not
> write it because the source has no matching column, and NGSI
> `POST .../attrs` is append/update, so leaving it out keeps the
> existing value undisturbed.

### 2.6 TimeInstant metadata

Every attribute the pipeline writes carries NGSI metadata
`TimeInstant = dateObservedFrom`. When the STH-Comet subscription
requests `metadata: ["TimeInstant"]`, Comet uses this value as the
stored history timestamp instead of the wall-clock receive time. This
aligns Comet `recvTime` with the logical aggregate window start.

### 2.7 `null` vs `0`

For all numeric attributes: `null` means "no observation," `0` means
"observed zero." The two are semantically different and the pipeline
preserves both. In particular, nullable source numeric values are sent
as JSON `null`, not coerced to zero.

### 2.8 Time zone

All `DateTime` values are explicit JST (`+09:00`). NTP sync is required
on the pipeline host. Orion v2 rejects sub-second precision in
DateTime values; the pipeline truncates to whole seconds before
emitting.

### 2.9 Idempotency

In normal send mode, a prior successful target result is terminal for
`(product, interval, source window, entity_id)`. Failed or missing
targets are retried; prior-`ok` targets are not re-POSTed even if
source aggregates later drift. This avoids duplicate STH-Comet history
rows.

Once STH-Comet subscriptions are enabled, correction by normal repost
is not history-idempotent. Treat Comet deletion/replay as an operator
repair workflow; upstream STH-Comet deletion is coarse (service /
service path, entity, or entity attribute), not a normal per-window
update path.

### 2.10 Scheduling and retry

| Topic | Rule |
|---|---|
| Schedule | Cron (or systemd timer), every 5 minutes. |
| Source stability delay | Process windows whose `startdate` is at or before `now − SOURCE_STABILITY_DELAY_HOURS` (default 3h). Separate from the 72h retry horizon. |
| Catch-up | Each run reprocesses a rolling lookback against the per-window state store. Missed or failed targets are picked up on the next run. |
| Retry | Exponential backoff on `5xx` and network errors (1s, 2s, 4s, 8s, 16s). On `429`, the client honors a `Retry-After` header when present and otherwise falls back to the same backoff. Single `401` triggers a forced token refresh and one extra retry. Other `4xx` is fatal for that POST. |
| Token refresh | OAuth2 client-credentials; proactive on expiry and on `401`. |
| Logging | One structured line per POST in `logs/{product}.log` (rotating). The line carries the target entity id, HTTP status, and a payload hash + byte count. Whether the full request/response bodies are also logged is controlled by `LOG_PAYLOAD_MODE` — `hash` (always hash only), `failure` (default — hash on success, body excerpt on failure), or `full` (always body). |

### 2.11 Rollout gates

Use product-specific batch gates to control deployment scope. The
current target configuration is `TARGET_FLOW_BATCHES=2023,2026` and
`TARGET_DIRECTION_BATCHES=2023,2026`.

---

## 3. Data Product A — Per-place counts

Target entity types: `Blesensor.per3600.<N>` (60-min), `Blesensor.per300.<N>` (5-min).

### 3.1 Source schema — `flow_metrics2_per_place2_agg_imputed`

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
| `imputation_tier` | Product A source-quality gate; only rows at or below `SOURCE_MAX_IMPUTATION_TIER` are read. |
| `flow_gt_m60`, `flow_gt_m80`, `flow_gt_m120` | Count attributes. |
| `stay_gt_m60`, `stay_gt_m80` | Occupancy attributes. |

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

### 3.3 Attribute mapping (request body)

| Attribute | NGSI type | Value source |
|---|---|---|
| `dateObservedFrom` | `DateTime` | `startdate` parsed as JST → ISO 8601 (`YYYY-MM-DDTHH:MM:00+09:00`) |
| `dateObservedTo` | `DateTime` | `dateObservedFrom + interval_min` minutes |
| `peopleCount_immedate` | `number` | `flow_gt_m60` |
| `peopleCount_near` | `number` | `flow_gt_m80` |
| `peopleCount_far` | `number` | `flow_gt_m120` |
| `peopleOccupancy_immedate` | `number` | `stay_gt_m60` |
| `peopleOccupancy_near` | `number` | `stay_gt_m80` |

The same column names (`flow_gt_mXX`, `stay_gt_mXX`) are used for both
5-min and 60-min rows; the interval is carried by the row's
`interval_min` column, not the column name.

---

## 4. Data Product B — Inter-place flow

Target entities: the **same per-place entities as Product A**
(`Blesensor.per3600.<N>` for 60-min, `Blesensor.per300.<N>` for 5-min).
Product B contributes a single `peopleCount_flow` attribute plus the
shared envelope.

### 4.1 Source schema — `direction_metrics2_per_place2_agg`

Load-bearing columns:

| Column | Use |
|---|---|
| `startdate` | Source window, format `YYYYMMDD_HHMM`. |
| `from_group_place_id`, `to_group_place_id` | Source place keys; may also be literal `'ALL'` for aggregate rows. |
| `from_device_type`, `to_device_type` | Product B device-population filter; both sides must match the selected device type for the interval. |
| `interval_min` | See §2.1. |
| `count` | Movement count used inside `peopleCount_flow`. |

### 4.2 Filtering rules

Per-row order (matches `transform_direction.py`):

1. Drop if `interval_min ∉ {5, 60}`.
2. Drop if **either** `from_group_place_id` or `to_group_place_id`
   matches an `IGNORED_PLACE_PREFIXES` entry. The literal `'ALL'` is
   exempt — it is a real aggregation key (see §4.3).
3. Resolve each non-`ALL` side via metadata. The source-side batch
   (derived from the `sendai2023.` / `sendai202603.` prefix) must
   match the metadata batch. Drop if either side fails to resolve.
4. Drop self-loops (`from == to`). Keep cross-batch pairs when both
   sides resolve.
5. Select the Product B device type from the oldest active target batch
   for the interval, then drop if either of `from_device_type` /
   `to_device_type` disagrees with that selected type. With active 2023
   and 2026 targets, the selected type is `Pixel3aUT`; with only 2026
   active targets, it is `M5Stack`.

Cross-batch direction pairs are valid Product B observations. The same
selected device type is used for pairwise rows and `'ALL'` rows so the
inter-place values and the pre-computed deduplicated totals come from
the same source device population.

The source table stores parallel pairwise and `'ALL'` rows under both
`(Pixel3aUT, Pixel3aUT)` and `(M5Stack, M5Stack)` for every per-place
target. Mixed `(Pixel3aUT, M5Stack)` pairings do not occur. The
device-type filter is therefore a required disambiguator, not just a
cross-batch exclusion.

### 4.3 Attribute mapping (request body)

One POST per active target per source window. The body is:

| Attribute | NGSI type | Value source |
|---|---|---|
| `identifcation` | `Text` | metadata `identifcation` column (place number as string, e.g. `"105"`) |
| `dateObservedFrom` | `DateTime` | `startdate` parsed as JST |
| `dateObservedTo` | `DateTime` | `dateObservedFrom + interval_min` minutes |
| `dateRetrieved` | `DateTime` | now in JST (truncated to whole seconds) |
| `peopleCount_flow` | `StructuredValue` | see below |

The `peopleCount_flow` value, for the entity's own place `N`:

```json
{
  "from": { "all": <int|null>, "<M>": <int|null>, ... },
  "to":   { "all": <int|null>, "<M>": <int|null>, ... }
}
```

- `from.<M>` = unique BLEID count moving *into* `N` from place `M` in
  this window. Built from rows where `from_group_place_id` → `M` and
  `to_group_place_id` → `N`.
- `to.<M>` = unique BLEID count moving *out of* `N` to place `M` in
  this window. Built from rows where `from_group_place_id` → `N` and
  `to_group_place_id` → `M`.
- `from.all` / `to.all` come from **pre-computed `'ALL'`-keyed rows in
  the same table**, which already carry the deduplicated unique-BLEID
  total. The pipeline **must not** sum the pairwise rows to approximate
  `all`. Specifically:
  - `peopleCount_flow.from.all` = `count` from rows matching this window
    and interval where `from_group_place_id = 'ALL'` and
    `to_group_place_id` resolves to place `N`, with both device-type
    fields equal to the interval's selected Product B device type.
  - `peopleCount_flow.to.all` = `count` from rows matching this window
    and interval where `from_group_place_id` resolves to place `N` and
    `to_group_place_id = 'ALL'`, with both device-type fields equal to
    the interval's selected Product B device type.

Targets with no surviving observations still receive a payload with
sentinel `peopleCount_flow = {"from": {"all": null}, "to": {"all":
null}}` so Comet history remains continuous.

**Naming note.** A DB row with `from_group_place_id = 'ALL'` and
`to_group_place_id` resolving to place `N` populates
`peopleCount_flow.from.all` for entity `N`, because the JSON `from`
field is from the perspective of place `N` ("came from all places into
N"). The DB `to_group_place_id` column names the movement destination,
not the JSON key.

### 4.4 Full body example (60-min, single entity)

2024-07-20 10:00–11:00 JST, place 3 — entity
`jp.sendai.Blesensor.per3600.3`. Every attribute carries the same
`TimeInstant` metadata value (the window start, §2.6); shown in full on
the first attribute and elided as `{…}` on the rest for readability:

```json
{
  "identifcation":    {
    "type": "Text",
    "value": "3",
    "metadata": {"TimeInstant": {"type": "DateTime", "value": "2024-07-20T10:00:00+09:00"}}
  },
  "dateObservedFrom": {"type": "DateTime", "value": "2024-07-20T10:00:00+09:00", "metadata": {…}},
  "dateObservedTo":   {"type": "DateTime", "value": "2024-07-20T11:00:00+09:00", "metadata": {…}},
  "dateRetrieved":    {"type": "DateTime", "value": "2024-07-22T13:25:43+09:00", "metadata": {…}},
  "peopleCount_flow": {
    "type": "StructuredValue",
    "value": {
      "from": { "all": 85, "2": 24, "4": 31, "5": 12, "7": 8, "13": 5 },
      "to":   { "all": 82, "2": 22, "4": 28, "5": 14, "7": 7, "13": 4 }
    },
    "metadata": {…}
  }
}
```

(The 5-min variant has the same body shape, but on
`jp.sendai.Blesensor.per300.3` with `dateObservedTo = +300 s`.)

---

## 5. Endpoint and example POST

Both products POST to the same metadata-driven URL:

```
POST {url_orion_v20_entities}/{metadata.entity_id}/attrs
     ?type={metadata.entity_type}
```

Product A and Product B issue separate POSTs to the same entity for the
same window — A writes its count/occupancy attributes, B writes
`peopleCount_flow` and `identifcation` / `dateRetrieved`, the shared
`dateObserved*` envelope computes identically from both sides.

### Example curl (Product B — 60-min, place 3)

```sh
curl -X POST \
  "${url_orion_v20_entities}/jp.sendai.Blesensor.per3600.3/attrs?type=Blesensor.per3600" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${accessToken}" \
  -H "Fiware-Service: ${FIWARE_SERVICE}" \
  -H "Fiware-ServicePath: ${FIWARE_SERVICE_PATH}" \
  --data @/path/to/payload.json
```

### STH-Comet subscriptions

STH-Comet subscriptions for both products are created after the
first-ok-wins send policy is deployed and the cron is stopped for
cutover. Subscription creation uses
`scripts/create_sth_subscriptions.py`, which reads the private
`COMET_NOTIFY_URL` from runtime configuration and creates dry-run-first
Orion subscriptions with:

- `notification.attrsFormat: "legacy"`.
- `options=skipInitialNotification` (URL query parameter on the
  subscription `POST`, not a body field).
- `notification.metadata: ["TimeInstant"]`.
- `subject.condition.notifyOnMetadataChange: true` (placed in the
  condition, not the notification, so a `TimeInstant`-only change on
  the trigger attribute still fires the subscription — without this,
  two consecutive windows publishing the same trigger value would be
  silently dropped from Comet history).
- `subject.condition.attrs` set to a product-exclusive attribute so
  the peer product's normal updates do not fire this subscription and
  corrupt Comet history with the wrong window's values: Product A
  uses `peopleCount_immedate`, Product B uses `peopleCount_flow`.
  Both are written only by their owning product, so each subscription
  fires only on its own pipeline's updates.
- `notification.attrs` equal to the owning product's attributes:
  Product A is the §3.3 list; Product B is `dateObservedFrom`,
  `dateObservedTo`, `peopleCount_flow`.
