# Configuration Reference

Every environment variable the pipeline reads from `.env` (or the
shell environment). Group order matches `.env.example`. Variables
marked **required** must be set; everything else has a sensible
default.

For the deployment workflow that uses these, see
[deployment.md](deployment.md).

## FIWARE platform

| Variable | Default | Purpose |
|---|---|---|
| `FIWARE_BASE_URL` | **required** | `https://<FQDN>` of Sendai's FIWARE platform. The Orion API and (by default) the OAuth2 token endpoint both live under this host. |
| `FIWARE_CONSUMER_KEY` | **required** | OAuth2 client-credentials consumer key. Required to start the runners even in dry-run, because the runners build `AuthSettings.from_env()` unconditionally. (`scripts/create_entities.py` and `scripts/create_sth_subscriptions.py` skip `AuthSettings` construction in dry-run, so they don't need this when invoked without `--send`.) |
| `FIWARE_CONSUMER_SECRET` | **required** | OAuth2 client-credentials consumer secret. Same requirement as the key. |
| `FIWARE_TOKEN_URL` | `${FIWARE_BASE_URL}/oauth2/token` | Override the OAuth2 token endpoint if WSO2 is hosted elsewhere. |
| `FIWARE_TOKEN_SCOPE` | `default` | OAuth2 scope requested at token exchange. |
| `FIWARE_TOKEN_CACHE_PATH` | `state/token.json` | Where the OAuth access token is cached between runs so back-to-back cron jobs don't refresh it on every invocation. Because the two product runners can fire within seconds of each other, the code uses a sibling lock file (the cache path with its extension replaced by `.lock` — e.g. `state/token.lock` for the default cache path) to serialize refreshes, and writes the token via a temp file + atomic rename. The path must be on a local filesystem that supports file locks (most do — avoid NFS / network shares). |
| `FIWARE_TOKEN_REFRESH_MARGIN_SECONDS` | `60` | Refresh the cached access token this many seconds before its stated expiry, to avoid using a token that expires mid-request. |
| `FIWARE_TOKEN_TIMEOUT_SECONDS` | `10` | HTTP timeout (seconds) for the OAuth2 token-exchange request. |
| `FIWARE_TIMEOUT_SECONDS` | `10` | HTTP timeout (seconds) for all FIWARE platform requests: Orion (`POST .../attrs`, `GET .../entities`), STH-Comet reads via `comet_client.py`, entity bootstrap, and subscription creation. |
| `FIWARE_SERVICE` | empty | Value for the `Fiware-Service` request header. Header is omitted when this is empty. Production should set it to the assigned tenant. |
| `FIWARE_SERVICE_PATH` | `/` | Value for the `Fiware-ServicePath` request header. Always emitted; an empty value resolves to `/`. |
| `FIWARE_VERIFY_TLS` | `true` | Verify the FIWARE platform's TLS certificate. Set to `false` only for local development against a host whose certificate isn't in the system trust store. Production must verify. |

## STH-Comet subscriptions

Only `scripts/create_sth_subscriptions.py` reads these — the runners do
not.

| Variable | Default | Purpose |
|---|---|---|
| `COMET_NOTIFY_URL` | **required** | Internal URL Orion should POST history notifications to. Required even in dry-run because `scripts/create_sth_subscriptions.py` builds settings unconditionally and prints the redacted URL in the body. Treat as private. |
| `STH_SUBSCRIPTION_EXPIRES` | unset (no expiry) | ISO-8601 `expires` field to put on each subscription. Leave unset for permanent subscriptions. |
| `STH_SUBSCRIPTION_SKIP_INITIAL` | `true` | When `true`, add `options=skipInitialNotification` to the subscription POST so existing entity state is not replayed. |
| `STH_SUBSCRIPTION_THROTTLING_SECONDS` | unset | Optional NGSI `throttling` field. Limits notification rate per subscription. |

## MySQL source

| Variable | Default | Purpose |
|---|---|---|
| `MYSQL_HOST` | **required** | Source MySQL host. |
| `MYSQL_USER` | **required** | Read-only user for `bleData2025d`. |
| `MYSQL_PASSWORD` | **required** | Password for `MYSQL_USER`. |
| `MYSQL_DATABASE` | **required** | Source database name. |
| `MYSQL_PORT` | `3306` | MySQL TCP port. |
| `MYSQL_CONNECT_TIMEOUT` | `10` | Seconds to wait for the TCP connection. |
| `MYSQL_READ_TIMEOUT` | `30` | Seconds to wait for a query to return data. |
| `MYSQL_CHARSET` | `utf8mb4` | Connection charset. |

## Sensor metadata

| Variable | Default | Purpose |
|---|---|---|
| `SENSOR_METADATA_PATH` | `metadata/sensors.csv` | Final runtime metadata CSV. The pipeline reads only this file. |
| `SENSOR_METADATA_STABLE_PATH` | `metadata/sensors_stable.csv` | Refresh-script input: operator-maintained stable seed (currently the 2023 batch). Same canonical schema as `SENSOR_METADATA_PATH`. |
| `SENSOR_METADATA_STAGED_PATH` | `metadata/sensors_refreshable.csv.staged` | Refresh-script input: latest staged refresh (currently the 2026 batch). Same schema as `SENSOR_METADATA_PATH` except the staged file uses an `ID` column where the runtime CSV uses `identifcation`; the refresh script renames `ID` to `identifcation` when writing `SENSOR_METADATA_PATH`. |

If you maintain one complete CSV by hand at `SENSOR_METADATA_PATH`, do
not run `scripts/refresh_metadata.py`.

## Filters and rollout gates

| Variable | Default | Purpose |
|---|---|---|
| `TARGET_FLOW_BATCHES` | unset → **no flow batches publish** | Comma-separated list of install batches to publish for Product A flow (e.g. `2023,2026`). Empty / unset is a safe default. Every entry must appear as a `batch` value in the metadata CSV; a typo fails the flow run before any SQL or POST. |
| `TARGET_DIRECTION_BATCHES` | unset → **no direction batches publish** | Comma-separated list of install batches to publish for Product B direction (e.g. `2023,2026`). Empty / unset is a safe default. Every entry must appear as a `batch` value in the metadata CSV; a typo fails the direction run before any SQL or POST. |
| `FLOW_SEND_MODE` | `dry-run` | Product A POST gate. `dry-run` builds and logs the payload but does not POST to `/orion/v2.0/entities/<id>/attrs`; `send` performs those live POSTs. Note: dry-run still makes read-only `GET` calls to Orion to validate that the configured target entities exist (the entity-map check), so Orion is contacted either way once `TARGET_FLOW_BATCHES` is non-empty. |
| `DIRECTION_SEND_MODE` | `dry-run` | Product B POST gate. Same semantics as `FLOW_SEND_MODE`. |
| `IGNORED_PLACE_PREFIXES` | `quick.,test` | Comma-separated prefixes used to filter source rows: any row whose `group_place_id` column (Product A) or whose `from_group_place_id` / `to_group_place_id` column (Product B) starts with one of these prefixes is excluded from publishing. Excluded rows produce a `DEBUG`-level `ignored_place_prefix` log event and nothing else — no Orion POST, no WARN. Empty / unset applies the default. Set to a single comma to disable noise filtering entirely. The literal `'ALL'` is never matched. |
| `SOURCE_MAX_IMPUTATION_TIER` | `2` | Product A source-quality gate. Rows from `flow_metrics2_per_place2_agg_imputed` publish only when `imputation_tier <= SOURCE_MAX_IMPUTATION_TIER`; the default keeps tiers `0`, `1`, and `2`, matching the rule "smaller than 3." Empty / unset applies the default. Values must be non-negative integers. `scripts/resend.py flow --max-imputation-tier N` can override this for one explicit resend; Product B source selection does not apply this setting. |

## Window timing and retry horizons

| Variable | Default | Purpose |
|---|---|---|
| `SOURCE_STABILITY_DELAY_HOURS` | `3` | Wait this many hours before publishing a source window, to give the source aggregator time to finish. Separate from the retry horizon. |
| `REPROCESS_HOURS_PER3600` | `12` | Normal-run rolling lookback floor for 60-minute windows. |
| `REPROCESS_HOURS_PER300` | `2` | Normal-run rolling lookback floor for 5-minute windows. |
| `MAX_LOOKBACK_HOURS_PER3600` | `72` | Maximum age at which an open 60-minute window is still retried. Both intervals default to 72h because source rows can arrive at MySQL up to 3 days late. |
| `MAX_LOOKBACK_HOURS_PER300` | `72` | Same for 5-minute windows. |

`MAX_LOOKBACK_HOURS_*` must be ≥ `REPROCESS_HOURS_*` for the same
interval, or the run aborts at startup.

## Logging

| Variable | Default | Purpose |
|---|---|---|
| `LOG_LEVEL` | `INFO` | One of `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. |
| `LOG_FORMAT` | `json` | `json` (one JSON object per line, production) or `text` (human-readable, for local dev). |
| `LOG_PAYLOAD_MODE` | `failure` | Per-POST body verbosity. `hash` always logs only a SHA-256 + byte count. `failure` logs hash on success and a body excerpt on failure. `full` always logs the body excerpt — use only for dry-run verification. Body excerpts are always capped by `LOG_PAYLOAD_MAX_BYTES`, even in `full` mode. |
| `LOG_PAYLOAD_MAX_BYTES` | `16384` | Cap on logged request-body bytes (UTF-8). Applies to all `LOG_PAYLOAD_MODE` settings that emit a body. |
| `LOG_RESPONSE_MAX_BYTES` | `2048` | Cap on logged response excerpts (UTF-8). |
| `LOG_DIR` | `logs` | Directory for rotating per-product log files (`logs/{product}.log`). |
