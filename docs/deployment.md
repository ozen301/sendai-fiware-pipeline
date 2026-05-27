# Deployment Guide

Follow these steps to bring this pipeline up on a new host. Read
[overview.md](overview.md) first for the vocabulary used here.

## 1. Prerequisites

Install [`uv`](https://docs.astral.sh/uv/). It manages the Python environment and dependencies for this project.

## 2. Install

```sh
git clone https://github.com/ozen301/sendai-fiware-pipeline.git
cd sendai-fiware-pipeline
uv sync
```

Run every command in this guide as `uv run python …`. Do not activate
`.venv` and call `python` directly — `uv run` keeps Python and
dependencies pinned to `uv.lock`.

## 3. Configure `.env`

Copy the template:

```sh
cp .env.example .env
```

`.env` is gitignored. Fill in the placeholder values and review
optional settings as needed; see [configuration.md](configuration.md)
for what every variable does.

The most important values are the **rollout gates** at the bottom of
`.env`:

| Variable | Default | What it controls |
|---|---|---|
| `TARGET_FLOW_BATCHES` | unset → flow runner exits early without selecting targets | Comma list of install batches to process for Product A (e.g., `2023,2026`). |
| `TARGET_DIRECTION_BATCHES` | unset → direction runner exits early without selecting targets | Comma list of install batches to process for Product B (e.g., `2023,2026`). |
| `FLOW_SEND_MODE` | `dry-run` | Product A: `dry-run` logs payloads but does not POST them; `send` performs the live POSTs. |
| `DIRECTION_SEND_MODE` | `dry-run` | Product B: same semantics. |

The target batch variables are empty by default — meaning the matching
runner exits early without selecting targets, contacting Orion, or
logging any payload. The send modes default to `dry-run`, which logs the
would-be POST body but does not perform the attribute-update POST (the
runner still makes read-only `GET` calls to Orion for entity-map
validation whenever its target batch variable is non-empty). You will
set the target batch variables and flip the send modes in §7.

## 4. Provision sensor metadata

Place `metadata/sensors.csv` in the repo (it is gitignored). The
pipeline refuses to start if this file is missing or malformed.

The public repository includes `metadata/sensors.example.csv` as a
small, sanitized shape reference. Copy it only as a starting point:

```sh
cp metadata/sensors.example.csv metadata/sensors.csv
```

Then replace the rows with the real Sendai deployment metadata before
creating entities or running the pipelines.

The required header (note `identifcation` is deliberately misspelled
to match Sendai Orion's existing attribute name):

```csv
place_number,batch,expected_device_type,interval_min,entity_type,entity_id,identifcation,active
```

Per-column rules are in [pipeline_spec.md §2](pipeline_spec.md). Either
maintain the full file by hand, or use the refresh workflow: keep
`metadata/sensors_stable.csv` plus
`metadata/sensors_refreshable.csv.staged` and run

```sh
uv run python scripts/refresh_metadata.py
```

to atomically merge them into `metadata/sensors.csv`. If the sensor metadata
does not change often, maintaining `sensors.csv` by hand is simpler.

## 5. Bootstrap Orion entities

Create each target entity once on the platform. NGSI
`POST .../attrs` updates attributes on existing entities; it does not
create them, so this step must run before any live publication for a
new batch.

`scripts/create_entities.py` takes one or more positional arguments,
each in `entity_id:entity_type` form. You can pass every entity you need to
create in a single invocation:

```sh
uv run python scripts/create_entities.py \
  jp.sendai.Blesensor.per3600.101:Blesensor.per3600 \
  jp.sendai.Blesensor.per300.101:Blesensor.per300 \
  jp.sendai.Blesensor.per3600.102:Blesensor.per3600 \
  jp.sendai.Blesensor.per300.102:Blesensor.per300
```

Without `--send` this runs in dry-run (no FIWARE credentials needed).
The plan is logged to `logs/create_entities.log` at INFO; tail that
log to inspect. When the plan looks right, re-run with `--send` to
perform live creates (requires `FIWARE_CONSUMER_KEY` /
`FIWARE_CONSUMER_SECRET`):

```sh
uv run python scripts/create_entities.py --send \
  jp.sendai.Blesensor.per3600.101:Blesensor.per3600 \
  jp.sendai.Blesensor.per300.101:Blesensor.per300
```

`201` is treated as created, `409` / `422` as already-existing. The
operation is safe to re-run.

## 6. Create STH-Comet subscriptions

Do this **before** any live POST. Subscriptions ship with
`options=skipInitialNotification`, which means Orion will not replay
any *existing* attribute value into Comet when the subscription is
created — only attribute updates that happen *after* the subscription
exists are forwarded. Creating subscriptions before the first live
send guarantees those cutover windows appear in Comet history.

Set `COMET_NOTIFY_URL` in `.env` (the internal URL Orion should POST
notifications to). Dry-run to inspect:

```sh
uv run python scripts/create_sth_subscriptions.py
```

Add `--product a` or `--product b` to inspect one at a time, or
`--no-show-body` to suppress the printed body. When the bodies look
right:

```sh
uv run python scripts/create_sth_subscriptions.py --send
```

The creator is idempotent (it matches existing subscriptions on either
the description prefix or the structural shape), so re-running is
safe.

## 7. Cut over

In `.env`, set:

- `TARGET_FLOW_BATCHES=2023,2026`.
- `TARGET_DIRECTION_BATCHES=2026` — keep direction on 2026 until the
  upstream aggregation issue is resolved.
- `FLOW_SEND_MODE=send`.
- `DIRECTION_SEND_MODE=send`.

Run each pipeline once by hand to confirm it works:

```sh
uv run python -m sendai_pipeline.run_flow
uv run python -m sendai_pipeline.run_direction
```

Inspect `logs/{product}.log` for `post_succeeded` events and the
`run_summary` record. Then proceed to §8 (scheduling). If you
intentionally narrow either target-batch variable for validation, widen
it only after logs, state, and Comet history look correct.

After the first cron cycle runs, verify with
`scripts/show_data.py --source comet` that Comet rows are indexed by
`TimeInstant` (the source window start), not the wall-clock publish
time.

> **Note (sanity-check dry-run).** If you want to inspect a would-be
> POST body before going live — useful after a code change, or on a
> new platform deployment — leave the send modes at `dry-run` and set
> `TARGET_FLOW_BATCHES=2026` and/or `TARGET_DIRECTION_BATCHES=2026`.
> The runner will log the full payload to `logs/{product}.log` without
> POSTing to Orion. (It still makes read-only `GET` calls to Orion for
> entity-map validation, so OAuth credentials must be valid.) This is
> optional for routine deployments
> of code that's already known to work.

## 8. Schedule the runners (cron)

Both runners are designed to be invoked every five minutes so the
5-minute aggregates are republished promptly. Each takes a
non-blocking `fcntl` lock on its own lock file (`state/flow.lock` /
`state/direction.lock`) before doing work; the two products use
separate locks and can run concurrently. A run that overlaps a still-
running prior invocation of the same product bails immediately.

Edit the user crontab with `crontab -e` and adjust `REPO` and `UV` for
the host. Find the absolute path to `uv` with `which uv`:

```cron
SHELL=/bin/bash
REPO=/home/sendai/sendai-fiware-pipeline
UV=/usr/local/bin/uv

# Product A — per-place counts. Every 5 minutes at :02/:07/:12/…
2-59/5 * * * * cd $REPO && $UV run python -m sendai_pipeline.run_flow >> $REPO/logs/cron.flow.log 2>&1

# Product B — inter-place flow. Every 5 minutes at :04/:09/:14/… (staggered).
4-59/5 * * * * cd $REPO && $UV run python -m sendai_pipeline.run_direction >> $REPO/logs/cron.direction.log 2>&1

# Optional: refresh runtime metadata daily at 04:30 JST.
# Enable only if you use the refresh workflow from §4.
30 4 * * * cd $REPO && $UV run python scripts/refresh_metadata.py >> $REPO/logs/cron.refresh_metadata.log 2>&1
```

`cd $REPO` is required because the default `metadata/`, `state/`, and
`logs/` paths resolve relative to cwd. Cron runs with a minimal
`PATH`, so `$UV` must be an absolute path. The runners' rotating JSON
log lives in `logs/{product}.log`; the cron-tail log captures
stdout/stderr (WARNING+ only) and normally stays empty on a healthy
run.

## 9. Day-to-day operations

After cutover, the pipeline runs unattended. The tools you'll reach
for most often:

| Tool | Use it when |
|---|---|
| `scripts/show_data.py --source orion` | Inspect what Orion currently stores for one or more entity ids. |
| `scripts/show_data.py --source comet` | Inspect what STH-Comet has recorded for an attribute over time. |
| `scripts/delete_entities.py` / `scripts/delete_history.py` | Remove reviewed Orion entities or Comet history. Dry-run first. |
| `scripts/delete_subscriptions.py` | Retire stale Orion subscriptions by id (e.g. old STH-Comet subscriptions). Dry-run first. |
| `scripts/state_doctor.py` | A window is stuck in `pending` or `partial` and you need to see why. |
| `scripts/state_repair.py` / `scripts/resend.py` | Repair stuck aggregate state or replay one or more specific windows. Run only after `state_doctor.py`. |

Full per-script reference and symptom-keyed troubleshooting:
[tools_and_troubleshooting.md](tools_and_troubleshooting.md).
