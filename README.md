# Sendai FIWARE Pipeline

This project publishes pedestrian presence and movement data from BLE
sensors deployed in Sendai to the city's FIWARE open-data platform.

Bluetooth Low Energy (BLE) sensors installed at locations around the
city detect nearby mobile devices and use those detections to estimate
how many people are around each sensor and how they move between
sensors. Those measurements land in a private database. This pipeline
reads them on a schedule and publishes them to Sendai's FIWARE
platform in two forms so other applications can use them:

- *Product A, per-place counts:* how many distinct devices the sensor saw
  near it in a given time window, and how many were present on
  average over that window.
- *Product B, inter-place flow:* for each pair of sensors, how many distinct
  devices moved from one to the other in the same window.

Two independent pipelines run every five minutes and write to separate
FIWARE entities. Product A publishes per-place counts to one entity per
place and interval, at both 5-minute and 60-minute aggregation. Product B
publishes inter-place flow to a single aggregate entity, at 60-minute
aggregation only. Both feed the FIWARE platform's time-series history service, so values are
available both as "current" (via the *Orion API*) and as history (via the
*STH-Comet API*).
Source schemas, entity ids, batch names, and deployment procedures intentionally match
the Sendai environment; runtime configuration, real sensor metadata,
logs, state, and local reference material are not included.

For the technical overview and the canonical data contract, see
the docs index below.

## Quick start

```sh
git clone https://github.com/ozen301/sendai-fiware-pipeline.git
cd sendai-fiware-pipeline
uv sync
cp .env.example .env
# Edit .env: fill in FIWARE_*, MYSQL_*, COMET_NOTIFY_URL.
cp metadata/sensors.example.csv metadata/sensors.csv
# Edit metadata/sensors.csv for the real deployed sensors.
```

Then follow [docs/deployment.md](docs/deployment.md) end-to-end: it
walks through metadata provisioning, Orion entity bootstrap, STH-Comet
subscription creation, the live cutover, and cron scheduling.

## Documentation

| Doc | What you'll find |
|---|---|
| [docs/overview.md](docs/overview.md) | Mental model, vocabulary, data flow, repo map. **Read first.** |
| [docs/deployment.md](docs/deployment.md) | First-time install, cutover gates, cron setup. |
| [docs/configuration.md](docs/configuration.md) | Every environment variable read from `.env`: defaults, accepted values, purpose. |
| [docs/tools_and_troubleshooting.md](docs/tools_and_troubleshooting.md) | Per-script reference for everything under `scripts/`, plus incident playbooks organized by symptom. |
| [docs/pipeline_spec.md](docs/pipeline_spec.md) | Canonical data contract: column → attribute mappings, filter rules, payload shapes. |
| [AGENTS.md](AGENTS.md) | Contributor guide for both human developers and AI agents: workflow, cross-agent review, code style, logging, testing, and commit conventions. |

## Common commands

```sh
# Run a pipeline by hand (dry-run by default; see overview.md "Send mode").
uv run python -m sendai_pipeline.run_flow
uv run python -m sendai_pipeline.run_direction

# Inspect an entity in Orion.
uv run python scripts/show_data.py --source orion --flow-attrs \
  jp.sendai.Blesensor.per3600.101

# Inspect STH-Comet history for an attribute.
uv run python scripts/show_data.py --source comet \
  --attrs peopleCount_immedate --last-n 20 \
  jp.sendai.Blesensor.per300.101

# Inspect the Product B aggregate entity's history (attributes auto-enumerated).
uv run python scripts/show_data.py --source comet --last-n 20 \
  jp.sendai.Blesensor.flow

# Diagnose stuck windows (read-only).
uv run python scripts/state_doctor.py flow --pretty
uv run python scripts/state_doctor.py direction --pretty
```

Full per-script reference in
[docs/tools_and_troubleshooting.md](docs/tools_and_troubleshooting.md).

## Repository layout

- `sendai_pipeline/`: production package (the only thing cron runs).
- `scripts/`: operator-facing CLI shims (entity bootstrap, state
  inspection, repair, replay).
- `docs/`: the docs linked above.
- `tests/`: pytest suite with module-focused and cross-cutting
  contract/integration tests.
- `metadata/`, `state/`, `logs/`, `ref_docs/`, `.env`:
  **gitignored.** Runtime data, credentials, and local-only reference
  material. Do not commit.

## Requirements

Install [`uv`](https://docs.astral.sh/uv/) on the host. It manages the
Python version and dependencies for this project. Run every command
through `uv run`. The pipeline host also needs network access to the
private MySQL server, Sendai's Orion base URL, and the WSO2 token
endpoint.
