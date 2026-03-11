# nanobot-community-hub

`nanobot-community-hub` is the first V1 cut of the Nanobot ecosystem backend.

It provides:

- MCP discovery and marketplace metadata
- MCP detail pages with tools, known issues, and recommended config
- community stats and overview metrics
- public MCP repository submissions with duplicate detection
- stack presets
- basic showcase entries
- telemetry ingest for anonymous MCP runtime signals

Tech stack:

- FastAPI
- Jinja2 templates
- HTMX
- PostgreSQL for the shared stack deployment
- SQLite fallback for local development and tests

## Run locally

```bash
uv venv
source .venv/bin/activate
uv pip install -e .[dev]
uvicorn nanobot_hub.app:create_app --factory --host 0.0.0.0 --port 18811
```

## Environment

- `NANOBOT_HUB_DATABASE_URL` preferred, for example: `postgresql+psycopg://user:pass@postgres:5432/nanobot_community_hub_db`
- `NANOBOT_HUB_DB_PATH` fallback for local SQLite mode: `./data/nanobot-community-hub.sqlite3`
- `NANOBOT_HUB_PUBLIC_URL` default: empty
- `NANOBOT_HUB_INSTANCE_NAME` default: `nanobot-community-hub`

In the deployed stack, `nanobot-community-hub` is expected to use the shared `apps-stack` PostgreSQL instance over the `apps-shared` Docker network.

## API

Base path:

```text
/api/v1
```

Important endpoints:

- `GET /api/v1/health`
- `GET /api/v1/marketplace`
- `GET /api/v1/marketplace/{slug}`
- `GET /api/v1/marketplace/{slug}/recommendation`
- `GET /api/v1/marketplace/resolve?repo_url=...`
- `POST /api/v1/submissions/mcp`
- `GET /api/v1/stacks`
- `GET /api/v1/stacks/{slug}`
- `GET /api/v1/showcase`
- `GET /api/v1/stats/overview`
- `POST /api/v1/telemetry/events`

## Submission Flow

`nanobot-community-hub` now supports a first public MCP submission flow.

- users can submit GitHub MCP repository URLs from the Hub UI
- the backend rejects obvious secret-like strings
- known repository URLs are deduplicated
- successful submissions are published straight into the marketplace with `verified=false` and `status=submitted`

This keeps the first version simple while still allowing the community registry to grow beyond the original seed dataset.
