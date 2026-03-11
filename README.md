# nanobot-community-hub

`nanobot-community-hub` is the first V1 cut of the Nanobot ecosystem backend.

It provides:

- MCP discovery and marketplace metadata
- MCP detail pages with tools, known issues, and recommended config
- community stats and overview metrics
- stack presets
- basic showcase entries
- telemetry ingest for anonymous MCP runtime signals

Tech stack:

- FastAPI
- Jinja2 templates
- HTMX
- SQLite for the first deployable version

## Run locally

```bash
uv venv
source .venv/bin/activate
uv pip install -e .[dev]
uvicorn nanobot_hub.app:create_app --factory --host 0.0.0.0 --port 18811
```

## Environment

- `NANOBOT_HUB_DB_PATH` default: `./data/nanobot-community-hub.sqlite3`
- `NANOBOT_HUB_PUBLIC_URL` default: empty
- `NANOBOT_HUB_INSTANCE_NAME` default: `nanobot-community-hub`

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
- `GET /api/v1/stacks`
- `GET /api/v1/stacks/{slug}`
- `GET /api/v1/showcase`
- `GET /api/v1/stats/overview`
- `POST /api/v1/telemetry/events`
