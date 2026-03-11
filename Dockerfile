FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

COPY pyproject.toml README.md ./
RUN mkdir -p nanobot_hub && touch nanobot_hub/__init__.py && \
    uv pip install --system --no-cache . && \
    rm -rf nanobot_hub

COPY nanobot_hub/ nanobot_hub/
RUN uv pip install --system --no-cache .

RUN mkdir -p /data

EXPOSE 18811

CMD ["uvicorn", "nanobot_hub.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "18811"]
