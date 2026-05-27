# syntax=docker/dockerfile:1

FROM python:3.12-slim-bookworm AS builder

ARG SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0+container
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${SETUPTOOLS_SCM_PRETEND_VERSION}
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

COPY --from=ghcr.io/astral-sh/uv:0.11.16 /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY admin ./admin
COPY daylily_tapdb ./daylily_tapdb
COPY schema ./schema

RUN uv sync --frozen --no-dev --extra admin --extra cli --extra aurora

FROM python:3.12-slim-bookworm AS runtime

ENV PATH="/app/.venv/bin:${PATH}"
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system tapdb \
    && useradd --system --gid tapdb --home-dir /app --create-home tapdb

WORKDIR /app

COPY --from=builder --chown=tapdb:tapdb /app /app
COPY --chown=tapdb:tapdb docker/entrypoint.sh /usr/local/bin/tapdb-entrypoint

RUN chmod 0755 /usr/local/bin/tapdb-entrypoint

USER tapdb
EXPOSE 8910

ENTRYPOINT ["tapdb-entrypoint"]
CMD ["python", "-m", "daylily_tapdb.container_entry"]
