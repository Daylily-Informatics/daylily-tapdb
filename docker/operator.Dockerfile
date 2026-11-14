# syntax=docker/dockerfile:1
# One Debian Bookworm runtime with its native PostgreSQL 16.15 client closure.
FROM postgres:16.15-bookworm@sha256:5264710be20f409750ac6dc4e5706dcef929c87e241605463f0f9aefa932a8d9

COPY --from=ghcr.io/astral-sh/uv:0.11.16@sha256:440fd6477af86a2f1b38080c539f1672cd22acb1b1a47e321dba5158ab08864d /uv /usr/local/bin/uv

ARG TAPDB_VERSION=14.0.0
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${TAPDB_VERSION} \
    UV_PYTHON_INSTALL_DIR=/opt/python \
    UV_PROJECT_ENVIRONMENT=/opt/tapdb \
    UV_LINK_MODE=copy \
    PATH=/opt/tapdb/bin:/usr/lib/postgresql/16/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /opt/tapdb-source
RUN uv python install 3.12.12
COPY pyproject.toml uv.lock README.md ./
COPY admin ./admin
COPY daylily_tapdb ./daylily_tapdb
COPY schema ./schema
COPY docs ./docs
RUN uv sync --frozen --python 3.12.12 --no-dev --extra cli --extra aurora \
    && dpkg-query -W -f='${Package}=${Version}\n' > /opt/operator-os-packages.txt \
    && useradd --uid 1000 --create-home --shell /bin/bash tapdb-operator

LABEL org.opencontainers.image.source="https://github.com/Daylily-Informatics/daylily-tapdb" \
      org.opencontainers.image.version="14.0.0" \
      org.opencontainers.image.description="TapDB operator: Python 3.12.12, PostgreSQL 16.15, Meridian 1.0.0"

USER tapdb-operator
WORKDIR /home/tapdb-operator
ENTRYPOINT ["/opt/tapdb/bin/tapdb"]
CMD ["--help"]
