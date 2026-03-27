FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONNODEBUGRANGES=1 \
    PIP_NO_CACHE_DIR=1 \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    APP_HOME=/app \
    APP_USER=appuser \
    PATH="/app/.venv/bin:${PATH}"

WORKDIR ${APP_HOME}

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        unar \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /bin/bash ${APP_USER}

COPY pyproject.toml uv.lock alembic.ini ${APP_HOME}/
COPY migrations ${APP_HOME}/migrations
COPY src ${APP_HOME}/src

RUN python -m pip install --upgrade pip setuptools wheel uv \
    && uv sync --frozen --no-dev --no-editable \
    && mkdir -p /tmp/extg-archive-imports \
    && chown -R ${APP_USER}:${APP_USER} ${APP_HOME} /tmp/extg-archive-imports

USER ${APP_USER}

CMD ["extg-bot"]
