FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS uv

ENV PYTHONPATH=/app

WORKDIR $PYTHONPATH

ENV PATH="/app/.venv/bin:$PATH"

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY pyproject.toml $PYTHONPATH
COPY uv.lock $PYTHONPATH

COPY requirements.txt $PYTHONPATH

RUN pip install uvicorn

RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
    gnupg \
    && curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

RUN node -v && npx -v

# ADD https://astral.sh/uv/install.sh /uv-installer.sh
# RUN sh /uv-installer.sh && rm /uv-installer.sh

RUN uv sync --frozen --no-install-project
# RUN uv sync --frozen --no-install-project

COPY . $PYTHONPATH

# FROM python:3.11-slim-bookworm

# WORKDIR /app

# RUN wich uv

# COPY --from=uv /app/.venv /app/.venv
# COPY --from=uv /app /app


# ENTRYPOINT ["python", "./mcps1/main.py"]
ENTRYPOINT ["uv", "run"]
CMD [ "mcp", "dev", "./src/mcps1/main.py" ]
