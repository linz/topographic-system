FROM ghcr.io/linz/action-kart:v0.1.2

WORKDIR /scripts

COPY packages/kart/pyproject.toml ./
COPY packages/kart/src/ ./src/

RUN uv pip install --system --break-system-packages .

WORKDIR /tmp/data

ENTRYPOINT ["uv", "run", "/scripts/src/cli.py"]
