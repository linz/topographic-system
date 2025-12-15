FROM ghcr.io/linz/action-kart:v0.1.2

WORKDIR /app

WORKDIR /app/packages/kart

# Copy only necessary files
COPY packages/kart/pyproject.toml ./
COPY packages/kart/src/ ./src/

RUN uv pip install --system --break-system-packages .

ENTRYPOINT ["python3", "/app/packages/kart/src/cli.py"]
