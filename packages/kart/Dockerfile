FROM ghcr.io/linz/action-kart:v0.5.1-3-g8c71777

# Copy the validation python code into kart cli and install
COPY packages/validation /packages/validation
RUN uv sync --directory /packages/validation

COPY packages/data-prep /packages/data-prep
RUN uv sync --directory /packages/data-prep

# kart-import has no lockfile of its own and depends on data-prep, so unlike the two packages
# above it can only be installed through the repo's root uv workspace (pyproject.toml + uv.lock).
# The workspace goes under /workspace rather than /, so it can't take over the standalone
# /packages/validation and /packages/data-prep installs above.

# 1. The workspace definition: which packages exist, and every pinned dependency version
COPY pyproject.toml uv.lock /workspace/
# 2. kart-import, and data-prep which it imports in its fixups
COPY packages/data-prep /workspace/packages/data-prep
COPY packages/kart-import /workspace/packages/kart-import
# 3. Install only kart-import and its dependencies. --frozen installs straight from uv.lock (the
#    workspace's other members, such as qgis, aren't copied, so uv must not try to re-lock), and
#    --no-dev skips the root dev tools (pytest, mypy, ruff).
RUN uv sync --directory /workspace --frozen --no-dev --package topographic-system-kart-import
# 4. kart-import reads schemas from <workspace root>/schema; point that at /schema, copied below
RUN ln --symbolic /schema /workspace/schema
# 5. Never re-sync at runtime. Everything above is installed at build time, and a sync of the
#    partial workspace fails (its other members aren't copied). Set as an env var rather than
#    left to a `--no-sync` flag, so a plain `uv run snakemake ...` can't be run wrong in here.
#    The e2e overrides this with UV_NO_SYNC=0, as it runs the mounted source checkout instead.
ENV UV_NO_SYNC=1

COPY packages/kart/dist/ /app/

# TODO: In future, this may want to live in https/s3/somewhere publicly accessible
# Copy manually created test schema -- validate-schema's default --schema-directory is
# /schema/v0.3 (the current published version), so test fixtures must land there too.
COPY e2e/assets/test*json /schema/v0.3/
# Copy tsp emitted json schema
COPY schema /schema

WORKDIR /tmp/kart

# Disabling kart helper to avoid a bug with handling empty environment variables when running in CI environments in kart v0.17.0
ENV KART_USE_HELPER=0

# Disabling git terminal prompt to avoid hanging (asking for user/pass) when git credentials are missing
ENV GIT_TERMINAL_PROMPT=0

ENTRYPOINT ["node", "/app/index.cjs"]
