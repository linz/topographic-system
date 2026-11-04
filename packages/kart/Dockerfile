FROM ghcr.io/linz/action-kart:v0.5.1-3-g8c71777

# Copy the validation python code into kart cli and install
COPY packages/validation /packages/validation
RUN uv sync --directory /packages/validation

COPY packages/data-prep /packages/data-prep
RUN uv sync --directory /packages/data-prep

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
