FROM ghcr.io/linz/action-kart:v0.5.1

# Copy the validation python code into kart cli and install
COPY packages/validation /packages/validation
RUN uv sync --directory /packages/validation

COPY packages/data-prep /packages/data-prep
RUN uv sync --directory /packages/data-prep

COPY packages/kart/dist/ /app/

# TODO: In future, this may want to live in https/s3/somewhere publicly accessible
# Copy manually created test schema
COPY e2e/assets/test*json /schema/
# Copy tsp emitted json schema
COPY schema /schema

WORKDIR /tmp/kart

# Disabling git terminal prompt to avoid hanging (asking for user/pass) when git credentials are missing
ENV GIT_TERMINAL_PROMPT=0

ENTRYPOINT ["node", "/app/index.cjs"]
