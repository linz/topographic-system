FROM ghcr.io/linz/action-kart:v0.2.1
COPY packages/kart/dist/ /scripts/

# Copy the validation python code into kart cli and install
COPY packages/validation /packages/validation
RUN uv sync --directory /packages/validation

COPY packages/data-prep /packages/data-prep
RUN uv sync --directory /packages/data-prep

WORKDIR /tmp/kart

# Disabling kart helper to avoid a bug with handling empty environment variables when running in CI environments in kart v0.17.0
ENV KART_USE_HELPER=0

# Disabling git terminal prompt to avoid hanging (asking for user/pass) when git credentials are missing
ENV GIT_TERMINAL_PROMPT=0

ENTRYPOINT ["node", "/scripts/index.cjs"]
