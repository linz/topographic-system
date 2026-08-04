FROM ghcr.io/linz/action-kart:v0.5.1

# Copy the validation python code into kart cli and install
COPY . /packages/validation
RUN uv sync --directory /packages/validation

WORKDIR /packages/validation


# Disabling git terminal prompt to avoid hanging (asking for user/pass) when git credentials are missing
ENV GIT_TERMINAL_PROMPT=0


