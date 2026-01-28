FROM ghcr.io/linz/action-kart:v0.2.1
ADD packages/kart/dist/index.* /scripts/

# Copy the validation python code into kart cli and install
COPY packages/validation /packages/validation
RUN uv sync --directory /packages/validation

WORKDIR /tmp/kart

ENV KART_USE_HELPER=0
ENV GIT_TERMINAL_PROMPT=0

ENTRYPOINT ["node", "/scripts/index.js"]
