FROM ghcr.io/linz/action-kart:v0.2.0-2-g2a2a6ab
ADD packages/kart/dist/index.* /scripts/

# Copy the validation python code into kart cli and install
COPY packages/validation /packages/validation
RUN uv sync --directory /packages/validation

WORKDIR /tmp/kart

ENTRYPOINT ["node", "/scripts/index.js"]
