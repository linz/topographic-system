FROM ghcr.io/linz/action-kart:v0.5.1

COPY packages/kart-import /packages/kart-import
RUN uv sync --directory /packages/kart-import --no-dev

WORKDIR /packages/kart-import

ENV UV_NO_SYNC=1
ENV KART_USE_HELPER=0
ENV GIT_TERMINAL_PROMPT=0

ENTRYPOINT ["uv", "run", "--directory", "/packages/kart-import", "--no-sync", "snakemake"]
CMD ["--help"]