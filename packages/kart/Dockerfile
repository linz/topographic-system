FROM ghcr.io/linz/action-kart:v0.2.0-2-g2a2a6ab
ADD packages/kart/dist/index.* /scripts/

WORKDIR /tmp/kart

ENV KART_USE_HELPER=0
ENV GIT_TERMINAL_PROMPT=0

ENTRYPOINT ["node", "/scripts/index.js"]

