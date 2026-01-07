#FROM ghcr.io/linz/action-kart:v0.2.0
FROM ghcr.io/linz/action-kart:v0.2.0-2-g2a2a6ab
ADD packages/kart/dist/index.* /scripts/

WORKDIR /tmp/kart

ENTRYPOINT ["node", "/scripts/index.js"]

