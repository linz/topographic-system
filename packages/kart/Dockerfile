FROM ghcr.io/linz/action-kart:v0.2.0

ADD packages/kart/dist/index.* /scripts/

WORKDIR /tmp/kart

ENTRYPOINT ["node", "/scripts/index.js"]

