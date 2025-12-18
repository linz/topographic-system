FROM ghcr.io/linz/action-kart:v0.2.0

ADD dist/index.js /scripts/

WORKDIR /tmp/kart

ENTRYPOINT ["node", "/scripts/index.js"]

