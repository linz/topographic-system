FROM ghcr.io/linz/action-kart:v0.2.0-2-g2a2a6ab
ADD packages/kart/dist/index.* /scripts/
ADD packages/kart/entrypoint.sh /entrypoint.sh

WORKDIR /tmp/kart

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

