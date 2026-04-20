FROM ghcr.io/linz/qgis-flatpak:linz-qgis_f04b83-922072_build-38

WORKDIR /app

COPY packages/map/dist/ /app/
COPY packages/qgis /app/qgis

ENTRYPOINT [ "node", "/app/index.cjs" ]