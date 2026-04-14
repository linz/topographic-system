FROM ghcr.io/linz/qgis-flatpak:linz-qgis_1ddcee-a6a40d_build-34

WORKDIR /app

COPY packages/map/dist/ /app/
COPY packages/qgis /app/qgis

ENTRYPOINT [ "node", "/app/index.cjs" ]