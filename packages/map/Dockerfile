FROM ghcr.io/linz/qgis-flatpak:linz-qgis_70e40a-ff162c_build-43
WORKDIR /app

COPY packages/map/dist/ /app/
COPY packages/qgis /app/qgis

ENTRYPOINT [ "node", "/app/index.cjs" ]