FROM ghcr.io/linz/qgis-flatpak:linz-qgis_v4.2.2_f1431d-297752_build-48
WORKDIR /app

COPY packages/map/dist/ /app/
COPY packages/qgis /app/qgis

ENTRYPOINT [ "node", "/app/index.cjs" ]