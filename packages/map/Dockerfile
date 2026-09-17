FROM ghcr.io/linz/qgis-flatpak:linz-qgis_v4.2.2_f1431d-c6b79d_build-47
WORKDIR /app

COPY packages/map/dist/ /app/
COPY packages/qgis /app/qgis

ENTRYPOINT [ "node", "/app/index.cjs" ]