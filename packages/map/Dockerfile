FROM ghcr.io/linz/qgis-flatpak:linz-qgis_1ddcee-a6a40d_build-34

ENV OTEL_METRICS_EXPORTER=otlp
ENV OTEL_LOGS_EXPORTER=otlp

WORKDIR /app

COPY packages/map/dist/ /app/

# Copy the qgis python code into map cli
COPY packages/qgis ./packages/map/qgis

WORKDIR /app/packages/map

ENTRYPOINT [ "node", "/app/index.cjs" ]