# OpenTelemetry Tracing

`kart-import` includes OpenTelemetry (OTel) instrumentation for distributed tracing and log export across Snakemake workflow executions and asset tasks.

## Local Setup

1. **Install:**

   ```bash
   go install github.com/CtrlSpice/otel-desktop-viewer@latest
   ```

2. **Start the viewer:**

   ```bash
   otel-desktop-viewer
   ```

   - OTLP HTTP receiver: `http://localhost:4318`
   - Web UI: opens automatically at [http://localhost:8000](http://localhost:8000)

## Running Jobs

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4318"
uv run snakemake theme_airport --cores=8
```

Or inline:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4318" uv run snakemake theme_airport --cores=8
```
