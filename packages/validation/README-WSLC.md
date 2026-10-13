# WSLC — Running Docker Containers on Windows

Guide to using WSLC (Windows Subsystem for Linux Container) to build and run the `kart` Docker image locally on Windows.

---

## Running Schema Validation Locally (without Docker)

Requires Node.js. This repo targets Node 24 (`^24.5.0`).

Install dependencies from the repo root:

```bash
npm install
```

Verify versions:

```
node -v  # e.g. v24.15.0
npm -v   # e.g. 11.12.1
```

Run from the `topographic-system` root. On Windows, use a `file://` URL if the file is not relative to the working directory:

```bash
node packages/kart/src/index.ts validate-schema --schema schema/next/airport.json file:///c:/data/temp/airport.parquet

node packages/kart/src/index.ts validate-schema --schema file:///c:/Data/toposource/schema_model/airport.json file:///c:/data/temp/airport.parquet
```

---

## Prerequisites for Docker (WSLC)

- **WSL** — included by default on Windows 11 (Windows Subsystem for Linux)
- **WSLC** — built-in Linux container support; see [announcement](https://devblogs.microsoft.com/commandline/wsl-container-is-now-available-for-public-preview/)

Update WSL (and install the latest pre-release for WSLC support):

```bash
wsl --version
wsl --update
# or, while WSLC is in pre-release:
wsl --update --pre-release
```

---

## Build the Docker Image

Bundle the kart package (required if code has changed):

```bash
npm run -w @linzjs/topographic-system-kart bundle
```

Build the image:

```bash
wslc build -t kart -f packages/kart/Dockerfile .
```

The build includes schema and topology validation tooling.

---

## Schema Validation

### Against the live schema

```bash
wslc run --rm -it -v C:\data\temp:/data kart validate-schema --schema /schema/airport.json /data/airport.parquet
```

### Against the next schema

```bash
wslc run --rm -it -v C:\data\temp:/data kart validate-schema --schema /schema/next/road_line.json /data/road_line.parquet
```

### Targeting a custom schema folder

```bash
wslc run --rm -it -v C:\data\temp\amcmenamin:/data -v C:\Data\toposource\schema_model:/schema kart validate-schema --schema /schema/airport.json /data/airport.parquet
```

### Help

```bash
wslc run --rm -it kart --help
```

---

## Topology Validation

Topology validation is invoked via `--entrypoint uv`.

### Help

```bash
wslc run --rm \
  -v c:/Data/toposource/topographic-data:/input \
  -v c:/Data/toposource/validation-results:/output \
  --entrypoint uv kart:latest \
  run --directory /packages/validation python src/topographic_validation/cli.py --help
```

### Run validation

```bash
wslc run --rm \
  -v c:/Data/toposource/topographic-data:/input \
  -v c:/Data/toposource/validation-results:/output \
  --entrypoint uv kart:latest \
  run --directory /packages/validation python src/topographic_validation/cli.py \
  --verbose \
  --config-file config/default_config.json \
  --bbox 174.824 -36.92 174.829 -36.919 \
  --db-path /input/topographic-data.gpkg \
  --output-dir /output
```

---

## Troubleshooting — Locked GeoPackage File

If a run fails and leaves the `.gpkg` file locked, verify the file state inside the container:

```bash
wslc run --rm -it -v c:/Data/toposource/topographic-data:/input kart sh -lc "ls -la /input/topographic-data.gpkg*"
```

Run an integrity check on the GeoPackage:

```bash
wslc run --rm -it -v c:/Data/toposource/topographic-data:/input kart \
  uv run python -c "import sqlite3; c=sqlite3.connect('/input/topographic-data.gpkg'); print(c.execute('PRAGMA integrity_check').fetchone()[0])"
```

---

## Useful WSLC Commands

### List containers and images

```bash
wslc container ps -a
wslc image ls
```

### Delete containers and images

```bash
wslc container delete <id|name>
wslc image delete <id|name>

# Examples:
wslc image delete kart
wslc image delete 20649cd1bb45
```

### Sessions

```bash
wslc system session list
wslc system session terminate
```

### Open a shell in the container

```bash
wslc run --rm -it kart bash
```

### Find a file inside the container

```bash
wslc run --rm kart find / -name "airport.json"
```
