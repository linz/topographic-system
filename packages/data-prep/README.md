# Data Preparation

Scripts for preparing NZ Topo50 data.

## Setup

Install dependencies with [uv](https://docs.astral.sh/uv/):

```sh
uv sync
```

## Scripts

### Ice Contour

Intersects Topo50 contour lines with ice landcover polygons. Contour geometries are split at landcover boundaries, with each segment tagged with the landcover feature type it falls within.

The output schema is defined in [`ice_contour.yaml`](src/data_prep/ice_contour.yaml).

```sh
uv run src/data_prep/ice_contour.py \
  --contour contour.parquet \
  --landcover landcover.parquet \
  --output output.parquet
```

| Argument      | Description                                                              |
| ------------- | ------------------------------------------------------------------------ |
| `--contour`   | Path to input contour GeoParquet                                         |
| `--landcover` | Path to input landcover GeoParquet (filtered to `feature_type == "ice"`) |
| `--output`    | Path to write the output GeoParquet                                      |

Processing is parallelised across available CPU cores. Input contours are split into chunks and overlaid against the landcover polygons independently.

### Coastline Polygon

Builds the derived coastlines and islands polygon. The coastline (polyline) is converted to land polygons and merged with the island polygons. The major land polygons are named (North Island, South Island, Stewart Island).

```sh
uv run src/data_prep/coastline_polygon.py \
  --coastline coastline.parquet \
  --island island.parquet \
  --output output.parquet
```

| Argument      | Description                                |
| ------------- | ------------------------------------------ |
| `--coastline` | Path to input coastline GeoParquet (lines) |
| `--island`    | Path to input island GeoParquet (polygons) |
| `--output`    | Path to write the output GeoParquet        |

### Rock Line

Extract rock boundary lines that don't coincide coastlines, island shorelines and lake shorelines. Produces a cartographic layer for symbolising rock outlines without duplicating linework already drawn by the coastline, island and lake.

```sh
uv run src/data_prep/rock_line.py \
  --marine marine.parquet \
  --coastline coastline.parquet \
  --island island.parquet \
  --water water.parquet \
  --output output.parquet
```

| Argument      | Description                                                    |
| ------------- | -------------------------------------------------------------- |
| `--marine`    | Path to input marine GeoParquet (filtered to `type == "rock"`) |
| `--coastline` | Path to input coastline GeoParquet                             |
| `--island`    | Path to input island GeoParquet                                |
| `--water`     | Path to input water GeoParquet (filtered to `type == "lake"`)  |
| `--output`    | Path to write the output GeoParquet                            |

Inputs must be in NZGD2000(EPSG:4167). Buffering of linework done in NZTM2000(EPSG:2193) and the result is reprojected back to NZGD2000 before writing.
