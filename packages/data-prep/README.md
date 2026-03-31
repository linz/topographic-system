# Data Preparation

Scripts for preparing NZ Topo50 data.

## Setup

Install dependencies with [uv](https://docs.astral.sh/uv/):

```sh
uv sync
```

## Scripts

### Contour with landcover

Intersects Topo50 contour lines with ice landcover polygons. Contour geometries are split at landcover boundaries, with each segment tagged with the landcover feature type it falls within.

The output schema is defined in [`contour_with_landcover.yaml`](src/data_prep/contour_with_landcover.yaml).

```sh
uv run src/data_prep/contour_with_landcover.py \
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

## Output format

Output files are written as GeoParquet (schema version 1.1.0) with zstd compression and covering bounding boxes. See `parquet_utils.py` for details.

## Tests

```sh
uv run pytest
```

Test fixtures are in `test/` as parquet files.
