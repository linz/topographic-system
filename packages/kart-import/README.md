# Import nztopo50 history

This project is a Dagster-based pipeline for importing topographic data from LINZ Data Service (via Kart) and transforming it into themed geopackages that are then imported back into Kart

## Flow

Datasets are first cloned then transformed and loaded,

using `dg` individual datasets can be cloned

```bash
uv run dg launch --assets clone_nz_airport_polygons
```

or entire themes can be imported, which will clone both the NZ and Chatham Islands airports

```shell
uv run dg launch --assets theme_airport
```
