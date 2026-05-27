# Import nztopo50 history

This project is a Dagster-based pipeline for importing topographic data from LINZ Data Service (via Kart) and transforming it into themed geopackages that are then imported back into Kart

## Flow

Datasets are first cloned then transformed and loaded,

using `dg` individual datasets can be cloned

```bash
uv run snakemake --cores=4 clone_all --quiet | pjl
```

or entire themes can be imported, which will clone both the NZ and Chatham Islands airports

```shell
uv run snakemake --cores=4 theme_airport --quiet | pjl
```

## LDS Backup

git bundles are stored of all kart repositories in cloudfront to enable fast cloning

```shell
git clone --bundle-uri=https://d1jzh93b1t1cv.cloudfront.net/source/nz_airport_polygons.bundle kart@data.koordinates.com:linz/nz-airport-polygons-topo-150k
```

These are created with the "\*bundle_all" assets.

To turn bundle usage off

```shell
export GIT_BUNDLE=false; uv run snakemake --cores=4 clone_nz_airport_polygons --quiet | pjl
```
