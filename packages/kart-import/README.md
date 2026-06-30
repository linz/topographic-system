# Import nztopo50 history

This project is a snakemake-based pipeline for importing topographic data from LINZ Data Service (via Kart) and transforming it into themed geopackages that are then imported back into Kart

## Flow

Datasets are first cloned then transformed and loaded,

using `snakemake` individual datasets can be cloned

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

These are created with the "bundle_all" assets.

```shell
uv run snakemake --cores=4 bundle_all
```

To turn bundle usage off

```shell
export GIT_BUNDLE=false; uv run snakemake --cores=4 clone_nz_airport_polygons --quiet | pjl
```

# Example YAML Configuration Files

```yaml
name: road_line
target_repo: topographic-data
target_epsg: EPSG:4167

datasets:
  - source: kart@data.koordinates.com:linz/nz-road-centrelines-topo-150k
    name: road_line
    mapping:
      id: $t50_fid # target column `id` is based on source column `t50_fid`
      feature_type: road # target column `feature_type` gets populated with literal value `road` for all rows
      status: $ # plain `$` resolves to the source column of the same name (i.e. `status` in this case)
      name: { source: $, default: 'unnamed road' } # use source value if present, default value if null
      highway_number: # same as above but with a different notation style
        source: $hway_num
        default: 888
      way_count: $
      road_access: $
    # NOTE: Fictional examples for illustrative purposes :-) 
    corrections: # declarative value corrections, applied after `mapping` (operate on target column names).
      # keys are matched on their raw YAML value, so the key's type must match the column's:
      # use an int key (`1`) for an int column and a quoted string (`'1'`) for a string column.
      # a type mismatch (e.g. string key vs int column) raises rather than silently matching nothing.
      # `replace`: remap values within a single column (multiple old -> new pairs allowed)
      - { column: way_count, replace: { 1: 'one way' } }
      - { column: road_access, replace: { m: mp } }
      # `set` + `where`: set a column on the rows where every `where` condition matches.
      # entries apply in order, so later ones see the results of earlier ones.
      - { column: road_access, set: private, where: { status: closed } }
    fixups: # release-aware Python repairs registered in `kart_import.fixups.FIXUPS`
      - fn: change_type_to_none
        releases: [64, 65] # omit `releases` to apply the fixup to every release
```
