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

To (re)create the bundle for a single dataset, target its `.bundle_created`
sentinel (there is no per-dataset named rule for bundling):

```shell
uv run snakemake --cores=4 data/source/nz_airport_polygons/.bundle_created --quiet | pjl
```

### Prerequisites for bundling

Bundling clones each Kart repo, packs it into a git `.bundle`, and uploads the
bundle plus a per-commit JSON export to S3. For this to work you need:

- **AWS credentials with write access to the bundle store.** The upload uses
  `aws s3 cp` against `GIT_BUNDLE_S3_URL` (default
  `s3://linz-topography-nonprod/source/`). Log in first so the AWS CLI has
  write-credentials, e.g.:

  ```shell
  aws sso login --profile <your-topography-nonprod-profile>
  export AWS_PROFILE=<your-topography-nonprod-profile>
  ```

  To write somewhere else, override the target bucket/prefix:

  ```shell
  export GIT_BUNDLE_S3_URL=s3://my-bucket/source/
  ```

- **The `aws` CLI installed** and on `PATH` (the upload shells out to it).
- **`kart` and `git` installed**, plus SSH access to
  `kart@data.koordinates.com` so the source repositories can be cloned/pulled.

The uploaded bundles are served read-only from CloudFront
(`GIT_BUNDLE_URL`, default `https://d1jzh93b1t1cv.cloudfront.net/source/`),
which is what the clone step reads from when `GIT_BUNDLE=true`.

To turn bundle usage off

```shell
export GIT_BUNDLE=false; uv run snakemake --cores=4 clone_nz_airport_polygons --quiet | pjl
```

## Push

Once a target repo has been built (`data/output/<repo>` exists with an
`.imported` sentinel), it can be pushed to its GitHub remote.
The push goes to a release-named branch (`feat/release<N>`, where `N` is the latest
configured release, or `import` when no releases are configured). The branch
carries the entire import history, ready to open a PR into `master`.

Push a single repo, or every repo, via snakemake:

```shell
uv run snakemake --cores=4 push_topographic-data --quiet | pjl
uv run snakemake --cores=4 push_all --quiet | pjl
```

A successful push writes a `data/output/<repo>/.pushed` sentinel (`<url> <ref>`).

### Push to master / force push

To push to `master` instead of the release branch, or to force-push, set the env
flags (this is the only way through the snakemake rules, which take no arguments):

```shell
# force-push the release branch
KART_PUSH_FORCE=true uv run snakemake --cores=4 push_topographic-data --quiet | pjl
# push to master, force (destructive full reload)
KART_PUSH_MASTER=true KART_PUSH_FORCE=true uv run snakemake --cores=4 push_topographic-data --quiet | pjl
```

The module can also be invoked directly with equivalent CLI flags (`--master`,
`--force`); a flag is enabled if either its CLI flag or its env var is set:

```shell
uv run python -m kart_import.assets.kart_push_repo topographic-data --master --force
```

### Remote configuration

Each target repo's GitHub remote URL is defined in `config/repos.yml`, keyed by
the `target_repo` field used in the theme configs:

```yaml
repos:
  topographic-data: git@github.com:linz/topographic-data
  topographic-contour-data: git@github.com:linz/topographic-contour-data
```

Pushing requires SSH access to these GitHub repositories. The push step
re-points the built repo's `origin` remote at the configured URL before pushing,
so any pre-existing `origin` is replaced.

## Config schema check

On load, each theme's `mapping` is statically checked against `schema/<theme>.json` as a
cheap, early guard for authoring mistakes (unknown target column, a literal that violates a
`const`/`enum`/`type`, or a `null` into a non-nullable field). It does not replace the 
GeoParquet data validation run in CI. Columns tagged `fixup: true` are skipped.

Controlled by env vars:

```shell
# warn (default): log problems and continue | strict: raise | off: skip
export KART_SCHEMA_CHECK=strict            # e.g. in CI or a pre-commit hook
export KART_SCHEMA_SET=next                 # check against schema/next/ instead of schema/
export KART_SCHEMA_DIR=/path/to/schema      # override the schema root (folder must exist)
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
      # `fixup: true`: this column is modified by a dataset fixup (listed under `fixups:` below),
      # so the static schema check skips it.
      # Use for a placeholder the fixup fills, or a transient input column it consumes and drops:
      origin_x: { fixup: true }
      example_name: { source: $source_name, fixup: true }
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
      - fn: map_sheet_origin
        releases: [64, 65] # omit `releases` to apply the fixup to every release
```

## Left Join Example

```yaml
name: road_line_with_lookup
target_repo: topographic-data-demo
target_epsg: EPSG:4167

lookups:
  - name: road_width_lkp
    source:
      url: git@github.com:linz/topographic-source-data
      dataset: linz_road_cl # lookup dataset name in the repository
    key: t50_fid # key column in the *lookup* dataset
    columns:
      - width # source column(s) to bring in from the lookup

datasets:
  - source: kart@data.koordinates.com:linz/nz-road-centrelines-topo-150k
    name: road_line_with_lookup
    mapping:
      id: $t50_fid # target column `id` is based on source column `t50_fid`
      feature_type: road # target column `feature_type` gets populated with literal value `road` for all rows
      status: $ # plain `$` resolves to the source column of the same name (i.e. `status` in this case)
      name: { source: $, default: 'unnamed road' } # use source value if present, default value if null
      highway_number: # same as above but with a different notation style
        source: $hway_num
        default: 888
      width_indicator: $road_width_lkp.width # populated from the lookup defined at the top of the file, using the `width` column from that lookup
      width_indicator2:
        source: $road_width_lkp.width
        default: 'wide' # lookups also support defaults if the key value is not found in the lookup dataset
    joins:
      - lookup: road_width_lkp
        left_on: t50_fid # key column in the *source* dataset to join on
```
