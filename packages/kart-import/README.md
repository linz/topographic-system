# Import nztopo50 history

This project is a snakemake-based pipeline for importing topographic data from LINZ Data Service (via Kart) and transforming it into themed geopackages that are then imported back into Kart

## Prerequisites

- **`uv`** with Python 3.12 or 3.13 (see `requires-python` in `pyproject.toml`). All commands
  below are run from `packages/kart-import/`.
- **`kart` v0.17.1 or above**, on `PATH`. Earlier versions are not supported. Check with:

  ```shell
  kart --version
  ```

- **`git`** on `PATH`.
- **An SSH key registered with the LINZ Data Service.** Every source dataset is cloned over
  SSH from `kart@data.koordinates.com` (see the `source:` values in `config/themes/*.yml`),
  which authenticates with your LDS user's SSH key. Add your public key (e.g. `~/.ssh/id_ed25519.pub`) under _SSH keys_ in your
  [LINZ Data Service](https://data.linz.govt.nz/) profile settings, then confirm the key is
  accepted:

  ```shell
  ssh -T kart@data.koordinates.com
  ```

  A successful connection is authenticated but has no shell, so a "does not provide shell
  access"-style message is the expected result; `Permission denied (publickey)` means the key
  is not registered (or your agent is not offering it: `ssh-add ~/.ssh/id_ed25519`).
  The same key is used whether cloning direct or via a bundle, because bundle clones still
  pull the tip from the source repo.

- **`pjl`** (optional) pretty-prints the pipeline's JSON logs; the examples below pipe to it.
  It ships as a workspace dependency, so `npm install` at the repo root puts it on `PATH`.

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

### Targets

Each stage has a named rule, so you can stop at (or resume from) any point. Later targets pull
in everything they need, so `all` on its own builds the lot.

| Target                                  | Builds                                           | What it does                                                                                      |
| --------------------------------------- | ------------------------------------------------ | ------------------------------------------------------------------------------------------------- |
| `clone_<dataset>` / `clone_all`         | `data/source/<dataset>/.cloned`                  | clone the LDS source repos                                                                        |
| `theme_<theme>`                         | `data/working/theme/release_<n>/<theme>.geojson` | export, transform and merge the theme's datasets, for every configured release                    |
| `kart_theme_<theme>` / `kart_theme_all` | `data/output/<theme>.bundle`                     | replay the theme's releases into a Kart repo, one commit per release, and pack it as a git bundle |
| `kart_import_<repo>`                    | `data/output/<repo>/.imported`                   | combine the bundles of every theme whose `target_repo` is `<repo>` into one git history           |
| `push_<repo>` / `push_all`              | `data/output/<repo>/.pushed`                     | push the built repo to its GitHub remote (see [Push](#push))                                      |
| `all` (default)                         | every repo's `.imported`                         | the full pipeline for all configured repos                                                        |
| `bundle_all`                            | `data/source/<dataset>/.bundle_created`          | maintenance only: refresh the source bundles in S3 (see [LDS Backup](#lds-backup))                |

Rule names come from the config, so `<theme>` is a file in `config/themes/` and `<repo>` is a
key of `config/repos.yml` **with hyphens replaced by underscores** — snakemake rule names cannot
contain `-`. Repo `topographic-data` therefore gives `kart_import_topographic_data`:

```shell
uv run snakemake --cores=4 kart_import_topographic_data --quiet | pjl
```

`uv run snakemake --list` prints every rule the current config generates.

Two flags worth knowing when re-running:

- `--rerun-incomplete` retries jobs snakemake marked incomplete after an interrupted run.
- `--forceall` rebuilds the **entire** DAG behind the target, which includes re-cloning every
  source dataset. To redo only the import stage, delete the sentinels you want rebuilt
  (`data/output/<repo>/.imported`, `data/output/<theme>.bundle`) and re-run without it.

### Limiting the work

For local runs, the pipeline can be narrowed by env var rather than by target. These are read at
config-load time, so they shrink the DAG itself (which themes and releases exist as far as
snakemake is concerned):

```shell
# only load these themes (comma separated, matches config/themes/<name>.yml)
export KART_IMPORT_THEME=airport,water_point
# only process these releases (comma separated release ids)
export KART_IMPORT_RELEASE=66,65,64
# human-readable transform intermediates; slower and larger than the parquet default
export KART_TRANSFORM_FORMAT=geojson
```

Because they change which files the rules expect, keep them exported for every command in a
given run. Flipping one mid-run invalidates the targets built before it.

## Transform

The stages between `clone` and `kart_theme` (`export`, `lifecycle`, `prepare_lookup`, `transform`,
`theme_release`) are wildcard rules driven by the theme config, so they have no named targets,
`theme_<theme>` pulls them all in. To run one on its own, ask for its output file, or invoke the
module directly to bypass snakemake:

```shell
uv run snakemake --cores=4 data/working/transform/release_66/nz_airport_polygons.parquet --quiet | pjl
uv run python -m kart_import.assets.transform nz_airport_polygons 66
```

### Releases resolve to commits

`config/topo50_release.yml` maps each release id to a cutoff timestamp. Every stage resolves a
release to the last source commit at or before that cutoff, taken from the remote-tracking tip
rather than local `HEAD`. Releases whose cutoff predates a source's history resolve to nothing and
are skipped for that dataset.

Consecutive releases frequently resolve to the same commit, so the work is deduplicated: `export`
runs `kart export` once per commit and symlinks it into each `release_<n>/` directory, and
`transform` writes one output for the earliest release with the same fingerprint (resolved source
export + the commit each join's lookup resolves to) and symlinks the rest to it. This is why a
fixup gated to a non-canonical release is rejected with an error naming the release to gate to
instead: that release is never transformed in its own right.

### Feature identity

`lifecycle` walks the releases in order, diffs each commit against the previous
(`kart diff --delta-filter=++`) and records the commit where each feature first appeared, into
`data/working/lifecycle/<dataset>_release<first>-<last>.json`. The key is `t50_fid` if the source
schema ever gained one, otherwise Kart's `auto_pk`.

From that first-seen commit, `transform` derives the feature's `id` (a reproducible UUIDv7 seeded
with the commit timestamp and the fid), plus `created_at` and `updated_at`. IDs are therefore
stable across re-runs, but only for a given release span: the filename encodes the span, and
narrowing it with `KART_IMPORT_RELEASE` makes some features first appear later than they really
did, changing their IDs. _Run the full range for anything you intend to push._

### Order of operations

Per dataset, per release, `transform` applies:

1. **joins**: left-join each configured lookup's selected columns, namespaced as
   `<lookup>.<column>`. Unmatched keys, and releases predating the lookup's own history, give
   nulls; a key-type mismatch raises before the frame is touched.
2. **lifecycle**: attach `id`, `created_at` and `updated_at`. A feature absent from the lifecycle
   file is an error.
3. **projection**: reproject to the theme's `target_epsg` and snap coordinates to `1e-8` degrees
   (~1mm) to keep floating-point noise out of the diffs.
4. **mapping**: build the target columns from `mapping`. `$` / `$col` reference a source column,
   anything else is a literal; `default` fills nulls; a missing source column raises unless
   `since_release` excuses it for this release.
5. **corrections**: declarative `replace` and `set`/`where`, in config order, against the
   **target** column names from step 4. Matching is type-strict.
6. **fixups**: Python repairs from `kart_import.fixups.FIXUPS`, in config order, optionally
   gated to specific releases.

The result is `data/working/transform/release_<n>/<dataset>.parquet` (`.json` under
`KART_TRANSFORM_FORMAT=geojson`). `theme_release` then concatenates the theme's datasets for that
release, sorts by `id` for a stable row order, and writes
`data/working/theme/release_<n>/<theme>.geojson` — the file `kart_theme_<theme>` commits.

Lookups take a slightly different path: they are exported once per distinct commit
(`export_lookup`), then `prepare_lookup` slims each export to the key plus the selected columns,
dropping keyless rows and deduplicating on the key so a join cannot fan out rows. Lookups are
never emitted as theme features. See [Left Join Example](#left-join-example).

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
- **`kart`, `git` and LDS SSH access** as per [Prerequisites](#prerequisites).

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
uv run snakemake --cores=4 push_topographic_data --quiet | pjl
uv run snakemake --cores=4 push_all --quiet | pjl
```

A successful push writes a `data/output/<repo>/.pushed` sentinel (`<url> <ref>`).

### Push to master / force push

To push to `master` instead of the release branch, or to force-push, set the env
flags (this is the only way through the snakemake rules, which take no arguments):

```shell
# force-push the release branch
KART_PUSH_FORCE=true uv run snakemake --cores=4 push_topographic_data --quiet | pjl
# push to master, force (destructive full reload)
KART_PUSH_MASTER=true KART_PUSH_FORCE=true uv run snakemake --cores=4 push_topographic_data --quiet | pjl
```

The module can also be invoked directly with equivalent CLI flags (`--master`,
`--force`); a flag is enabled if either its CLI flag or its env var is set. Note that it takes
the repo name as configured (hyphenated), not the underscored snakemake rule name:

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
cheap, early guard for authoring mistakes:

1. **unknown target column**: a mapping key that is not a schema property.
2. **bad literal constant**: a literal value that violates the property's `const`/`enum`/`type`.
3. **null into a non-nullable field**: `col: null` where the schema forbids null.
4. **missing required column**: a schema `required` property that is neither mapped nor
   supplied by the pipeline, so the output row would omit it.

It does not replace the GeoParquet data validation run in CI. Columns tagged `fixup: true`
are skipped for the value checks (2/3) but still count as _present_ for the required check (4).

Some columns are populated by the pipeline rather than a mapping and so are always treated as
present for the required check: `id`, `created_at`, `updated_at` (import), `geometry`
(`kart export`), and `bbox` (`to-parquet`). Any other required column must be mapped explicitly.

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
      # `since_release`: this source column first appears in this release. Used when a source
      # schema gained a column part way through its history. Earlier releases emit NULL instead
      # of failing; from this release on an absent column is an error
      # A column that *is* present is always mapped whatever the release,
      # so a boundary set too late loses no data.
      orientation: { source: $orientatn, since_release: 49 }
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
