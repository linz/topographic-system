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
- **The generated JSON schemas** in `schema/` at the repo root. They are built from
  `packages/schema` and are not committed, so a fresh checkout has none. From the repo root:

  ```shell
  npm ci && npm run -w packages/schema bundle
  ```

  Without them the pipeline only warns, and skips the column type conversion (see
  [Config schema check](#config-schema-check)). With `KART_SCHEMA_CHECK=strict` it stops instead.

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

- **`pjl`** (optional) pretty-prints the pipeline's JSON logs; the examples below pipe to it.
  `npm install` at the repo root installs it, but does not add it to `PATH`, hence run with `npx`.

## Flow

Datasets are first cloned then transformed and loaded,

using `snakemake` individual datasets can be cloned

```bash
uv run snakemake --cores=4 clone_all --quiet | npx pjl
```

or entire themes can be imported, which will clone both the NZ and Chatham Islands airports

```shell
uv run snakemake --cores=4 theme_airport --quiet | npx pjl
```

### Targets

Each stage has a named rule, so you can stop at (or resume from) any point. Later targets pull
in everything they need, so `all` on its own builds the lot.

| Target                                  | Builds                                       | What it does                                                                                      |
| --------------------------------------- | -------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `clone_<dataset>` / `clone_all`         | `data/source/<source>/.cloned`               | clone the source repos (once per source repo; see [Source repos](#source-repos))                  |
| `theme_<theme>`                         | `data/working/theme/release_<n>/<theme>.fgb` | export, transform and merge the theme's datasets, for every configured release                    |
| `kart_theme_<theme>` / `kart_theme_all` | `data/output/<theme>.bundle`                 | replay the theme's releases into a Kart repo, one commit per release, and pack it as a git bundle |
| `kart_import_<repo>`                    | `data/output/<repo>/.imported`               | combine the bundles of every theme whose `target_repo` is `<repo>` into one git history           |
| `push_<repo>` / `push_all`              | `data/output/<repo>/.pushed`                 | push the built repo to its GitHub remote (see [Push](#push))                                      |
| `all` (default)                         | every repo's `.imported`                     | the full pipeline for all configured repos                                                        |
| `extract_<repo>` / `extract_all`        | `data/working/.extracted_<repo>` / `.extracted` | stage 1: clone + export + lifecycle for `<repo>` / everything, then upload to S3 (see [Two stages](#two-stages-extract-and-process)) |
| `process_<repo>` / `process_all`        | `data/output/<repo>/.imported` / every repo's | stage 2: same target as `kart_import_<repo>` / `all`, named for the split above                   |
| `pull_all`                              | `data/source/<source>/.pulled`               | refresh already-cloned sources from the real source                                               |

Rule names come from the config, so `<theme>` is a file in `config/themes/` and `<repo>` is a
key of `config/repos.yml` **with hyphens replaced by underscores** — snakemake rule names cannot
contain `-`. Repo `topographic-data` therefore gives `kart_import_topographic_data`:

```shell
uv run snakemake --cores=4 kart_import_topographic_data --quiet | npx pjl
```

`uv run snakemake --list` prints every rule the current config generates.

Two flags worth knowing when re-running:

- `--rerun-incomplete` retries jobs snakemake marked incomplete after an interrupted run.
- `--forceall` rebuilds the **entire** DAG behind the target, which includes re-cloning every
  source dataset. To redo only the import stage, delete the sentinels you want rebuilt
  (`data/output/<repo>/.imported`, `data/output/<theme>.bundle`) and re-run without it.

### Source repos

Each source repo is cloned once under `data/source/<source>/`, and every dataset and lookup sourced
from it reads that clone. A single-dataset Koordinates repo is named after its dataset
(`data/source/nz_airport_polygons`); a multi-dataset repo, one whose entries set `source.dataset`, is
named after the repo and shared, so every dataset pulling from `topographic-source-data` uses
`data/source/topographic-source-data`.

To keep clones somewhere other than `data/source/` under this package -- e.g. on a separate disk,
shared and persistent across worktrees -- override it with `KART_SOURCE_DIR` (it doesn't need to
already exist; `clone_repo` creates it):

```shell
export KART_SOURCE_DIR=~/data/kart-source
```

Per-commit exports live the same way, one directory per source repo, but always under `data/export/`
(not under `SOURCE_DIR`, and not configurable) -- e.g. `data/export/nz_airport_polygons/nz_airport_polygons_<sha>.parquet`,
or `data/export/topographic-source-data/<dataset-or-lookup-name>_<sha>.parquet` for entries sharing
a multi-dataset repo. Every reader (`transform`, `prepare_lookup`, `assets.working_data`) gets there
through `export_paths.export_entry_dir`/`entry_export_filename`, never a hardcoded path, so a
dataset/lookup's own filename prefix is what keeps it from colliding with siblings in the same
directory. `data/export/` is exactly what the two-stage split's S3 handoff syncs (see
[Two stages](#two-stages-extract-and-process)) -- kept separate from `SOURCE_DIR` so that sync
never has to touch (or exclude) a real clone's `.kart`/`.git` contents, and separate from
`data/working/` so the per-release symlink view there (`data/working/export/release_<n>/<dataset>.parquet`)
stays purely local/derived, rebuildable from a dataset's release map without re-syncing anything.

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
# human-readable theme merges; carries no column types, so kart guesses them (use for dev only)
export KART_THEME_FORMAT=geojson
```

Because they change which files the rules expect, keep them exported for every command in a
given run. Flipping one mid-run invalidates the targets built before it.

## Two stages: extract and process

The pipeline splits into two independently runnable stages, so the clone/export side (which needs
an LDS-registered SSH key) and everything downstream of it (which doesn't) can run on different
machines/credentials:

1. **`extract_all`** — clone every source repo, export every dataset/lookup's per-release commits
   and lifecycle data, then upload `data/export` and `data/working/lifecycle` to S3
   (`assets.working_data`).

   ```shell
   uv run snakemake --cores=4 extract_all --quiet | npx pjl
   ```

   To extract just what one target repo needs (its themes, plus anything they transitively depend
   on via `depends_on_themes`) rather than everything, use `extract_<repo>` (same `<repo>` naming
   as `kart_import_<repo>` / `push_<repo>`):

   ```shell
   uv run snakemake --cores=4 extract_topographic_data --quiet | npx pjl
   ```

   For local development with no AWS credentials or S3 bucket to push to, set
   `KART_IMPORT_SKIP_UPLOAD` to keep the whole run local -- `extract_all`/`extract_<repo>` still
   clone and export exactly the same (those always write straight to disk), the only thing skipped
   is the push to S3 (`summary.json` is still written to `data/working/`, just not uploaded):

   ```shell
   export KART_IMPORT_SKIP_UPLOAD=true
   ```

2. **`process_all`** — download that data back from S3, then run prepare/transform/theme/import,
   with **no LDS SSH access needed**: schema and release→commit resolution are read from what
   `extract_all` captured (JSON sidecars next to each per-commit export), not from a live clone.

   Run the download first, as its own command, **not** chained with `process_all` in the same
   `snakemake` invocation — Snakemake decides which rules need to (re)run from on-disk state as an
   invocation starts, so the download has to have already happened:

   ```shell
   uv run python -m kart_import.assets.working_data download
   uv run snakemake --cores=4 process_all --quiet | npx pjl
   ```

   `download` always fetches S3's `summary.json` first and compares it to the copy it cached last
   time it actually synced; the expensive `aws s3 sync` of the exports/lifecycle themselves only
   runs when that comparison finds a difference. The summary is a manifest of every exported
   dataset's/lookup's release map, plus a hash of `config/topo50_release.yml`, so a release
   definition change (a cutoff date edited, a release added) is caught too, even before it has
   changed any dataset's resolved commits. This is what makes running `process_all` several times
   a day cheap: once `extract_all` hasn't produced anything new, `download` is a single small S3
   fetch, not a full re-sync.

   To process (and import/push) just one target repo rather than everything, use `process_<repo>`:

   ```shell
   uv run snakemake --cores=4 process_topographic_data --quiet | npx pjl
   ```

Both invocations must use the same `KART_IMPORT_THEME`/`KART_IMPORT_RELEASE` scoping (see
[Limiting the work](#limiting-the-work)) — `process_all`/`process_<repo>` request exactly the
lifecycle/export filenames that scoping implies, which only exist if `extract_all`/`extract_<repo>`
was run with the same values.

```shell
# where the handoff lives (default s3://linz-topography-nonprod/working/)
export KART_IMPORT_S3_URL=s3://my-bucket/working/
```

## Transform

The stages between `clone` and `kart_theme` (`export`, `prepare_lookup`, `transform`,
`theme_release`) are wildcard rules driven by the theme config, so they have no named targets,
`theme_<theme>` pulls them all in. To run one on its own, ask for its output file, or invoke the
module directly to bypass snakemake:

```shell
uv run snakemake --cores=4 data/working/transform/release_66/nz_airport_polygons.parquet --quiet | npx pjl
uv run python -m kart_import.assets.transform nz_airport_polygons 66
uv run python -m kart_import.assets.transform nz_airport_polygons
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
release, reconciles the column types (below), sorts by `id` for a stable row order, and writes
`data/working/theme/release_<n>/<theme>.fgb` (the file `kart_theme_<theme>` commits).

### Column types

Kart takes each column's type from the file it imports, so the merge has to hand it the type we
intend rather than one that happens to survive a concatenation. Two things get in the way:

- A dataset mapped `col: null` contributes a column that is NULL for every row. Concatenated with
  a sibling's integer column, pandas widens the result to `object`, and pyogrio writes an `object`
  column as `OFTString`. An integer would reach kart as text.
- FlatGeobuf declares each column's type; GeoJSON does not, leaving kart to auto-detect from the
  JSON text. Hence the `.fgb` default, and why `KART_THEME_FORMAT=geojson` is dev only. Do not
  import a repo built that way into production.

So `theme_release` reconciles types in two passes around the concatenation:

1. **unify** (before): give every frame one dtype per column, taken from the datasets that
   actually carry values and pushed onto the ones that don't, always as a nullable dtype (`Int32`,
   not `int32`) so a NULL cannot degrade it. Consults no schema files.
   Where sources genuinely disagree (e.g. text in one, integer in another) it declines to
   invent a common type and leaves the column to the next pass.
2. **coerce** (after): force the dtypes `schema/<theme>.json` declares. A source handing us a
   float where the schema says `integer` is converted here if it is exactly representable, and
   raises naming the theme and column if it is not, rather than silently becoming text
   downstream. Every column the schema describes is cast, including one whose dtype is merely
   _compatible_ with the target. Note: integer values are capped at `Int32` due to hyparquet / Ajv
   constraints. This will need fixing if t50_fid grows beyond `Int32`.
   Columns the schema does not describe, and themes with no schema at all, keep their unified
   dtype.

Which schema file the coerce pass reads is selected by a theme's own **`schema_version`**, the
same selection used for the static check, so it decides output dtypes as well as config
validation. See [Schema check](#schema-check).

A column the coerce pass could not type because no dataset in the release carried a value for it
_and_ the schema had no dtype to settle it with, stays `object` and is written as text. That is
logged as a warning naming the columns:

```
airport: no dtype for metadata, t50_fid; will be written as text
```

### Timestamps

`created_at` and `updated_at` are normalised to RFC 3339 UTC text (`2015-11-19T02:26:49Z`) during
the coerce pass. They are handled by column name rather than left to the schema, so a theme without
a schema emits the same text as one with.

The values are parsed and reformatted rather than passed through: `format: date-time` requires an
offset, and `astype("string")` over a datetime column yields a space-separated form that fails it.

Lookups take a slightly different path: they are exported once per distinct commit
(`export_lookup`), then `prepare_lookup` slims each export to the key plus the selected columns,
dropping keyless rows and deduplicating on the key so a join cannot fan out rows. Lookups are
never emitted as theme features. See [Left Join Example](#left-join-example).

## Push

Once a target repo has been built (`data/output/<repo>` exists with an
`.imported` sentinel), it can be pushed to its GitHub remote.
The push goes to a release-named branch (`feat/release<N>`, where `N` is the latest
configured release, or `import` when no releases are configured). The branch
carries the entire import history, ready to open a PR into `master`.

Push a single repo, or every repo, via snakemake:

```shell
uv run snakemake --cores=4 push_topographic_data --quiet | npx pjl
uv run snakemake --cores=4 push_all --quiet | npx pjl
```

A successful push writes a `data/output/<repo>/.pushed` sentinel (`<url> <ref>`).

### Push to master / force push

To push to `master` instead of the release branch, or to force-push, set the env
flags (this is the only way through the snakemake rules, which take no arguments):

```shell
# force-push the release branch
KART_PUSH_FORCE=true uv run snakemake --cores=4 push_topographic_data --quiet | npx pjl
# push to master, force (destructive full reload)
KART_PUSH_MASTER=true KART_PUSH_FORCE=true uv run snakemake --cores=4 push_topographic_data --quiet | npx pjl
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

The same schemas are read a second time, at merge rather than load, to supply the dtypes
[`theme_release` coerces to](#column-types): `integer` → `Int64`, `number` → `Float64`,
`string` → `string`, `boolean` → `boolean`, all nullable. `anyOf: [{...}, {type: null}]` is read
as the nullable form of its non-null branch, and `$ref` is resolved within the document. A
property the mapping does not supply a scalar for (i.e. `geometry`, declared only as
`not: {type: null}`; and `bbox`, declared as object) contributes no dtype, and neither does a theme with
no schema file.

A missing schema file is reported for every version a theme's `schema_version` uses. In `warn`
mode that allows a schema-less dev run, whose output keeps the dtypes the sources carried; in
`strict` mode it raises, so a run that is meant to be checked can't silently go without schemas.

Some columns are populated by the pipeline rather than a mapping and so are always treated as
present for the required check: `id`, `created_at`, `updated_at` (import), `geometry`
(`kart export`), and `bbox` (`to-parquet`). Any other required column must be mapped explicitly.

Every theme declares its own `schema_version` in its yaml config; there is no global default, so
this field is required. It's a release-keyed mapping with a mandatory `default` plus the release
id each later version takes over from -- a theme with only one version still needs a mapping,
just with no overrides:

```yaml
schema_version:
  default: v0.3
  67: v0.4 # releases before 67 are checked/coerced against v0.3, 67 onward against v0.4
```

`kart_import_theme` records which version applies as the `linz_schema` kart meta item on the
dataset. When the mapping's resolved version changes between consecutive imported releases, the
meta update rides along in that release's own import commit (via `kart meta set --amend`) rather
than a separate commit, with the commit message noting it:

```
feat(airport): import release 67

schema changed to v0.4
```

Controlled by env vars:

```shell
# warn (default): log problems and continue | strict: raise | off: skip
export KART_SCHEMA_CHECK=strict          # e.g. in CI or a pre-commit hook
export KART_SCHEMA_DIR=/path/to/schema   # override the schema root (folder must exist)
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
    # `backfill`: releases predating the source's first commit reuse its earliest export instead of
    # an empty layer, and each release is transformed on its own so fixups can rewind it
    # (e.g. `map_sheet_published` picks the edition published by the release's date)
    backfill: true
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
