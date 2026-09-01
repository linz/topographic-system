# Kart Import Process

## Purpose

`kart-import` rebuilds historical LINZ topographic datasets into a harmonised set of themed Kart repositories. It reads release dates and theme mappings from YAML, obtains source history from Kart or Git repositories, transforms each configured release, and replays the results into new Git histories.

Snakemake coordinates the process. Files and sentinel files under `data/` represent completed work, allowing interrupted or partial runs to resume.

## Process at a Glance

```text
configuration
    |
    v
clone source repositories
    |
    +--> resolve source commits for configured releases
    |
    +--> calculate feature lifecycle and stable identities
    |
    +--> export source datasets and lookup tables
    |
    v
join, map, correct and transform each dataset release
    |
    v
merge datasets into one file per theme and release
    |
    v
replay releases into one Kart history per theme
    |
    v
combine theme histories into target repositories
    |
    v
optionally push target repositories to GitHub
```

## Inputs and Configuration

The pipeline loads configuration when Snakemake evaluates the `Snakefile`. Invalid configuration therefore normally fails before any jobs run.

### Theme configuration

Files in `config/themes/*.yml` define:

- the theme name and target repository;
- the target coordinate reference system;
- one or more source datasets;
- source-to-target column mappings;
- optional lookup tables and joins;
- declarative corrections; and
- named Python fixups for cases that cannot be expressed declaratively.

A source may be a single-dataset LDS Kart URL or a multi-dataset repository. Names can be derived from normal Koordinates URLs; other sources must provide explicit dataset names and, where necessary, the dataset identifier inside the repository.

Only mapped columns are emitted. A mapping may copy the same-named source column with `$`, copy another column with `$column`, provide a literal, create an all-null column, provide a default, or declare the release from which a source column exists.

### Releases

`config/topo50_release.yml` maps release identifiers to cutoff timestamps. For each source, a release means the most recent source commit at or before that timestamp. A source that did not yet exist for a release contributes no features for that release.

### Target repositories

`config/repos.yml` maps logical target repository names to Git remote URLs. Several themes can target the same repository.

### Target schemas

Target JSON schemas are stored in the repository-level `schema/` directory, not inside `packages/kart-import`.

Schemas are used twice:

1. At configuration load, mappings are checked for unknown columns, invalid literals, invalid nulls and missing required fields.
2. During theme merging, schema types are used to coerce output columns before Kart imports them.

`KART_SCHEMA_SET=next` selects `schema/next/`; `KART_SCHEMA_DIR` can provide another schema root. A container must make the selected schema directory available at the path expected by the package, or set `KART_SCHEMA_DIR` explicitly.

## Startup and DAG Generation

The `Snakefile` loads themes, releases and repositories through `kart_import.config`. It then creates concrete convenience targets from the configuration:

- `clone_<dataset>` and `clone_all`;
- `theme_<theme>`;
- `kart_theme_<theme>` and `kart_theme_all`;
- `kart_import_<repo>`;
- `push_<repo>` and `push_all`; and
- `bundle_all`.

Hyphens in repository names become underscores in Snakemake rule names.

At startup, the pipeline also creates or propagates a W3C `TRACEPARENT`. Its trace ID becomes the run ID used to correlate structured logs and isolate bundle-maintenance log files.

## Processing Stages

### 1. Clone source repositories

The clone stage creates `data/source/<dataset>/.cloned`.

By default, the pipeline first attempts to use a downloadable Git bundle from `GIT_BUNDLE_URL`. The bundle reduces the amount of source history fetched over SSH, but the clone still contacts the configured remote to obtain its current tip. If a bundle is unavailable or unusable, the code falls back to a normal clone.

The clone is checked to ensure it contains the configured Kart dataset. LDS sources require an SSH key accepted by `kart@data.koordinates.com`.

Set `GIT_BUNDLE=false` to bypass the bundle path.

### 2. Build the feature lifecycle

The lifecycle stage writes:

```text
data/working/lifecycle/<dataset>_release<first>-<last>.json
```

It walks the configured releases and source history to determine when each feature first appeared and when it changed. `t50_fid` is preferred as the source identity where available; otherwise the source Kart primary key is used.

The lifecycle data supplies:

- a reproducible UUIDv7 `id`;
- `created_at`; and
- `updated_at`.

The generated identity is stable when the same source history and release range are used. Restricting the release range can change which release appears to introduce a feature, so restricted runs are suitable for development but should not be treated as authoritative full-history imports.

### 3. Export source releases

For every dataset and configured release, the export stage resolves the corresponding commit and writes a GeoJSON snapshot under:

```text
data/working/export/release_<release>/<dataset>.json
```

Several releases often resolve to the same source commit. The implementation exports that commit once and links the equivalent release outputs to the shared result.

Lookup sources follow a similar process, but exports are organised by commit under `data/working/export/lookup/<lookup>/`.

### 4. Prepare lookup tables

A lookup is supporting data used to enrich emitted features; it does not become a theme dataset itself.

For each relevant lookup commit, preparation:

- retains only the configured key and selected columns;
- drops rows with null keys;
- deduplicates repeated keys;
- normalises source integer types; and
- writes a compact Parquet table under `data/working/lookup/<lookup>/`.

### 5. Transform each dataset release

The transform stage combines one source export with lifecycle data and any configured lookup tables. It writes:

```text
data/working/transform/release_<release>/<dataset>.parquet
```

GeoJSON can be selected for development with `KART_TRANSFORM_FORMAT=geojson`.

The transformation order is significant:

1. **Joins** perform type-checked left joins and expose lookup values as `<lookup>.<column>`.
2. **Lifecycle enrichment** adds `id`, `created_at` and `updated_at`.
3. **Projection** reprojects geometry to the theme CRS and reduces coordinate precision to suppress insignificant floating-point changes.
4. **Mapping** builds the target columns and drops unlisted source columns.
5. **Corrections** apply configured value replacements or conditional assignments against target columns.
6. **Fixups** invoke registered Python functions, optionally only for selected releases.

Missing required source columns, incompatible join keys, unknown fixups and invalid correction types fail explicitly.

### 6. Merge a theme release

All transformed datasets belonging to one theme are concatenated into:

```text
data/working/theme/release_<release>/<theme>.fgb
```

FlatGeobuf is the default because it preserves declared field types for Kart. `KART_THEME_FORMAT=geojson` is intended only for inspection and development because GeoJSON does not carry an equivalent typed schema.

Before writing, the merge reconciles nullable column types across datasets. It then applies target schema types where available and converts lifecycle timestamps to RFC 3339 UTC strings. Conversions that would lose data, such as a fractional number into an integer field, fail rather than silently changing the value or field type.

### 7. Build a historical Kart bundle for each theme

The theme-import stage creates a temporary Kart repository and processes releases from oldest to newest. For each release it imports the merged theme file with `id` as the primary key and dates the commit using the configured release timestamp.

Releases with no features or no changes do not introduce meaningless data changes. The resulting repository is packed as:

```text
data/output/<theme>.bundle
```

The bundle contains the theme's release history, not merely its latest state.

### 8. Combine themes into a target repository

For each target repository, the pipeline reads all theme bundles assigned to it. Theme histories are expected to own disjoint root-level dataset paths.

The combination code orders commits chronologically and uses Git fast-import to construct one linear history. Marker commits identify release boundaries. The completed repository is represented by:

```text
data/output/<repo>/.imported
```

### 9. Optionally push the result

Push rules use the remote from `config/repos.yml`. By default, output is pushed to a release-named feature branch suitable for a pull request. `KART_PUSH_MASTER=true` targets `master`, and `KART_PUSH_FORCE=true` enables force pushing.

Successful completion writes the remote and ref to:

```text
data/output/<repo>/.pushed
```

Pushing requires an SSH identity authorised for the target GitHub repository.

### 10. Maintain source bundles

The `bundle_<dataset>` and `bundle_all` targets are maintenance operations rather than part of the normal import path. They refresh source bundles and upload them with the AWS CLI to `GIT_BUNDLE_S3_URL`.

This path requires AWS write credentials. It writes per-dataset logs under `logs/bundle/<run-id>/` and uses `.bundle_created` sentinels under `data/source/`.

## Data Layout

```text
data/
  source/                         cloned source repositories and clone sentinels
  working/
    lifecycle/                    feature identity and timestamp history
    export/                       source snapshots by release or commit
    lookup/                       prepared lookup tables
    transform/                    mapped dataset releases
    theme/                        merged theme releases
  output/
    <theme>.bundle                historical Kart bundle for one theme
    <repo>/                       combined target repository
    <repo>/.imported              import completion sentinel
    <repo>/.pushed                push completion record
```

The `data/` directory can become large. In a container it should normally be a persistent volume so work survives container removal and subsequent runs can resume.

## Limiting a Run

The following environment variables narrow the DAG at configuration-load time:

```text
KART_IMPORT_THEME=airport,water_point
KART_IMPORT_RELEASE=66,65,64
```

Format and schema controls include:

```text
KART_TRANSFORM_FORMAT=parquet|geojson
KART_THEME_FORMAT=fgb|geojson
KART_SCHEMA_CHECK=warn|strict|off
KART_SCHEMA_SET=current|next
KART_SCHEMA_DIR=/path/to/schema
```

Bundle and push controls include:

```text
GIT_BUNDLE=true|false
GIT_BUNDLE_URL=https://example/source/
GIT_BUNDLE_S3_URL=s3://bucket/source/
KART_PUSH_MASTER=true|false
KART_PUSH_FORCE=true|false
```

Because theme and release filters change the expected graph and feature lifecycle, use the same values throughout a run.

## Resume and Rebuild Behaviour

Snakemake compares declared inputs and outputs and skips completed work. Directory contents, generated files and sentinel files therefore form the pipeline's durable state.

- Use `--rerun-incomplete` after an interrupted job.
- Use `--forceall` only when the entire dependency graph should be rebuilt.
- Delete a specific output or sentinel to rerun that stage and anything downstream from it.
- Preserve `data/` between container invocations to retain downloads and intermediate work.

Changing environment filters or output formats does not automatically make every old artifact semantically compatible with the new run. Remove affected intermediates when changing those controls.

## Runtime Requirements

Normal import processing requires:

- Python 3.12 or 3.13 with the project dependencies installed by `uv`;
- Kart 0.17.1 or newer;
- Git and SSH;
- an LDS-authorised SSH key; and
- network access to the configured LDS, bundle and Git remotes.

Bundle maintenance additionally requires the AWS CLI and AWS credentials. Pushing requires GitHub write access.

The container image installs the Python environment internally, so a host Python virtual environment is not required. Host-mounted storage and credentials are still required for durable processing and authenticated network operations.

## Example Targets

From `packages/kart-import` in a local `uv` environment:

```shell
uv run snakemake --list
uv run snakemake --cores=4 clone_all
uv run snakemake --cores=4 theme_airport
uv run snakemake --cores=4 kart_theme_airport
uv run snakemake --cores=4 kart_import_topographic_data
```

If the container entrypoint is already `uv run snakemake`, pass only Snakemake arguments:

```powershell
wslc run --rm kart-import --list
wslc run --rm kart-import --cores=4 theme_airport
```

For real work, mount persistent `data/`, the required schemas and SSH credentials according to the host environment and container runtime security policy.