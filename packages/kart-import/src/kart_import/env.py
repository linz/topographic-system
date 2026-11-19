import os
from pathlib import Path


def env_transform_format() -> str:
    """Output format for the working/transform intermediates.

    KART_TRANSFORM_FORMAT=parquet|geojson (default parquet). GeoParquet is far
    faster/smaller; set geojson for local dev when you want human-readable
    intermediates:

        export KART_TRANSFORM_FORMAT=geojson
    """
    fmt = os.getenv("KART_TRANSFORM_FORMAT", "parquet").lower()
    if fmt not in ("parquet", "geojson"):
        raise ValueError(f"KART_TRANSFORM_FORMAT must be 'parquet' or 'geojson', got {fmt!r}")
    return fmt


def env_export_format() -> str:
    """Output format for the per-commit source exports, the input to transform.

    KART_EXPORT_FORMAT=parquet|geojson (default parquet). GeoParquet exports and reads back far
    faster than GeoJSON for a large dataset; set geojson for local dev when you want
    human-readable exports, or if your `kart` binary doesn't support Parquet export:

        export KART_EXPORT_FORMAT=geojson
    """
    fmt = os.getenv("KART_EXPORT_FORMAT", "parquet").lower()
    if fmt not in ("parquet", "geojson"):
        raise ValueError(f"KART_EXPORT_FORMAT must be 'parquet' or 'geojson', got {fmt!r}")
    return fmt


def env_theme_format() -> str:
    """Output format for the merged per-theme releases, the input to `kart import`.

    KART_THEME_FORMAT=fgb|geojson (default fgb). FlatGeobuf declares each column's type, so
    kart records the type the pipeline intends. Set geojson for local dev when you want a
    human-readable merge:

        export KART_THEME_FORMAT=geojson

    Dev only: GeoJSON carries no types, so kart auto-detects them from the JSON text.
    Don't import a repo built this way into anywhere that matters.
    """
    fmt = os.getenv("KART_THEME_FORMAT", "fgb").lower()
    if fmt not in ("fgb", "geojson"):
        raise ValueError(f"KART_THEME_FORMAT must be 'fgb' or 'geojson', got {fmt!r}")
    return fmt


def env_themes() -> set[str] | None:
    """
    Limit the number of themes to be processed and loaded based off a comma separated env var

    KART_IMPORT_THEME=airport,vegetation
    """
    base_themes = os.getenv("KART_IMPORT_THEME", None)
    if base_themes is None:
        return None
    return set(t.strip() for t in base_themes.lower().split(","))


def env_releases() -> set[str] | None:
    """
    Limit the number of releases to be processed and loaded based off a comma separated env var

    KART_IMPORT_RELEASE=66,65,64
    """
    base_releases = os.getenv("KART_IMPORT_RELEASE", None)
    if base_releases is None:
        return None
    return set(t.strip() for t in base_releases.lower().split(","))


def env_schema_dir_override() -> str | None:
    """Override the root `schema_dir` resolves a schema version against (bypassing the repo's
    real ``schema/`` root), e.g. for a test fixture.

    KART_SCHEMA_DIR=/path/to/schema (default: the repo's ``schema/`` dir). Returns None
    when unset, i.e. use the default. A set-but-missing path is an operator config error
    (it would otherwise silently make every theme report "no schema" and disable the check),
    so it is rejected here.

        export KART_SCHEMA_DIR=/tmp/schemas
    """
    base = os.getenv("KART_SCHEMA_DIR")
    if base is not None and not Path(base).is_dir():
        raise FileNotFoundError(f"KART_SCHEMA_DIR is set but not a directory: {base!r}")
    return base


def env_schema_check_mode() -> str:
    """Behaviour of the static theme schema check run at config-load time.

    KART_SCHEMA_CHECK=warn|strict|off (default warn):
    ``warn`` logs problems and continues
    ``strict`` raises
    ``off`` skips the check entirely

        export KART_SCHEMA_CHECK=strict
    """
    mode = os.getenv("KART_SCHEMA_CHECK", "warn").lower()
    if mode not in ("warn", "strict", "off"):
        raise ValueError(f"KART_SCHEMA_CHECK must be 'warn', 'strict' or 'off', got {mode!r}")
    return mode


def env_push_to_master() -> bool:
    """
    Push the built target repo to `master` instead of a release-named branch.

    KART_PUSH_MASTER=true
    """
    return os.getenv("KART_PUSH_MASTER", "false").lower() == "true"


def env_push_force() -> bool:
    """
    Force-push the built target repo (combine with KART_PUSH_MASTER for a destructive full reload).

    KART_PUSH_FORCE=true
    """
    return os.getenv("KART_PUSH_FORCE", "false").lower() == "true"


def env_working_data_s3_url() -> str:
    """
    Location of the S3 handoff between `export_all` (clone+export+lifecycle, needs LDS SSH
    access) and the rest of the pipeline (`process_all`, needs only AWS credentials):
    `assets.working_data` syncs `data/working/export` and `data/working/lifecycle` here.

    KART_IMPORT_S3_URL=s3://linz-topography-nonprod/working/
    """
    s3_uri = os.getenv("KART_IMPORT_S3_URL", "s3://linz-topography-nonprod/working/")
    if not s3_uri.endswith("/"):
        s3_uri += "/"
    return s3_uri


def env_schema_host() -> str:
    """
    Public URL the schema package's JSON schemas are published under, as synced by
    `.github/workflows/deploy-schema.yml`. Used to populate a dataset's `linz_schema` kart meta item.

    SCHEMA_HOST_URL=https://d1jzh93b1t1cv.cloudfront.net/schema/
    """
    base_url = os.getenv("SCHEMA_HOST_URL", "https://d1jzh93b1t1cv.cloudfront.net/schema/")
    if not base_url.endswith("/"):
        base_url += "/"
    return base_url


def env_commit_split_bytes() -> int:
    """Max new pack bytes one linearised commit may introduce before it's split into several
    same-release commits, so `kart_push_repo`'s per-commit pushes never alone exceed GitHub's
    push pack limit (see `kart_import.git.linearise`).

    KART_COMMIT_SPLIT_BYTES=1500000000 (default 1.5 GB)

        export KART_COMMIT_SPLIT_BYTES=500000000
    """
    return int(os.getenv("KART_COMMIT_SPLIT_BYTES", "1500000000"))
