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


def env_themes() -> set[str] | None:
    """
    Limit the number of themes to be processed and loaded based off a comma seperated env var

    KART_IMPORT_THEME=airport,vegetation
    """
    base_themes = os.getenv("KART_IMPORT_THEME", None)
    if base_themes is None:
        return None
    return set(t.strip() for t in base_themes.lower().split(","))


def env_releases() -> set[str] | None:
    """
    Limit the number of releases to be processed and loaded based off a comma seperated env var

    KART_IMPORT_RELEASE=66,65,64
    """
    base_releases = os.getenv("KART_IMPORT_RELEASE", None)
    if base_releases is None:
        return None
    return set(t.strip() for t in base_releases.lower().split(","))


def env_schema_set() -> str:
    """Which schema set the static schema check validates a theme's mapping against.

    KART_SCHEMA_SET=current|next (default current):
    ``current`` -> ``schema/`` ; ``next`` -> ``schema/next/``.

        export KART_SCHEMA_SET=next
    """
    return os.getenv("KART_SCHEMA_SET", "current")


def env_schema_dir_override() -> str | None:
    """Override the root of the ``current`` schema set (``next`` is its ``next/`` child).

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


def env_use_bundle() -> bool:
    """
    Should git bundles be used to speed up download process

    GIT_BUNDLE=true
    """
    return os.getenv("GIT_BUNDLE", "true").lower() == "true"


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


def env_bundle_url(dataset_name: str) -> str:
    """
    Location to git bundles stored for easy access

    GIT_BUNDLE_URL=https://d1jzh93b1t1cv.cloudfront.net/source/
    """
    base_url = os.getenv("GIT_BUNDLE_URL", "https://d1jzh93b1t1cv.cloudfront.net/source/")
    if not base_url.endswith("/"):
        base_url += "/"
    return f"{base_url}{dataset_name}.bundle"


def env_bundle_s3_url() -> str:
    """
    Location to a AWS writeable bundle store, used for initial bundle seeding

    GIT_BUNDLE_S3_URL=s3://linz-topography-nonprod/source/
    """
    s3_uri = os.getenv("GIT_BUNDLE_S3_URL", "s3://linz-topography-nonprod/source/")
    if not s3_uri.endswith("/"):
        s3_uri += "/"
    return s3_uri
