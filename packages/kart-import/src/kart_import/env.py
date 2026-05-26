import os


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


def env_use_bundle() -> bool:
    """
    Should git bundles be used to speed up download process

    GIT_BUNDLE=true
    """
    return os.getenv("GIT_BUNDLE", "true").lower() == "true"


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
