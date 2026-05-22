import os
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from pydantic import BaseModel, model_validator

# Paths mapped in docker-compose
DATA_DIR = Path(__file__).parent.parent.parent / "data"
CONFIG_DIR = Path(__file__).parent.parent.parent / "config"

CONFIG_DIR_THEMES = CONFIG_DIR / "themes"
CONFIG_DIR_RELEASE = CONFIG_DIR / "topo50_release.yml"

# source/ — raw Kart repos and GeoJSON release snapshots
SOURCE_DIR = DATA_DIR / "source"

# working/ — per-dataset intermediate
WORKING_DIR = DATA_DIR / "working"
# Exports of all the datasets one file per dataset per release
WORKING_EXPORTS_DIR = WORKING_DIR / "export"
WORKING_TRANSFORM_DIR = WORKING_DIR / "transform"
WORKING_THEME_DIR = WORKING_DIR / "theme"
WORKING_LIFECYCLE_DIR = WORKING_DIR / "lifecycle"

# output/ — final merged theme GeoPackages
OUTPUT_DIR = DATA_DIR / "output"


def is_use_bundle() -> bool:
    return os.getenv("GIT_BUNDLE", "true").lower() == "true"


def get_bundle_url(dataset_name: str) -> str:
    base_url = os.getenv("GIT_BUNDLE_URI", "https://d1jzh93b1t1cv.cloudfront.net/source/")
    return f"{base_url}{dataset_name}.bundle"


def get_s3_bundle_uri() -> str:
    s3_uri = os.getenv("GIT_BUNDLE_S3_URI", "s3://linz-topography-nonprod/source/")
    if not s3_uri.endswith("/"):
        s3_uri += "/"
    return s3_uri


def get_dataset_name(source: str) -> str:
    """Convert a Kart/Gtihb source name into a human friendly name"""
    if not source.startswith("kart@data.koordinates.com:linz/"):
        raise ValueError(f"Invalid source format: {source}")

    # "kart@data.koordinates.com:linz/nz-chatham-island-airport-polygons-topo-150k"
    # converts to "nz-chatham-island-airport-polygons"
    parts = source.split("linz/")[1].split("-")
    if len(parts) > 4:
        trimmed = parts[:-2]
        return "_".join(trimmed)
    else:
        raise ValueError(f"Invalid layer ID format: {source}")


class ThemeDataset(BaseModel):
    source: str
    name: str = ""
    mapping: dict = {}

    @model_validator(mode="before")
    @classmethod
    def populate_name(cls, data):
        if isinstance(data, dict) and data.get("source") and not data.get("name"):
            data["name"] = get_dataset_name(data["source"])
        return data


class Theme(BaseModel):
    name: str
    datasets: list[ThemeDataset]


class Themes(BaseModel):
    themes: list[Theme]

    def append(self, theme: Theme):
        self.themes.append(theme)


ALL_THEMES = Themes(themes=[])
ALL_DATASETS: set[str] = set()


def load_config(file_name: str) -> Theme:
    file = CONFIG_DIR_THEMES / f"{file_name}.yml"
    with open(file) as f:
        data = yaml.safe_load(f)
        if not isinstance(data, dict):
            raise Exception(f"Invalid theme config format in {file_name}.yml")
        return Theme(**data)


def get_themes() -> list[Theme]:
    return ALL_THEMES.themes


def get_datasets() -> list[str]:
    return list(ALL_DATASETS)


class Release(BaseModel):
    id: int
    date: datetime
    until: datetime = datetime.now()


def get_releases() -> list[Release]:
    if not CONFIG_DIR_RELEASE.exists():
        raise Exception(f"Missing {CONFIG_DIR_RELEASE}")

    with open(CONFIG_DIR_RELEASE) as f:
        raw = yaml.safe_load(f)

    releases: list[Release] = []
    for entry in raw.get("releases", []):
        for key, timestamp in entry.items():
            date = datetime.fromisoformat(str(timestamp))
            if releases:
                day_before = date - timedelta(days=14)
                releases[-1].until = day_before
            releases.append(Release(id=int(key), date=date))

    return releases


for cfg in CONFIG_DIR_THEMES.glob("*.yml"):
    theme = load_config(cfg.stem)
    if theme:
        ALL_THEMES.append(theme)
        for dataset in theme.datasets:
            ALL_DATASETS.add(dataset.source)

get_releases()
