import os
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from pydantic import BaseModel, model_validator

from kart_import.env import env_releases

from .env import env_themes

# Set KART_USE_HELPER globally to 0 to disable the background helper process
# as the helper is limited to 4 threads
os.environ["KART_USE_HELPER"] = "0"

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
    """
    Theme name, e.g. "airport"
    """

    target_repo: str
    """
    target kart repository to store the theme in, e.g. "topographic-data"
    """

    target_epsg: str
    """
    target epsg projection code
    """

    datasets: list[ThemeDataset]
    """
    list of datasets to include in the theme
    """


class Release(BaseModel):
    id: int
    date: datetime
    until: datetime = datetime.now()


class Themes(BaseModel):
    themes: list[Theme]

    def append(self, theme: Theme):
        self.themes.append(theme)


ALL_THEMES = Themes(themes=[])
ALL_DATASETS: set[str] = set()
ALL_KART_REPOS: set[str] = set()
ALL_RELEASES: list[Release] = []


def load_config(file_name: str) -> Theme:
    file = CONFIG_DIR_THEMES / f"{file_name}.yml"
    with open(file) as f:
        data = yaml.safe_load(f)
        if not isinstance(data, dict):
            raise Exception(f"Invalid theme config format in {file_name}.yml")
        return Theme(**data)


def get_releases() -> list[Release]:
    return ALL_RELEASES


def get_themes() -> list[Theme]:
    return ALL_THEMES.themes


def get_datasets() -> list[str]:
    return list(ALL_DATASETS)


def get_kart_repos() -> list[str]:
    return list(ALL_KART_REPOS)


def load_from_yaml():
    base_themes = env_themes()
    base_releases = env_releases()

    for cfg in CONFIG_DIR_THEMES.glob("*.yml"):
        theme = load_config(cfg.stem)

        if base_themes and theme.name not in base_themes:
            continue

        ALL_THEMES.append(theme)
        ALL_KART_REPOS.add(theme.target_repo)
        for dataset in theme.datasets:
            ALL_DATASETS.add(dataset.source)

    if not CONFIG_DIR_RELEASE.exists():
        raise Exception(f"Missing {CONFIG_DIR_RELEASE}")

    with open(CONFIG_DIR_RELEASE) as f:
        raw = yaml.safe_load(f)

        for entry in raw.get("releases", []):
            for key, timestamp in entry.items():
                if base_releases and key not in base_releases:
                    continue

                date = datetime.fromisoformat(str(timestamp))
                if ALL_RELEASES:
                    day_before = date - timedelta(days=14)
                    ALL_RELEASES[-1].until = day_before
                ALL_RELEASES.append(Release(id=int(key), date=date))


load_from_yaml()
