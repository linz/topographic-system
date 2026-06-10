import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from .env import env_releases, env_themes, env_transform_format

logger = logging.getLogger("kart_import")

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

# Format of the working/transform intermediates (GeoParquet by default)
TRANSFORM_FORMAT = env_transform_format()
TRANSFORM_SUFFIX = ".parquet" if TRANSFORM_FORMAT == "parquet" else ".json"


KOORDINATES_PREFIX = "kart@data.koordinates.com:linz/"


def get_dataset_name(source: "Source") -> str:
    """Derive a human friendly dataset name from a source.

    Only Koordinates layer URLs can be derived automatically; any other source
    (e.g. a multi-dataset github repo) must declare an explicit ``name`` in the
    theme config.
    """
    url = source.url
    if url.startswith(KOORDINATES_PREFIX):
        # "kart@data.koordinates.com:linz/nz-chatham-island-airport-polygons-topo-150k"
        # converts to "nz_chatham_island_airport_polygons"
        parts = url.split("linz/")[1].split("-")
        if len(parts) > 4:
            return "_".join(parts[:-2])
        raise ValueError(f"Invalid Koordinates layer URL: {url}")
    raise ValueError(f"Cannot derive a name for source {url!r}; set 'name:' explicitly in the theme config")


class Source(BaseModel):
    """Where a dataset comes from.

    A plain string in the YAML is coerced to ``{"url": <string>}``. ``dataset``
    selects a single dataset inside a multi-dataset Kart repo; when omitted the
    repo's sole dataset id is auto-detected.
    """

    url: str
    dataset: str | None = None

    @model_validator(mode="before")
    @classmethod
    def coerce_string(cls, data):
        if isinstance(data, str):
            return {"url": data}
        return data


class FieldSpec(BaseModel):
    """A single target column's mapping rule.

    In the YAML a mapping value is either a scalar shorthand or a dict:

        name: $                       # copy same-named source column
        highway_number: $hway_num     # copy a named source column
        feature_type: road            # literal constant
        version: 1                    # literal constant (non-string is fine)
        topo_id: null                 # create the column populated with NULL
        name: {source: $, default: "Unknown"}   # default when the value is NULL

    An unlisted source column is dropped.
    """

    model_config = ConfigDict(extra="forbid")

    source: Any = None
    """Column reference (``$`` / ``$col``), a literal constant, or ``None`` for an all-NULL column."""
    default: Any = None
    """Value substituted when the resolved source is NULL/NaN."""

    @classmethod
    def parse(cls, value: Any) -> "FieldSpec":
        if isinstance(value, dict):
            return cls(**value)
        return cls(source=value)


class Fixup(BaseModel):
    """Reference to a release-aware fixup function (see ``kart_import.fixups``)."""

    model_config = ConfigDict(extra="forbid")

    fn: str
    """Name of a function registered in ``kart_import.fixups.FIXUPS``."""
    releases: list[int] | None = None
    """Release ids to apply the fixup to; ``None`` applies it to every release."""

    @field_validator("fn")
    @classmethod
    def known_fn(cls, value: str) -> str:
        from .fixups import FIXUPS

        if value not in FIXUPS:
            raise ValueError(f"Unknown fixup '{value}'. Available: {sorted(FIXUPS)}")
        return value


class ThemeDataset(BaseModel):
    source: Source
    name: str = ""
    mapping: dict = {}
    fixups: list[Fixup] = []

    @model_validator(mode="before")
    @classmethod
    def populate_name(cls, data):
        if isinstance(data, dict) and data.get("source") and not data.get("name"):
            data["name"] = get_dataset_name(Source.model_validate(data["source"]))
        return data

    @model_validator(mode="after")
    def validate_mapping(self):
        # Parse eagerly so a malformed field spec fails at config load, not mid-run.
        self.field_specs()
        return self

    def field_specs(self) -> dict[str, FieldSpec]:
        """The mapping parsed into normalized per-column rules."""
        return {target: FieldSpec.parse(value) for target, value in self.mapping.items()}


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


ALL_THEMES: list[Theme] = []
ALL_DATASETS: set[str] = set()
ALL_KART_REPOS: set[str] = set()
ALL_RELEASES: list[Release] = []
DATASET_MAP: dict[str, ThemeDataset] = {}
DATASET_TO_THEME_MAP: dict[str, Theme] = {}


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
    return ALL_THEMES


def get_theme_by_name(theme_name: str) -> Theme:
    for t in ALL_THEMES:
        if t.name == theme_name:
            return t
    raise Exception(f"{theme_name} does not exist")


def get_datasets() -> list[str]:
    return list(ALL_DATASETS)


def get_kart_repos() -> list[str]:
    return list(ALL_KART_REPOS)


def get_dataset_by_name(dataset_name: str) -> ThemeDataset:
    dataset = DATASET_MAP.get(dataset_name)
    if dataset is None:
        raise Exception(f"Dataset {dataset_name} not found")
    return dataset


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
            ALL_DATASETS.add(dataset.source.url)
            if dataset.name in DATASET_MAP:
                raise Exception(f"Dataset {dataset.name} is defined in more than one theme")
            DATASET_MAP[dataset.name] = dataset
            DATASET_TO_THEME_MAP[dataset.name] = theme

    if not CONFIG_DIR_RELEASE.exists():
        raise Exception(f"Missing {CONFIG_DIR_RELEASE}")

    with open(CONFIG_DIR_RELEASE) as f:
        raw = yaml.safe_load(f)

        for entry in raw.get("releases", []):
            for key, timestamp in entry.items():
                if base_releases and str(key) not in base_releases:
                    continue

                date = datetime.fromisoformat(str(timestamp))
                if ALL_RELEASES:
                    day_before = date - timedelta(days=14)
                    ALL_RELEASES[-1].until = day_before
                ALL_RELEASES.append(Release(id=int(key), date=date))

    logger.info(
        "config-loaded",
        extra={
            "themes": [t.name for t in ALL_THEMES],
            "releases": ",".join([str(r.id) for r in ALL_RELEASES]),
        },
    )


load_from_yaml()
