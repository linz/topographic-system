from dagster import Definitions, load_assets_from_modules
from .assets import clone

all_assets_modules = []
all_assets_modules.append(clone)


defs = Definitions(
    assets=[*load_assets_from_modules(all_assets_modules)],
)
