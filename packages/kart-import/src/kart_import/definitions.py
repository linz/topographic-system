from dagster import Definitions, load_assets_from_modules

from .assets import bundle, clone, export

all_assets_modules = []
all_assets_modules.append(bundle)
all_assets_modules.append(clone)
all_assets_modules.append(export)

defs = Definitions(
    assets=[*load_assets_from_modules(all_assets_modules)],
)
