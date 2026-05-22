from dagster import Definitions, load_assets_from_modules

from .assets import bundle, clone, export, transform, theme

all_assets_modules = []
all_assets_modules.append(bundle)
all_assets_modules.append(clone)
all_assets_modules.append(export)
all_assets_modules.append(transform)
all_assets_modules.append(theme)

defs = Definitions(
    assets=[*load_assets_from_modules(all_assets_modules)],
)
