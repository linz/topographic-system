from dagster import definitions, load_from_defs_folder
from .defs.config import PROJECT_DIR


@definitions
def defs():
    return load_from_defs_folder(project_root=PROJECT_DIR)
