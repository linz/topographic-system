"""Check one theme's mapping against its JSON schema (see `kart_import.schema_check`).

Run by the `schema_check` rule for each theme a run builds, before any of the theme's datasets
are processed. Checking per theme, rather than every theme at config load, means a target only
checks the themes it builds: with `KART_SCHEMA_CHECK=strict`, a problem in another repo's theme
doesn't fail `bundle_<repo>`.
"""

import logging

from kart_import.log import log_context

from ..config import WORKING_SCHEMA_CHECK_DIR, get_theme_by_name
from ..schema_check import check_theme_or_warn

logger = logging.getLogger("kart_import")


def schema_check_theme(theme_name: str) -> None:
    # Raises SchemaCheckError in strict mode, so the sentinel is only written when the check passes
    problems = check_theme_or_warn(get_theme_by_name(theme_name))
    logger.info("Schema checked", extra={"theme": theme_name, "problems": len(problems)})

    WORKING_SCHEMA_CHECK_DIR.mkdir(parents=True, exist_ok=True)
    (WORKING_SCHEMA_CHECK_DIR / f"{theme_name}.checked").write_text(f"{len(problems)} problem(s)\n")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m kart_import.assets.schema_check_theme <theme_name>")
        sys.exit(1)
    with log_context(action="schema_check_theme", theme=sys.argv[1]):
        schema_check_theme(sys.argv[1])
