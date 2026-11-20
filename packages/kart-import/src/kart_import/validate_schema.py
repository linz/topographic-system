"""Post-import data validation: checks a just-imported release's data against its published
JSON schema.

This is the real, per-row check -- complementary to `schema_check`'s cheap, config-load-time
static check of a theme's yaml *mapping*. The per-row validator (ajv, against the same JSON
schema) already exists as the `validate-schema` CLI in `packages/kart`
(`action.validate.schema.ts`), used by the PR data-review flow; ajv validates a release's rows
far faster than a Python `jsonschema` loop would, so `validate_dataset` shells out to it rather
than duplicating it here.
"""

import json
import subprocess
from pathlib import Path

import pyogrio
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from .command import run_command
from .env import env_validate_schema_cmd
from .schema_check import schema_path


def _validation_summary(stdout: str) -> str:
    """Pull the ajv per-error summaries out of the validator's own JSON log lines.

    The CLI logs via `pino`, which defaults to stdout (see `packages/shared/src/log.ts`), and
    its uncaught-error handler (`console.log(err)` in `instrument.cli.ts`) does too -- so
    `run_command`'s failure handling, which only inspects `stderr`, never sees any of this;
    `stderr` is empty. Without pulling it out here, a failure surfaces as a bare "exit status 1"
    with no indication of which column or rows broke.
    """
    lines = []
    for raw_line in stdout.splitlines():
        try:
            record = json.loads(raw_line)
        except ValueError:
            continue
        if record.get("msg") == "ValidateSchema:ErrorSummary":
            lines.append(f"  {record.get('error')}: {record.get('count')} row(s), e.g. {record.get('samples')}")
    return "\n".join(lines) if lines else stdout


def _current_span_env() -> dict[str, str]:
    """W3C trace headers for the *currently active* span, to hand the validator subprocess.

    `Snakefile` sets `TRACEPARENT` once, in the process environment, for the whole run's root
    span. `run_command` otherwise inherits that unchanged, so without this every subprocess --
    however deeply nested its own `log_context` span -- would point the validator's CLI (which
    reads `TRACEPARENT` itself; see `packages/shared/src/otel/otel.ts`'s `createOtelSdk`) at
    that stale root instead of its real parent, the current `validate_schema: ...` span.
    """
    carrier: dict[str, str] = {}
    TraceContextTextMapPropagator().inject(carrier)
    # The propagator injects lowercase W3C header names (`traceparent`); both this pipeline's own
    # extraction (`log.setup_logging`) and the validator's (`createOtelSdk`) read the uppercase
    # env var name instead, and Linux env var names are case-sensitive.
    return {key.upper(): value for key, value in carrier.items()}


def _as_parquet(dataset_file: Path, parquet_file: Path) -> None:
    """Write `dataset_file` (the merged theme release, FlatGeobuf by default) out as GeoParquet,
    the only format the TS validator reads.

    Round-tripped through `pyogrio`/`geopandas` rather than `ogr2ogr`, matching how
    `kart_import.assets.transform` already writes parquet, so the schema-coerced nullable dtypes
    `theme.coerce_dtypes` assigned the release survive into the file being validated.
    """
    gdf = pyogrio.read_dataframe(dataset_file)
    gdf.to_parquet(parquet_file, compression="zstd", index=False)


def validate_dataset(theme_name: str, schema_version: str, dataset_file: Path) -> None:
    """Validate every row of `dataset_file` (a release's merged theme output, just imported)
    against `schema/<schema_version>/<theme_name>.json`, raising if any row fails.

    Unlike the static mapping check (`schema_check.check_theme`), a missing schema file is
    itself a hard failure here, not a skip: every theme's yaml now requires a `schema_version`,
    so a missing file means the build artifact (`npm run bundle` in `packages/schema`, or a
    wrong `KART_SCHEMA_DIR`) hasn't been generated, not that the theme has no schema. Silently
    skipping would otherwise look identical, in the logs, to "validated, no problems".
    """
    schema_file = schema_path(theme_name, schema_version)
    if not schema_file.exists():
        raise FileNotFoundError(
            f"{theme_name}: no schema at {schema_file}; run `npm run bundle` in packages/schema "
            "(or check KART_SCHEMA_DIR) before importing"
        )

    parquet_file = dataset_file.with_suffix(".validate.parquet")
    try:
        _as_parquet(dataset_file, parquet_file)
        cmd = [*env_validate_schema_cmd(), "validate-schema", "--schema", str(schema_file), str(parquet_file)]
        run_command(cmd, env=_current_span_env())
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"{theme_name}: schema validation failed for {dataset_file.name} against {schema_file}:\n"
            f"{_validation_summary(e.output)}"
        ) from None
    finally:
        parquet_file.unlink(missing_ok=True)
