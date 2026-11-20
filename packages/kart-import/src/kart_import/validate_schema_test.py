import subprocess
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from opentelemetry import trace

from .validate_schema import _current_span_env, _validation_summary, validate_dataset

MODULE = "kart_import.validate_schema"


@pytest.fixture
def env(tmp_path, monkeypatch):
    dataset_file = tmp_path / "airport.fgb"
    dataset_file.write_text("not real geodata, only the path is used by the mocks below")

    schema_file = tmp_path / "airport.json"
    schema_file.write_text("{}")

    mocks = SimpleNamespace(
        run_command=MagicMock(return_value=""),
        as_parquet=MagicMock(),
        dataset_file=dataset_file,
        schema_file=schema_file,
        parquet_file=dataset_file.with_suffix(".validate.parquet"),
    )

    monkeypatch.setattr(f"{MODULE}.run_command", mocks.run_command)
    monkeypatch.setattr(f"{MODULE}._as_parquet", mocks.as_parquet)
    monkeypatch.setattr(f"{MODULE}.schema_path", lambda theme_name, schema_version: schema_file)
    monkeypatch.setattr(f"{MODULE}.env_validate_schema_cmd", lambda: ["node", "/app/index.cjs"])
    monkeypatch.setattr(f"{MODULE}._current_span_env", lambda: {"TRACEPARENT": "00-trace-span-01"})

    return mocks


def test_validates_the_converted_parquet_against_the_resolved_schema(env):
    validate_dataset("airport", "v0.4", env.dataset_file)

    env.as_parquet.assert_called_once_with(env.dataset_file, env.parquet_file)
    env.run_command.assert_called_once_with(
        ["node", "/app/index.cjs", "validate-schema", "--schema", str(env.schema_file), str(env.parquet_file)],
        env={"TRACEPARENT": "00-trace-span-01"},
    )


def test_removes_the_temporary_parquet_file_after_validating(env):
    env.as_parquet.side_effect = lambda _src, dest: dest.write_text("parquet bytes")

    validate_dataset("airport", "v0.4", env.dataset_file)

    assert not env.parquet_file.exists()


def test_removes_the_temporary_parquet_file_even_when_validation_fails(env):
    env.as_parquet.side_effect = lambda _src, dest: dest.write_text("parquet bytes")
    env.run_command.side_effect = subprocess.CalledProcessError(1, ["node"], output="", stderr="")

    with pytest.raises(RuntimeError):
        validate_dataset("airport", "v0.4", env.dataset_file)

    assert not env.parquet_file.exists()


def test_raises_a_clear_error_with_the_ajv_summary_when_validation_fails(env):
    """The CLI logs its ajv error detail to stdout (pino's default stream, see
    `packages/shared/src/log.ts`) and leaves stderr empty, so the raised error has to be built
    from `stdout` -- a bare `CalledProcessError` here would just say "exit status 1"."""
    stdout = (
        '{"level":30,"msg":"ValidateSchema: Started"}\n'
        '{"level":50,"error":"/type:must be equal to constant","count":3,"samples":["bad-type"],'
        '"msg":"ValidateSchema:ErrorSummary"}\n'
        '{"level":60,"msg":"Error"}\n'
    )
    env.run_command.side_effect = subprocess.CalledProcessError(1, ["node"], output=stdout, stderr="")

    with pytest.raises(RuntimeError, match=r"3 row\(s\), e\.g\. \['bad-type'\]") as exc_info:
        validate_dataset("airport", "v0.4", env.dataset_file)

    assert "schema validation failed" in str(exc_info.value)


def test_raises_when_the_schema_file_is_missing(env, monkeypatch, tmp_path):
    """A missing schema file is a hard failure, not a skip: every theme requires a
    `schema_version`, so this means the build artifact wasn't generated (or `KART_SCHEMA_DIR`
    is wrong) -- not that the theme has no schema."""
    missing_schema_file = tmp_path / "missing.json"
    monkeypatch.setattr(f"{MODULE}.schema_path", lambda theme_name, schema_version: missing_schema_file)

    with pytest.raises(FileNotFoundError, match="no schema at"):
        validate_dataset("airport", "v0.4", env.dataset_file)

    env.as_parquet.assert_not_called()
    env.run_command.assert_not_called()


def test_validation_summary_extracts_every_ajv_error_summary_line():
    stdout = (
        '{"msg":"ValidateSchema: Started"}\n'
        '{"error":"/type:must be equal to constant","count":2,"samples":["bad"],"msg":"ValidateSchema:ErrorSummary"}\n'
        '{"error":"/name:must be string","count":1,"samples":[],"msg":"ValidateSchema:ErrorSummary"}\n'
        '{"invalid":3,"msg":"ValidateSchema:Done"}\n'
    )

    summary = _validation_summary(stdout)

    assert "/type:must be equal to constant: 2 row(s), e.g. ['bad']" in summary
    assert "/name:must be string: 1 row(s), e.g. []" in summary


def test_validation_summary_falls_back_to_the_raw_output_when_nothing_parses():
    assert _validation_summary("not json, e.g. a stack trace") == "not json, e.g. a stack trace"


def test_current_span_env_is_empty_with_no_active_span():
    assert _current_span_env() == {}


def test_current_span_env_carries_the_innermost_active_span_uppercased():
    """The validator subprocess reads `TRACEPARENT` (uppercase) itself (see
    `packages/shared/src/otel/otel.ts`'s `createOtelSdk`); the propagator injects the W3C header
    name (lowercase `traceparent`), which a case-sensitive env var lookup would miss."""
    tracer = trace.get_tracer("test")
    with (
        tracer.start_as_current_span("kart_import: airport [r67]") as outer,
        tracer.start_as_current_span("validate_schema: airport [r67]") as inner,
    ):
        result = _current_span_env()

    assert set(result) == {"TRACEPARENT"}
    traceparent = result["TRACEPARENT"]
    assert format(inner.get_span_context().trace_id, "032x") in traceparent
    assert format(inner.get_span_context().span_id, "016x") in traceparent
    # The point of the fix: this is the innermost span, not the outer one.
    assert format(outer.get_span_context().span_id, "016x") not in traceparent
