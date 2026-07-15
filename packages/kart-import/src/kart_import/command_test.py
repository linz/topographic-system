import subprocess
from unittest.mock import MagicMock

import pytest

from . import command
from .command import AuthenticationError, run_command


def _mock_run_sequence(monkeypatch, results: list):
    """One result per subprocess.run call; no real sleeps between retries."""
    run_mock = MagicMock(side_effect=[MagicMock(returncode=rc, stdout=out, stderr=err) for rc, out, err in results])
    monkeypatch.setattr(subprocess, "run", run_mock)
    monkeypatch.setattr(command.time, "sleep", MagicMock())
    return run_mock


def _mock_run(monkeypatch, returncode, stdout="", stderr=""):
    return _mock_run_sequence(monkeypatch, [(returncode, stdout, stderr)])


def test_returns_stdout_on_success(monkeypatch):
    _mock_run(monkeypatch, 0, stdout="ok")
    assert run_command(["echo", "ok"]) == "ok"


def test_raises_called_process_error_on_failure(monkeypatch):
    _mock_run(monkeypatch, 1, stderr="boom")
    with pytest.raises(subprocess.CalledProcessError):
        run_command(["false"])


def test_auth_error_raises_and_is_not_retried(monkeypatch):
    run_mock = _mock_run(monkeypatch, 1, stderr="Unable to locate credentials")
    with pytest.raises(AuthenticationError):
        run_command(["aws", "s3", "cp", "x", "y"], retries=3)
    assert run_mock.call_count == 1


@pytest.mark.parametrize(
    "kwargs",
    [
        {"check_error": False},
        {"allow_error": "Unable to locate credentials"},
    ],
)
def test_auth_error_wins_over_flags(monkeypatch, kwargs):
    """Auth failure fails fast regardless of check_error/allow_error."""
    _mock_run(monkeypatch, 1, stderr="Unable to locate credentials")
    with pytest.raises(AuthenticationError):
        run_command(["aws", "s3", "ls", "s3://bucket/key"], **kwargs)


def test_retries_then_succeeds(monkeypatch):
    run_mock = _mock_run_sequence(
        monkeypatch,
        [(1, "", "connection reset"), (1, "", "connection reset"), (0, "ok", "")],
    )
    assert run_command(["git", "ls-remote", "x"], retries=3) == "ok"
    assert run_mock.call_count == 3


def test_retries_exhausted_raises(monkeypatch):
    run_mock = _mock_run_sequence(monkeypatch, [(1, "", "boom")] * 3)
    with pytest.raises(subprocess.CalledProcessError):
        run_command(["git", "ls-remote", "x"], retries=2)
    assert run_mock.call_count == 3  # initial + 2 retries


@pytest.mark.parametrize(
    "kwargs, stderr, expected",
    [
        ({"check_error": False}, "", ""),
        ({"allow_error": "already exists"}, "already exists", "already exists"),
    ],
)
def test_tolerated_failure_is_not_retried(monkeypatch, kwargs, stderr, expected):
    """A tolerated failure is a result, not a transient error to retry."""
    run_mock = _mock_run(monkeypatch, 1, stderr=stderr)
    assert run_command(["cmd"], retries=3, **kwargs) == expected
    assert run_mock.call_count == 1
