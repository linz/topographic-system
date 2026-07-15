import logging
import os
import subprocess
import time
from pathlib import Path
from time import perf_counter

logger = logging.getLogger("kart_import")

AUTH_ERROR_MARKERS = (
    "Token has expired and refresh failed",
    "Error when retrieving token from sso",
    "Unable to locate credentials",
    "ExpiredToken",
    "InvalidToken",
)


class AuthenticationError(RuntimeError):
    """Raised when an authentication failure is encountered."""

    pass


def run_command(
    cmd: list[str],
    cwd: Path | str | None = None,
    env: dict | None = None,
    allow_error: str | None = None,
    check_error: bool = True,
    retries: int = 0,
    retry_delay: float = 2.0,
) -> str:
    """Runs a command using subprocess.run and returns the stdout.

    Raises an exception if the command fails, unless prevented by `allow_error` or `check_error`.
    :param cmd: command to run as a list of strings
    :param cwd: working directory to run the command in
    :param env: additional environment variables to set when running the command
    :param allow_error: if provided, if the command fails but the stderr contains this string, the function will return stderr instead of raising an exception
    :param check_error: if False, the function will return stdout even if the command fails, but will still log the error. If True, the function will raise an exception if the command fails and allow_error conditions are not met.
    :param retries: number of extra attempts on a genuine (would-raise) failure, with exponential backoff. Use for flaky network commands (e.g. git ls-remote/clone/pull, aws s3 cp). Auth failures and `allow_error`/`check_error=False` outcomes are never retried.
    :param retry_delay: base seconds to sleep before the first retry; doubles each subsequent attempt.
    :return: stdout (or stderr) string
    """
    full_env = os.environ.copy()
    full_env.setdefault("KART_ALLOW_FROM_GIT", "1")  # requires `kart` version 0.17.1 or above
    if env:
        full_env.update(env)
    # Ensure cwd is a string if it's a Path
    cwd_str = str(cwd) if cwd is not None else None

    attempt = 0
    while True:
        start_time = perf_counter()
        result = subprocess.run(cmd, cwd=cwd_str, capture_output=True, text=True, env=full_env)
        duration = perf_counter() - start_time
        logger.debug(
            "command",
            extra={
                "cmd": cmd[0],
                "cmd_args": cmd[1:],
                "code": result.returncode,
                "duration": round(duration * 1000, 4),
                "attempt": attempt,
            },
        )
        if result.returncode == 0:
            return result.stdout

        # always fail on auth errors
        if any(marker in result.stderr for marker in AUTH_ERROR_MARKERS):
            logger.error(f"Authentication failure running [{' '.join(cmd)}]:\n{result.stderr}")
            raise AuthenticationError(
                "AWS credentials are expired or missing. Re-authenticate (e.g. `aws sso login`) and re-run.\n"
                f"Command: {' '.join(cmd)}\n{result.stderr.strip()}"
            )

        if allow_error is not None and allow_error in result.stderr:
            return result.stderr
        if not check_error:
            msg = f"output:\n{result.stdout}" if result.stderr.strip() else "no error output"
            logger.warning(f"Command [{' '.join(cmd)}] failed (not raising) with {msg}")
            return result.stdout

        # Genuine failure that would raise: retry with backoff.
        if attempt < retries:
            delay = retry_delay * (2**attempt)
            attempt += 1
            logger.warning(
                f"Command [{' '.join(cmd)}] failed (attempt {attempt}/{retries}), "
                f"retrying in {delay:.1f}s:\n{result.stderr}"
            )
            time.sleep(delay)
            continue

        logger.error(f"Command failed with output:\n{result.stderr}")
        raise subprocess.CalledProcessError(result.returncode, cmd, output=result.stdout, stderr=result.stderr)
