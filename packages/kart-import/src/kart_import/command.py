import logging
import os
import subprocess
from pathlib import Path
from time import perf_counter

logger = logging.getLogger("kart_import")


def run_command(
    cmd: list[str],
    cwd: Path | str | None = None,
    env: dict | None = None,
    allow_error: str | None = None,
    check_error: bool = True,
) -> str:
    """Runs a command using subprocess.run and returns the stdout.

    Raises an exception if the command fails, unless prevented by `allow_error` or `check_error`.
    :param cmd: command to run as a list of strings
    :param cwd: working directory to run the command in
    :param env: additional environment variables to set when running the command
    :param allow_error: if provided, if the command fails but the stderr contains this string, the function will return stderr instead of raising an exception
    :param check_error: if False, the function will return stdout even if the command fails, but will still log the error. If True, the function will raise an exception if the command fails and allow_error conditions are not met.
    :return: stdout (or stderr) string
    """
    start_time = perf_counter()
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    # Ensure cwd is a string if it's a Path
    cwd_str = str(cwd) if cwd is not None else None
    result = subprocess.run(cmd, cwd=cwd_str, capture_output=True, text=True, env=full_env)
    duration = perf_counter() - start_time
    logger.debug(
        "command",
        extra={"cmd": cmd[0], "cmd_args": cmd[1:], "code": result.returncode, "duration": round(duration * 1000, 4)},
    )
    if result.returncode != 0:
        if allow_error is not None and allow_error in result.stderr:
            return result.stderr
        if not check_error:
            logger.warning(f"Command [{' '.join(cmd)}] failed (not raising) with output:\n{result.stderr}")
            return result.stdout
        logger.error(f"Command failed with output:\n{result.stderr}")
        raise subprocess.CalledProcessError(result.returncode, cmd, output=result.stdout, stderr=result.stderr)
    return result.stdout
