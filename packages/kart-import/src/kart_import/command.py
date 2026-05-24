import os
import subprocess
from pathlib import Path

from dagster import AssetExecutionContext


def run_command(
    context: AssetExecutionContext,
    cmd: list[str],
    cwd: Path | str | None = None,
    env: dict | None = None,
    allow_error: str | None = None,
) -> str:
    context.log.info(f"Running command: {' '.join(cmd)}")
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    # Ensure cwd is a string if it's a Path
    cwd_str = str(cwd) if cwd is not None else None
    result = subprocess.run(cmd, cwd=cwd_str, capture_output=True, text=True, env=full_env)
    if result.returncode != 0:
        if allow_error is not None and allow_error in result.stderr:
            return result.stderr
        context.log.error(f"Command failed with output:\n{result.stderr}")
        raise Exception(f"Command failed: {result.stderr}")
    return result.stdout
