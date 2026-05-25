import os
import subprocess
from pathlib import Path
from time import perf_counter

from dagster import AssetExecutionContext


def run_command(
    context: AssetExecutionContext,
    cmd: list[str],
    cwd: Path | str | None = None,
    env: dict | None = None,
    allow_error: str | None = None,
) -> str:
    start_time = perf_counter()
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    # Ensure cwd is a string if it's a Path
    cwd_str = str(cwd) if cwd is not None else None
    result = subprocess.run(cmd, cwd=cwd_str, capture_output=True, text=True, env=full_env)
    duration = perf_counter() - start_time
    context.log.debug(f"  command: {' '.join(cmd)} completed: {result.returncode} in {duration:.4f}s")
    if result.returncode != 0:
        if allow_error is not None and allow_error in result.stderr:
            return result.stderr
        context.log.error(f"Command failed with output:\n{result.stderr}")
        raise Exception(f"Command failed: {result.stderr}")
    return result.stdout
