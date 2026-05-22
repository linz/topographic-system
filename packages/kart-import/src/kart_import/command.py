import os
import subprocess

from dagster import AssetExecutionContext


def run_command(context: AssetExecutionContext, cmd: list[str], cwd: str | None = None, env: dict = None) -> str:
    context.log.info(f"Running command: {' '.join(cmd)}")
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, env=full_env)
    if result.returncode != 0:
        context.log.error(f"Command failed with output:\n{result.stderr}")
        raise Exception(f"Command failed: {result.stderr}")
    return result.stdout
