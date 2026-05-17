import subprocess
from typing import List, Optional
from dagster import AssetExecutionContext


def run_command(
    context: AssetExecutionContext, cmd: List[str], cwd: Optional[str] = None
) -> str:
    context.log.info(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        context.log.error(f"Command failed with output:\n{result.stderr}")
        raise Exception(f"Command failed: {result.stderr}")
    return result.stdout
