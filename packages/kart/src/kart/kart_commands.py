import os
import subprocess
import traceback

from concurrent.futures import ThreadPoolExecutor, as_completed
from kart.utils import repository_data_path, time_in_ms, ensure_git_credentials

from linz_logger import get_log


class KartExecutionException(Exception):
    pass


def get_kart_command(
    command: str, global_options: list[str], command_options: list[str]
) -> list[str]:
    """Build a `kart` command.

    Args:
        command: kart command to run (e.g. 'clone', 'diff', etc.)
        global_options: options to pass before the command (e.g. -C PATH)
        command_options: options to pass after the command

    Returns:
        a list of arguments for `kart`
    """
    get_log().info(
        "kart command",
        command=command,
        global_options=global_options,
        command_options=command_options,
    )
    if command not in [
        "clone",
        "fetch",
        "export",
        "data",
    ]:
        raise ValueError(f"Unsupported kart command: {command}")

    kart_command: list[str] = []
    kart_command.extend(global_options)
    kart_command.append(command)
    kart_command.extend(command_options)
    return kart_command


def get_kart_clone_command(repository: str, token: str | None = None) -> list[str]:
    """Build a `kart clone` command.

    Args:
        repository: Address (URL) of the repository to clone
        token: Optional token for authentication

    Returns:
        a list of arguments for `kart`
    """
    get_log().info("kart clone command", repository=repository, token=bool(token))
    global_options = ["-C", repository_data_path(repository)]
    command_options = [repository, "--no-checkout"]
    return get_kart_command("clone", global_options, command_options)


def get_kart_data_ls_command(repository_path: str | None) -> list[str]:
    """Build a `kart data ls` command.

    Args:
        repository_path: Local path of the repository to list data from (optional, defaults to current directory)

    Returns:
        a list of arguments for `kart`
    """
    get_log().info("kart data ls command", repository_path=repository_path)
    global_options = ["-C", repository_path] if repository_path else []
    command_options = ["ls"]
    return get_kart_command("data", global_options, command_options)


def get_kart_export_command(
    layer: str, repository_path: str | None = None, sha: str | None = None
) -> list[str]:
    """Build a `kart export` command.

    Args:
        layer: The layer to export
        repository_path: Local path of the repository to export from (optional, defaults to current directory)
        sha: Optional commit SHA to export

    Returns:
        a list of arguments for `kart`
    """
    global_options = ["-C", repository_path] if repository_path else []
    command_options = ["--ref", sha] if sha else []
    command_options.extend([layer, f"/tmp/data/{layer}.gpkg"])
    return get_kart_command("export", global_options, command_options)


def get_kart_fetch_command(repository_path: str, sha: str) -> list[str]:
    """Build a `kart fetch` command.

    Args:
        repository_path: Local path of the repository to fetch from
        sha: The commit SHA to fetch

    Returns:
        a list of arguments for `kart`
    """
    global_options = ["-C", repository_path]
    command_options = ["origin", sha]
    return get_kart_command("fetch", global_options, command_options)


def run_kart_clone(
    repository: str,
    sha: str,
) -> tuple[subprocess.CompletedProcess[bytes], ...]:
    """Run the kart clone command.

    Args:
        repository: Address (URL) of the repository to clone
        sha: The commit SHA to fetch after cloning

    Returns:
        tuple[subprocess.CompletedProcess, subprocess.CompletedProcess]: the output processes for clone and fetch.
    """
    kart_exec = os.environ.get("KART_EXECUTABLE", "kart")

    kart_clone_command = [
        kart_exec,
        *get_kart_clone_command(repository),
    ]
    kart_fetch_command = [
        kart_exec,
        *get_kart_fetch_command(repository_data_path(repository), sha),
    ]

    procs = []
    for kart_command in [kart_clone_command, kart_fetch_command]:
        proc = execute_command(kart_command)
        procs.append(proc)

    return tuple(procs)


def run_kart_export(
    repository_path: str | None = None,
    sha: str | None = None,
    layers: list[str] | None = None,
    num_procs: int | None = None,
) -> tuple[subprocess.CompletedProcess[bytes], ...]:
    """Run the kart export command in parallel.

    Args:
        repository_path: Local path of the repository to export from (optional, defaults to current directory)
        sha: The commit SHA to export (optional, defaults to HEAD)
        layers: List of layers to export (optional, defaults to all layers)
        num_procs: Number of parallel processes to use (optional, defaults to number of CPUs)

    Returns:
        tuple[subprocess.CompletedProcess, ...]: the output processes for export commands.
    """
    kart_exec = os.environ.get("KART_EXECUTABLE", "kart")

    kart_data_ls_command = [
        kart_exec,
        *get_kart_data_ls_command(repository_path),
    ]

    kart_repo_layers = (
        execute_command(kart_data_ls_command).stdout.decode().splitlines()
    )
    if layers:
        layers_to_export = [layer for layer in layers if layer in kart_repo_layers]
        if len(layers_to_export) != len(layers):
            missing_layers = set(layers) - set(layers_to_export)
            raise KartExecutionException(
                f"Missing layers: {missing_layers}, repository_path={repository_path}"
            )
    else:
        layers_to_export = kart_repo_layers

    kart_export_commands = [
        [kart_exec, *get_kart_export_command(layer, repository_path, sha)]
        for layer in layers_to_export
    ]

    if num_procs is None:
        num_procs = os.cpu_count() or 1

    procs = []
    with ThreadPoolExecutor(max_workers=num_procs) as executor:
        future_to_cmd = {
            executor.submit(execute_command, cmd): cmd for cmd in kart_export_commands
        }
        for future in as_completed(future_to_cmd):
            proc = future.result()
            procs.append(proc)

    return tuple(procs)


def execute_command(command: list[str]) -> subprocess.CompletedProcess[bytes]:
    """Execute a command and return the completed process.

    Args:
        command: The command to execute as a list of strings

    Returns:
        The completed process
    """
    start_time = time_in_ms()
    kart_env = os.environ.copy()
    ensure_git_credentials()
    get_log().debug(
        "run_kart_start",
        command=" ".join(command),
        has_token="GITHUB_TOKEN" in kart_env,
    )
    proc = None
    try:
        proc = subprocess.run(
            command,
            env=kart_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
    except subprocess.CalledProcessError as cpe:
        get_log().error(
            "run_kart_failed",
            command=" ".join(command),
            error=str(cpe.stderr, "utf-8"),
            exception=traceback.format_exc(),
        )
        raise KartExecutionException(f"kart {str(cpe.stderr, 'utf-8')}") from cpe
    finally:
        get_log().info(
            "run_kart_end",
            command=" ".join(command),
            duration=time_in_ms() - start_time,
        )
        if proc and proc.stderr:
            get_log().warning(
                "run_kart_stderr",
                command=" ".join(command),
                stderr=proc.stderr.decode(),
            )
        if proc:
            get_log().trace(
                "run_kart_stdout",
                command=" ".join(command),
                stdout=proc.stdout.decode(),
            )
    return proc
