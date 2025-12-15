import os
import subprocess
import time

from linz_logger import get_log


class KartExecutionException(Exception):
    get_log().error("kart execution error", Exception=Exception)
    pass


def get_kart_command(command: str, options: list[str]) -> list[str]:
    """Build a `kart` command.

    Args:
        command: kart command to run (e.g. 'translate', 'info', etc.)
        options: options to pass to the kart command

    Returns:
        a list of arguments for `kart`
    """
    get_log().info("kart command", command=command, options=options)
    if command not in [
        "clone",
        "fetch",
        "diff",
    ]:
        raise ValueError(f"Unsupported kart command: {command}")

    kart_command: list[str] = [command]
    kart_command.extend(options)

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
    kart_clone_options = [
        "-C",
        repository_name_from_url(repository),
        repository,
        "--no-checkout",
    ]
    return get_kart_command("clone", kart_clone_options)


def get_kart_fetch_command(repository: str, sha: str) -> list[str]:
    """Build a `kart fetch` command.

    Args:
        repository: Address (URL) of the repository to fetch from
        sha: The commit SHA to fetch

    Returns:
        a list of arguments for `kart`
    """
    return get_kart_command(
        "fetch",
        [
            "-C",
            repository_name_from_url(repository),
            "origin",
            sha,
        ],
    )


def repository_name_from_url(repository_url: str) -> str:
    """Extract the repository name from its URL.
    Args:
        repository_url: The URL of the repository

    Returns:
        The name of the repository
    """
    return repository_url.split("/")[-1].replace(".git", "")


def time_in_ms() -> float:
    """

    Returns:
        the current time in ms
    """
    return time.time() * 1000


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
    start_time = time_in_ms()
    kart_env = os.environ.copy()
    kart_exec = os.environ.get("KART_EXECUTABLE", "kart")

    kart_clone_command = [
        kart_exec,
        *get_kart_clone_command(repository, kart_env.get("GITHUB_TOKEN")),
    ]
    kart_fetch_command = [kart_exec, *get_kart_fetch_command(repository, sha)]

    procs = []
    for kart_command in [kart_clone_command, kart_fetch_command]:
        try:
            get_log().debug("run_kart_start", command=" ".join(kart_command))
            proc = subprocess.run(
                kart_command,
                env=kart_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )
        except subprocess.CalledProcessError as cpe:
            get_log().error(
                "run_kart_failed",
                command=" ".join(kart_command),
                error=str(cpe.stderr, "utf-8"),
            )
            raise KartExecutionException(f"kart {str(cpe.stderr, 'utf-8')}") from cpe
        finally:
            get_log().info(
                "run_kart_end",
                command=" ".join(kart_command),
                duration=time_in_ms() - start_time,
            )

        if proc.stderr:
            get_log().warning(
                "run_kart_stderr",
                command=" ".join(kart_command),
                stderr=proc.stderr.decode(),
            )

        get_log().trace(
            "run_kart_succeeded",
            command=" ".join(kart_command),
            stdout=proc.stdout.decode(),
        )

        procs.append(proc)

    return tuple(procs)
