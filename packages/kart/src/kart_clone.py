import argparse

from linz_logger import get_log

from kart.kart_commands import run_kart_clone, time_in_ms


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--repository",
        dest="repository",
        required=True,
        help="Full URL of the repository to clone",
    )
    parser.add_argument(
        "--sha",
        dest="sha",
        help="The commit SHA to checkout after cloning. If not provided, the default branch is used.",
        required=False,
        default="master",
    )
    return parser.parse_args()


def clone_repository(repository: str, sha: str) -> None:
    start_time = time_in_ms()

    get_log().info("starting clone", repository=repository, sha=sha)
    process_results = run_kart_clone(repository, sha)
    if process_results[0].returncode != 0:
        get_log().error(
            "clone_failed",
            repository=repository,
            sha=sha,
            stderr=process_results[0].stderr.decode("utf-8"),
        )
    elif process_results[1].returncode != 0:
        get_log().error(
            "fetch_failed",
            repository=repository,
            sha=sha,
            stderr=process_results[1].stderr.decode("utf-8"),
        )
    get_log().info(
        "clone completed",
        repository=repository,
        sha=sha,
        duration=time_in_ms() - start_time,
    )


def main() -> None:
    arguments = parse_args()
    clone_repository(arguments.repository, arguments.sha)


if __name__ == "__main__":
    main()
