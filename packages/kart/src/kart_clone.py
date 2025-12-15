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


def main() -> None:
    arguments = parse_args()
    start_time = time_in_ms()

    get_log().info("starting clone", repository=arguments.repository, sha=arguments.sha)
    process_results = run_kart_clone(arguments.repository, arguments.sha)
    if process_results[0].returncode != 0:
        get_log().error(
            "clone_failed",
            repository=arguments.repository,
            sha=arguments.sha,
            stderr=process_results[0].stderr.decode("utf-8"),
        )
    elif process_results[1].returncode != 0:
        get_log().error(
            "fetch_failed",
            repository=arguments.repository,
            sha=arguments.sha,
            stderr=process_results[1].stderr.decode("utf-8"),
        )
    get_log().info(
        "clone completed",
        repository=arguments.repository,
        sha=arguments.sha,
        duration=time_in_ms() - start_time,
    )


if __name__ == "__main__":
    main()
