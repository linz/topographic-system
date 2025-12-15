import argparse
import sys
from kart_clone import clone_repository


def main() -> None:
    parser = argparse.ArgumentParser(description="Kart Docker Entrypoint")
    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")

    clone_parser = subparsers.add_parser("clone", help="Clone a repository")
    clone_parser.add_argument(
        "--repository",
        dest="repository",
        required=True,
        help="Full URL of the repository to clone",
    )
    clone_parser.add_argument(
        "--sha",
        dest="sha",
        help="The commit SHA to checkout after cloning. If not provided, the default branch is used.",
        required=False,
        default="master",
    )

    args = parser.parse_args()

    if args.command == "clone":
        clone_repository(args.repository, args.sha)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
