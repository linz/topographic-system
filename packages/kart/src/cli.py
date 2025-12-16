import argparse
import sys
from kart.kart_commands import run_kart_clone, run_kart_export


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

    export_parser = subparsers.add_parser(
        "export", help="Export layers from an already cloned repository"
    )
    export_parser.add_argument(
        "--repository_path",
        dest="repository_path",
        required=True,
        help="Local path of the repository to export from",
    )
    export_parser.add_argument(
        "--sha",
        dest="sha",
        help="The commit SHA to export. If not provided, the default branch is used.",
        required=False,
        default=None,
    )
    export_parser.add_argument(
        "--layers",
        dest="layers",
        nargs="*",
        help="Layer(s) to export. If not provided, all layers are exported.",
        required=False,
        default=None,
    )
    export_parser.add_argument(
        "--num-procs",
        dest="num_procs",
        type=int,
        help="Number of parallel export processes (defaults to number of CPUs)",
        required=False,
        default=None,
    )

    args = parser.parse_args()

    if args.command == "clone":
        run_kart_clone(repository=args.repository, sha=args.sha)
    elif args.command == "export":
        run_kart_export(
            repository_path=args.repository_path,
            sha=args.sha,
            layers=args.layers,
            num_procs=args.num_procs,
        )
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
