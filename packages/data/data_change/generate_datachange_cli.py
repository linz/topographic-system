#!/usr/bin/env python3
"""CLI wrapper for running GeoParquet release comparison with explicit arguments."""

import argparse

from packages.data.data_change.generate_datachange import GeoParquetTableDiffComparer


def parse_cli_args() -> argparse.Namespace:
    """Parse command line arguments for release comparison and changelog output."""
    parser = argparse.ArgumentParser(
        description="Compare two GeoParquet releases and export added/removed/updated changelogs.",
    )
    parser.add_argument(
        "--current-release-name",
        default="release64",
        help="Current release name used in output paths/files (default: release64).",
    )
    parser.add_argument(
        "--previous-release-name",
        default="release62",
        help="Previous release name for metadata/reference (default: release62).",
    )
    parser.add_argument(
        "--release-date",
        default="2025-09-25",
        help="Release date in YYYY-MM-DD format added to changelog records.",
    )
    parser.add_argument(
        "--current-release-path",
        default=r"C:/temp/release64",
        help="Folder, S3 prefix, or HTTPS parquet file URL for current release.",
    )
    parser.add_argument(
        "--previous-release-path",
        default=r"C:/temp/release62",
        help="Folder, S3 prefix, or HTTPS parquet file URL for previous release.",
    )
    parser.add_argument(
        "--change-logs-path",
        default=r"c:/temp/data-changes",
        help="Destination root path for changelog parquet output.",
    )
    parser.add_argument(
        "--use-hive-partitioning",
        action="store_true",
        help="Write changelogs using hive partitions (year/month/day/change_type).",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entry point for running the release comparison."""
    args = parse_cli_args()
    comparator = GeoParquetTableDiffComparer(
        current_release_name=args.current_release_name,
        previous_release_name=args.previous_release_name,
        release_date=args.release_date,
        current_release_path=args.current_release_path,
        previous_release_path=args.previous_release_path,
        use_hive_partitioning=args.use_hive_partitioning,
        change_logs_path=args.change_logs_path,
    )
    comparator.run()


if __name__ == "__main__":
    main()
