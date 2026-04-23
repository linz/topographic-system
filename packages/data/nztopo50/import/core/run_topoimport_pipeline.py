import argparse
import os

from load_shp_to_themes import run_load_shp_to_themes
from postgis_create_model import run_model_creation
from postgis_manage_fields import run_postgis_manage_fields


def resolve_default_paths(path_profile, release):
    if path_profile == "linux":
        return {
            "data_folder": f"/data/Topo50/{release}_NZ50_Shape",
            "count_log": "/data/Model/count_log.txt",
        }

    # Windows defaults
    return {
        "data_folder": rf"C:\Data\Topo50\{release}_NZ50_Shape",
        "count_log": r"C:\Data\Model\count_log.txt",
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run Topo50 import workflow stages in order or individually."
    )
    parser.add_argument(
        "--stage",
        choices=["create_model", "load_shp", "manage_fields", "all"],
        default="all",
        help="Workflow stage to run. Default: all",
    )
    parser.add_argument(
        "--path-profile",
        choices=["windows", "linux"],
        default="windows",
        help="Default path profile to use. Default: windows",
    )
    parser.add_argument(
        "--release",
        default="release64",
        help="Release tag used in schema and default data folder.",
    )
    parser.add_argument(
        "--schema",
        default=None,
        help="Schema name override. Defaults to --release.",
    )

    # Optional path overrides
    parser.add_argument("--data-folder", default=None, help="Shapefile source directory")
    parser.add_argument("--count-log", default=None, help="Count log output file")
    parser.add_argument(
        "--layer-info-file",
        default=None,
        help="layers_info.csv path override",
    )
    parser.add_argument(
        "--model-fields-file",
        default=None,
        help="datasets_fields.csv path override",
    )

    # create_model options
    parser.add_argument(
        "--create-commands",
        default="drop_tables,create_tables",
        help="Comma-separated commands: drop_tables,create_tables,create_indexes",
    )
    parser.add_argument(
        "--create-primary-key-type",
        choices=["none", "int", "uuid"],
        default="none",
        help="Primary key mode for create_model stage.",
    )

    # manage_fields options
    parser.add_argument(
        "--manage-option",
        default="all",
        help="manage_fields workflow option (all or step name)",
    )
    parser.add_argument(
        "--manage-primary-key-type",
        choices=["int", "uuid"],
        default="uuid",
        help="Primary key mode for manage_fields stage.",
    )
    parser.add_argument(
        "--release-date",
        default="2025-09-25",
        help="Release date passed to manage_fields workflow.",
    )
    parser.add_argument(
        "--no-full-metadata-fields",
        action="store_true",
        help="Disable full metadata field set in manage_fields stage.",
    )

    # DB options
    parser.add_argument("--db-name", default="topo")
    parser.add_argument("--db-user", default="postgres")
    parser.add_argument("--db-password", default="landinformation")
    parser.add_argument("--db-host", default="localhost")
    parser.add_argument("--db-port", type=int, default=5432)

    return parser.parse_args()


def run_pipeline(args):
    schema_name = args.schema or args.release
    defaults = resolve_default_paths(args.path_profile, args.release)

    data_folder = args.data_folder or defaults["data_folder"]
    count_log = args.count_log or defaults["count_log"]

    core_dir = os.path.dirname(__file__)
    model_fields_file = args.model_fields_file or os.path.join(core_dir, "datasets_fields.csv")
    layer_info_file = args.layer_info_file or os.path.join(core_dir, "layers_info.csv")

    db_params = {
        "dbname": args.db_name,
        "user": args.db_user,
        "password": args.db_password,
        "host": args.db_host,
        "port": args.db_port,
    }

    create_commands = [
        command.strip()
        for command in args.create_commands.split(",")
        if command.strip()
    ]

    stages = [args.stage] if args.stage != "all" else [
        "create_model",
        "load_shp",
        "manage_fields",
    ]

    for stage in stages:
        print(f"\n=== Running stage: {stage} ===")
        if stage == "create_model":
            run_model_creation(
                schema_name=schema_name,
                commands_options=create_commands,
                model_fields_file=model_fields_file,
                primary_key_type=args.create_primary_key_type,
                db_params_override=db_params,
            )
        elif stage == "load_shp":
            run_load_shp_to_themes(
                release=args.release,
                data_folder=data_folder,
                layer_info_file=layer_info_file,
                count_log=count_log,
                database=schema_name,
            )
        elif stage == "manage_fields":
            run_postgis_manage_fields(
                schema_name=schema_name,
                option=args.manage_option,
                add_full_metadata_fields=not args.no_full_metadata_fields,
                primary_key_type=args.manage_primary_key_type,
                release_date=args.release_date,
                db_params=db_params,
            )


if __name__ == "__main__":
    run_pipeline(parse_args())
