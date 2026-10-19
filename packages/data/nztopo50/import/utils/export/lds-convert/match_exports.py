import argparse
import logging
import math
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyogrio

lds_direct_export_folder = r"C:\Data\Topo50\kart-source\Latest"
new_lds_shapefiles_folder = r"C:\Temp\export\lds_model"
layer_info_file = Path(__file__).resolve().parents[3] / "core" / "layers_info.csv"

ID_FIELD = "t50_fid"
SAMPLE_FRACTION = 0.10
ALL_ROWS_THRESHOLD = 101
RANDOM_SEED = 42
KART_LAYER_ALIASES = {
    "nz_snow_polygons_topo_150k": "snow_poly",
}


class ShapefileExportComparator:
    def __init__(
        self,
        lds_direct_export_folder,
        new_lds_shapefiles_folder,
        *,
        layer_info=layer_info_file,
        skip_shp_file_id_missing=False,
        first_value_mismatch_only=False,
        sample_fraction=SAMPLE_FRACTION,
        all_rows_threshold=ALL_ROWS_THRESHOLD,
        random_seed=RANDOM_SEED,
    ):
        self.lds_direct_export_folder = Path(lds_direct_export_folder)
        self.new_lds_shapefiles_folder = Path(new_lds_shapefiles_folder)
        self.layer_info = Path(layer_info)
        self.log_file = self.new_lds_shapefiles_folder / "match_exports.log"
        self.skip_shp_file_id_missing = skip_shp_file_id_missing
        self.first_value_mismatch_only = first_value_mismatch_only
        self.sample_fraction = sample_fraction
        self.all_rows_threshold = all_rows_threshold
        self.random_seed = random_seed
        self.logger = self.create_logger()

    def create_logger(self):
        logger = logging.getLogger(f"match_exports.{id(self)}")
        logger.setLevel(logging.INFO)
        logger.propagate = False
        handler = logging.FileHandler(self.log_file, mode="w", encoding="utf-8")
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        )
        logger.addHandler(handler)
        return logger

    def close_logger(self):
        for handler in self.logger.handlers[:]:
            handler.flush()
            handler.close()
            self.logger.removeHandler(handler)

    @staticmethod
    def values_match(field, lds_direct_export_value, shp_file_value):
        lds_direct_export_is_null = bool(pd.isna(lds_direct_export_value))
        shp_file_is_null = bool(pd.isna(shp_file_value))
        if field == "rna_sufi" and (
            lds_direct_export_is_null != shp_file_is_null
        ):
            non_null_value = (
                shp_file_value
                if lds_direct_export_is_null
                else lds_direct_export_value
            )
            return non_null_value == 0
        if lds_direct_export_is_null or shp_file_is_null:
            return lds_direct_export_is_null and shp_file_is_null
        return lds_direct_export_value == shp_file_value

    @staticmethod
    def mismatch_signature(field, lds_direct_export_value, shp_file_value):
        return (
            field,
            type(lds_direct_export_value).__name__,
            repr(lds_direct_export_value),
            type(shp_file_value).__name__,
            repr(shp_file_value),
        )

    def create_sample(self, lds_direct_export):
        if len(lds_direct_export) < self.all_rows_threshold:
            return lds_direct_export.copy()
        sample_size = math.ceil(len(lds_direct_export) * self.sample_fraction)
        return lds_direct_export.sample(n=sample_size, random_state=self.random_seed)

    def discover_lds_direct_export_layers(self):
        layer_info = pd.read_csv(self.layer_info)
        target_by_kart_layer = {
            str(row.kart_layer_name).lower(): str(row.shp_name).lower()
            for row in layer_info.itertuples(index=False)
            if pd.notna(row.kart_layer_name)
        }
        target_by_kart_layer.update(KART_LAYER_ALIASES)

        lds_direct_export_layers = {}
        discovery_mismatches = 0
        for gpkg_path in sorted(self.lds_direct_export_folder.rglob("*.gpkg")):
            layers = pyogrio.list_layers(gpkg_path)
            if len(layers) != 1:
                discovery_mismatches += 1
                self.logger.error(
                    "LDS_DIRECT_EXPORT_GPKG_LAYER_COUNT path=%s layer_count=%s",
                    gpkg_path,
                    len(layers),
                )
                continue

            kart_layer = str(layers[0][0])
            target_layer = target_by_kart_layer.get(kart_layer.lower())
            if target_layer is None:
                discovery_mismatches += 1
                self.logger.error(
                    "LDS_DIRECT_EXPORT_LAYER_UNMAPPED path=%s kart_layer=%s",
                    gpkg_path,
                    kart_layer,
                )
                continue
            if target_layer in lds_direct_export_layers:
                existing_path, _existing_layer = lds_direct_export_layers[target_layer]
                self.logger.warning(
                    "LDS_DIRECT_EXPORT_LAYER_DUPLICATE target_layer=%s using=%s ignored=%s",
                    target_layer,
                    existing_path,
                    gpkg_path,
                )
                continue
            lds_direct_export_layers[target_layer] = (gpkg_path, kart_layer)

        return lds_direct_export_layers, discovery_mismatches

    def compare_layer(self, layer_name, lds_direct_export_shp_file, shp_file_path):
        lds_direct_export_path, lds_direct_export_layer = lds_direct_export_shp_file
        lds_direct_export_info = pyogrio.read_info(
            lds_direct_export_path,
            layer=lds_direct_export_layer,
        )
        id_is_native_fid = (
            lds_direct_export_info.get("fid_column") == ID_FIELD
        )
        lds_direct_export = pyogrio.read_dataframe(
            lds_direct_export_path,
            layer=lds_direct_export_layer,
            read_geometry=False,
            fid_as_index=id_is_native_fid,
        )
        if id_is_native_fid:
            lds_direct_export.index.name = ID_FIELD
            lds_direct_export = lds_direct_export.reset_index()

        shp_file = pyogrio.read_dataframe(
            shp_file_path,
            read_geometry=False,
            fid_as_index=False,
        )

        if ID_FIELD not in lds_direct_export.columns or ID_FIELD not in shp_file.columns:
            self.logger.error(
                "ID_FIELD_MISSING layer=%s lds_direct_export_has_id=%s "
                "shp_file_has_id=%s",
                layer_name,
                ID_FIELD in lds_direct_export.columns,
                ID_FIELD in shp_file.columns,
            )
            self.logger.info(
                "LAYER_SKIPPED layer=%s reason=ID_FIELD_MISSING required_field=%s",
                layer_name,
                ID_FIELD,
            )
            return 0, 1

        lds_direct_export_fields = set(lds_direct_export.columns)
        shp_file_fields = set(shp_file.columns)
        for field in sorted(lds_direct_export_fields - shp_file_fields):
            self.logger.error(
                "FIELD_MISSING layer=%s shp_file_field=%s", layer_name, field
            )
        for field in sorted(shp_file_fields - lds_direct_export_fields):
            self.logger.error(
                "FIELD_EXTRA layer=%s shp_file_field=%s", layer_name, field
            )

        sample = self.create_sample(lds_direct_export)
        sampled_row_count = len(sample)

        null_lds_direct_export_ids = sample[ID_FIELD].isna()
        for row_index in sample.index[null_lds_direct_export_ids]:
            self.logger.error(
                "LDS_DIRECT_EXPORT_ID_NULL layer=%s row=%s", layer_name, row_index
            )
        sample = sample[~null_lds_direct_export_ids]

        duplicate_lds_direct_export_ids = set(
            sample.loc[
                sample[ID_FIELD].duplicated(keep=False),
                ID_FIELD,
            ]
        )
        duplicate_shp_file_ids = set(
            shp_file.loc[shp_file[ID_FIELD].duplicated(keep=False), ID_FIELD]
        )
        for feature_id in sorted(duplicate_lds_direct_export_ids):
            self.logger.error(
                "LDS_DIRECT_EXPORT_ID_DUPLICATE layer=%s t50_fid=%s",
                layer_name,
                feature_id,
            )
        duplicate_shp_file_sample_ids = duplicate_shp_file_ids & set(sample[ID_FIELD])
        for feature_id in sorted(duplicate_shp_file_sample_ids):
            self.logger.error(
                "SHP_FILE_ID_DUPLICATE layer=%s t50_fid=%s", layer_name, feature_id
            )

        ambiguous_ids = duplicate_lds_direct_export_ids | duplicate_shp_file_sample_ids
        sample = sample[~sample[ID_FIELD].isin(ambiguous_ids)]
        shp_file_sample = shp_file[shp_file[ID_FIELD].isin(sample[ID_FIELD])]
        shp_file_by_id = shp_file_sample.set_index(ID_FIELD)

        shp_file_ids = set(shp_file_by_id.index)
        missing_ids = set(sample[ID_FIELD]) - shp_file_ids
        if not self.skip_shp_file_id_missing:
            for feature_id in sorted(missing_ids):
                self.logger.error(
                    "SHP_FILE_ID_MISSING layer=%s t50_fid=%s", layer_name, feature_id
                )

        compare_fields = sorted((lds_direct_export_fields & shp_file_fields) - {ID_FIELD})
        mismatch_count = (
            len(lds_direct_export_fields ^ shp_file_fields)
            + len(ambiguous_ids)
            + len(missing_ids)
        )
        compared_rows = 0
        logged_signatures = set()
        suppressed_value_mismatches = 0
        for _, lds_direct_export_row in sample.iterrows():
            feature_id = lds_direct_export_row[ID_FIELD]
            if feature_id not in shp_file_ids:
                continue
            compared_rows += 1
            shp_file_row = shp_file_by_id.loc[feature_id]
            for field in compare_fields:
                lds_direct_export_value = lds_direct_export_row[field]
                shp_file_value = shp_file_row[field]
                if self.values_match(field, lds_direct_export_value, shp_file_value):
                    continue

                mismatch_count += 1
                signature = self.mismatch_signature(
                    field, lds_direct_export_value, shp_file_value
                )
                if self.first_value_mismatch_only and signature in logged_signatures:
                    suppressed_value_mismatches += 1
                    continue
                logged_signatures.add(signature)
                self.logger.error(
                    "VALUE_MISMATCH layer=%s t50_fid=%s field=%s "
                    "lds_direct_export=%r shp_file=%r",
                    layer_name,
                    feature_id,
                    field,
                    lds_direct_export_value,
                    shp_file_value,
                )

        self.logger.info(
            "LAYER_COMPLETE layer=%s lds_direct_export_rows=%s sampled_rows=%s "
            "compared_rows=%s mismatches=%s suppressed_value_mismatches=%s",
            layer_name,
            len(lds_direct_export),
            sampled_row_count,
            compared_rows,
            mismatch_count,
            suppressed_value_mismatches,
        )
        return compared_rows, mismatch_count

    def compare(self):
        start_time = datetime.now().astimezone()
        start_counter = time.perf_counter()
        self.logger.info(
            "COMPARISON_STARTED start=%s lds_direct_export_folder=%s "
            "new_lds_shapefiles_folder=%s "
            "skip_shp_file_id_missing=%s first_value_mismatch_only=%s",
            start_time.isoformat(),
            self.lds_direct_export_folder,
            self.new_lds_shapefiles_folder,
            self.skip_shp_file_id_missing,
            self.first_value_mismatch_only,
        )

        lds_direct_export_layers, discovery_mismatches = (
            self.discover_lds_direct_export_layers()
        )
        shp_file_layers = {
            path.stem.lower(): path
            for path in self.new_lds_shapefiles_folder.glob("*.shp")
        }

        missing_layers = sorted(
            set(lds_direct_export_layers) - set(shp_file_layers)
        )
        extra_layers = sorted(set(shp_file_layers) - set(lds_direct_export_layers))
        for layer_name in missing_layers:
            self.logger.error("LAYER_MISSING shp_file_layer=%s", layer_name)
        for layer_name in extra_layers:
            self.logger.error("LAYER_EXTRA shp_file_layer=%s", layer_name)

        compared_rows = 0
        mismatch_count = (
            discovery_mismatches + len(missing_layers) + len(extra_layers)
        )
        compared_layers = 0
        try:
            for layer_name in sorted(
                set(lds_direct_export_layers) & set(shp_file_layers)
            ):
                try:
                    layer_rows, layer_mismatches = self.compare_layer(
                        layer_name,
                        lds_direct_export_layers[layer_name],
                        shp_file_layers[layer_name],
                    )
                except Exception:
                    mismatch_count += 1
                    self.logger.exception("LAYER_ERROR layer=%s", layer_name)
                    continue
                compared_layers += 1
                compared_rows += layer_rows
                mismatch_count += layer_mismatches
        finally:
            end_time = datetime.now().astimezone()
            elapsed_seconds = time.perf_counter() - start_counter
            self.logger.info(
                "COMPARISON_COMPLETE start=%s end=%s elapsed_seconds=%.3f "
                "compared_layers=%s compared_rows=%s mismatches=%s",
                start_time.isoformat(),
                end_time.isoformat(),
                elapsed_seconds,
                compared_layers,
                compared_rows,
                mismatch_count,
            )
            self.close_logger()

        print(f"Start: {start_time.isoformat()}")
        print(f"End: {end_time.isoformat()}")
        print(f"Elapsed: {elapsed_seconds:.3f} seconds")
        print(f"Compared {compared_layers} layers and {compared_rows} sampled rows")
        print(f"Found {mismatch_count} mismatches")
        print(f"Log: {self.log_file}")
        return mismatch_count


def parse_args():
    parser = argparse.ArgumentParser(description="Compare sampled LDS shapefiles")
    parser.add_argument(
        "--lds-direct-export-folder",
        type=Path,
        default=lds_direct_export_folder,
    )
    parser.add_argument(
        "--new-lds-shapefiles-folder",
        type=Path,
        default=new_lds_shapefiles_folder,
    )
    parser.add_argument("--layer-info", type=Path, default=layer_info_file)
    parser.add_argument("--skip-shp-file-id-missing", action="store_true")
    parser.add_argument(
        "--first-value-mismatch-only",
        action="store_true",
        help=(
            "Log only the first mismatch per layer/field/"
            "lds-direct-export-value/shp-file-value "
            "combination"
        ),
    )
    parser.add_argument("--sample-fraction", type=float, default=SAMPLE_FRACTION)
    parser.add_argument("--all-rows-threshold", type=int, default=ALL_ROWS_THRESHOLD)
    parser.add_argument("--random-seed", type=int, default=RANDOM_SEED)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    comparator = ShapefileExportComparator(
        args.lds_direct_export_folder,
        args.new_lds_shapefiles_folder,
        layer_info=args.layer_info,
        skip_shp_file_id_missing=args.skip_shp_file_id_missing,
        first_value_mismatch_only=args.first_value_mismatch_only,
        sample_fraction=args.sample_fraction,
        all_rows_threshold=args.all_rows_threshold,
        random_seed=args.random_seed,
    )
    
    print("Starting comparison...")
    comparator.compare()
