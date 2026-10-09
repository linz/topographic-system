import argparse
import logging
import math
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyogrio

lds_export_folder = r"C:\Data\Topo50\kart-source\Release66_NZ50"
source_export_folder = r"C:\Temp\export\lds_model"
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
        master_folder,
        source_folder,
        *,
        layer_info=layer_info_file,
        skip_source_id_missing=False,
        first_value_mismatch_only=False,
        sample_fraction=SAMPLE_FRACTION,
        all_rows_threshold=ALL_ROWS_THRESHOLD,
        random_seed=RANDOM_SEED,
    ):
        self.master_folder = Path(master_folder)
        self.source_folder = Path(source_folder)
        self.layer_info = Path(layer_info)
        self.log_file = self.source_folder / "match_exports.log"
        self.skip_source_id_missing = skip_source_id_missing
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
    def values_match(master_value, source_value):
        master_is_null = bool(pd.isna(master_value))
        source_is_null = bool(pd.isna(source_value))
        if master_is_null or source_is_null:
            return master_is_null and source_is_null
        return master_value == source_value

    @staticmethod
    def mismatch_signature(field, master_value, source_value):
        return (
            field,
            type(master_value).__name__,
            repr(master_value),
            type(source_value).__name__,
            repr(source_value),
        )

    def create_sample(self, master):
        if len(master) < self.all_rows_threshold:
            return master.copy()
        sample_size = math.ceil(len(master) * self.sample_fraction)
        return master.sample(n=sample_size, random_state=self.random_seed)

    def discover_master_layers(self):
        layer_info = pd.read_csv(self.layer_info)
        target_by_kart_layer = {
            str(row.kart_layer_name).lower(): str(row.shp_name).lower()
            for row in layer_info.itertuples(index=False)
            if pd.notna(row.kart_layer_name)
        }
        target_by_kart_layer.update(KART_LAYER_ALIASES)

        master_layers = {}
        discovery_mismatches = 0
        for gpkg_path in sorted(self.master_folder.rglob("*.gpkg")):
            layers = pyogrio.list_layers(gpkg_path)
            if len(layers) != 1:
                discovery_mismatches += 1
                self.logger.error(
                    "MASTER_GPKG_LAYER_COUNT path=%s layer_count=%s",
                    gpkg_path,
                    len(layers),
                )
                continue

            kart_layer = str(layers[0][0])
            target_layer = target_by_kart_layer.get(kart_layer.lower())
            if target_layer is None:
                discovery_mismatches += 1
                self.logger.error(
                    "MASTER_LAYER_UNMAPPED path=%s kart_layer=%s",
                    gpkg_path,
                    kart_layer,
                )
                continue
            if target_layer in master_layers:
                existing_path, _existing_layer = master_layers[target_layer]
                self.logger.warning(
                    "MASTER_LAYER_DUPLICATE target_layer=%s using=%s ignored=%s",
                    target_layer,
                    existing_path,
                    gpkg_path,
                )
                continue
            master_layers[target_layer] = (gpkg_path, kart_layer)

        return master_layers, discovery_mismatches

    def compare_layer(self, layer_name, master_source, source_path):
        master_path, master_layer = master_source
        master = pyogrio.read_dataframe(
            master_path, layer=master_layer, read_geometry=False
        )
        source = pyogrio.read_dataframe(source_path, read_geometry=False)

        if ID_FIELD not in master.columns or ID_FIELD not in source.columns:
            self.logger.error(
                "ID_FIELD_MISSING layer=%s master_has_id=%s source_has_id=%s",
                layer_name,
                ID_FIELD in master.columns,
                ID_FIELD in source.columns,
            )
            return 0, 1

        master_fields = set(master.columns)
        source_fields = set(source.columns)
        for field in sorted(master_fields - source_fields):
            self.logger.error(
                "FIELD_MISSING layer=%s source_field=%s", layer_name, field
            )
        for field in sorted(source_fields - master_fields):
            self.logger.error(
                "FIELD_EXTRA layer=%s source_field=%s", layer_name, field
            )

        sample = self.create_sample(master)
        sampled_row_count = len(sample)

        null_master_ids = sample[ID_FIELD].isna()
        for row_index in sample.index[null_master_ids]:
            self.logger.error("MASTER_ID_NULL layer=%s row=%s", layer_name, row_index)
        sample = sample[~null_master_ids]

        duplicate_master_ids = set(
            sample.loc[sample[ID_FIELD].duplicated(keep=False), ID_FIELD]
        )
        duplicate_source_ids = set(
            source.loc[source[ID_FIELD].duplicated(keep=False), ID_FIELD]
        )
        for feature_id in sorted(duplicate_master_ids):
            self.logger.error(
                "MASTER_ID_DUPLICATE layer=%s t50_fid=%s", layer_name, feature_id
            )
        duplicate_source_sample_ids = duplicate_source_ids & set(sample[ID_FIELD])
        for feature_id in sorted(duplicate_source_sample_ids):
            self.logger.error(
                "SOURCE_ID_DUPLICATE layer=%s t50_fid=%s", layer_name, feature_id
            )

        ambiguous_ids = duplicate_master_ids | duplicate_source_sample_ids
        sample = sample[~sample[ID_FIELD].isin(ambiguous_ids)]
        source_sample = source[source[ID_FIELD].isin(sample[ID_FIELD])]
        source_by_id = source_sample.set_index(ID_FIELD)

        source_ids = set(source_by_id.index)
        missing_ids = set(sample[ID_FIELD]) - source_ids
        if not self.skip_source_id_missing:
            for feature_id in sorted(missing_ids):
                self.logger.error(
                    "SOURCE_ID_MISSING layer=%s t50_fid=%s", layer_name, feature_id
                )

        compare_fields = sorted((master_fields & source_fields) - {ID_FIELD})
        mismatch_count = (
            len(master_fields ^ source_fields)
            + len(ambiguous_ids)
            + len(missing_ids)
        )
        compared_rows = 0
        logged_signatures = set()
        suppressed_value_mismatches = 0
        for _, master_row in sample.iterrows():
            feature_id = master_row[ID_FIELD]
            if feature_id not in source_ids:
                continue
            compared_rows += 1
            source_row = source_by_id.loc[feature_id]
            for field in compare_fields:
                master_value = master_row[field]
                source_value = source_row[field]
                if self.values_match(master_value, source_value):
                    continue

                mismatch_count += 1
                signature = self.mismatch_signature(
                    field, master_value, source_value
                )
                if self.first_value_mismatch_only and signature in logged_signatures:
                    suppressed_value_mismatches += 1
                    continue
                logged_signatures.add(signature)
                self.logger.error(
                    "VALUE_MISMATCH layer=%s t50_fid=%s field=%s master=%r source=%r",
                    layer_name,
                    feature_id,
                    field,
                    master_value,
                    source_value,
                )

        self.logger.info(
            "LAYER_COMPLETE layer=%s master_rows=%s sampled_rows=%s "
            "compared_rows=%s mismatches=%s suppressed_value_mismatches=%s",
            layer_name,
            len(master),
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
            "COMPARISON_STARTED start=%s master_folder=%s source_folder=%s "
            "skip_source_id_missing=%s first_value_mismatch_only=%s",
            start_time.isoformat(),
            self.master_folder,
            self.source_folder,
            self.skip_source_id_missing,
            self.first_value_mismatch_only,
        )

        master_layers, discovery_mismatches = self.discover_master_layers()
        source_layers = {
            path.stem.lower(): path for path in self.source_folder.glob("*.shp")
        }

        missing_layers = sorted(set(master_layers) - set(source_layers))
        extra_layers = sorted(set(source_layers) - set(master_layers))
        for layer_name in missing_layers:
            self.logger.error("LAYER_MISSING source_layer=%s", layer_name)
        for layer_name in extra_layers:
            self.logger.error("LAYER_EXTRA source_layer=%s", layer_name)

        compared_rows = 0
        mismatch_count = (
            discovery_mismatches + len(missing_layers) + len(extra_layers)
        )
        compared_layers = 0
        try:
            for layer_name in sorted(set(master_layers) & set(source_layers)):
                try:
                    layer_rows, layer_mismatches = self.compare_layer(
                        layer_name,
                        master_layers[layer_name],
                        source_layers[layer_name],
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
    parser.add_argument("--master-folder", type=Path, default=lds_export_folder)
    parser.add_argument("--source-folder", type=Path, default=source_export_folder)
    parser.add_argument("--layer-info", type=Path, default=layer_info_file)
    parser.add_argument("--skip-source-id-missing", action="store_true")
    parser.add_argument(
        "--first-value-mismatch-only",
        action="store_true",
        help=(
            "Log only the first mismatch per layer/field/master-value/source-value "
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
        args.master_folder,
        args.source_folder,
        layer_info=args.layer_info,
        skip_source_id_missing=args.skip_source_id_missing,
        first_value_mismatch_only=args.first_value_mismatch_only,
        sample_fraction=args.sample_fraction,
        all_rows_threshold=args.all_rows_threshold,
        random_seed=args.random_seed,
    )
    comparator.compare()
