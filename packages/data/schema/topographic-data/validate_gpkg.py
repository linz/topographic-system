"""
Validate all GeoPackage layers against their corresponding Pydantic models.

Reads each layer from the GeoPackage using geopandas (pyogrio engine), maps the
layer name to the matching model class in pydantic_models_classes.py, and runs
model_validate on every row.  Layers with no matching model are skipped with a
warning.

Usage:
    python validate_gpkg.py [path/to/topographic-data.gpkg]

If no path is supplied the default path is used.
"""

from __future__ import annotations

import csv
import importlib.util
import sys
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd
import pyogrio
from pydantic import BaseModel, ValidationError

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DEFAULT_GPKG = Path("C:/Data/toposource/topographic-data/topographic-data.gpkg")
MODELS_FILE = Path(__file__).resolve().parent / "pydantic_models_classes.py"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _snake_to_pascal(name: str) -> str:
    """Convert snake_case layer name to PascalCase model class name."""
    return "".join(part.capitalize() for part in name.split("_"))


def _load_model_classes(models_file: Path) -> dict[str, type[BaseModel]]:
    """Dynamically load all BaseModel subclasses from pydantic_models_classes.py."""
    spec = importlib.util.spec_from_file_location("pydantic_models_classes", models_file)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module from {models_file}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]

    classes: dict[str, type[BaseModel]] = {}
    for attr_name, attr_value in module.__dict__.items():
        if (
            isinstance(attr_value, type)
            and issubclass(attr_value, BaseModel)
            and attr_value is not BaseModel
            and attr_name not in ("BaseModel", "BaseTopoModel")
        ):
            classes[attr_name] = attr_value
    return classes


def _geometry_to_dict(geom: Any) -> dict[str, Any] | None:
    """Convert a Shapely geometry to a GeoJSON-style dict via __geo_interface__."""
    if geom is None:
        return None
    try:
        return geom.__geo_interface__
    except Exception:
        return None


def _sanitise_value(value: Any) -> Any:
    """Convert pandas/numpy scalars to plain Python types Pydantic can handle."""
    if value is None:
        return None
    # pandas NA / numpy NaN / float NaN → None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    # pandas Timestamp → datetime
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    # numpy int/float → Python int/float
    if hasattr(value, "item"):
        return value.item()
    return value


def _row_to_dict(row: pd.Series) -> dict[str, Any]:
    """Convert a GeoDataFrame row to a plain dict suitable for model_validate."""
    data: dict[str, Any] = {}
    for col, value in row.items():
        if col == "geometry":
            data[col] = _geometry_to_dict(value)
        else:
            data[col] = _sanitise_value(value)
    return data


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_layer(
    gpkg_path: Path,
    layer_name: str,
    model_class: type[BaseModel],
    max_errors: int = 10,
) -> tuple[int, int, list[dict[str, Any]]]:
    """
    Validate all rows in a GeoPackage layer against model_class.

    Returns (valid_count, error_count, error_records).
    Each error record has keys: layer, feature_id, field, message.
    """
    gdf: gpd.GeoDataFrame = gpd.read_file(
        gpkg_path,
        layer=layer_name,
        engine="pyogrio",
    )

    valid_count = 0
    error_count = 0
    reported_errors = 0
    error_records: list[dict[str, Any]] = []

    for idx, row in gdf.iterrows():
        row_data = _row_to_dict(row)
        try:
            model_class.model_validate(row_data)
            valid_count += 1
        except ValidationError as exc:
            error_count += 1
            feature_id = row_data.get("id", idx)
            for err in exc.errors():
                loc = " -> ".join(str(l) for l in err["loc"])
                error_records.append({
                    "layer": layer_name,
                    "feature_id": feature_id,
                    "field": loc,
                    "message": err["msg"],
                    "input_value": repr(err.get("input")),
                })
            if reported_errors < max_errors:
                print(f"    [ERROR] row {feature_id}: {exc.error_count()} error(s)")
                for err in exc.errors():
                    loc = " -> ".join(str(l) for l in err["loc"])
                    print(f"      . {loc}: {err['msg']} (got {err.get('input')!r})")
                reported_errors += 1
            elif reported_errors == max_errors:
                print(f"    [ERROR] ... (further errors suppressed, showing first {max_errors})")
                reported_errors += 1

    return valid_count, error_count, error_records


def validate_gpkg(gpkg_path: Path) -> None:
    """Validate all layers in the GeoPackage and write a CSV report alongside it."""
    report_path = gpkg_path.with_name(gpkg_path.stem + "_validation_report.csv")

    print(f"GeoPackage : {gpkg_path}")
    print(f"Models file: {MODELS_FILE}")
    print(f"Report     : {report_path}")
    print()

    if not gpkg_path.exists():
        print(f"ERROR: GeoPackage not found: {gpkg_path}")
        sys.exit(1)

    if not MODELS_FILE.exists():
        print(f"ERROR: Models file not found: {MODELS_FILE}")
        sys.exit(1)

    model_classes = _load_model_classes(MODELS_FILE)
    layers = pyogrio.list_layers(gpkg_path)

    total_valid = 0
    total_errors = 0
    skipped: list[str] = []
    validated: list[str] = []
    all_errors: list[dict[str, Any]] = []

    for layer_name, geom_type in layers:
        class_name = _snake_to_pascal(layer_name)
        model_class = model_classes.get(class_name)

        if model_class is None:
            warnings.warn(
                f"No model found for layer '{layer_name}' "
                f"(looked for '{class_name}') - skipping.",
                stacklevel=1,
            )
            skipped.append(layer_name)
            continue

        print(f">> {layer_name}  ->  {class_name}  [{geom_type}]")
        valid, errors, error_records = validate_layer(gpkg_path, layer_name, model_class)
        total_valid += valid
        total_errors += errors
        all_errors.extend(error_records)
        status = "OK" if errors == 0 else "FAIL"
        print(f"  {status} valid={valid}  errors={errors}\n")
        validated.append(layer_name)

    # Write CSV report
    run_at = datetime.now().isoformat(timespec="seconds")
    with report_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["run_at", "gpkg", "layer", "feature_id", "field", "message", "input_value"])
        for rec in all_errors:
            writer.writerow([
                run_at,
                gpkg_path.name,
                rec["layer"],
                rec["feature_id"],
                rec["field"],
                rec["message"],
                rec["input_value"],
            ])

    # Summary
    print("=" * 60)
    print(f"Layers validated : {len(validated)}")
    print(f"Layers skipped   : {len(skipped)}")
    if skipped:
        print(f"  Skipped: {', '.join(skipped)}")
    print(f"Total rows valid : {total_valid}")
    print(f"Total row errors : {total_errors}")
    print(f"Report written   : {report_path}  ({len(all_errors)} error rows)")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    gpkg_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_GPKG
    validate_gpkg(gpkg_path)
