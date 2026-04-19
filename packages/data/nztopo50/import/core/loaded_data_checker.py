import os
from typing import Any
import pandas as pd
from psycopg import sql
from db_common import DBTables

# Database connection parameters
db_params: dict[str, Any] = {
    "dbname": "topo",
    "user": "postgres",
    "password": "landinformation",
    "host": "localhost",
    "port": 5432,
}

topographic_data_layers: dict[str, list[str]] = {
    "airport": ["airport"],
    "structure_point": [
        "beacon",
        "bivouac",
        "buoy",
        "chimney",
        "dredge",
        "flare",
        "floodgate",
        "gate",
        "ladder",
        "mast",
        "redoubt",
        "shaft",
        "siphon",
        "stockyard",
        "tank",
        "tower",
        "well",
        "windmill",
        "wreck",
    ],
    "structure_line": [
        "boatramp",
        "boom",
        "breakwater",
        "cableway_industrial",
        "cableway_people",
        "dam",
        "flume",
        "ladder",
        "marine_farm",
        "ski_lift",
        "ski_tow",
        "slipway",
        "spillway_edge",
        "weir",
        "wharf",
        "wharf_edge",
    ],
    "bridge_line": ["bridge"],
    "building": ["building"],
    "building_point": ["building"],
    "water": ["canal", "lagoon", "lake", "pond", "river", "swamp"],
    "water_line": ["canal", "drain", "river"],
    "place_point": [
        "cave",
        "cemetery",
        "grave",
        "historic_site",
        "mine",
        "monument",
        "pa",
        "radar_dome",
        "saddle",
        "satellite_station",
    ],
    "landuse": [
        "cemetery",
        "golf_course",
        "gravel_pit",
        "landfill",
        "mine",
        "pumice_pit",
        "quarry",
        "racetrack",
        "rifle_range",
        "showground",
        "sportsfield",
    ],
    "relief_line": ["cliff_edge", "cutting_edge", "embankment", "slip_edge"],
    "coastline": ["coastline"],
    "descriptive_text": ["descriptive_text"],
    "landcover_line": ["dredge_tailing"],
    "structure": [
        "dry_dock",
        "fish_farm",
        "marine_farm",
        "reservoir",
        "siphon",
        "tank",
    ],
    "vegetation": [
        "exotic",
        "native",
        "orchard",
        "scattered_scrub",
        "scrub",
        "vineyard",
    ],
    "fence_line": ["fence"],
    "ferry_crossing": ["ferry_crossing"],
    "transport_point": ["ford", "helipad"],
    "landcover_point": ["fumarole", "rock_outcrop"],
    "physical_infrastructure_point": ["gas_valve", "geo_bore", "pylon"],
    "geographic_name": ["geographic_name"],
    "relief_point": ["height", "sinkhole"],
    "landcover": ["ice", "moraine", "moraine_wall", "mud", "sand", "scree", "shingle"],
    "island": ["island"],
    "marine": ["mangrove", "reef", "rock", "shoal"],
    "physical_infrastructure_line": [
        "pipeline",
        "powerline",
        "telephone",
        "walkwire",
        "water_race",
    ],
    "railway_station": ["rail_station"],
    "railway_line": ["railway"],
    "waterway_feature": ["rapid", "waterfall"],
    "waterway_feature_line": ["rapid", "waterfall", "waterfall_edge"],
    "residential_area": ["residential_area"],
    "road_line": ["road"],
    "water_point": ["rock", "soakhole", "spring", "swamp", "waterfall"],
    "runway": ["runway"],
    "vegetation_line": ["shelter_belt"],
    "track_line": ["track"],
    "tree_locations": ["tree"],
    "trig_point": ["trig"],
    "tunnel_line": ["tunnel"],
}

contour_layers: dict[str, list[str]] = {
    "contour": ["contour"],
}

product_layers: dict[str, list[str] | None] = {
    "nz_topo50_map_sheet": ["nz_topo50_map_sheet"],
    "nz_topo50_carto_text": None,
    "nz_topo50_dms_grid": None,
    "nz_topo50_grid": None,
}

process_schemas: dict[str, dict[str, list[str] | None]] = {
    "release64": {**topographic_data_layers, **contour_layers},
    "carto": product_layers,
}


def check_layer_has_records_and_features(
    db_tables: DBTables,
    schema: str,
    layer: str,
    expected_features: list[str] | None,
) -> dict[str, Any]:
    """Check row count and expected feature presence for one layer.

    A layer passes when it has more than one row and, if expected features are
    provided, at least one row exists with a matching ``feature_type`` value.
    """
    conn = db_tables.get_connection()

    with conn.cursor() as cur:
        count_query = sql.SQL("SELECT COUNT(*) FROM {}.{};").format(
            sql.Identifier(schema), sql.Identifier(layer)
        )
        cur.execute(count_query)
        row_count = cur.fetchone()[0]

    has_more_than_one_record = row_count > 1
    has_expected_feature = expected_features is None

    if expected_features is not None:
        if not db_tables.column_exists(schema, layer, "feature_type"):
            has_expected_feature = False
        else:
            with conn.cursor() as cur:
                feature_query = sql.SQL(
                    """
                    SELECT EXISTS(
                        SELECT 1
                        FROM {}.{}
                        WHERE feature_type = ANY(%s)
                    );
                    """
                ).format(sql.Identifier(schema), sql.Identifier(layer))
                cur.execute(feature_query, (expected_features,))
                has_expected_feature = bool(cur.fetchone()[0])

    return {
        "schema": schema,
        "layer": layer,
        "layer_exists": True,
        "row_count": row_count,
        "has_more_than_one_record": has_more_than_one_record,
        "has_expected_feature": has_expected_feature,
        "is_valid": has_more_than_one_record and has_expected_feature,
        "passed": has_more_than_one_record and has_expected_feature,
    }


def check_process_schemas(
    db_tables: DBTables, schema_layers: dict[str, dict[str, list[str] | None]]
) -> list[dict[str, Any]]:
    """Check layer existence and data validity for all configured schemas."""
    results: list[dict[str, Any]] = []

    for schema, layers in schema_layers.items():
        for layer, expected_features in layers.items():
            if not db_tables.table_exists(schema, layer):
                results.append(
                    {
                        "schema": schema,
                        "layer": layer,
                        "layer_exists": False,
                        "row_count": 0,
                        "has_more_than_one_record": False,
                        "has_expected_feature": False,
                        "is_valid": False,
                        "passed": False,
                    }
                )
                continue

            layer_result = check_layer_has_records_and_features(
                db_tables=db_tables,
                schema=schema,
                layer=layer,
                expected_features=expected_features,
            )
            results.append(layer_result)

    return results


def run_loaded_data_checks() -> pd.DataFrame:
    """Connect to DB and run all configured layer checks."""
    db_tables = DBTables(db_params)
    try:
        results = check_process_schemas(db_tables, process_schemas)
        return pd.DataFrame(results)
    finally:
        db_tables.close()


# ============================================================================
# TEST FUNCTIONS FOR DATA PROCESSING WORKFLOW SECTIONS
# ============================================================================


def test_coordinate_system_conversions(db_tables: DBTables) -> dict[str, Any]:
    """Test 1: Verify Coordinate System Conversions (EPSG:4167 for main data).

    Expected: Most layers should be in EPSG:4167 (NZGD2000)
    Exceptions: nz_topo50_map_sheet, carto_text, dms_grid in NZTM2000/WGS84
    """
    conn = db_tables.get_connection()
    results: dict[str, Any] = {
        "test_name": "Coordinate System Conversions",
        "details": [],
    }

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT f_table_schema, f_table_name, srid 
                FROM geometry_columns 
                WHERE f_table_schema IN ('release64', 'carto')
                ORDER BY f_table_schema, f_table_name
                """
            )
            rows = cur.fetchall()

            if not rows:
                results["status"] = "FAIL"
                results["message"] = "No geometry columns found"
                return results

            for schema, table, srid in rows:
                if table in ["nz_topo50_map_sheet", "carto_text", "nz_topo50_dms_grid"]:
                    expected_srid = [2193, 4326]  # NZTM2000 or WGS84
                else:
                    expected_srid = [4167]  # EPSG:4167 for main data

                passed = srid in expected_srid
                results["details"].append(
                    {  # type: ignore
                        "schema": schema,
                        "table": table,
                        "srid": srid,
                        "expected_srid": expected_srid,
                        "passed": passed,
                    }
                )

            all_passed = all(d["passed"] for d in results["details"])  # type: ignore
            results["status"] = "PASS" if all_passed else "FAIL"
            results["message"] = f"Checked {len(rows)} layers for correct CRS"
    except Exception as e:
        results["status"] = "ERROR"
        results["message"] = str(e)

    return results


def test_field_management(db_tables: DBTables) -> dict[str, Any]:
    """Test 2: Verify Field Management (feature_type, topo_id, metadata fields).

    Expected: All layers should have feature_type and topo_id columns.
    Metadata fields: capture_method, change_type, update_date, create_date, version
    """
    conn = db_tables.get_connection()
    results: dict[str, Any] = {"test_name": "Field Management", "details": []}
    required_fields = [
        "feature_type",
        "topo_id",
        "capture_method",
        "change_type",
        "update_date",
        "create_date",
        "version",
    ]

    try:
        for schema in ["release64", "carto"]:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = %s AND table_type = 'BASE TABLE'
                    """,
                    (schema,),
                )
                tables = [row[0] for row in cur.fetchall()]

            for table in tables:
                missing_fields = []
                for field in required_fields:
                    if not db_tables.column_exists(schema, table, field):
                        missing_fields.append(field)

                passed = len(missing_fields) == 0
                results["details"].append(
                    {  # type: ignore
                        "schema": schema,
                        "table": table,
                        "missing_fields": missing_fields,
                        "passed": passed,
                    }
                )

        all_passed = all(d["passed"] for d in results["details"])  # type: ignore
        results["status"] = "PASS" if all_passed else "FAIL"
        results["message"] = (
            f"Verified {len(results['details'])} tables for required fields"
        )
    except Exception as e:
        results["status"] = "ERROR"
        results["message"] = str(e)

    return results


def test_data_cleanup(db_tables: DBTables) -> dict[str, Any]:
    """Test 3: Verify Data Cleanup (ESRI_OID removed, geometry ordered last).

    Expected: No ESRI_OID columns, geometry column should be last in columns.
    """
    conn = db_tables.get_connection()
    results: dict[str, Any] = {"test_name": "Data Cleanup", "details": []}

    try:
        for schema in ["release64", "carto"]:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = %s AND table_type = 'BASE TABLE'
                    """,
                    (schema,),
                )
                tables = [row[0] for row in cur.fetchall()]

            for table in tables:
                esri_oid_exists = db_tables.column_exists(schema, table, "ESRI_OID")

                # Check if geometry column is last
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT column_name, ordinal_position 
                        FROM information_schema.columns 
                        WHERE table_schema = %s AND table_name = %s
                        ORDER BY ordinal_position DESC
                        LIMIT 1
                        """,
                        (schema, table),
                    )
                    last_col = cur.fetchone()
                    geom_is_last = last_col and last_col[0] in [
                        "geom",
                        "geometry",
                        "wkb_geometry",
                    ]

                passed = not esri_oid_exists and geom_is_last
                results["details"].append(
                    {  # type: ignore
                        "schema": schema,
                        "table": table,
                        "esri_oid_exists": esri_oid_exists,
                        "geometry_last": geom_is_last,
                        "passed": passed,
                    }
                )

        all_passed = all(d["passed"] for d in results["details"])  # type: ignore
        results["status"] = "PASS" if all_passed else "FAIL"
        results["message"] = (
            f"Checked {len(results['details'])} tables for cleanup issues"
        )
    except Exception as e:
        results["status"] = "ERROR"
        results["message"] = str(e)

    return results


def test_island_classification(db_tables: DBTables) -> dict[str, Any]:
    """Test 4: Verify Specialized Processing - Island Classification.

    Expected: Island layer should have 'location' field with values 0 or 1.
    """
    results: dict[str, Any] = {"test_name": "Island Classification", "details": []}

    try:
        if not db_tables.table_exists("release64", "island"):
            results["status"] = "SKIP"
            results["message"] = "Island table not found"
            return results

        if not db_tables.column_exists("release64", "island", "location"):
            results["status"] = "FAIL"
            results["message"] = "Island table missing 'location' field"
            return results

        conn = db_tables.get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT location, COUNT(*) as count 
                FROM release64.island 
                GROUP BY location
                ORDER BY location
                """
            )
            location_counts = cur.fetchall()

            for location, count in location_counts:
                passed = location in [0, 1]
                results["details"].append(
                    {  # type: ignore
                        "location": location,
                        "count": count,
                        "passed": passed,
                    }
                )

        all_passed = all(d["passed"] for d in results["details"])  # type: ignore
        results["status"] = "PASS" if all_passed else "FAIL"
        results["message"] = "Island location classification verified"
    except Exception as e:
        results["status"] = "ERROR"
        results["message"] = str(e)

    return results


def test_data_quality_corrections(db_tables: DBTables) -> dict[str, Any]:
    """Test 5: Verify Data Quality Corrections (value standardizations, corrections).

    Expected: Specific corrections applied (e.g., tunnel_line 'livestock', road_line corrections)
    """
    results: dict[str, Any] = {"test_name": "Data Quality Corrections", "details": []}
    checks: list[dict[str, Any]] = []

    try:
        conn = db_tables.get_connection()

        # Check 1: tunnel_line typo correction ('ivestock' → 'livestock')
        if db_tables.table_exists(
            "release64", "tunnel_line"
        ) and db_tables.column_exists("release64", "tunnel_line", "tunnel_use"):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM release64.tunnel_line WHERE tunnel_use = 'ivestock'"
                )
                bad_count = cur.fetchone()[0]
            checks.append(
                {
                    "check": "tunnel_line typo correction",
                    "passed": bad_count == 0,
                    "details": f"Found {bad_count} instances of 'ivestock' (should be 0)",
                }
            )

        # Check 2: trig_point trig_type consistency
        if db_tables.table_exists(
            "release64", "trig_point"
        ) and db_tables.column_exists("release64", "trig_point", "trig_type"):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(DISTINCT trig_type) FROM release64.trig_point WHERE trig_type IS NOT NULL"
                )
                distinct_count = cur.fetchone()[0]
            checks.append(
                {
                    "check": "trig_point trig_type consistency",
                    "passed": distinct_count <= 2,
                    "details": f"Found {distinct_count} distinct trig_type values",
                }
            )

        # Check 3: road_line way_count standardization
        if db_tables.table_exists("release64", "road_line") and db_tables.column_exists(
            "release64", "road_line", "way_count"
        ):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM release64.road_line WHERE way_count = '1' AND way_count != 'one way'"
                )
                incorrect_count = cur.fetchone()[0]
            checks.append(
                {
                    "check": "road_line way_count standardization",
                    "passed": incorrect_count == 0,
                    "details": f"Found {incorrect_count} instances not standardized",
                }
            )

        results["details"] = checks
        all_passed = all(c["passed"] for c in checks)
        results["status"] = "PASS" if all_passed else "FAIL"
        results["message"] = f"Verified {len(checks)} data quality corrections"
    except Exception as e:
        results["status"] = "ERROR"
        results["message"] = str(e)

    return results


def test_name_field_additions(db_tables: DBTables) -> dict[str, Any]:
    """Test 6: Verify Name Field Additions.

    Expected: Specific layers should have 'name' field:
    physical_infrastructure_point, physical_infrastructure_line, structure,
    vegetation, landcover, landcover_line, ferry_crossing
    """
    results: dict[str, Any] = {"test_name": "Name Field Additions", "details": []}
    required_name_fields_layers = [
        "physical_infrastructure_point",
        "physical_infrastructure_line",
        "structure",
        "vegetation",
        "landcover",
        "landcover_line",
        "ferry_crossing",
    ]

    try:
        for layer in required_name_fields_layers:
            has_name = db_tables.column_exists("release64", layer, "name")
            results["details"].append(
                {  # type: ignore
                    "layer": layer,
                    "has_name_field": has_name,
                    "passed": has_name,
                }
            )

        all_passed = all(d["passed"] for d in results["details"])  # type: ignore
        results["status"] = "PASS" if all_passed else "FAIL"
        results["message"] = (
            f"Verified {len(results['details'])} layers for name fields"
        )
    except Exception as e:
        results["status"] = "ERROR"
        results["message"] = str(e)

    return results


def test_default_value_assignment(db_tables: DBTables) -> dict[str, Any]:
    """Test 7: Verify Default Value Assignment (null values replaced with defaults).

    Expected: Specific fields should have default values:
    - runway.surface → 'grass'
    - vegetation.species (for exotic) → 'coniferous'
    - railway_line.vehicle_type → 'train'
    """
    results: dict[str, Any] = {"test_name": "Default Value Assignment", "details": []}
    checks: list[dict[str, Any]] = []

    try:
        conn = db_tables.get_connection()

        # Check 1: runway surface defaults
        if db_tables.table_exists("release64", "runway"):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM release64.runway WHERE surface IS NULL"
                )
                null_count = cur.fetchone()[0]
            checks.append(
                {
                    "check": "runway.surface defaults",
                    "passed": null_count == 0,
                    "details": f"Found {null_count} NULL values (should be 0)",
                }
            )

        # Check 2: vegetation.species defaults for exotic
        if db_tables.table_exists("release64", "vegetation"):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) FROM release64.vegetation 
                    WHERE feature_type = 'exotic' AND (species IS NULL OR species = '')
                    """
                )
                null_count = cur.fetchone()[0]
            checks.append(
                {
                    "check": "vegetation.species defaults (exotic)",
                    "passed": null_count == 0,
                    "details": f"Found {null_count} NULL/empty species for exotic features",
                }
            )

        # Check 3: railway_line vehicle_type defaults
        if db_tables.table_exists("release64", "railway_line"):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM release64.railway_line WHERE vehicle_type IS NULL"
                )
                null_count = cur.fetchone()[0]
            checks.append(
                {
                    "check": "railway_line.vehicle_type defaults",
                    "passed": null_count == 0,
                    "details": f"Found {null_count} NULL values (should be 0)",
                }
            )

        results["details"] = checks  # type: ignore
        all_passed = all(c["passed"] for c in checks)
        results["status"] = "PASS" if all_passed else "FAIL"
        results["message"] = f"Verified {len(checks)} default value assignments"
    except Exception as e:
        results["status"] = "ERROR"
        results["message"] = str(e)

    return results


def test_metadata_schema(db_tables: DBTables) -> dict[str, Any]:
    """Test 8: Verify Metadata Schema (standard metadata fields and defaults).

    Expected: All layers should have metadata fields with proper defaults:
    - capture_method (DEFAULT 'manual')
    - change_type (DEFAULT 'new')
    - update_date (DEFAULT CURRENT_DATE)
    - topo_id (DEFAULT gen_random_uuid())
    - create_date (DEFAULT CURRENT_DATE)
    - version (DEFAULT 1)
    """
    results: dict[str, Any] = {"test_name": "Metadata Schema", "details": []}

    try:
        conn = db_tables.get_connection()

        for schema in ["release64", "carto"]:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = %s AND table_type = 'BASE TABLE'
                    LIMIT 1
                    """,
                    (schema,),
                )
                sample_table = cur.fetchone()

                if not sample_table:
                    continue

                table_name = sample_table[0]

                # Check column defaults
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT column_name, column_default, data_type 
                        FROM information_schema.columns 
                        WHERE table_schema = %s AND table_name = %s 
                        AND column_name IN ('capture_method', 'change_type', 'version', 'topo_id', 'create_date', 'update_date')
                        """,
                        (schema, table_name),
                    )
                    columns = cur.fetchall()

                    for col_name, col_default, data_type in columns:
                        has_default = col_default is not None
                        results["details"].append(
                            {  # type: ignore
                                "schema": schema,
                                "table": table_name,
                                "column": col_name,
                                "data_type": data_type,
                                "has_default": has_default,
                                "default_value": col_default,
                            }
                        )

        has_defaults = all(d["has_default"] for d in results["details"])  # type: ignore
        results["status"] = (
            "PASS" if has_defaults and len(results["details"]) > 0 else "FAIL"
        )
        results["message"] = (
            f"Verified metadata schema on {len(results['details'])} field definitions"
        )
    except Exception as e:
        results["status"] = "ERROR"
        results["message"] = str(e)

    return results


def run_all_workflow_tests() -> pd.DataFrame:
    """Run all data processing workflow tests and return results as DataFrame."""
    db_tables = DBTables(db_params)
    test_results = []

    try:
        tests = [
            test_coordinate_system_conversions,
            test_field_management,
            test_data_cleanup,
            test_island_classification,
            test_data_quality_corrections,
            test_name_field_additions,
            test_default_value_assignment,
            test_metadata_schema,
        ]

        for test_func in tests:
            try:
                result = test_func(db_tables)
                test_results.append(result)
            except Exception as e:
                test_results.append(
                    {
                        "test_name": test_func.__name__,
                        "status": "ERROR",
                        "message": str(e),
                        "details": [],
                    }
                )
    finally:
        db_tables.close()

    return pd.DataFrame(test_results)


def create_layer_failures_report(layer_checks: pd.DataFrame, log_folder: str) -> None:
    """Extract layer check failures and write to separate file.

    Filters layer checks where passed = False,
    then writes to layers_failures_report.csv
    """
    failures_data = []

    if "passed" in layer_checks.columns:
        layer_failures = layer_checks[~layer_checks["passed"]].copy()
        if len(layer_failures) > 0:
            for _, row in layer_failures.iterrows():
                failures_data.append(
                    {
                        "schema": row["schema"],
                        "layer": row["layer"],
                        "status": "FAIL",
                        "row_count": row.get("row_count", 0),
                        "has_more_than_one": row.get("has_more_than_one_record", False),
                        "has_expected_feature": row.get("has_expected_feature", False),
                    }
                )

    # Write layer failures report
    if failures_data:
        failures_df = pd.DataFrame(failures_data)
        failures_file = os.path.join(log_folder, "layers_failures_report.csv")
        failures_df.to_csv(failures_file, index=False)

        print("\n" + "=" * 80)
        print("LAYER CHECK FAILURES REPORT")
        print("=" * 80)
        print(failures_df.to_string(index=False))
        print(f"\nTotal layer failures: {len(failures_df)}")
        print(f"Saved to: {failures_file}")
    else:
        print("\n" + "=" * 80)
        print("LAYER CHECK FAILURES REPORT")
        print("=" * 80)
        print("NO FAILURES FOUND - All layer checks passed!")


def create_workflow_failures_report(
    workflow_tests: pd.DataFrame, log_folder: str
) -> None:
    """Extract workflow test failures and write to separate file.

    Filters workflow tests for non-PASS status,
    then writes to workflow_failures_report.csv
    """
    failures_data = []

    if "status" in workflow_tests.columns:
        workflow_failures = workflow_tests[workflow_tests["status"] != "PASS"].copy()
        for _, row in workflow_failures.iterrows():
            test_name = row.get("test_name", "unknown")
            status = row.get("status", "UNKNOWN")
            message = row.get("message", "")
            details = row.get("details", [])

            # If status is FAIL, include failed sub-checks from details
            if status == "FAIL" and isinstance(details, list):
                for detail in details:
                    if isinstance(detail, dict) and not detail.get("passed", True):
                        failures_data.append(
                            {
                                "test_name": test_name,
                                "status": "FAIL",
                                "item": str(detail),
                            }
                        )
            else:
                # Include ERROR or other non-PASS statuses
                failures_data.append(
                    {
                        "test_name": test_name,
                        "status": status,
                        "item": message,
                    }
                )

    # Write workflow failures report
    if failures_data:
        failures_df = pd.DataFrame(failures_data)
        failures_file = os.path.join(log_folder, "workflow_failures_report.csv")
        failures_df.to_csv(failures_file, index=False)

        print("\n" + "=" * 80)
        print("WORKFLOW TEST FAILURES REPORT")
        print("=" * 80)
        print(failures_df.to_string(index=False))
        print(f"\nTotal workflow failures: {len(failures_df)}")
        print(f"Saved to: {failures_file}")
    else:
        print("\n" + "=" * 80)
        print("WORKFLOW TEST FAILURES REPORT")
        print("=" * 80)
        print("NO FAILURES FOUND - All workflow tests passed!")


if __name__ == "__main__":
    log_folder = r"c:\Data\logs"
    os.makedirs(log_folder, exist_ok=True)

    # Run basic layer data checks
    print("=" * 80)
    print("RUNNING BASIC LAYER DATA CHECKS")
    print("=" * 80)
    report = run_loaded_data_checks()
    print(report.to_string(index=False))
    report.to_csv(os.path.join(log_folder, "layers_check_report.csv"), index=False)

    print("\n" + "=" * 80)
    print("RUNNING DATA PROCESSING WORKFLOW TESTS")
    print("=" * 80)
    workflow_report = run_all_workflow_tests()
    print(workflow_report.to_string(index=False))
    workflow_report.to_csv(
        os.path.join(log_folder, "workflow_tests_report.csv"), index=False
    )

    # Generate failures reports
    create_layer_failures_report(report, log_folder)
    create_workflow_failures_report(workflow_report, log_folder)

    print("\n" + "=" * 80)
    print("Reports saved to:", log_folder)
    print("=" * 80)
