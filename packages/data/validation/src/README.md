# Validate topographic datasets

**Core code**

validate_dataset.py - contains various validation routinues including geometry and attribute checks

run from VS Code manually
run_validation.py - configuration in file

Settings

    get the validation collections : example - 
    POSTGRES
    layers = options_layer_postgres()
    db_url = "postgresql://postgres:<pwd>@localhost:5432/topo"

    GPKG
    point_in_poly_layers, layers, null_columns, query_rules = options_layer_gpkg()
    db_url = r"C:\Data\topoedit\topographic-data\topographic-data.gpkg"

    output_dir = r"C:\Data\topoedit\validation-data"

    area_crs = 2193

    EXPORT OPTIONS
    export_parquet = False
    export_parquet_by_geometry_type = False
    export_gpkg = True
    use_date_folder = False  Note: when running local just overwrite last run. Easier for QGIS layer. But central useful save by date option.

    PROCESS QUERY TYPES
    process_queries = True
    process_self_intersections = True
    process_features_on_layer = True

**Concepts**

Locate features that fail a rule

**Supported Data Sources**

GKPG (sqllite)

PostGIS


**Classes**

TopologyValidator - main validation controls

TopologyValidatorTools - utilities

    create folders
    format dates

**Validation Options**

point in poly - point intersects polygons (2 layers)

    building points in building polygons

self intersection - polygon/line layer must not self intersect (1 layer)

    vegetation intersects vegetation

null columns - column must not be null (1 layer)

    column [name] is null

query rules - features = query rule 

    "rule": "species IN ('coniferous', 'non-coniferous')"

**Configuration Options**

***point_in_poly_layers and self_intersection_layers***

    "point_table": "building_point"

    "poly_table": "building"

    "layername": "building-points-in-building-polygons"

    "message": "Building point features must not fall within building polygon features"
    
    Optional 
    "where": "topo_id in ('uuid', 'uuid') 
    
    date or weeks (only have none or one option)
    "date": "today"     note: will run against changes today
    "date": "2025-10-01"  note: will run against change from (including) date
    "weeks": 1  note: will run on changes over the last week

    OR
    "bbox": (174.81, -41.31, 174.82, -41.30)

***null_columns*** 

    "table": "descriptive_text"

    "column": "info_display"

    "message": "Descriptive text features must have an info_display attribute"

    Optional

    "where": "(feature_type = 'waterfall' OR feature_type = 'soakhole')"

***query_rules***

    "table": "vegetation"

    "column": "species"

    "where": "feature_type = 'exotic'"

    "rule": "species IN ('coniferous', 'non-coniferous')"

    "message": 'Exotic vegetation must have species as coniferous or non-coniferous'