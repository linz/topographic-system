# The following PRE-PROCESS STEPS are required

## DATABASE

**POSTGRES/POSTGIS** - check load schema has been manually created (named after release) for example **release65**

**POSTGRES/POSTGIS** - check carto schema has been manually created called **carto** - some core data is copied into carto (currently map_sheets)

**POSTGRES/POSTGIS** - check lookups schema has been manually created called **lookups** - some directly exported data from LAMPS is imported in this repo (currently roads, tbc water names)

## ROADS AND WATER NAMES

**See: processing_steps.py - for code and manual SQL to use for roads with 0 as t50_fid.**
This is a work in progress as we determine how to correctly set the fids.

**RUN: pre_processing_lookups** - this loads the LAMPS road data that is not exported to LDS. This data is joined to the loaded roads as part of the main load process.

        "UFID": "t50_fid"
        "road_acces": "road_access"
        "width": "width_indicator"

NOT currently implemented but the expectation is this is extended to water names.

Also **CHECK** if it has to50_fid still set to 0. The pre-processing.py script has a SQL command to update the IDS once in Postgres.

If road_line - t50_fid in database has 0 values - run SQL to assign a value. Mainly used when aligning id between releases. This is a work around.

## LINZ MAP SHEET

This is export directly from LAMPS for addition example information. This replaces the LDS version in the shape folder. It is processed as part of the core and copied into the carto schema in the postgis_manage_fields script.

This requires **post processing step** as new field example_point_id is now link to feature id in related trig or geographic name tables.

## ISLANDS POLYGONS - THIS STEP HAS BEEN STOPPED UNTIL CLEAR REQUIREMENTS DRAFT. FIELD DROPPED MAY2026

**islands_poly.shp** - this needs the additional field (location) and calculation of offshore (1) or inland island (0) - added using the pre_processing_steps.py script. The sea_coastline poly shapefile create from coastline and outer box

## CONTOURS

When contours are exported from LDS they are 3D (Z enabled). Kart has an issue export Z enabled (TO test suggested option from Kart Issues).

Contours are processed to drop Z value before load. This will need to be re-added on the LDS prep stage. Expectations is this is not managed at the QGIS stage.

Suggest approach - to test - kart export dataset_with_z /tmp/dataset_with_z.fgb --crs=EPSG:4326 --override-geometry-type=GEOMETRY