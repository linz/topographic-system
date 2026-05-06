
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

If road_line - t50_fid in database has 0 values - run SQL to assign a value. Mainly used when aligning topo_id between releases. This is a work around.

## ISLANDS POLYGONS

**islands_poly.shp** - this needs the additional field (location) and calculation of offshore (1) or inland island (0) - added using the pre_processing_steps.py script. The sea_coastline poly shapefile create from coastline and outer box
