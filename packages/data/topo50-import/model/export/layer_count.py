import os
from pyogrio import list_layers # type: ignore

tables = [
    "airport",
    "bridge_line",
    "building",
    "building_point",
    "coastline",
    "collections",
    "contour",
    "descriptive_text",
    "fence_line",
    "ferry_crossing",
    "geographic_name",
    "island",
    "landcover",
    "landcover_line",
    "landcover_point",
    "marine",
    "nz_topo_map_sheet",
    "physical_infrastructure_line",
    "physical_infrastructure_point",
    "place",
    "railway_line",
    "railway_station",
    "relief_line",
    "relief_point",
    "residential_area",
    "river",
    "river_line",
    "road_line",
    "runway",
    "snow_ice",
    "structure",
    "structure_line",
    "structure_point",
    "track_line",
    "transport_point",
    "tree_locations",
    "tree_locations_copy",
    "trig_point",
    "tunnel_line",
    "vegetation",
    "vegetation_line",
    "water",
    "water_line",
    "water_point",
]

format = "geoparquet"
source_path = r"C:\Data\topo50maps"

# format = "geopackage"
# source_path = r"C:\Data\toposource\topographic-data"
##source_path = r"C:\Data\topoedit\topographic-data"
database = "topographic-data.gpkg"

if format == "geoparquet":
    search_ext = ".parquet"
    files = [f for f in os.listdir(source_path) if f.endswith(search_ext)]
    table_files = [t + search_ext for t in tables]
    extra_files = [f for f in files if f not in table_files]
    for f in extra_files:
        print(f)
elif format == "geopackage":
    layers = list_layers(os.path.join(source_path, database))
    for layer in layers:
        if layer[0] not in tables:
            print(layer[0])
