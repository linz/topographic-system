import geopandas as gpd
from sqlalchemy import create_engine
import time

# Database connection parameters
db_url = "postgresql://postgres:landinformation@localhost:5432/topo"

# Table and schema
table = "toposource.building"

# Create connection
engine = create_engine(db_url)
start_time = time.time()

# Read data
gdf = gpd.read_postgis(f"SELECT * FROM {table}", engine, geom_col="geom")
# Get the spatial index
sindex = gdf.sindex

# find all the possible intersections
candidate_pairs = set()
for idx1, row in gdf.iterrows():
    for idx2 in sindex.query(row.geom):
        if idx1 < idx2:
            candidate_pairs.add((idx1, idx2))

# candidate_pairs = sorted(list(set(candidate_pairs)))

# print(len(candidate_pairs))
# print(candidate_pairs)

# true_intersections = []
intersection_geometries = []
intersection_geometries_point = []
intersection_geometries_line = []

geomtypes = []

for idx1, idx2 in candidate_pairs:
    geom1 = gdf.geometry.iloc[idx1]
    geom2 = gdf.geometry.iloc[idx2]

    # intersection_geom = geom1.intersection(geom2)
    # Perform the precise intersection test
    if geom1.intersects(geom2):
        # Calculate the geometry of the intersection
        intersection_geom = geom1.intersection(geom2)
        geomtypes.append(intersection_geom.geom_type)

        # Check if the intersection is not empty (e.g. they touch only at a point or line)
        if not intersection_geom.is_empty:
            original_names = (gdf.name.iloc[idx1], gdf.name.iloc[idx2])
            # true_intersections.append(((idx1, idx2), original_names))

            if intersection_geom.geom_type in "Point":
                intersection_geometries_point.append(
                    {
                        "geometry": intersection_geom,
                        "pair_names": f"{original_names[0]}-{original_names[1]}",
                    }
                )
            elif intersection_geom.geom_type in ["LineString", "MultiLineString"]:
                intersection_geometries_line.append(
                    {
                        "geometry": intersection_geom,
                        "pair_names": f"{original_names[0]}-{original_names[1]}",
                        # 'length': intersection_geom.length
                    }
                )
            elif intersection_geom.geom_type == "GeometryCollection":
                for geom in intersection_geom.geoms:
                    print(geom.geom_type)
                    if geom.geom_type == "Point":
                        intersection_geometries_point.append(
                            {
                                "geometry": geom,
                                "pair_names": f"{original_names[0]}-{original_names[1]}",
                            }
                        )
                    elif geom.geom_type in ["LineString", "MultiLineString"]:
                        intersection_geometries_line.append(
                            {
                                "geometry": geom,
                                "pair_names": f"{original_names[0]}-{original_names[1]}",
                            }
                        )
                    else:
                        intersection_geometries.append(
                            {
                                "geometry": geom,
                                "pair_names": f"{original_names[0]}-{original_names[1]}",
                            }
                        )
            else:
                # print(intersection_geom.geom_type)
                intersection_geometries.append(
                    {
                        "geometry": intersection_geom,
                        "pair_names": f"{original_names[0]}-{original_names[1]}",
                        # 'area': area
                    }
                )

unique_geomtypes = list(set(geomtypes))
print("Unique intersection geometry types:", unique_geomtypes)

# Combine all intersection geometries into a single list
all_intersection_geometries = (
    intersection_geometries
    + intersection_geometries_point
    + intersection_geometries_line
)
intersections_gdf = gpd.GeoDataFrame(all_intersection_geometries, crs=gdf.crs)
intersections_gdf["status"] = "error"
intersections_gdf.to_parquet(r"c:\temp\topology_checks.parquet")

intersections_gdf = gpd.GeoDataFrame(intersection_geometries, crs=gdf.crs)
intersections_gdf = intersections_gdf.to_crs(epsg=2193)
intersections_gdf["Area"] = intersections_gdf.geometry.area
intersections_gdf.to_file(
    r"c:\temp\topology_checks.gpkg", layer="topology_errors_areas", driver="GPKG"
)
intersections_gdf.to_parquet(r"c:\temp\topology_checks_poly.parquet")

intersections_gdf = gpd.GeoDataFrame(intersection_geometries_point, crs=gdf.crs)
# intersections_gdf = intersections_gdf.to_crs(epsg=2193)
intersections_gdf.to_file(
    r"c:\temp\topology_checks.gpkg", layer="topology_errors_points", driver="GPKG"
)
intersections_gdf.to_parquet(r"c:\temp\topology_checks_point.parquet")

intersections_gdf = gpd.GeoDataFrame(intersection_geometries_line, crs=gdf.crs)
# intersections_gdf = intersections_gdf.to_crs(epsg=2193)
intersections_gdf.to_file(
    r"c:\temp\topology_checks.gpkg", layer="topology_errors_lines", driver="GPKG"
)
intersections_gdf.to_parquet(r"c:\temp\topology_checks_line.parquet")
