import duckdb
import geopandas as gpd
from shapely import wkb

# Provided SQL query (for reference, not executable in DuckDB as-is)
sql = """
SELECT id_left, id_right,geom_type,
       ST_Area(ST_Transform(intersection_geom, 2193)) AS intersection_area,
       intersection_geom
FROM
(SELECT
    a.id AS id_left,
    b.id AS id_right,
    GeometryType(ST_Intersection(a.geom, b.geom)) AS geom_type,
    ST_Intersection(a.geom, b.geom) AS intersection_geom,
    ST_Area(ST_Intersection(a.geom, b.geom)) AS intersection_area
FROM
    toposource.building a
JOIN
    toposource.building b
ON
    a.id <> b.id
    AND ST_Intersects(a.geom, b.geom));
"""

# Example DuckDB usage (replace with your actual table and logic)
con = duckdb.connect()
# con.execute(sql)  # This will not work unless your DuckDB has spatial extension and compatible data
result = con.fetchall().df()

# Assuming 'result' is a DataFrame with a column 'intersection_geom' containing WKB geometries
result['geometry'] = result['intersection_geom'].apply(lambda x: wkb.loads(x) if x is not None else None)
gdf = gpd.GeoDataFrame(result, geometry='geometry')


print("DuckDB SQL embedded. Adapt logic for spatial operations using GeoPandas/Shapely if needed.")