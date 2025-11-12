import psycopg
import geopandas as gpd

# Database connection parameters
db_params = {
    'dbname': 'topo',
    'user': 'postgres',
    'password': 'landinformation',
    'host': 'localhost',
    'port': 5432
}

# Connect to the PostgreSQL database
conn = psycopg.connect(**db_params)

# Define SQL query to select all from tree_locations
# min = 3593709
#max = 4908228
#load via kart
# delete from tree_locations where t50_fid > 3722219
### DO A KART COMMIT BETWEEN EACH OF THESE
#sql = "SELECT * FROM release62.tree_locations where t50_fid <= 3722219"
#sql = "SELECT * FROM release62.tree_locations where t50_fid > 3722219 and t50_fid < 3902324"

#sql = "SELECT * FROM release62.tree_locations where t50_fid >= 3902324 and t50_fid < 4056631"
#sql = "SELECT * FROM release62.tree_locations where t50_fid >= 4056631 and t50_fid < 4210939"
#sql = "SELECT * FROM release62.tree_locations where t50_fid >= 4210939 and t50_fid < 4365246"
#sql = "SELECT * FROM release62.tree_locations where t50_fid >= 4365246 and t50_fid < 4565246"
#sql = "SELECT * FROM release62.tree_locations where t50_fid >= 4565246 and t50_fid < 4765246"
#sql = "SELECT * FROM release62.tree_locations where t50_fid >= 4765246 and t50_fid < 4965246"
sql = "SELECT * FROM release62.tree_locations where t50_fid >= 4965246 ;"


# Read the SQL query into a GeoDataFrame
gdf = gpd.read_postgis(sql, conn, geom_col='geom')

# Close the connection
conn.close()
# Read new tree_locations from GeoPackage
#new_gdf = gpd.read_file("tree_locations.gpkg", layer="tree_locations")

# Append previous gdf to new_gdf
#combined_gdf = new_gdf.append(gdf, ignore_index=True)

gdf.to_file(r"C:\\Data\\toposource\\topographic-data\\topographic-data.gpkg", layer="tree_locations", driver="GPKG", mode="a")
print("Done: " + sql)
