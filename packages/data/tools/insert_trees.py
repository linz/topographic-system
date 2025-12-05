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
# Approach START HERE...DO THIS BEFORE OTHER IMPORTS MAY MAKE IT FASTER - commit slow.
# 1 & 2 In database make a copy of all tree_locations and delete some records

# 1
##create table release62.tree_locations_master as
##select * from release62.tree_locations;

# 2
##delete from release62.tree_locations where t50_fid > 3722219;
##commit;

# 3
#load via kart import 
##kart import postgresql://postgres:landinformation@localhost/topo/release62  --primary-key topo_id tree_locations 
##kart push origin main

# In database drop tree_locations and rename master to tree_locations
##drop table release62.tree_locations;
##alter table release62.tree_locations_master rename to tree_locations;
##commit;

# 4
# Start with the first SQL and work you way down

# uncomment SQL and run script
# manually run  push commands between each sql 
# kart commit -m "insert trees"
# kart push origin main
# repeat for next sql until all done 
# NOTE: commit can take a long long long time

### DO A KART COMMIT BETWEEN EACH OF THESE
## This one will be done by default as initial load skip - #sql = "SELECT * FROM release62.tree_locations where t50_fid <= 3722219"

sql = "SELECT * FROM release62.tree_locations where t50_fid > 3722219 and t50_fid < 3902324"

#sql = "SELECT * FROM release62.tree_locations where t50_fid >= 3902324 and t50_fid < 4056631"
#sql = "SELECT * FROM release62.tree_locations where t50_fid >= 4056631 and t50_fid < 4210939"
#sql = "SELECT * FROM release62.tree_locations where t50_fid >= 4210939 and t50_fid < 4365246"
#sql = "SELECT * FROM release62.tree_locations where t50_fid >= 4365246 and t50_fid < 4565246"
#sql = "SELECT * FROM release62.tree_locations where t50_fid >= 4565246 and t50_fid < 4765246"
#sql = "SELECT * FROM release62.tree_locations where t50_fid >= 4765246 and t50_fid < 4965246"
#sql = "SELECT * FROM release62.tree_locations where t50_fid >= 4965246 ;"


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
