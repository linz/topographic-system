import psycopg
import geopandas as gpd # ignore


# THERE ARE A SET OF MANUAL SQL STEPS TO DO BEFORE RUNNING THIS SCRIPT - SEE README_TREES_LOCATIONS.md and follow instructions
STEP = 1
target_database = r"C:\\Data\\toposource\\topographic-data\\topographic-data.gpkg"

# remove the exit() just there for safetly than command is not run before reading steps in readme
exit()

# Database connection parameters
db_params = {
    "dbname": "topo",
    "user": "postgres",
    "password": "landinformation",
    "host": "localhost",
    "port": 5432,
}

# Connect to the PostgreSQL database
conn = psycopg.connect(**db_params)


if STEP == 1:
    sql = "SELECT * FROM release64.tree_locations where t50_fid > 3722219 and t50_fid < 3902324"
elif STEP == 2:
    sql = "SELECT * FROM release64.tree_locations where t50_fid >= 3902324 and t50_fid < 4056631"
elif STEP == 3:
    sql = "SELECT * FROM release64.tree_locations where t50_fid >= 4056631 and t50_fid < 4210939"
elif STEP == 4:
    sql = "SELECT * FROM release64.tree_locations where t50_fid >= 4210939 and t50_fid < 4365246"
elif STEP == 5:
    sql = "SELECT * FROM release64.tree_locations where t50_fid >= 4365246 and t50_fid < 4565246"
elif STEP == 6:
    sql = "SELECT * FROM release64.tree_locations where t50_fid >= 4565246 and t50_fid < 4765246"
elif STEP == 7:
    sql = "SELECT * FROM release64.tree_locations where t50_fid >= 4765246 and t50_fid < 4965246"
elif STEP == 8:
    sql = "SELECT * FROM release64.tree_locations where t50_fid >= 4965246"


# Read the SQL query into a GeoDataFrame
gdf = gpd.read_postgis(sql, conn, geom_col="geometry")

# Close the connection
conn.close()

gdf.to_file(
    target_database,
    layer="tree_locations",
    driver="GPKG",
    mode="a",
)
print("Done: " + sql)

# manually run the commit & push command - see instructions in README_TREES_LOCATIONS.md
