from sqlalchemy import create_engine, text
import geopandas as gpd 


# THERE ARE A SET OF MANUAL SQL STEPS TO DO BEFORE RUNNING THIS SCRIPT - SEE README_TREES_LOCATIONS.md and follow instructions
STEP = 1  # Change this value to run different steps (1-8)
target_database = r"C:\\Data\\toposource\\topographic-data\\topographic-data.gpkg"

# remove the exit() just there for safetly that command is not run before reading steps in readme
exit()

# Database connection using SQLAlchemy
DATABASE_URL = "postgresql+psycopg://postgres:landinformation@localhost:5432/topo"

# Create SQLAlchemy engine
engine = create_engine(
    DATABASE_URL,
    echo=False,  # Set to True for SQL debugging
    pool_pre_ping=True,  # Verify connections before using
    pool_recycle=3600,  # Recycle connections after 1 hour
)


def get_step_query(step: int) -> str:
    """Get the SQL query for a specific step."""
    queries = {
        1: "SELECT * FROM release64.tree_locations WHERE t50_fid > 3722219 AND t50_fid < 3902324",
        2: "SELECT * FROM release64.tree_locations WHERE t50_fid >= 3902324 AND t50_fid < 4056631",
        3: "SELECT * FROM release64.tree_locations WHERE t50_fid >= 4056631 AND t50_fid < 4210939",
        4: "SELECT * FROM release64.tree_locations WHERE t50_fid >= 4210939 AND t50_fid < 4365246",
        5: "SELECT * FROM release64.tree_locations WHERE t50_fid >= 4365246 AND t50_fid < 4565246",
        6: "SELECT * FROM release64.tree_locations WHERE t50_fid >= 4565246 AND t50_fid < 4765246",
        7: "SELECT * FROM release64.tree_locations WHERE t50_fid >= 4765246 AND t50_fid < 4965246",
        8: "SELECT * FROM release64.tree_locations WHERE t50_fid >= 4965246",
    }
    return queries.get(step, "")


def main():
    """Main function to process tree locations."""
    try:
        # Test connection
        with engine.connect():
            print("Connected to database successfully")

            # Get the query for the current step
            sql_query = get_step_query(STEP)
            if not sql_query:
                raise ValueError(f"Invalid STEP value: {STEP}")

            print(f"Executing step {STEP}: {sql_query}")

            gdf = gpd.read_postgis(sql=text(sql_query), con=engine, geom_col="geometry")

            # Write to GeoPackage (same as original)
            gdf.to_file(
                target_database,
                layer="tree_locations",
                driver="GPKG",
                mode="a",
            )

            print(f"Done: {sql_query}")
            print(f"Processed {len(gdf)} records")
            print(f"LAST Executing step was number: {STEP}")

    except Exception as e:
        print(f"Error occurred: {e}")
        raise
    finally:
        # SQLAlchemy automatically manages connections, but we can explicitly dispose
        engine.dispose()


if __name__ == "__main__":
    main()

# manually run the commit & push command - see instructions in README_TREES_LOCATIONS.md
