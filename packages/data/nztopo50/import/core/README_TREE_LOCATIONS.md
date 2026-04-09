Tree Locations are a big dataset. This layer frequently fails to load - times out connection to github.

This has mainly been processed using Windows and from the office location. ZScaler turn off if possibe.

When this fails - the set up process needs to be repeat (very quick) - instructions in the main README_LOAD_TOPO.md

The workaround process involves a semi manual set of steps - inserting the data via code **see: insert_trees_workaround.py** then doing a **commit** (which takes time - very slow) and then doing the **push**.

Note: Experience shows do this layer first will be a bit faster but can be down at the end of the main run.

_You should follow the steps in the main topographic data load up to the import data stage..._

The process involves:

1. Initial set up of data and import into the repo - using SQL and Kart Import
2. Check the script **insert_trees_workaround.py** - that the variable STEP = 1
3. Run the first version of the script **insert_trees_workaround.py** - this will insert a chunck of data into the GPKG from the POSTGRES database.
4. Manually in the local Kart repo location - Do a Kart Commit and then Kart Push to push data into the repo
5. Edit the script - update the variable STEP = 2 (repeat + 1...) - and re-run
6. Repeat until all steps completed.

Using **pgAdmin App** to access the POSTGRES database - currently the SQL commands are run manually.

## Define SQL query to select all from tree_locations

_min = 3593709_

_max = 4908228_ - the last insert just uses > so ok if number grows (not by to much)

_Step 1 & 2 In database make a copy of all tree_locations and delete some records_

## 1 Create a temporary backup of the data table (update release64 text if using different version)

create table release64.tree_locations_master as select \* from release64.tree_locations;

## 2 Delete all the feature over the initial start values

delete from release64.tree_locations where t50_fid > 3722219;

commit;

## 3 Move into command line in working repo

**Clone the repo and load data via kart import**

If you are starting from scratch...

> kart clone git@github.com:linz/topographic-data

> cd topographic-data

> kart import postgresql://postgres:landinformation@localhost/topo/release64 --primary-key topo_id tree_locations

**ONLY if doing first use force to clear repo - see main process**

> kart push origin master [--force]

# 4 In database drop tree_locations and rename master to tree_locations

drop table release64.tree_locations;

alter table release64.tree_locations_master rename to tree_locations;

# 5 Start with the first SQL and work you way down doing commit / push after each run

run script - _check STEP set to 1_

manually run commit and push commands between each sql

> kart commit -m "insert trees"

> kart push origin master

repeat for next steps - update value + 1 until all done

NOTE: commit can take a long long long time\*\*

**### **DO A KART COMMIT BETWEEN EACH OF THESE\*\*

This one will be done by default as initial load skip - so is not run in the manual process
sql = "SELECT \* FROM release64.tree_locations where t50_fid <= 3722219"

SQL Examples - see script for details

sql = "SELECT \* FROM release64.tree_locations where t50_fid > 3722219 and t50_fid < 3902324"

sql = "SELECT \* FROM release64.tree_locations where t50_fid >= 3902324 and t50_fid < 4056631"

sql = "SELECT \* FROM release64.tree_locations where t50_fid >= 4965246"
