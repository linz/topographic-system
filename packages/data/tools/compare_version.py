import os
import geopandas as gpd

release_folder1 = r"C:\Data\Topo50\Release62_NZ50_Shape"
release_folder2 = r"C:\Data\Topo50\Release63_NZ50_Shape"

compare_file = "tree_pnt.shp"

file1 = os.path.join(release_folder1, compare_file)
file2 = os.path.join(release_folder2, compare_file)
gdf1 = gpd.read_file(file1)
gdf2 = gpd.read_file(file2)

ids1 = set(gdf1["t50_fid"])
ids2 = set(gdf2["t50_fid"])

removed = ids1 - ids2
added = ids2 - ids1

print("TOPO50_FID removed from second file:", removed)
print("TOPO50_FID added in second file:", added)

common_ids = ids1 & ids2
diff_ids = []

for fid in common_ids:
    print(f"Comparing TOPO50_FID: {fid}")
    row1 = gdf1[gdf1["t50_fid"] == fid].iloc[0]
    row2 = gdf2[gdf2["t50_fid"] == fid].iloc[0]
    # Ignore the first column (assumed to be 'FID')
    row1_data = row1.iloc[1:]
    row2_data = row2.iloc[1:]
    if not row1_data.equals(row2_data):
        diff_ids.append(fid)

print("TOPO50_FID with differences between files:", diff_ids)