import os
import pandas as pd

folder = r"C:\Data\Model"
gpkg_path = r"C:\Data\Topo50\layers_geopackage\data"
layer_info_path = os.path.join(folder, "layers_info.xlsx")


df = pd.read_excel(layer_info_path)
keys = df["key"].dropna().unique()


# Create a mapping for replacements
replace_map = {
    "pnt": "points",
    "cl": "centrelines",
    "poly": "polygons",
    "edges": "edge",
    "edge": "edges",
    "_": "-",
}


# Function to apply replacements in a string
def replace_layer_terms(name):
    for k, v in replace_map.items():
        name = name.replace(k, v)
    return name


# Example usage: create a new list with replacements applied
gpkg_keys = [replace_layer_terms(key) for key in keys]


actual_gpkg_files = set(f for f in os.listdir(gpkg_path) if f.lower().endswith(".gpkg"))
# Loop through .gpkg files, split by '-', and join from 'nz' to the part ending with 'topo'
for fname in actual_gpkg_files:
    parts = fname.split("-")
    if "nz" in parts:
        nz_index = parts.index("nz") + 1
        # Find the part that ends with 'topo'
        topo_index = None
        for i, part in enumerate(parts[nz_index:], start=nz_index):
            if part.startswith("topo"):
                topo_index = i - 1
                break
        if topo_index is not None:
            joined = "-".join(parts[nz_index : topo_index + 1])
            print(joined)
            # Collect joined values into a set
            if "joined_set" not in locals():
                joined_set = set()
            joined_set.add(joined)

actual_gpkg_files = joined_set

# Compare gpkg_keys and joined_set for differences
gpkg_keys_set = set(gpkg_keys)

# not SOME WEIRD HAPPENING NOT ACTUAL RESSULT - SEEMS TO LOSS SOME RECORDS!

missing_in_gpkg = gpkg_keys_set - actual_gpkg_files
extra_in_gpkg = actual_gpkg_files - gpkg_keys_set

print("\nMissing in .gpkg files (in spreadsheet, not in gpkg):")
for f in sorted(missing_in_gpkg):
    print(f)

print("\nExtra in .gpkg files (in gpkg, not in spreadsheet):")
for f in sorted(extra_in_gpkg):
    print(f)
