# code to join two Excel files based on a common column 'key'
import os
import pandas as pd

path = r"C:\Data\Topo50\themes"
file1 = os.path.join(path, "topo50-data-layers-sources.xlsx")
file2 = os.path.join(path, "layers_info.xlsx")
outfile = os.path.join(path, "topo50_layers_info.xlsx")

# Read the Excel files into DataFrames
df1 = pd.read_excel(file1, sheet_name="Sheet1", engine="openpyxl")
df2 = pd.read_excel(file2, sheet_name="Sheet1", engine="openpyxl")

# Merge DataFrames on the 'shp_name' column
merged_df = pd.merge(df1, df2, on="key", how="inner")

# Find records in df1 not in df2
not_in_df2 = df1[~df1["key"].isin(df2["key"])]
print("Records in df1 not in df2:")
print(not_in_df2)

# Find records in df2 not in df1
not_in_df1 = df2[~df2["key"].isin(df1["key"])]
print("Records in df2 not in df1:")
print(not_in_df1)

# Optionally, save the merged DataFrame to a new Excel file
merged_df.to_excel(outfile, index=False)
