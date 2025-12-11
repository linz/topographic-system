import os
import pandas as pd

folder = r"C:\Data\Model"
layer_info_path = os.path.join(folder, "layers_info.xlsx")
feat_codes = os.path.join(folder, "tdd-feat-codes.csv")

# Read the Excel file and get the 'shp_name' column
df = pd.read_excel(layer_info_path)
keys = df["shp_name"].dropna().unique()


# Read the feature codes CSV file
feat_df = pd.read_csv(feat_codes)
feat_keys = feat_df["object class"].dropna().unique()

# Find keys in 'keys' not in 'feat_keys'
missing_in_feat = set(keys) - set(feat_keys)
print("Keys in layer_info but not in feat_codes:", missing_in_feat)

# Find keys in 'feat_keys' not in 'keys'
missing_in_layer = set(feat_keys) - set(keys)
print("Keys in feat_codes but not in layer_info:", missing_in_layer)
