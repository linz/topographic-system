import pandas as pd
import os

# Path to the Excel file
base_path = r"C:\Data\Model"
excel_path = os.path.join(base_path, r"overture_featuretypes.xlsx")
feature_type_map_path = os.path.join(base_path, r"feature_type_map.xlsx")
output_path = os.path.join(base_path, r"matched_types.csv")

# Read the Excel file
df = pd.read_excel(excel_path)

# Display columns related to 'type', 'class'
columns_of_interest = [
    col for col in df.columns if any(key in col.lower() for key in ["type", "class"])
]
print(df[columns_of_interest].head())

class_type_dict = dict(zip(df["class"], df["type"]))
print(class_type_dict)

# Read the 'feature_type_map.xlsx' file and extract 'feature_type' and 'field' columns

df_feature_type_map = pd.read_excel(feature_type_map_path)

# Display the 'feature_type' and 'field' columns
print(df_feature_type_map[["feature_type"]].head())

with open(output_path, "a") as f:
    for feature_type in df_feature_type_map["feature_type"]:
        matched_type = class_type_dict.get(feature_type)
        print(f"Feature type: {feature_type}, Matched type: {matched_type}")

        if matched_type is None and "_" in feature_type:
            feature_type = feature_type.split("_")[0]
            matched_type = class_type_dict.get(feature_type)
            print(f"Feature type (base): {feature_type}, Matched type: {matched_type}")

        if matched_type is None:
            matched_type = "nomatch"
        print(f"Final matched type: {matched_type}")

        f.write(f"{feature_type},{matched_type}\n")
