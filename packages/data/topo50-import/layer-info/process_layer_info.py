import os
import pandas as pd

folder = r"C:\Data\Model"
layer_info_path = os.path.join(folder, "layers_info.xlsx")
layer_summary_path = os.path.join(folder, "layers_summary.xlsx")

df = pd.read_excel(layer_info_path)
distinct_rows = df[["theme", "lds_theme", "layer_name"]].drop_duplicates()
output_path = os.path.join(folder, layer_summary_path)
with pd.ExcelWriter(layer_summary_path) as writer:
    distinct_rows.to_excel(writer, sheet_name="themes-layers", index=False)


grouped = (
    df.groupby(["schema", "layer_name", "feature_type", "theme", "lds_theme"])
    .size()
    .reset_index(name="count")
)
with pd.ExcelWriter(layer_summary_path, mode="a") as writer:
    grouped.to_excel(writer, sheet_name="schema-features", index=False)
