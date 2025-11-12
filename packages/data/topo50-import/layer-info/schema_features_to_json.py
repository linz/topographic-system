import pandas as pd
import json

xlsx_path = r'C:\Data\Model\themes_by_layers.xlsx'  # Update with your actual file path
sheet_name = 'schema-features'

# Read the Excel sheet
df = pd.read_excel(xlsx_path, sheet_name=sheet_name)

# Group by 'layer' column (update column name if needed)
grouped = df.groupby('layer')

result = {}
for layer, group in grouped:
    # Convert each row to dict, drop the 'layer' key
    features = group.drop(columns=['layer']).to_dict(orient='records')
    result[layer] = features

# Write to JSON
with open(r'C:\Data\Model\schema_features_by_layer.json', 'w', encoding='utf-8') as f:
    json.dump(result, f, indent=2, ensure_ascii=False)