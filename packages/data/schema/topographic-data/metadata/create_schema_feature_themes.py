import os
import pandas as pd

model_folder = r"C:\Data\Model"
layers_info_file = os.path.join(model_folder, "layers_info.xlsx")
metadata_source_folder = os.path.join(model_folder, "metadata_source")
features_bythemes_file = os.path.join(metadata_source_folder, "schema_features_theme.csv")


def main() -> None:
	layers_df = pd.read_excel(
		layers_info_file,
		sheet_name="Sheet1",
		usecols=["layer_name", "theme", "type"],
	)

	features_bythemes_df = layers_df.rename(columns={"layer_name": "table"})[
		["table", "theme", "type"]
	]
	features_bythemes_df["table"] = features_bythemes_df["table"].astype(str).str.lower()
	features_bythemes_df["type"] = (
		features_bythemes_df["type"].astype(str).str.lower()
	)

	features_bythemes_df.to_csv(features_bythemes_file, index=False)


if __name__ == "__main__":
	main()

