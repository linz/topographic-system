
import csv
import re
from pathlib import Path


YML_SOURCE = Path(r"C:\Temp\import\config-master\themes")
#YML_SOURCE = Path(r"C:\Temp\import\config-draft\themes")
OUTPUT_FILE = YML_SOURCE / "yml_extracted_layers.csv"

TOP_LEVEL_NAME = re.compile(r"^name\s*:\s*(.+?)\s*$")
DATASET_SOURCE = re.compile(r"^(?P<indent>\s*)-\s*source\s*:\s*(.+?)\s*$")
DATASET_NAME = re.compile(r"^(?P<indent>\s+)name\s*:\s*(.+?)\s*$")
DATASET_THEME = re.compile(r"^\s+theme\s*:\s*(.+?)\s*$")
SCALE = re.compile(r"(?:^|_)(150|1250|125)k(?:_|$)", re.IGNORECASE)
LOCATIONS = ("antipodes", "auckland", "bounty", "chatham", "campbell","kermadec", "ross", "snares")


def unquote(value: str) -> str:
	return value.strip().strip("'\"")


def dataset_location(dataset_name: str) -> str:
	for location in LOCATIONS:
		if dataset_name.startswith((f"nz_{location}", f"nz-{location}")):
			return location
	return "mainland"


def dataset_scale(dataset_name: str) -> str:
	match = SCALE.search(dataset_name.replace("-", "_"))
	return f"{match.group(1)}K" if match else ""


def extract_rows(yml_file: Path) -> list[dict[str, str]]:
	layer_name = ""
	rows: list[dict[str, str]] = []
	current_row: dict[str, str] | None = None
	dataset_name_indent: int | None = None

	for line in yml_file.read_text(encoding="utf-8").splitlines():
		if "#" in line:
			continue
		if match := TOP_LEVEL_NAME.match(line):
			layer_name = unquote(match.group(1))
		elif match := DATASET_SOURCE.match(line):
			source = unquote(match.group(2))
			dataset_name_indent = len(match.group("indent")) + 2
			current_row = {
				"name": layer_name,
				"dataset_source": source,
				"dataset_name": source.rsplit("/", 1)[-1],
				"dataset_theme": "",
			}
			rows.append(current_row)
		elif current_row and (match := DATASET_NAME.match(line)) and len(match.group("indent")) == dataset_name_indent:
			current_row["dataset_name"] = unquote(match.group(2))
		elif current_row and (match := DATASET_THEME.match(line)):
			current_row["dataset_theme"] = unquote(match.group(1))

	return rows


def main() -> None:
	rows = [
		{
			**row,
			"location": dataset_location(row["dataset_name"]),
			"scale": dataset_scale(row["dataset_name"]),
		}
		for yml_file in YML_SOURCE.glob("*.yml")
		for row in extract_rows(yml_file)
		if row["dataset_name"].startswith(("nz_", "nz-"))
	]

	with OUTPUT_FILE.open("w", encoding="utf-8", newline="") as csv_file:
		writer = csv.DictWriter(
			csv_file,
			fieldnames=[
				"name",
				"dataset_source",
				"dataset_name",
				"dataset_theme",
				"location",
				"scale",
			],
		)
		writer.writeheader()
		writer.writerows(rows)

	print(f"Wrote {len(rows)} rows to {OUTPUT_FILE}")


if __name__ == "__main__":
	main()

