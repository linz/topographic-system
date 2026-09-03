import subprocess
from pathlib import Path

#wslc run --rm -it -v C:\data\temp\amcmenamin:/data -v C:\Data\toposource\schema_model:/schema kart validate-schema --schema /schema/marine_point.json /data/marine_point.parquet
parquet_folder = r"C:\Data\temp\amcmenamin"
#schema_folder = "/schema/next"
schema_folder = r"C:\data\toposource\schema_model"
data_mount = "/data"
log_file = Path(parquet_folder) / "schema_validation.log"

parquet_files = sorted(Path(parquet_folder).glob("*.parquet"))

log_file.write_text("", encoding="utf-8")

for parquet_file in parquet_files:
	print(parquet_file)
	layer_name = parquet_file.stem
	print(f"Layer name: {layer_name}")
	schema = f"/schema/{layer_name}.json"
	data_file = f"{data_mount}/{parquet_file.name}"
	val_command = [
		"wslc",
		"run",
		"--rm",
		"-it",
		"-v",
		f"{parquet_folder}:{data_mount}",
		"-v",
		f"{schema_folder}:/schema",
		"kart",
		"validate-schema",
		"--schema",
		schema,
		data_file,
	]
	print(" ".join(val_command))
	result = subprocess.run(val_command, check=False, capture_output=True, text=True)
	with log_file.open("a", encoding="utf-8") as log_handle:
		log_handle.write(f"Command: {' '.join(val_command)}\n")
		log_handle.write(f"Return code: {result.returncode}\n")
		validation_errors = [
			line.replace("\x1b", " ")
			for output in (result.stdout, result.stderr)
			for line in output.splitlines()
			if "ValidateSchema:ErrorSummary" in line
		]
		if validation_errors:
			log_handle.write("STDOUT:\n")
			log_handle.write("\n".join(validation_errors) + "\n")
		log_handle.write("-" * 80 + "\n")
	print(f"Validation completed for {layer_name} with schema {schema}")


