import subprocess
from pathlib import Path

parquet_folder = r"C:\Data\temp"
schema_folder = "/schema/next"
data_mount = "/data"
log_file = Path(parquet_folder) / "schema_validation.log"

parquet_files = sorted(Path(parquet_folder).glob("*.parquet"))

log_file.write_text("", encoding="utf-8")

for parquet_file in parquet_files:
	print(parquet_file)
	layer_name = parquet_file.stem
	print(f"Layer name: {layer_name}")
	schema = f"{schema_folder}/{layer_name}.json"
	data_file = f"{data_mount}/{parquet_file.name}"
	val_command = [
		"wslc",
		"run",
		"--rm",
		"-it",
		"-v",
		f"{parquet_folder}:{data_mount}",
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
		if result.stdout:
			log_handle.write("STDOUT:\n")
			log_handle.write(result.stdout)
			if not result.stdout.endswith("\n"):
				log_handle.write("\n")
		if result.stderr:
			log_handle.write("STDERR:\n")
			log_handle.write(result.stderr)
			if not result.stderr.endswith("\n"):
				log_handle.write("\n")
		log_handle.write("-" * 80 + "\n")
	print(f"Validation completed for {layer_name} with schema {schema}")


