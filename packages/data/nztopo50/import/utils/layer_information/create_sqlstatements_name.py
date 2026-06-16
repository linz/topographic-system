"""Generate SQL statements for NAME-based feature checks.

This script reads grouped schema metadata from schema_columns_groupby.csv,
filters rows to NAME columns, and writes one SQL statement per matching table.
Each statement selects type and NAME values, excludes NULL names, and
groups/sorts the output to help with data validation and review.
"""

import csv
from pathlib import Path


if __name__ == "__main__":

	schema_name = "release66"

	source_file = Path(r"C:\Data\model\schema_columns_groupby.csv")
	output_file = source_file.with_name("schema_columns_groupby_name_queries.sql")

	statements = []

	with source_file.open("r", newline="", encoding="utf-8-sig") as f:
		reader = csv.DictReader(f)

		for row in reader:
			table_name = (row.get("table") or "").strip()
			column_name = (row.get("columns") or "").strip()
			group_by_value = (row.get("group_by") or "").strip().upper()

			if not table_name or not column_name:
				continue

			if column_name.upper() != "NAME":
				continue

			if "." in table_name:
				table_ref = table_name
			else:
				table_ref = f"{schema_name}.{table_name}" if schema_name else table_name

			statement = (
				f"SELECT type, {column_name} FROM {table_ref} "
				f"WHERE {column_name} IS NOT NULL "
				f"GROUP BY type, {column_name} "
				f"ORDER BY {column_name};"
			)
			statements.append(statement)

	with output_file.open("w", encoding="utf-8") as f:
		f.write("\n".join(statements))
		f.write("\n")

	print(f"Wrote {len(statements)} SQL statements to {output_file}")
