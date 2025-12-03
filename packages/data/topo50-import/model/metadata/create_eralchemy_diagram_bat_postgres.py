# this creates a .bat file to run eralchemy to create an ER diagram from the database
# can then be run from command line
# for formatting this creates one big file and then each table
import os
import sqlite3

#Note run this file from the folder above the local database folder
parent_folder = r"C:\Data\toposource"
database = "topographic-data/topographic-data.gpkg"
diagrams = "model-diagrams"
output_bat_file = os.path.join(parent_folder, "create_eralchemy_diagrams.bat")
connection = f'eralchemy -i sqlite:///{database} '

output_diagrams = f'-o {os.path.join(parent_folder,diagrams)}'
db_path = os.path.join(parent_folder, database)
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get list of tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

# Print table names
print("Tables in the database:")
exclude = ''
for table in tables:
    if table[0].startswith('gpkg') or table[0].startswith('rtree') or table[0].startswith('sqlite_sequence'):
        exclude += table[0] + ' '
        continue

    print(table[0])
    output_string = f'{connection} --include-tables {table[0]} {output_diagrams}\\svg\\{table[0]}.svg\n'
    output_string2 = f'{connection} --include-tables {table[0]} {output_diagrams}\\png\\{table[0]}.png\n'
    with open(output_bat_file, 'a') as f:
        f.write(output_string)
        f.write(output_string2)

conn.close()

output_string = f'{connection} --exclude-tables {exclude} {output_diagrams}\\full_database.svg\n'
output_string2 = f'{connection} --exclude-tables {exclude} {output_diagrams}\\full_database.pdf\n'
output_string3 = f'{connection} --exclude-tables {exclude} {output_diagrams}\\full_database.er\n'
with open(output_bat_file, 'a') as f:
    f.write(output_string)
    f.write(output_string2)
    f.write(output_string3)
