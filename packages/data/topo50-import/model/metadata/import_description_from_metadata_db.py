#code example
import sqlite3

def get_object_description(conn, name):
    cursor = conn.cursor()
    
    # Execute the SQL query
    cursor.execute("""
    SELECT
        o.id AS objectclass_id,
        o.name,
        o.entityclass,
        o.objectinheritance AS geometry_type,
        d.text AS description_text
    FROM objectclass o
    LEFT JOIN description d ON d.objectclass_id = o.id
    WHERE d.type = 'description'
    AND o.name = ?;
    """, (name,))
    
    return cursor.fetchall()
if __name__ == "__main__":
    # Path to the SQLite database
    catalog_db = r"C:\Data\repos\topographic-system\packages\data\topo50-import\model\metadata\catalog.db"

    # Connect to the SQLite database
    conn = sqlite3.connect(catalog_db)
    results = get_object_description(conn, 'airport_pnt')

    # Loop through the results
    for row in results:
            objectclass_id, name, entityclass, geometry_type, description_text = row
            print(f"ID: {objectclass_id}, Name: {name}, Description: {description_text}")

    # Close the connection
    conn.close()

