import psycopg
import csv
import openpyxl
#from postgis_data_checks import DataChecker

# Database connection parameters
DB_PARAMS = {
    'dbname': 'topo',
    'user': 'postgres',
    'password': 'landinformation',
    'host': 'localhost',
    'port': 5432
}

class DataChecker:
    def __init__(self, db_params):
        self.db_params = db_params
        self.conn = None
        self.connect()

    def connect(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg.connect(**self.db_params)

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()

    def field_exists(self, schema, table, field):
        query = """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = %s
                    AND table_name = %s
                    AND column_name = %s
            )
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (schema, table, field))
            return cur.fetchone()[0]

    def query_classification_macronated_null(self, schema, table):
        query = f"""
            SELECT feature_type, macronated, COUNT(*) as count
            FROM {schema}.{table}
            WHERE macronated IS NULL
            GROUP BY feature_type, macronated
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()
        
    def query_classification_macronated_name_null(self, schema, table):
        query = f"""
            SELECT feature_type, macronated, COUNT(*) as count
            FROM {schema}.{table}
            WHERE macronated IS NULL AND name IS NOT NULL
            GROUP BY feature_type, macronated
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()

    def query_classification_name_null(self, schema, table):
        query = f"""
            SELECT feature_type, name, COUNT(*) as count
            FROM {schema}.{table}
            WHERE name IS NULL
            GROUP BY feature_type, name
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()
        
    def query_classifications(self, schema, table):
        query = f"""
            SELECT theme, feature_type, object_name, COUNT(*) as count
            FROM {schema}.{table}
            GROUP BY theme, feature_type, object_name
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()

    def count_records(self, schema, table_name):
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
            count = cur.fetchone()[0]
        return count
    
    def get_schema_tables_list(self, schema_tables: str):
        with open(schema_tables, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            next(reader)  # Skip header row
            tables = []
            for row in reader:
                schema, table = row[0], row[1]
                tables.append((schema, table))
        return tables

    def write_record_count_to_excel(self, ws, schema, table_name):
        ws.append(["Table", "Record Count"])
        ws.append([table_name, checker.count_records(schema, table_name)])
        ws.append([])  # Blank row for separation

    def write_macronated_results(self, schema, table_name, ws):
        field = "macronated"
        exists = self.field_exists(schema, table_name, field)

        if exists:
            results = self.query_classification_macronated_null(schema, table_name)
            if len(results) == 0:
                ws.append([f"No records with {field} NULL in {table_name}"])
            else:
                ws.append([f"Records with {field} NULL in {table_name}"])
                ws.append(["schema", "table_name","feature_type", "Macronated", "Count"])
                for feature_type, macronated, count in results:
                    ws.append([schema, table_name,feature_type, "NULL", count])

            ws.append([])  # Blank row for separation
            results = self.query_classification_macronated_name_null(schema, table_name)
            if len(results) == 0:
                ws.append([f"No records with {field} NULL and name not NULL in {table_name}"])
            else:
                ws.append([f"Records with {field} NULL and name not NULL in {table_name}"])
                ws.append(["schema", "table_name","feature_type", "Macronated", "Count"])
                for feature_type, macronated, count in results:
                    ws.append([schema, table_name,feature_type, "NULL", count])
        else:
            ws.append([f"Field '{field}' does not exist in {table_name}"])
        ws.append([])  # Blank row for separation

    def write_name_results(self, schema, table_name, ws):
        field = "name"
        exists = self.field_exists(schema, table_name, field)

        if exists:
            results = self.query_classification_macronated_name_null(schema, table_name)
            if len(results) == 0:
                ws.append([f"No records with {field} NULL in {table_name}"])
            else:
                ws.append([f"Records with {field} NULL in {table_name}"])
                ws.append(["schema", "table_name","feature_type", "Name", "Count"])
                for feature_type, name, count in results:
                    ws.append([schema, table_name,feature_type, "NULL", count])
        else:
            ws.append([f"Field '{field}' does not exist in {table_name}"])
        ws.append([])  # Blank row for separation

    def write_classications_results(self, schema, table_name, ws):

        results = self.query_classifications(schema, table_name)
        if len(results) == 0:
            ws.append([f"No records with classiciations in {table_name}"])
        else:
            ws.append([f"Classifications in {table_name}"])
            ws.append(["schema", "table_name","theme", "feature_type", "object_name", "Count"])
            for theme, feature_type, object_name, count in results:
                ws.append([schema, table_name, theme, feature_type, object_name, count])
        ws.append([])  # Blank row for separation


    def run(self, schema_tables):
        try:
            # Create a new Excel workbook and add a sheet with the table name
            wb = openpyxl.Workbook()

        
            tables = checker.get_schema_tables_list(schema_tables)

            for schema, table_name in tables:
                print(schema, table_name)
                
                wb.create_sheet(title=table_name)
                ws = wb[table_name]

                # Write record count to Excel
                checker.write_record_count_to_excel(ws, schema, table_name)
                checker.write_macronated_results(schema, table_name, ws)
                checker.write_name_results(schema, table_name, ws)
                checker.write_classications_results(schema, table_name, ws)

        finally:
            ws = wb["Sheet"]
            wb.remove(ws)  
            wb.save(r"C:\Data\Model\data_checks.xlsx")
        wb.close()
        checker.close()

if __name__ == "__main__":
    checker = DataChecker(DB_PARAMS)
    schema_tables = r"C:\Data\Model\schema_tables.csv"
    
    checker.run(schema_tables)