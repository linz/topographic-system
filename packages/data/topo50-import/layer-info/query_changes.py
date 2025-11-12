import psycopg
import csv

# Database connection parameters
DB_PARAMS = {
    'dbname': 'topo',
    'user': 'postgres',
    'password': 'landinformation',
    'host': 'localhost',
    'port': 5432
}

# SQL query as a string
query = """
SELECT event_id, schema_name, table_name, relid, session_user_name, action_tstamp_tx, action_tstamp_stm, action_tstamp_clk, transaction_id, application_name, client_addr, client_port, client_query, action, row_data, changed_fields, statement_only
FROM audit.logged_actions WHERE client_query NOT LIKE 'SELECT%';
"""

# Connect to the database and execute the query
with psycopg.connect(**DB_PARAMS) as conn:
    with conn.cursor() as cur:
        cur.execute(query)
        results = cur.fetchall()
        # Print results or process as needed
        for row in results:
            event_id = row[0]
            schema_name = row[1]
            table_name = row[2]
            relid = row[3]
            session_user_name = row[4]
            action_tstamp_tx = row[5]
            action_tstamp_stm = row[6]
            action_tstamp_clk = row[7]
            transaction_id = row[8]
            application_name = row[9]
            client_addr = row[10]
            client_port = row[11]
            client_query = row[12]
            action = row[13]
            row_data = row[14]
            changed_fields = row[15]
            statement_only = row[16]
            #print(f"event_id: {event_id}, schema_name: {schema_name}, table_name: {table_name}, relid: {relid}, session_user_name: {session_user_name}, action_tstamp_tx: {action_tstamp_tx}, action_tstamp_stm: {action_tstamp_stm}, action_tstamp_clk: {action_tstamp_clk}, transaction_id: {transaction_id}, application_name: {application_name}, client_addr: {client_addr}, client_port: {client_port}, client_query: {client_query}, action: {action}, row_data: {row_data}, changed_fields: {changed_fields}, statement_only: {statement_only}")

            print("--------------------------------------------------")
            print(client_query)
            query = client_query.replace('"', "")
            print(query)