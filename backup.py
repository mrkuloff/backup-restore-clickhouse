import clickhouse_connect
from datetime import datetime
import sys
from dotenv import load_dotenv
import urllib3

urllib3.disable_warnings()
# Load environment variables from a `.env` file
load_dotenv()
s3_access_id = ""
s3_secret_key = ""
s3_url = ""
pods=[''] #''
host = "localhost"
username = "default"
password = ""

# Get backup type
# Get backup current time
current_time = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")

non_tables = ['COLUMNS', 'KEY_COLUMN_USAGE', 'REFERENTIAL_CONSTRAINTS', 'SCHEMATA', 'STATISTICS', 'TABLES',
              'VIEWS', 'columns', 'key_column_usage', 'referential_constraints', 'schemata', 'statistics',
              'tables', 'views', 'COLUMNS', 'KEY_COLUMN_USAGE', 'REFERENTIAL_CONSTRAINTS', 'SCHEMATA',
              'STATISTICS', 'TABLES', 'VIEWS', 'columns', 'key_column_usage', 'referential_constraints',
              'schemata', 'statistics', 'tables', 'views', 'mysql_binlogs']

for pod in pods:
    # create connection to dbs on different hosts
    client = clickhouse_connect.get_client(host=host, username=username, password=password)

    # list of non system databases in clickhouse
    result = "\n".join(
        client.command(f"SELECT table_name, table_schema FROM INFORMATION_SCHEMA.TABLES  WHERE table_schema !='system'"
                       f" AND table_catalog not in ('INFORMATION_SCHEMA', 'information_schema')")).split('\n')
    tables = result[::2]
    databases = result[1::2]
    db_tables = list(zip(databases, tables))
    db_tables = [x for x in db_tables if x[1] not in non_tables]
    print("List of database tables to backup:")
    print(db_tables)

    # For each db found create a backup if no specific dbs have been parsed
    for table in db_tables:
        client.command(f"BACKUP TABLE {table[0]}.{table[1]} TO S3('{s3_url}/backup-test-clickhouse/{pod}/{current_time}/"
                       f"{current_time}__{table[0]}___{table[1]}',"
                       f" '{s3_access_id}', '{s3_secret_key}')")
