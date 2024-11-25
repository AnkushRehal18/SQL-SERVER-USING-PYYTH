# import pypyodbc as odbc

# # Connection details
# DRIVER_NAME = 'SQL SERVER'
# SERVER_NAME = 'DESKTOP-VL8R6HD'
# DATABASE_NAME = 'master'  # Use the 'master' database to query for all databases

# # Connection string
# connection_string = f"""
#     DRIVER={{{DRIVER_NAME}}};
#     SERVER={SERVER_NAME};
#     DATABASE={DATABASE_NAME};
#     Trust_Connection=yes;
# """

# # Establish the connection
# conn = odbc.connect(connection_string)
# cursor = conn.cursor()

# # Query to get all databases
# cursor.execute("SELECT name FROM sys.databases WHERE state = 0")  # Exclude offline databases
# databases = cursor.fetchall()

# # Dictionary to store tables for each database
# database_tables = {}

# print("Fetching tables from all databases...\n")
# for db in databases:
#     db_name = db[0]
#     try:
#         # Change the database context
#         cursor.execute(f"USE {db_name}")
        
#         # Query to get all tables in the current database
#         cursor.execute("""
#             SELECT TABLE_SCHEMA, TABLE_NAME
#             FROM INFORMATION_SCHEMA.TABLES
#             WHERE TABLE_TYPE = 'BASE TABLE'
#         """)
#         tables = cursor.fetchall()

#         # Store the tables in the dictionary
#         database_tables[db_name] = tables
#         print(f"Database: {db_name} - Tables Found: {len(tables)}")
#     except Exception as e:
#         print(f"Could not fetch tables from database '{db_name}'. Error: {e}")
#         database_tables[db_name] = []

# # Print all tables grouped by database
# for db_name, tables in database_tables.items():
#     print(f"\nDatabase: {db_name}")
#     if tables:
#         for schema, table in tables:
#             print(f"  {schema}.{table}")
#     else:
#         print("  No tables found.")

# # Close the connection
# cursor.close()
# conn.close()



import snowflake.connector
from snowflake.connector.errors import ProgrammingError

# Define your Snowflake connection parameters
snowflake_config = {
    "user": "Ankush18",           # Replace with your Snowflake username
    "password": "123asdQWE",       # Replace with your Snowflake password
    "account": "vl95947.ap-southeast-1",      # Replace with your Snowflake account identifier
    "warehouse": "COMPUTE_WH",     # Optional: Specify your Snowflake warehouse
    "database": "GARDEN_PLANTS",       # Optional: Specify your Snowflake database
    "schema": "VEGGIES"            # Optional: Specify your Snowflake schema
}

try:
    # Create a connection to Snowflake
    conn = snowflake.connector.connect(**snowflake_config)
    print("Connected to Snowflake successfully!")

    # Optionally, execute a test query
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_VERSION()")
    result = cursor.fetchone()
    print(f"Snowflake version: {result[0]}")

    # Fetch data from the "root_depth" table in the "VEGGIES" schema
    cursor.execute("SELECT * FROM VEGGIES.root_depth")  # Replace 'root_depth' with your actual table name if different
    rows = cursor.fetchall()  # Fetch all rows from the table

    # Print the fetched rows
    print("Fetched data from root_depth table:")
    for row in rows:
        print(row)  # This will print each row in the table

except ProgrammingError as e:
    print(f"Failed to connect to Snowflake: {e}")

finally:
    # Make sure to close the cursor and connection
    cursor.close()
    conn.close()
    print("Connection closed.")

