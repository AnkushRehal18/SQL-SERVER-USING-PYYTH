from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import json
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pypyodbc as odbc
from typing import Dict, List, Optional
import logging
from datetime import datetime
import pyodbc
import pandas as pd
import os
import math
from sqlalchemy import create_engine, text



logger = logging.getLogger(__name__)
GLOBAL_VARS = {}
@csrf_exempt
def snowflake_login(request):
    logger.debug(f"Request method: {request.method}")
    global GLOBAL_VARS

    if request.method == "POST":

        try:
            # Parse the request body
            data = json.loads(request.body)
            username = data.get("username")
            password = data.get("password")
            account = data.get("account")
            warehouse = data.get("warehouse")
            selected_database = data.get("selected_database")  # New field
            selectedSchema = data.get("selectedSchema")
            selectedTable1 = data.get("selectedTable")

            # Validate input fields
            missing_fields = [
                field for field in ["username", "password", "account", "warehouse"]
                if not data.get(field)
            ]
            if missing_fields:
                return JsonResponse(
                    {"message": f"Missing required fields: {', '.join(missing_fields)}"}, status=400
                )
            
            GLOBAL_VARS["snowflake_connection"]= {
                "user":username,
                "password":password,
                "account":account,
                "warehouse":warehouse
            }

            # Connect to Snowflake
            conn = snowflake.connector.connect(
                user=username,
                password=password,
                account=account,
                warehouse=warehouse
            )
            cursor = conn.cursor()

            if selected_database:
                # Fetch schemas for the selected database
                cursor.execute(f"SHOW SCHEMAS IN DATABASE {selected_database}")
                schemas = cursor.fetchall()
                schema_names = [row[1] for row in schemas]
                conn.close()
                return JsonResponse(
                    {"message": "Schemas fetched successfully!", 
                    "schemas": schema_names}, status=200
                )
            else:
                # Fetch the list of databases
                cursor.execute("SHOW DATABASES")
                databases = cursor.fetchall()
                database_names = [row[1] for row in databases]
                conn.close()
                return JsonResponse(
                    {"message": "Login successful!", "databases": database_names}, status=200
                )
            
        
        except snowflake.connector.errors.DatabaseError as e:
            error_code = e.errno
            error_message = str(e)

            # Handle specific Snowflake errors
            if error_code == 2002:
                return JsonResponse({"message": "Authentication failed. Check your username or password."}, status=401)
            elif error_code == 250001:
                return JsonResponse({"message": "Connection timeout. Verify your account details and network."}, status=408)
            else:
                return JsonResponse({"message": f"Snowflake error: {error_message}"}, status=400)

        except json.JSONDecodeError:
            return JsonResponse({"message": "Invalid JSON format in request body."}, status=400)

        except KeyError as e:
            return JsonResponse({"message": f"Missing key in request: {str(e)}"}, status=400)

        except Exception as e:
            return JsonResponse({"message": f"An unexpected error occurred: {str(e)}"}, status=500)

    return JsonResponse({"message": "Invalid request method. Use POST."}, status=405)
    



@csrf_exempt
def SSMS_Login_And_FetchData(request):
    global GLOBAL_VARS

    print("SSMS_Login_And_FetchData called at 77")
    if request.method == 'POST':
        try:
            # Parse JSON input from the frontend
            data = json.loads(request.body)
            print('data at 82',data)

            # Extract parameters
            server_name = data.get('server_name')
            username = data.get('username')
            password = data.get('password')
            selected_database = data.get('selected_database')
            selected_table = data.get('selected_table')

            # Handle login request and fetch databases
            if server_name and username and password:
                # Connection string for the 'master' database
                connection_string = f"""
                    DRIVER={{SQL SERVER}};
                    SERVER={server_name};
                    DATABASE=master;
                    UID={username};
                    PWD={password};
                    Trust_Connection=no;
                """
                try:
                    conn = odbc.connect(connection_string)
                    cursor = conn.cursor()

                    # Fetch all databases
                    cursor.execute("SELECT name FROM sys.databases")
                    databases = [db[0] for db in cursor.fetchall()]


                    # GLOBAL_VARS["ssms_connection"]={
                    #     "server_name":server_name,
                    #     "usename":username,
                    #     "password":password,
                    #     "selected_database":selected_database
                    # }

                    # Close the connection
                    cursor.close()
                    conn.close()

                    # Store connection details in the session
                    request.session['connection_details'] = {
                        'server_name': server_name,
                        'username': username,
                        'password': password
                    }

                    if not selected_database:
                            return JsonResponse({
                                'success': True,
                                'message': 'Login successful!',
                                'databases': databases,
                            })
                    

                except odbc.Error as e:
                    return JsonResponse({'error': f'Login failed: {str(e)}'}, status=401)

            # Check for valid session if login is not part of the request
            connection_details = request.session.get('connection_details')
            if not connection_details:
                return JsonResponse({'error': 'No valid connection found. Please log in first.'}, status=400)

            # Handle database selection to fetch tables or data
            print('134',selected_database)
            if selected_database:
                # Connection string for the selected database
                print('136',selected_database)
                connection_string = f"""
                    DRIVER={{SQL SERVER}};
                    SERVER={connection_details['server_name']};
                    DATABASE={selected_database};
                    UID={connection_details['username']};
                    PWD={connection_details['password']};
                    Trust_Connection=no;
                """
                try:
                    conn = odbc.connect(connection_string)
                    cursor = conn.cursor()

                    if not selected_table:
                        # Fetch all tables in the selected database
                        cursor.execute("""
                            SELECT TABLE_NAME
                            FROM INFORMATION_SCHEMA.TABLES
                            WHERE TABLE_TYPE = 'BASE TABLE'
                        """)
                        tables = [table[0] for table in cursor.fetchall()]

                        cursor.close()
                        conn.close()
                        return JsonResponse({
                            'database': selected_database,
                            'tables': tables
                        })
                    else:
                        # Validate if the table exists
                        cursor.execute("""
                            SELECT TABLE_NAME
                            FROM INFORMATION_SCHEMA.TABLES
                            WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = ?
                        """, (selected_table,))
                        table_exists = cursor.fetchone()

                        if not table_exists:
                            return JsonResponse({'error': f"Table '{selected_table}' does not exist."}, status=400)

                        # Fetch data from the selected table
                        cursor.execute(f"SELECT * FROM {selected_table}")
                        rows = cursor.fetchall()
                        columns = [column[0] for column in cursor.description]
                        data = [dict(zip(columns, row)) for row in rows]

                        cursor.close()
                        conn.close()
                        return JsonResponse({
                            'columns': columns,
                            'data': data
                        })

                except odbc.Error as e:
                    return JsonResponse({'error': f'Error fetching data: {str(e)}'}, status=500)

            return JsonResponse({'error': 'No login data or fetch data parameters provided.'}, status=400)

        except json.JSONDecodeError:
            return JsonResponse({'error': 'Invalid JSON format.'}, status=400)
        except Exception as e:
            return JsonResponse({'error': str(e)}, status=500)

    return JsonResponse({'error': 'Invalid request method.'}, status=405)


### ------------------------------load functionality here


class SSMSToSnowflakeETL:
    """
    A robust ETL class for migrating data from SQL Server to Snowflake
    with support for large datasets and advanced error handling.
    """
 
    def __init__(self, snowflake_config: Dict, chunk_size: int = 100000,
                 log_dir: Optional[str] = None):
        self.snowflake_config = snowflake_config
        self.chunk_size = chunk_size
        self.logger = self._setup_logging(log_dir)
 
    def _setup_logging(self, log_dir: Optional[str] = None) -> logging.Logger:
        """
        Configure logging with optional file logging.
        """
        logger = logging.getLogger('SSMSToSnowflakeETL')
        logger.setLevel(logging.INFO)
       
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s: %(message)s'))
        logger.addHandler(console_handler)
 
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
            log_file = os.path.join(log_dir, f'etl_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s: %(message)s'))
            logger.addHandler(file_handler)
 
        return logger
 
    def connect_to_ssms(self, server_name: str, database_name: str) -> create_engine:
        """
        Establish a connection to SQL Server using SQLAlchemy.
        """
        try:
            conn_url = f"mssql+pyodbc://{server_name}/{database_name}?driver=SQL+Server"
            print(f"Connecting to SSMS with URL: {conn_url}")
            return create_engine(conn_url)
        except Exception as e:
            print(f"SSMS Connection Error: {e}")
            self.logger.error(f"SSMS Connection Error: {e}")
            raise
 
    def connect_to_snowflake(self) -> snowflake.connector.SnowflakeConnection:
        """
        Establish a connection to Snowflake.
        """
        try:
            print(f"Connecting to Snowflake with config: {self.snowflake_config}")
            conn = snowflake.connector.connect(**self.snowflake_config)
            with conn.cursor() as cursor:
                cursor.execute(f"USE DATABASE {self.snowflake_config['database']}")
                cursor.execute(f"USE SCHEMA {self.snowflake_config['schema']}")
            print("Snowflake connection established.")
            return conn
        
        except ProgrammingError as e:
            print(f"Snowflake Connection Error: {e}")
            self.logger.error(f"Snowflake Connection Error: {e}")
            raise
 
    def fetch_data_with_sqlalchemy(self, engine, query, offset, chunk_size):
        """
        Fetch data from SQL Server using SQLAlchemy with offset and limit for chunking.
        """
        chunk_query = f"""
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS row_num
            FROM ({query}) subquery
        ) AS paginated
        WHERE row_num > :offset AND row_num <= :offset + :chunk_size
        """
 
        print(f"Executing chunk query: {chunk_query}")
       
        with engine.connect() as connection:
            result = connection.execute(
                text(chunk_query),
                {"offset": offset, "chunk_size": chunk_size}
            )
 
            # Fetch all rows from the result
            rows = result.fetchall()  # Fetching the rows
 
            # Check the raw result
            print(f"Raw result fetched: {rows}")
 
            # If rows are returned, convert them to a list of dicts
            try:
                if rows:
                    # Convert each row to a dictionary
                    data = [dict(zip(result.keys(), row)) for row in rows]
                    print(f"Data converted to list of dictionaries: {data[:2]}")
                else:
                    print("No data in this chunk. Skipping.")
                    data = []
           
            except Exception as e:
                print(f"Error converting result to dict: {e}")
                data = []  # Ensure data is empty in case of error
 
            return data
 
 
 
 
    def get_column_definitions_from_data(self, data: List[Dict], columns: List[str]) -> str:
        """
        Generate Snowflake column definitions dynamically based on data types.
        """
        type_mapping = {
            int: 'NUMBER',
            float: 'FLOAT',
            str: 'VARCHAR(16777216)',
            bool: 'BOOLEAN',
            datetime: 'TIMESTAMP_NTZ'
        }
 
        first_row = data[0]
        print(f"Generating column definitions from first row: {first_row}")
        column_definitions = [
            f'"{col.upper()}" {type_mapping.get(type(first_row[col]), "VARCHAR(16777216)")}'
            for col in columns
        ]
        column_def_str = ', '.join(column_definitions)
        print(f"Generated column definitions: {column_def_str}")
        return column_def_str
 
    def insert_data_to_snowflake(self, snow_conn, target_table, data, columns):
        """
        Insert data into Snowflake using plain SQL.
        """
        insert_query = f"""
        INSERT INTO {target_table} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})
        """
       
        print(f"Insert Query: {insert_query}")
        self.logger.debug(f"Insert Query: {insert_query}")
 
        with snow_conn.cursor() as cursor:
            for idx, row in enumerate(data):
                try:
                    # Convert each row (dict) to a tuple of values in the same order as columns
                    row_values = tuple(row.get(col, None) for col in columns)
                   
                    # Validate row values
                    if len(row_values) != len(columns):
                        raise ValueError(f"Row length mismatch. Expected {len(columns)}, got {len(row_values)}")
                   
                    # Debug the row and its values before execution
                    print(f"Inserting row #{idx}: {row_values}")
                    self.logger.debug(f"Inserting row #{idx}: {row_values}")
                   
                    cursor.execute(insert_query, row_values)
                except Exception as e:
                    print(f"Failed to insert row #{idx}: {row}. Error: {e}")
                    self.logger.error(f"Failed to insert row #{idx}: {row}. Error: {e}")
                    raise
 
    def extract_load_data(
        self,
        server_name: str,
        database_name: str,
        query: str,
        target_table: str,
        create_table: bool = True
    ) -> Dict[str, int]:
        """
        Perform the ETL process: extract data from SSMS and load into Snowflake.
        """
        ssms_engine = None
        snow_conn = None
        total_rows_transferred = 0
        total_chunks = 0
 
        try:
            ssms_engine = self.connect_to_ssms(server_name, database_name)
            snow_conn = self.connect_to_snowflake()
 
            count_query = f"SELECT COUNT(*) as total FROM ({query}) subquery"
            with ssms_engine.connect() as conn:
                total_rows = conn.execute(text(count_query)).scalar()
 
            print(f"Total rows to process: {total_rows}")
            self.logger.info(f"Total rows to process: {total_rows}")
            chunks = math.ceil(total_rows / self.chunk_size)
           
            for i in range(chunks):
                offset = i * self.chunk_size
                print(f"Processing chunk {i+1}/{chunks} with offset {offset}")
                data_chunk = self.fetch_data_with_sqlalchemy(ssms_engine, query, offset, self.chunk_size)
 
                if not data_chunk:
                    print("No data in this chunk. Skipping.")
                    break
 
                columns = list(data_chunk[0].keys())
                print(f"Columns detected: {columns}")
 
                if create_table and i == 0:
                    column_definitions = self.get_column_definitions_from_data(data_chunk, columns)
                    self._create_snowflake_table(snow_conn, target_table, column_definitions)
                    create_table = False
 
                self.insert_data_to_snowflake(snow_conn, target_table, data_chunk, columns)
 
                total_rows_transferred += len(data_chunk)
                total_chunks += 1
                print(f"Chunk {i+1}/{chunks}: Transferred {len(data_chunk)} rows")
                self.logger.info(f"Chunk {i+1}/{chunks}: Transferred {len(data_chunk)} rows")
 
            return {"total_rows": total_rows_transferred, "total_chunks": total_chunks}
 
        except Exception as e:
            print(f"ETL Process Error: {e}")
            self.logger.error(f"ETL Process Error: {e}")
            raise
        finally:
            if ssms_engine:
                ssms_engine.dispose()
            if snow_conn:
                snow_conn.close()
 
    def _create_snowflake_table(self, snow_conn, table_name, column_definitions):
        """
        Create or replace a table in Snowflake.
        """
        if not table_name or not column_definitions:
            raise ValueError("Table name or column definitions are missing.")
       
        create_table_sql = f"CREATE OR REPLACE TABLE {table_name} ({column_definitions})"
        print(f"Creating table with SQL: {create_table_sql}")
        self.logger.info(f"Creating table with SQL: {create_table_sql}")
 
        with snow_conn.cursor() as cursor:
            cursor.execute(create_table_sql)
 
        print(f"Table {table_name} created successfully.")
        self.logger.info(f"Table {table_name} created successfully.")
 
 
@csrf_exempt
def load_to_snowflake(request):
    try:
        data = json.loads(request.body)
 
        # Snowflake configuration
        snowflake_config = {
            'user': data.get('sfUsername'),
            'password': data.get('sfPassword'),
            'account': data.get('sfAccount'),
            'warehouse': data.get('sfWarehouse'),
            'database': data.get('selectedDatabase'),
            'schema': data.get('selectedSchema')
        }
 
        etl = SSMSToSnowflakeETL(
            snowflake_config,
            chunk_size=50000,
            log_dir='./etl_logs'
        )

        # Get the SSMS table name from the request
        ssms_table = data.get('ssmsSelectedTable')
        
        if not ssms_table:
            return JsonResponse({"msg": "SSMS table name is missing."}, status=400)

        # Use the SSMS table dynamically in the query
        query = f"SELECT * FROM {ssms_table}"
        
        # Perform ETL
        result = etl.extract_load_data(
            server_name=data.get('ssmsServerName'),
            database_name=data.get('ssmsDatabase'),
            query=query,
            target_table=data.get('userSelectedTable')
        )
 
        return JsonResponse({
            "msg": "ETL Process Completed",
            "details": {
                "total_rows": result["total_rows"],
                "total_chunks": result["total_chunks"]
            }
        }, status=200)
 
    except Exception as e:
        return JsonResponse({"msg": f"ETL Process Failed: {str(e)}"}, status=500)







### -----------     
# class SSMSToSnowflakeETL:
#     """
#     A robust ETL class for migrating data from SQL Server to Snowflake 
#     with support for large datasets and advanced error handling.
#     """

#     def __init__(self, chunk_size: int = 100000, log_dir: Optional[str] = None):
#         """
#         Initialize ETL process with configurable parameters.

#         Args:
#             chunk_size (int, optional): Number of rows to process in each chunk. Defaults to 100,000.
#             log_dir (str, optional): Directory for log files. Creates logs if specified.
#         """
#         self.chunk_size = chunk_size
#         self.logger = self._setup_logging(log_dir)

#     def _setup_logging(self, log_dir: Optional[str] = None) -> logging.Logger:
#         """
#         Configure logging with optional file logging.

#         Args:
#             log_dir (str, optional): Directory to store log files.

#         Returns:
#             logging.Logger: Configured logger.
#         """
#         logger = logging.getLogger('SSMSToSnowflakeETL')
#         logger.setLevel(logging.INFO)

#         # Console handler
#         console_handler = logging.StreamHandler()
#         console_handler.setLevel(logging.INFO)
#         console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s: %(message)s'))
#         logger.addHandler(console_handler)

#         # File handler if log_dir is provided
#         if log_dir:
#             os.makedirs(log_dir, exist_ok=True)
#             log_file = os.path.join(log_dir, f'etl_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
#             file_handler = logging.FileHandler(log_file)
#             file_handler.setLevel(logging.INFO)
#             file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s: %(message)s'))
#             logger.addHandler(file_handler)

#         return logger

#     def connect_to_snowflake(self) -> snowflake.connector.SnowflakeConnection:
#         """
#         Establish a connection to Snowflake using GLOBAL_VARS.

#         Returns:
#             snowflake.connector.SnowflakeConnection: Snowflake database connection.
#         """
#         global GLOBAL_VARS

#         try:
#             snowflake_connection = GLOBAL_VARS.get("snowflake_connection")
#             if not snowflake_connection:
#                 raise ValueError("Snowflake connection configuration is missing in GLOBAL_VARS.")

#             return snowflake.connector.connect(**snowflake_connection)
#         except snowflake.connector.errors.ProgrammingError as e:
#             self.logger.error(f"Snowflake Connection Error: {e}")
#             raise

#     def _create_snowflake_table(
#         self, 
#         snow_conn: snowflake.connector.SnowflakeConnection,
#         table_name: str, 
#         column_definitions: str
#     ) -> None:
#         """
#         Create or replace Snowflake table with specified column definitions.

#         Args:
#             snow_conn (snowflake.connector.SnowflakeConnection): Snowflake connection.
#             table_name (str): Target table name.
#             column_definitions (str): SQL column definitions.
#         """
#         try:
#             schema = GLOBAL_VARS.get("selectedSchema", "")
#             qualified_table_name = f"{schema}.{table_name}" if schema else table_name

#             create_table_sql = f"""
#             CREATE OR REPLACE TABLE {qualified_table_name} (
#                 {column_definitions}
#             )
#             """
#             with snow_conn.cursor() as cursor:
#                 cursor.execute(create_table_sql)

#             self.logger.info(f"Table {qualified_table_name} created successfully")
#         except Exception as e:
#             self.logger.error(f"Failed to create table {table_name}: {e}")
#             raise

#     def extract_load_data(
#         self, 
#         query: str,
#         target_table: str,
#         create_table: bool = True
#     ) -> Dict[str, int]:
#         """
#         Comprehensive ETL process with chunk-based loading for large datasets.

#         Args:
#             query (str): SQL query to extract data.
#             target_table (str): Snowflake target table name.
#             create_table (bool, optional): Whether to create/recreate table. Defaults to True.

#         Returns:
#             Dict[str, int]: Summary of data transfer.
#         """
#         snow_conn = None
#         total_rows_transferred = 0
#         total_chunks = 0

#         try:
#             # Connect to Snowflake
#             snow_conn = self.connect_to_snowflake()

#             # Chunk processing logic remains unchanged.
#             ...

#         except Exception as e:
#             self.logger.error(f"ETL Process Error: {e}")
#             raise
#         finally:
#             if snow_conn:
#                 snow_conn.close()

#         return {
#             'total_rows': total_rows_transferred,
#             'total_chunks': total_chunks
#         }


#     def _create_snowflake_table(
#         self, 
#         snow_conn: snowflake.connector.SnowflakeConnection,
#         table_name: str, 
#         column_definitions: str
#     ) -> None:
#         """
#         Create or replace Snowflake table with specified column definitions.

#         Args:
#             snow_conn (snowflake.connector.SnowflakeConnection): Snowflake connection
#             table_name (str): Target table name
#             column_definitions (str): SQL column definitions
#         """
#         create_table_sql = f"""
#         CREATE OR REPLACE TABLE {table_name} (
#             {column_definitions}
#         )
#         """
        
#         with snow_conn.cursor() as cursor:
#             cursor.execute(create_table_sql)
        
#         return JsonResponse({"message": f"Table created"})
#         self.logger.info(f"Table {table_name} created successfully")


# @csrf_exempt
# def load_to_snowflake(request):
#     global GLOBAL_VARS

#     # Parse the incoming JSON request body
#     data = json.loads(request.body)

#     # Check if connection details are available in the session or environment variables
#     server_name = data.get('ssmsServerName')
#     username = data.get('ssmsUsername')
#     password = data.get('ssmsPassword')

#     selected_database = data.get('ssmsDatabase')
#     selected_table = data.get('ssmsSelectedTable')

#     if not server_name or not username or not password:
#         return JsonResponse({"message":"Missing environment variables for SSMS connection."})

#     # Populate GLOBAL_VARS for Snowflake
#     GLOBAL_VARS["snowflake_connection"] = {
#         'user': data.get('sfUsername'),
#         'password': data.get('sfPassword'),
#         'account': data.get('sfAccount'),
#         'warehouse': data.get('sfWarehouse'),
#         'database': data.get('selectedDatabase'),
#         'schema': data.get('selectedSchema')
#     }

#     # Populate GLOBAL_VARS for SSMS
#     GLOBAL_VARS["ssms_connection"] = {
#         'server_name': server_name,
#         'username': username,
#         'password': password,
#         'selected_database': selected_database
#     }

#     # Test database connection to fetch tables or data
#     try:
#         connection_string = f"""
#             DRIVER={{SQL SERVER}};
#             SERVER={server_name};
#             DATABASE={selected_database or 'master'};
#             UID={username};
#             PWD={password};
#             Trust_Connection=no;
#         """
#         conn = odbc.connect(connection_string)
#         cursor = conn.cursor()

#         if selected_database:
#             # If a table is selected, validate its existence and fetch data
#             if selected_table:
#                 cursor.execute("""
#                     SELECT TABLE_NAME
#                     FROM INFORMATION_SCHEMA.TABLES
#                     WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = ?
#                 """, (selected_table,))
#                 table_exists = cursor.fetchone()

#                 if not table_exists:
#                     return JsonResponse({"message": f"Table '{selected_table}' does not exist."})

#                 print(f"Table '{selected_table}' found. Proceeding with ETL process.")
#             else:
#                 # Fetch all tables in the database
#                 cursor.execute("""
#                     SELECT TABLE_NAME
#                     FROM INFORMATION_SCHEMA.TABLES
#                     WHERE TABLE_TYPE = 'BASE TABLE'
#                 """)
#                 tables = [table[0] for table in cursor.fetchall()]
#                 print(f"Tables in database '{selected_database}': {tables}")
#         else:
#             # Fetch all databases
#             cursor.execute("SELECT name FROM sys.databases")
#             databases = [db[0] for db in cursor.fetchall()]
#             print(f"Available databases: {databases}")

#         cursor.close()
#         conn.close()
#     except odbc.Error as e:
#         return JsonResponse({"message": f"Error connecting to SSMS: {e}"})

#     # Initialize ETL
#     etl = SSMSToSnowflakeETL(
#         chunk_size=50000,
#         log_dir='./etl_logs'
#     )

#     # Example ETL process
#     query = f"SELECT * FROM {selected_table}" if selected_table else "SELECT TOP 1000 * FROM <some_table>"
#     try:
#         result = etl.extract_load_data(
#             query=query,
#             target_table='DEMO'
#         )
#         print(f"Data Transfer Summary: {result}")
#     except Exception as e:
#         print(f"ETL Process Failed: {e}")

#     return JsonResponse({"message": "fn executed completely"})


