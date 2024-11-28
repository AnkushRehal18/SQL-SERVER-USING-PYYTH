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
        """
        Initialize ETL process with configurable parameters.

        Args:
            snowflake_config (Dict): Snowflake connection configuration
            chunk_size (int, optional): Number of rows to process in each chunk. Defaults to 100,000.
            log_dir (str, optional): Directory for log files. Creates logs if specified.
        """
        self.snowflake_config = snowflake_config
        self.chunk_size = chunk_size
        
        # Setup logging
        self.logger = self._setup_logging(log_dir)

    def _setup_logging(self, log_dir: Optional[str] = None) -> logging.Logger:
        """
        Configure logging with optional file logging.

        Args:
            log_dir (str, optional): Directory to store log files

        Returns:
            logging.Logger: Configured logger
        """
        logger = logging.getLogger('SSMSToSnowflakeETL')
        logger.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s: %(message)s'))
        logger.addHandler(console_handler)

        # File handler if log_dir is provided
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
            log_file = os.path.join(log_dir, f'etl_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s: %(message)s'))
            logger.addHandler(file_handler)

        return logger

    def connect_to_ssms(self, server_name: str, database_name: str, 
                         timeout: int = 30) -> pyodbc.Connection:
        """
        Establish a connection to SQL Server with configurable timeout.

        Args:
            server_name (str): SQL Server instance name
            database_name (str): Target database name
            timeout (int, optional): Connection timeout in seconds. Defaults to 30.

        Returns:
            pyodbc.Connection: Database connection
        """
        try:
            conn_str = (
                f'Driver={{SQL Server}};'
                f'Server={server_name};'
                f'Database={database_name};'
                f'Trusted_Connection=yes;'
                f'Connection Timeout={timeout};'
            )
            return pyodbc.connect(conn_str)
        except pyodbc.Error as e:
            self.logger.error(f"SSMS Connection Error: {e}")
            raise

    def connect_to_snowflake(self) -> snowflake.connector.SnowflakeConnection:
        """
        Establish a connection to Snowflake with robust error handling.

        Returns:
            snowflake.connector.SnowflakeConnection: Snowflake database connection
        """
        try:
            return snowflake.connector.connect(**self.snowflake_config)
        except snowflake.connector.errors.ProgrammingError as e:
            self.logger.error(f"Snowflake Connection Error: {e}")
            raise

    def standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names to prevent Snowflake compatibility issues.

        Args:
            df (pd.DataFrame): Input DataFrame

        Returns:
            pd.DataFrame: DataFrame with standardized column names
        """
        df.columns = [
            col.upper()
            .replace(' ', '_')
            .replace('-', '_')
            .replace('.', '_')
            .replace('(', '')
            .replace(')', '')
            for col in df.columns
        ]
        return df

    def get_column_definitions(self, df: pd.DataFrame) -> str:
        """
        Generate Snowflake column definitions dynamically.

        Args:
            df (pd.DataFrame): Input DataFrame

        Returns:
            str: SQL column definitions
        """
        type_mapping = {
            'int64': 'NUMBER',
            'float64': 'FLOAT',
            'datetime64[ns]': 'TIMESTAMP_NTZ',
            'bool': 'BOOLEAN',
            'object': 'VARCHAR(16777216)'
        }

        columns = [
            f'"{col.upper()}" {type_mapping.get(str(dtype), "VARCHAR(16777216)")}'
            for col, dtype in df.dtypes.items()
        ]
        return ', '.join(columns)

    def extract_load_data(
        self, 
        server_name: str, 
        database_name: str, 
        query: str,
        target_table: str,
        create_table: bool = True
    ) -> Dict[str, int]:
        """
        Comprehensive ETL process with chunk-based loading for large datasets.

        Args:
            server_name (str): SQL Server instance name
            database_name (str): Source database name
            query (str): SQL query to extract data
            target_table (str): Snowflake target table name
            create_table (bool, optional): Whether to create/recreate table. Defaults to True.

        Returns:
            Dict[str, int]: Summary of data transfer
        """
        ssms_conn = None
        snow_conn = None
        total_rows_transferred = 0
        total_chunks = 0

        try:
            # Connect to source and target databases
            ssms_conn = self.connect_to_ssms(server_name, database_name)
            snow_conn = self.connect_to_snowflake()

            # Get total rows for progress tracking
            count_query = f"SELECT COUNT(*) as total FROM ({query}) subquery"
            total_rows = pd.read_sql(count_query, ssms_conn).iloc[0]['total']
            self.logger.info(f"Total rows to process: {total_rows}")

            # Chunk processing
            chunks = math.ceil(total_rows / self.chunk_size)
            
            for i in range(chunks):
                # Modify query to chunk data
                offset = i * self.chunk_size
                chunked_query = f"""
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as row_num 
                    FROM ({query}) subquery
                ) numbered_query
                WHERE row_num BETWEEN {offset + 1} AND {offset + self.chunk_size}
                """

                # Extract chunk
                df = pd.read_sql(chunked_query, ssms_conn)
                
                if df.empty:
                    break

                # Standardize column names
                df = self.standardize_column_names(df)

                # Create table on first iteration if required
                if create_table and i == 0:
                    column_definitions = self.get_column_definitions(df)
                    self._create_snowflake_table(snow_conn, target_table, column_definitions)
                    create_table = False  # Prevent recreation

                # Load chunk to Snowflake
                success, nchunks, nrows, _ = write_pandas(
                    conn=snow_conn,
                    df=df,
                    table_name=target_table,
                    database=self.snowflake_config['database'],
                    schema=self.snowflake_config['schema']
                )

                total_rows_transferred += nrows
                total_chunks += 1

                self.logger.info(f"Chunk {i+1}/{chunks}: Transferred {nrows} rows")

            return JsonResponse({"msg": "passed 465"})

            return {
                'total_rows': total_rows_transferred,
                'total_chunks': total_chunks
            }

        except Exception as e:
            self.logger.error(f"ETL Process Error: {e}")
            raise
        finally:
            # Ensure connections are closed
            if ssms_conn:
                ssms_conn.close()
            if snow_conn:
                snow_conn.close()

    def _create_snowflake_table(
        self, 
        snow_conn: snowflake.connector.SnowflakeConnection,
        table_name: str, 
        column_definitions: str
    ) -> None:
        """
        Create or replace Snowflake table with specified column definitions.

        Args:
            snow_conn (snowflake.connector.SnowflakeConnection): Snowflake connection
            table_name (str): Target table name
            column_definitions (str): SQL column definitions
        """
        create_table_sql = f"""
        CREATE OR REPLACE TABLE {table_name} (
            {column_definitions}
        )
        """
        
        with snow_conn.cursor() as cursor:
            cursor.execute(create_table_sql)
        
        self.logger.info(f"Table {table_name} created successfully")


# Example usage with enhanced configuration
@csrf_exempt
def load_to_snowflake(request):
    data = json.loads(request.body)

    # Secure configuration management recommended
    snowflake_config = {
        'user': data.get('sfUsername'),
        'password': data.get('sfPassword'),
        'account': data.get('sfAccount'),
        'warehouse': data.get('sfWarehouse'),
        'database': data.get('ssmsDatabase'),
        'schema': data.get('selectedSchema'),
    }

    # Initialize ETL with logging and chunk size
    etl = SSMSToSnowflakeETL(
        snowflake_config, 
        chunk_size=50000,  # Adjust based on your dataset and performance
        log_dir='./etl_logs'
    )


    # Example query
    query = "SELECT * FROM [export-18]"

    try:
        result = etl.extract_load_data(
            server_name=data.get('ssmsServerName'),
            database_name=data.get('ssmsDatabase'),
            query=query,
            target_table=data.get('userSelectedTable')
        )
        print(f"Data Transfer Summary: {result}")
        return JsonResponse({"msg": "passed"})

    except Exception as e:
        print(f"ETL Process Failed: {e}")
        return JsonResponse({"msg": f"ETL Process Failed: {e}"})


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


