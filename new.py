import pyodbc
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from typing import Dict, List, Optional
import logging
from datetime import datetime
import os
import math


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
def main():
    # Secure configuration management recommended
    snowflake_config = {
        'user': os.getenv('SNOWFLAKE_USER', 'ChinmaySnow'),
        'password': os.getenv('SNOWFLAKE_PASSWORD', 'Chinmay@11r'),
        'account': os.getenv('SNOWFLAKE_ACCOUNT', 'xt19268.ap-southeast-1'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        'database': os.getenv('SNOWFLAKE_DATABASE', 'SSMS_DATA'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
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
            server_name='LAPTOP-UTGONT28\\MSSQLSERVER01',
            database_name='firstDB',
            query=query,
            target_table='DEMO'
        )
        print(f"Data Transfer Summary: {result}")

    except Exception as e:
        print(f"ETL Process Failed: {e}")


if __name__ == "__main__":
    main()
