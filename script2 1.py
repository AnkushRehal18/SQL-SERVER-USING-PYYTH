import pyodbc
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from typing import Dict, List
import os
from datetime import datetime


class SSMSToSnowflakeETL:
    def __init__(self, snowflake_config: Dict):
        self.snowflake_config = snowflake_config

    def connect_to_ssms(self, server_name: str, database_name: str) -> pyodbc.Connection:
        conn_str = f'Driver={{SQL Server}};Server={server_name};Database={database_name};Trusted_Connection=yes;'
        return pyodbc.connect(conn_str)

    def connect_to_snowflake(self) -> snowflake.connector.SnowflakeConnection:
        return snowflake.connector.connect(**self.snowflake_config)

    def standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names to prevent issues with Snowflake
        """
        # Convert column names to uppercase and replace spaces/special characters
        df.columns = [col.upper().replace(' ', '_').replace('-', '_')
                      for col in df.columns]
        return df

    def get_column_definitions(self, df: pd.DataFrame) -> str:
        """
        Generate Snowflake column definitions based on DataFrame dtypes
        """
        type_mapping = {
            'int64': 'NUMBER',
            'float64': 'FLOAT',
            'datetime64[ns]': 'TIMESTAMP',
            'bool': 'BOOLEAN',
            'object': 'VARCHAR(16777216)'
        }

        columns = []
        for col_name, dtype in df.dtypes.items():
            sf_type = type_mapping.get(str(dtype), 'VARCHAR(16777216)')
            # Use uppercase column names and wrap in quotes
            col_name = col_name.upper().replace(' ', '_').replace('-', '_')
            columns.append(f'{col_name} {sf_type}')

        return ', '.join(columns)

    def create_snowflake_table(self, snow_conn: snowflake.connector.SnowflakeConnection,
                               table_name: str, column_definitions: str) -> None:
        """
        Create Snowflake table if it doesn't exist
        """
        # Drop the existing table if it exists
        drop_table_sql = f"DROP TABLE IF EXISTS {table_name}"
        create_table_sql = f"""
        CREATE TABLE {table_name} (
            {column_definitions}
        )
        """
        with snow_conn.cursor() as cursor:
            cursor.execute(drop_table_sql)
            cursor.execute(create_table_sql)

    def extract_load_data(self, server_name: str, database_name: str, query: str,
                          target_table: str) -> None:
        """
        Main ETL process
        """
        try:
            # Extract from SSMS
            print(f"Connecting to SSMS database: {database_name}")
            ssms_conn = self.connect_to_ssms(server_name, database_name)
            df = pd.read_sql(query, ssms_conn)
            print(f"Extracted {len(df)} rows from SSMS")

            # Standardize column names
            df = self.standardize_column_names(df)
            print("Column names after standardization:", df.columns.tolist())

            # Connect to Snowflake
            print("Connecting to Snowflake")
            snow_conn = self.connect_to_snowflake()

            # Create table if it doesn't exist
            column_definitions = self.get_column_definitions(df)
            print(f"Creating table with columns: {column_definitions}")
            self.create_snowflake_table(snow_conn, target_table, column_definitions)

            # Load data into Snowflake
            print(f"Loading data into Snowflake table: {target_table}")
            success, nchunks, nrows, _ = write_pandas(
                conn=snow_conn,
                df=df,
                table_name=target_table,
                database=self.snowflake_config['database'],
                schema=self.snowflake_config['schema']
            )

            print(f"Successfully loaded {nrows} rows in {nchunks} chunks")

        except Exception as e:
            print(f"Error during ETL process: {str(e)}")
            raise
        finally:
            # Close connections
            if 'ssms_conn' in locals():
                ssms_conn.close()
            if 'snow_conn' in locals():
                snow_conn.close()


# Example usage
if __name__ == "__main__":
    # Snowflake connection configuration
    snowflake_config = {
        'user': 'ChinmaySnow',
        'password': 'Chinmay@11r',
        'account': 'xt19268.ap-southeast-1',
        'warehouse': 'COMPUTE_WH',
        'database': 'SSMS_DATA',
        'schema': 'PUBLIC'
    }

    # Initialize ETL class
    etl = SSMSToSnowflakeETL(snowflake_config)

    # Example SQL query
    query = "SELECT * FROM [export-18]"

    # Run ETL process
    etl.extract_load_data(
        server_name='LAPTOP-UTGONT28\\MSSQLSERVER01',
        database_name='firstDB',
        query=query,
        target_table='DEMO'
    )
