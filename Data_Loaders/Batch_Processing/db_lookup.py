from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
from typing import Optional

def fetch_lookup_dataframe(
    mysql_conn_id: str,
    table_name: str,
    source_id: int
) -> Optional[pd.DataFrame]:
    """
    Fetch lookup values from a specific table and return them as a pandas DataFrame.

    Parameters:
    mysql_conn_id (str): The Airflow MySQL connection ID (e.g., 'mysql_project_connection').
    table_name (str): The name of the table to query (e.g., 'CSD_SUPPORT_AREAS', 'CSD_AGENTS', 'CSD_CUSTOMER_TYPES').
    source_id (int): The SOURCE_ID to filter the query.

    Returns:
    Optional[pd.DataFrame]: A pandas DataFrame with the table's ID and name/code column, or None if no data is found.
    """
    # Define column mappings for each table
    table_columns = {
        'CSD_SUPPORT_AREAS': {
            'id_column': 'SUPPORT_AREA_ID',
            'value_column': 'SUPPORT_AREA_NAME'
        },
        'CSD_AGENTS': {
            'id_column': 'AGENT_ID',
            'value_column': 'PSEUDO_CODE'
        },
        'CSD_CUSTOMER_TYPES': {
            'id_column': 'CUSTOMER_TYPE_ID',
            'value_column': 'CUSTOMER_TYPE_NAME'
        }
    }

    # Validate table name
    if table_name not in table_columns:
        raise ValueError(f"Unsupported table: {table_name}. Supported tables: {list(table_columns.keys())}")

    # Get column names for the specified table
    id_column = table_columns[table_name]['id_column']
    value_column = table_columns[table_name]['value_column']

    # Initialize MySqlHook with the provided connection ID
    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)

    # Construct the query
    query = f"""
    SELECT {id_column}, {value_column}
    FROM {table_name}
    WHERE SOURCE_ID = %s
    AND ACTIVE_FLAG = 1
    """

    # Execute the query and fetch results as a pandas DataFrame
    try:
        df = mysql_hook.get_pandas_df(sql=query, parameters=(source_id,))
        if df.empty:
            return None
        return df
    except Exception as e:
        raise Exception(f"Failed to fetch lookup DataFrame for {table_name}: {str(e)}")