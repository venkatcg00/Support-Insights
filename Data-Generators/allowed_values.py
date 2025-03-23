"""Database lookup utility for fetching allowed values from MySQL tables.

This module provides functions to connect to a MySQL database, fetch distinct values
from a specified column based on a source name, and close the database connection.
It integrates logging to allow the caller to handle log output, and is used by data
generator scripts (e.g., CSV_Data_Generator.py) running under Data_Generator_Orchestrator.py.

Example:
    >>> import logging
    >>> from allowed_values import connect_to_database, fetch_allowed_values
    >>> logger = logging.getLogger("my_app")
    >>> engine = connect_to_database("mysql", "3306", "csd_db", "csd_user", "csd_pass", logger)
    >>> values = fetch_allowed_values(logger, engine, "CSD_SUPPORT_AREAS", "'AT&T'", "SUPPORT_AREA_NAME")
    >>> print(values)
"""

from typing import List
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import logging
import os
from urllib.parse import quote_plus

def connect_to_database(
    db_host: str,
    db_port: str,
    db_name: str,
    db_user: str,
    db_password: str,
    logger: logging.Logger
) -> Engine:
    """Establish a connection to a MySQL database.

    Args:
        db_host (str): The database host address (e.g., "mysql").
        db_port (str): The database port (e.g., "3306").
        db_name (str): The name of the database to connect to.
        db_user (str): The database username.
        db_password (str): The database password.
        logger (logging.Logger): Logger instance for logging connection events.

    Returns:
        Engine: SQLAlchemy Engine instance for database operations.

    Raises:
        sqlalchemy.exc.OperationalError: If the connection to the database fails.
    """
    logger.debug(
        "Attempting to connect to database: %s:%s/%s as user %s",
        db_host, db_port, db_name, db_user
    )
    try:
        # Escape special characters in credentials
        safe_user = quote_plus(db_user)
        safe_password = quote_plus(db_password)
        connection_string = f"mysql+pymysql://{safe_user}:{safe_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)
        logger.info("Successfully connected to database: %s:%s/%s", db_host, db_port, db_name)
        return engine
    except Exception as e:
        logger.error("Failed to connect to database: %s", str(e), exc_info=True)
        raise

def fetch_allowed_values(
    logger: logging.Logger,
    engine: Engine,
    table_name: str,
    source_name: str,
    column_name: str
) -> List[str]:
    """Fetch distinct allowed values from a specified column in a MySQL table.

    Retrieves unique values from the specified column where the source matches
    the provided `source_name` and the `ACTIVE_FLAG` is 1, using a parameterized query.

    Args:
        logger (logging.Logger): Logger instance for logging query execution.
        engine (Engine): SQLAlchemy Engine instance for database connection.
        table_name (str): The name of the table to query (e.g., "CSD_SUPPORT_AREAS").
        source_name (str): The source name to filter by, as a quoted string (e.g., "'AT&T'").
        column_name (str): The name of the column to fetch distinct values from (e.g., "SUPPORT_AREA_NAME").

    Returns:
        List[str]: A list of distinct values from the specified column.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: If the query execution fails.
    """
    logger.debug(
        "Fetching allowed values from table '%s', column '%s' for source %s",
        table_name, column_name, source_name
    )
    try:
        with engine.begin() as conn:
            # Use string interpolation for table/column names, bind param for source_name
            query = text(
                f"SELECT DISTINCT t.{column_name} "
                f"FROM {table_name} t "
                "WHERE t.SOURCE_ID = (SELECT SOURCE_ID FROM CSD_SOURCES WHERE UPPER(SOURCE_NAME) = UPPER(:source_name)) "
                "AND t.ACTIVE_FLAG = 1"
            )
            params = {"source_name": source_name.strip("'")}
            logger.debug("Executing query: %s with params %s", query, params)
            result = conn.execute(query, params)
            values = [str(row[0]) for row in result.fetchall()]
            logger.info(
                "Successfully fetched %d allowed values from '%s.%s'",
                len(values), table_name, column_name
            )
            return values
    except Exception as e:
        logger.error(
            "Failed to fetch allowed values from '%s.%s': %s",
            table_name, column_name, str(e), exc_info=True
        )
        raise

def close_database_connection(logger: logging.Logger, engine: Engine) -> None:
    """Close the connection to the MySQL database.

    Args:
        logger (logging.Logger): Logger instance for logging connection closure.
        engine (Engine): SQLAlchemy Engine instance to dispose.

    Raises:
        Exception: If an error occurs while closing the connection.
    """
    logger.debug("Attempting to close database connection")
    try:
        engine.dispose()
        logger.info("Database connection closed successfully")
    except Exception as e:
        logger.error("Error closing database connection: %s", str(e), exc_info=True)
        raise