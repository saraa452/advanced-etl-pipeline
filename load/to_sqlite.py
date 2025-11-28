"""
SQLite Loading Module

This module provides functions to load data into SQLite databases.
Supports table creation, data insertion, and database management.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional, List, Dict, Any
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_database(
    database_path: str,
    tables: Optional[Dict[str, pd.DataFrame]] = None
) -> str:
    """
    Create a SQLite database and optionally populate with initial tables.
    
    Args:
        database_path: Path to the SQLite database file
        tables: Optional dictionary of table names to DataFrames
    
    Returns:
        Path to the created database
    """
    logger.info(f"Creating SQLite database: {database_path}")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(database_path), exist_ok=True)
    
    # Create engine and database
    engine = create_engine(f'sqlite:///{database_path}')
    
    if tables:
        for table_name, df in tables.items():
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            logger.info(f"Created table '{table_name}' with {len(df)} rows")
    
    engine.dispose()
    logger.info(f"Database created successfully: {database_path}")
    return database_path


def load_to_sqlite(
    df: pd.DataFrame,
    table_name: str,
    database_path: str,
    if_exists: str = 'replace',
    index: bool = False,
    dtype: Optional[Dict[str, Any]] = None,
    chunksize: Optional[int] = None
) -> int:
    """
    Load DataFrame to SQLite database table.
    
    Args:
        df: DataFrame to load
        table_name: Name of the target table
        database_path: Path to the SQLite database file
        if_exists: What to do if table exists ('fail', 'replace', 'append')
        index: Whether to include DataFrame index
        dtype: Optional dictionary of column data types
        chunksize: Number of rows to write at a time
    
    Returns:
        Number of rows loaded
    """
    logger.info(f"Loading {len(df)} rows to table '{table_name}' in {database_path}")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(database_path), exist_ok=True)
    
    try:
        engine = create_engine(f'sqlite:///{database_path}')
        
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=index,
            dtype=dtype,
            chunksize=chunksize
        )
        
        engine.dispose()
        logger.info(f"Successfully loaded {len(df)} rows to '{table_name}'")
        return len(df)
        
    except Exception as e:
        logger.error(f"Failed to load data to SQLite: {str(e)}")
        raise


def query_sqlite(
    database_path: str,
    query: str,
    params: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """
    Execute a SQL query and return results as DataFrame.
    
    Args:
        database_path: Path to the SQLite database file
        query: SQL query to execute
        params: Optional query parameters
    
    Returns:
        Query results as pandas DataFrame
    """
    logger.info(f"Executing query on {database_path}")
    
    if not os.path.exists(database_path):
        raise FileNotFoundError(f"Database not found: {database_path}")
    
    try:
        engine = create_engine(f'sqlite:///{database_path}')
        
        with engine.connect() as conn:
            result = pd.read_sql(text(query), conn, params=params)
        
        engine.dispose()
        logger.info(f"Query returned {len(result)} rows")
        return result
        
    except Exception as e:
        logger.error(f"Query failed: {str(e)}")
        raise


def get_table_info(
    database_path: str,
    table_name: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get information about database tables.
    
    Args:
        database_path: Path to the SQLite database file
        table_name: Optional specific table to get info for
    
    Returns:
        Dictionary with table information
    """
    logger.info(f"Getting table info from {database_path}")
    
    if not os.path.exists(database_path):
        raise FileNotFoundError(f"Database not found: {database_path}")
    
    engine = create_engine(f'sqlite:///{database_path}')
    
    with engine.connect() as conn:
        # Get list of tables
        tables_df = pd.read_sql(
            text("SELECT name FROM sqlite_master WHERE type='table'"),
            conn
        )
        tables = tables_df['name'].tolist()
        
        result = {'database': database_path, 'tables': {}}
        
        for tbl in tables:
            if table_name and tbl != table_name:
                continue
            
            # Get row count
            count_df = pd.read_sql(text(f"SELECT COUNT(*) as count FROM {tbl}"), conn)
            row_count = count_df['count'].iloc[0]
            
            # Get column info
            pragma_df = pd.read_sql(text(f"PRAGMA table_info({tbl})"), conn)
            columns = pragma_df[['name', 'type', 'notnull', 'pk']].to_dict('records')
            
            result['tables'][tbl] = {
                'row_count': row_count,
                'columns': columns
            }
    
    engine.dispose()
    return result


def delete_table(
    database_path: str,
    table_name: str
) -> bool:
    """
    Delete a table from the database.
    
    Args:
        database_path: Path to the SQLite database file
        table_name: Name of the table to delete
    
    Returns:
        True if successful
    """
    logger.info(f"Deleting table '{table_name}' from {database_path}")
    
    if not os.path.exists(database_path):
        raise FileNotFoundError(f"Database not found: {database_path}")
    
    try:
        engine = create_engine(f'sqlite:///{database_path}')
        
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            conn.commit()
        
        engine.dispose()
        logger.info(f"Table '{table_name}' deleted successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to delete table: {str(e)}")
        raise


def upsert_data(
    df: pd.DataFrame,
    table_name: str,
    database_path: str,
    key_columns: List[str]
) -> int:
    """
    Upsert (update or insert) data into SQLite table.
    
    Args:
        df: DataFrame to upsert
        table_name: Name of the target table
        database_path: Path to the SQLite database file
        key_columns: Columns to use for identifying existing records
    
    Returns:
        Number of rows affected
    """
    logger.info(f"Upserting {len(df)} rows to '{table_name}'")
    
    engine = create_engine(f'sqlite:///{database_path}')
    
    # Check if table exists
    with engine.connect() as conn:
        tables_df = pd.read_sql(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name=:table"),
            conn,
            params={'table': table_name}
        )
        table_exists = len(tables_df) > 0
    
    if not table_exists:
        # Create table with new data
        return load_to_sqlite(df, table_name, database_path, if_exists='replace')
    
    # Get existing data
    existing_df = query_sqlite(database_path, f"SELECT * FROM {table_name}")
    
    # Merge data - update existing, add new
    key_set = existing_df[key_columns].apply(tuple, axis=1).tolist()
    new_keys = df[key_columns].apply(tuple, axis=1)
    
    new_rows = df[~new_keys.isin(key_set)]
    
    # Append new rows
    if len(new_rows) > 0:
        load_to_sqlite(new_rows, table_name, database_path, if_exists='append')
        logger.info(f"Inserted {len(new_rows)} new rows")
    
    engine.dispose()
    return len(new_rows)
