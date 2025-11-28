"""Database Extractor - Extract data from SQL databases."""
from typing import Any, Dict, List, Optional
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .base_extractor import BaseExtractor


class DatabaseExtractor(BaseExtractor):
    """Extractor for SQL database sources."""

    def __init__(
        self,
        connection_string: str,
        name: str = "DatabaseExtractor"
    ):
        """Initialize the database extractor.
        
        Args:
            connection_string: SQLAlchemy connection string
            name: Name identifier for the extractor
        """
        super().__init__(name)
        self.connection_string = connection_string
        self.engine: Optional[Engine] = None

    def connect(self) -> bool:
        """Establish connection to the database.
        
        Returns:
            bool: True if connection successful
        """
        try:
            self.engine = create_engine(self.connection_string)
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.logger.info("Connected to database")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            return False

    def extract(
        self,
        query: Optional[str] = None,
        table: Optional[str] = None,
        columns: Optional[List[str]] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Extract data from the database.
        
        Args:
            query: Raw SQL query (takes precedence over other params)
            table: Table name to extract from
            columns: List of columns to select
            where: WHERE clause condition
            limit: Maximum number of rows to extract
            
        Returns:
            pd.DataFrame: Extracted data
        """
        if self.engine is None:
            self.connect()

        try:
            if query:
                sql = query
            elif table:
                cols = ", ".join(columns) if columns else "*"
                sql = f"SELECT {cols} FROM {table}"
                if where:
                    sql += f" WHERE {where}"
                if limit:
                    sql += f" LIMIT {limit}"
            else:
                raise ValueError("Either 'query' or 'table' must be provided")

            self.logger.info(f"Executing query: {sql[:100]}...")
            df = pd.read_sql(sql, self.engine)
            self.logger.info(f"Extracted {len(df)} records from database")
            return df

        except Exception as e:
            self.logger.error(f"Database extraction failed: {e}")
            return pd.DataFrame()

    def disconnect(self) -> bool:
        """Close database connection.
        
        Returns:
            bool: True if disconnection successful
        """
        try:
            if self.engine:
                self.engine.dispose()
                self.engine = None
            self.logger.info("Disconnected from database")
            return True
        except Exception as e:
            self.logger.error(f"Failed to disconnect from database: {e}")
            return False
