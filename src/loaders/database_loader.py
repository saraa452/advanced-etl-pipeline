"""Database Loader - Load data to SQL databases."""
from typing import Any, Dict, Optional
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from .base_loader import BaseLoader


class DatabaseLoader(BaseLoader):
    """Loader for SQL database destinations."""

    def __init__(
        self,
        connection_string: str,
        name: str = "DatabaseLoader"
    ):
        """Initialize the database loader.
        
        Args:
            connection_string: SQLAlchemy connection string
            name: Name identifier for the loader
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
            self.logger.info("Connected to database")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            return False

    def load(
        self,
        data: pd.DataFrame,
        table: str,
        if_exists: str = "append",
        index: bool = False,
        chunksize: Optional[int] = None
    ) -> bool:
        """Load data to the database.
        
        Args:
            data: DataFrame to load
            table: Target table name
            if_exists: Action if table exists ('fail', 'replace', 'append')
            index: Whether to write DataFrame index as a column
            chunksize: Number of rows to write at a time
            
        Returns:
            bool: True if load successful
        """
        if not self.validate_data(data):
            return False

        if self.engine is None:
            self.connect()

        try:
            data.to_sql(
                table,
                self.engine,
                if_exists=if_exists,
                index=index,
                chunksize=chunksize
            )
            self.logger.info(f"Loaded {len(data)} records to table: {table}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to load data to database: {e}")
            return False

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
