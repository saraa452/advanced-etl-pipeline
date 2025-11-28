"""File Loader - Load data to file destinations."""
from typing import Any, Dict, Optional
from pathlib import Path
import pandas as pd

from .base_loader import BaseLoader


class FileLoader(BaseLoader):
    """Loader for file destinations (CSV, JSON, Parquet)."""

    def __init__(
        self,
        output_dir: str,
        name: str = "FileLoader"
    ):
        """Initialize the file loader.
        
        Args:
            output_dir: Directory for output files
            name: Name identifier for the loader
        """
        super().__init__(name)
        self.output_dir = Path(output_dir)
        self._connected = False

    def connect(self) -> bool:
        """Ensure output directory exists.
        
        Returns:
            bool: True if directory exists or was created
        """
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            self._connected = True
            self.logger.info(f"Connected to output directory: {self.output_dir}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create output directory: {e}")
            return False

    def load(
        self,
        data: pd.DataFrame,
        filename: str,
        file_format: str = "csv",
        **kwargs
    ) -> bool:
        """Load data to a file.
        
        Args:
            data: DataFrame to save
            filename: Output filename (without extension)
            file_format: Output format ('csv', 'json', 'parquet')
            **kwargs: Additional arguments passed to pandas save method
            
        Returns:
            bool: True if save successful
        """
        if not self.validate_data(data):
            return False

        if not self._connected:
            self.connect()

        try:
            file_format = file_format.lower()
            filepath = self.output_dir / f"{filename}.{file_format}"

            if file_format == "csv":
                data.to_csv(filepath, index=False, **kwargs)
            elif file_format == "json":
                data.to_json(filepath, orient="records", **kwargs)
            elif file_format == "parquet":
                data.to_parquet(filepath, index=False, **kwargs)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")

            self.logger.info(f"Saved {len(data)} records to: {filepath}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to save data to file: {e}")
            return False

    def disconnect(self) -> bool:
        """Mark loader as disconnected.
        
        Returns:
            bool: Always True for file loader
        """
        self._connected = False
        self.logger.info("Disconnected from file loader")
        return True
