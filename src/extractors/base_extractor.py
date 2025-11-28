"""Base Extractor - Abstract base class for all extractors."""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import pandas as pd

from src.utils.logger import get_logger


class BaseExtractor(ABC):
    """Abstract base class for data extractors."""

    def __init__(self, name: str = "BaseExtractor"):
        """Initialize the extractor.
        
        Args:
            name: Name identifier for the extractor
        """
        self.name = name
        self.logger = get_logger(name)

    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the data source.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        pass

    @abstractmethod
    def extract(self, **kwargs) -> pd.DataFrame:
        """Extract data from the source.
        
        Args:
            **kwargs: Extraction parameters specific to the source
            
        Returns:
            pd.DataFrame: Extracted data as a pandas DataFrame
        """
        pass

    @abstractmethod
    def disconnect(self) -> bool:
        """Close connection to the data source.
        
        Returns:
            bool: True if disconnection successful, False otherwise
        """
        pass

    def validate_data(self, data: pd.DataFrame) -> bool:
        """Validate extracted data.
        
        Args:
            data: DataFrame to validate
            
        Returns:
            bool: True if data is valid
        """
        if data is None or data.empty:
            self.logger.warning("Extracted data is empty")
            return False
        self.logger.info(f"Validated {len(data)} records")
        return True
