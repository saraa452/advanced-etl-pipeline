"""Base Loader - Abstract base class for all loaders."""
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import pandas as pd

from src.utils.logger import get_logger


class BaseLoader(ABC):
    """Abstract base class for data loaders."""

    def __init__(self, name: str = "BaseLoader"):
        """Initialize the loader.
        
        Args:
            name: Name identifier for the loader
        """
        self.name = name
        self.logger = get_logger(name)

    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the destination.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        pass

    @abstractmethod
    def load(self, data: pd.DataFrame, **kwargs) -> bool:
        """Load data to the destination.
        
        Args:
            data: DataFrame to load
            **kwargs: Loading parameters specific to the destination
            
        Returns:
            bool: True if load successful, False otherwise
        """
        pass

    @abstractmethod
    def disconnect(self) -> bool:
        """Close connection to the destination.
        
        Returns:
            bool: True if disconnection successful, False otherwise
        """
        pass

    def validate_data(self, data: pd.DataFrame) -> bool:
        """Validate data before loading.
        
        Args:
            data: DataFrame to validate
            
        Returns:
            bool: True if data is valid
        """
        if data is None or data.empty:
            self.logger.warning("Data to load is empty")
            return False
        self.logger.info(f"Validated {len(data)} records for loading")
        return True
