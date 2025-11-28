"""Data Transformer - Transform and enrich data."""
from typing import Any, Callable, Dict, List, Optional, Union
import pandas as pd

from src.utils.logger import get_logger


class DataTransformer:
    """Transform and enrich data with various operations."""

    def __init__(self, name: str = "DataTransformer"):
        """Initialize the transformer.
        
        Args:
            name: Name identifier for the transformer
        """
        self.name = name
        self.logger = get_logger(name)
        self.transformations: List[Callable] = []

    def add_transformation(self, func: Callable[[pd.DataFrame], pd.DataFrame]) -> "DataTransformer":
        """Add a transformation function to the pipeline.
        
        Args:
            func: Function that takes a DataFrame and returns a transformed DataFrame
            
        Returns:
            DataTransformer: Self for method chaining
        """
        self.transformations.append(func)
        self.logger.info(f"Added transformation: {func.__name__}")
        return self

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply all transformations to the data.
        
        Args:
            data: Input DataFrame
            
        Returns:
            pd.DataFrame: Transformed DataFrame
        """
        result = data.copy()
        for transform_func in self.transformations:
            try:
                result = transform_func(result)
                self.logger.info(f"Applied transformation: {transform_func.__name__}")
            except Exception as e:
                self.logger.error(f"Transformation {transform_func.__name__} failed: {e}")
                raise
        return result

    def clean_data(self, data: pd.DataFrame, drop_duplicates: bool = True, 
                   drop_na: bool = False, fill_na: Optional[Any] = None) -> pd.DataFrame:
        """Clean data by removing duplicates and handling missing values.
        
        Args:
            data: Input DataFrame
            drop_duplicates: Whether to drop duplicate rows
            drop_na: Whether to drop rows with missing values
            fill_na: Value to fill missing values with
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        result = data.copy()
        initial_rows = len(result)

        if drop_duplicates:
            result = result.drop_duplicates()
            self.logger.info(f"Removed {initial_rows - len(result)} duplicate rows")

        if drop_na:
            before_na = len(result)
            result = result.dropna()
            self.logger.info(f"Removed {before_na - len(result)} rows with missing values")
        elif fill_na is not None:
            result = result.fillna(fill_na)
            self.logger.info(f"Filled missing values with: {fill_na}")

        return result

    def rename_columns(self, data: pd.DataFrame, 
                       mapping: Dict[str, str]) -> pd.DataFrame:
        """Rename columns in the DataFrame.
        
        Args:
            data: Input DataFrame
            mapping: Dictionary mapping old names to new names
            
        Returns:
            pd.DataFrame: DataFrame with renamed columns
        """
        result = data.rename(columns=mapping)
        self.logger.info(f"Renamed columns: {mapping}")
        return result

    def filter_rows(self, data: pd.DataFrame, 
                    condition: Union[str, Callable]) -> pd.DataFrame:
        """Filter rows based on a condition.
        
        Args:
            data: Input DataFrame
            condition: Query string or callable that returns boolean mask
            
        Returns:
            pd.DataFrame: Filtered DataFrame
        """
        if callable(condition):
            result = data[condition(data)]
        else:
            result = data.query(condition)
        self.logger.info(f"Filtered data: {len(data)} -> {len(result)} rows")
        return result

    def add_column(self, data: pd.DataFrame, column_name: str, 
                   value: Union[Any, Callable]) -> pd.DataFrame:
        """Add a new column to the DataFrame.
        
        Args:
            data: Input DataFrame
            column_name: Name of the new column
            value: Static value or callable to compute values
            
        Returns:
            pd.DataFrame: DataFrame with new column
        """
        result = data.copy()
        if callable(value):
            result[column_name] = value(result)
        else:
            result[column_name] = value
        self.logger.info(f"Added column: {column_name}")
        return result

    def convert_types(self, data: pd.DataFrame, 
                      type_mapping: Dict[str, str]) -> pd.DataFrame:
        """Convert column data types.
        
        Args:
            data: Input DataFrame
            type_mapping: Dictionary mapping column names to target types
            
        Returns:
            pd.DataFrame: DataFrame with converted types
        """
        result = data.copy()
        for column, dtype in type_mapping.items():
            if column in result.columns:
                try:
                    if dtype == 'datetime':
                        result[column] = pd.to_datetime(result[column])
                    else:
                        result[column] = result[column].astype(dtype)
                    self.logger.info(f"Converted column {column} to {dtype}")
                except Exception as e:
                    self.logger.warning(f"Failed to convert {column} to {dtype}: {e}")
        return result

    def aggregate(self, data: pd.DataFrame, group_by: List[str], 
                  aggregations: Dict[str, str]) -> pd.DataFrame:
        """Aggregate data by grouping.
        
        Args:
            data: Input DataFrame
            group_by: List of columns to group by
            aggregations: Dictionary mapping columns to aggregation functions
            
        Returns:
            pd.DataFrame: Aggregated DataFrame
        """
        result = data.groupby(group_by).agg(aggregations).reset_index()
        self.logger.info(f"Aggregated data by {group_by}")
        return result
