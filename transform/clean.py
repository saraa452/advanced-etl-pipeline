"""
Data Cleaning Module

This module provides functions for cleaning and preprocessing data.
Includes functions for handling missing values, duplicates, and data types.
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Any, Union
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_data(
    df: pd.DataFrame,
    remove_duplicates: bool = True,
    handle_missing: str = 'drop',
    strip_whitespace: bool = True,
    standardize_columns: bool = True
) -> pd.DataFrame:
    """
    Apply comprehensive data cleaning operations.
    
    Args:
        df: Input DataFrame
        remove_duplicates: Whether to remove duplicate rows
        handle_missing: How to handle missing values ('drop', 'fill', 'keep')
        strip_whitespace: Whether to strip whitespace from string columns
        standardize_columns: Whether to standardize column names
    
    Returns:
        Cleaned pandas DataFrame
    """
    logger.info(f"Starting data cleaning. Initial shape: {df.shape}")
    result = df.copy()
    
    # Standardize column names
    if standardize_columns:
        result = standardize_column_names(result)
        logger.info("Standardized column names")
    
    # Strip whitespace from string columns
    if strip_whitespace:
        result = strip_string_whitespace(result)
        logger.info("Stripped whitespace from string columns")
    
    # Handle missing values
    if handle_missing == 'drop':
        initial_rows = len(result)
        result = result.dropna()
        dropped = initial_rows - len(result)
        logger.info(f"Dropped {dropped} rows with missing values")
    elif handle_missing == 'fill':
        result = fill_missing_values(result)
    
    # Remove duplicates
    if remove_duplicates:
        initial_rows = len(result)
        result = result.drop_duplicates()
        dropped = initial_rows - len(result)
        logger.info(f"Removed {dropped} duplicate rows")
    
    logger.info(f"Cleaning complete. Final shape: {result.shape}")
    return result


def remove_duplicates(
    df: pd.DataFrame,
    subset: Optional[List[str]] = None,
    keep: str = 'first'
) -> pd.DataFrame:
    """
    Remove duplicate rows from DataFrame.
    
    Args:
        df: Input DataFrame
        subset: Columns to consider for duplicates (None for all)
        keep: Which duplicate to keep ('first', 'last', False)
    
    Returns:
        DataFrame with duplicates removed
    """
    logger.info(f"Removing duplicates. Initial rows: {len(df)}")
    
    result = df.drop_duplicates(subset=subset, keep=keep)
    
    removed = len(df) - len(result)
    logger.info(f"Removed {removed} duplicate rows. Final rows: {len(result)}")
    
    return result


def fill_missing_values(
    df: pd.DataFrame,
    strategy: str = 'auto',
    fill_value: Optional[Any] = None,
    columns: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Fill missing values in DataFrame.
    
    Args:
        df: Input DataFrame
        strategy: Fill strategy ('auto', 'mean', 'median', 'mode', 'constant', 'ffill', 'bfill')
        fill_value: Value to use when strategy is 'constant'
        columns: Columns to fill (None for all)
    
    Returns:
        DataFrame with missing values filled
    """
    logger.info(f"Filling missing values with strategy: {strategy}")
    result = df.copy()
    
    target_columns = columns if columns else result.columns
    
    for col in target_columns:
        if col not in result.columns:
            continue
            
        missing_count = result[col].isna().sum()
        if missing_count == 0:
            continue
        
        if strategy == 'auto':
            # Auto-detect based on dtype
            if pd.api.types.is_numeric_dtype(result[col]):
                result[col] = result[col].fillna(result[col].median())
            else:
                mode_values = result[col].mode()
                if len(mode_values) > 0:
                    result[col] = result[col].fillna(mode_values.iloc[0])
        elif strategy == 'mean':
            if pd.api.types.is_numeric_dtype(result[col]):
                result[col] = result[col].fillna(result[col].mean())
        elif strategy == 'median':
            if pd.api.types.is_numeric_dtype(result[col]):
                result[col] = result[col].fillna(result[col].median())
        elif strategy == 'mode':
            mode_values = result[col].mode()
            if len(mode_values) > 0:
                result[col] = result[col].fillna(mode_values.iloc[0])
        elif strategy == 'constant':
            result[col] = result[col].fillna(fill_value)
        elif strategy == 'ffill':
            result[col] = result[col].ffill()
        elif strategy == 'bfill':
            result[col] = result[col].bfill()
        
        logger.info(f"Filled {missing_count} missing values in column '{col}'")
    
    return result


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names to snake_case.
    
    Args:
        df: Input DataFrame
    
    Returns:
        DataFrame with standardized column names
    """
    result = df.copy()
    
    def to_snake_case(name: str) -> str:
        # Replace spaces and special characters with underscores
        name = re.sub(r'[\s\-\.]+', '_', str(name))
        # Insert underscore before uppercase letters
        name = re.sub(r'([a-z])([A-Z])', r'\1_\2', name)
        # Convert to lowercase
        name = name.lower()
        # Remove consecutive underscores
        name = re.sub(r'_+', '_', name)
        # Remove leading/trailing underscores
        name = name.strip('_')
        return name
    
    result.columns = [to_snake_case(col) for col in result.columns]
    return result


def strip_string_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """
    Strip leading and trailing whitespace from string columns.
    
    Args:
        df: Input DataFrame
    
    Returns:
        DataFrame with whitespace stripped from strings
    """
    result = df.copy()
    
    string_columns = result.select_dtypes(include=['object']).columns
    for col in string_columns:
        result[col] = result[col].astype(str).str.strip()
        # Convert 'nan' strings back to NaN
        result.loc[result[col] == 'nan', col] = np.nan
    
    return result


def convert_dtypes(
    df: pd.DataFrame,
    dtype_map: Dict[str, str]
) -> pd.DataFrame:
    """
    Convert column data types.
    
    Args:
        df: Input DataFrame
        dtype_map: Dictionary mapping column names to target dtypes
    
    Returns:
        DataFrame with converted dtypes
    """
    logger.info("Converting column data types")
    result = df.copy()
    
    for col, dtype in dtype_map.items():
        if col not in result.columns:
            logger.warning(f"Column '{col}' not found in DataFrame")
            continue
        
        try:
            if dtype == 'datetime':
                result[col] = pd.to_datetime(result[col])
            elif dtype == 'int':
                result[col] = pd.to_numeric(result[col], errors='coerce').astype('Int64')
            elif dtype == 'float':
                result[col] = pd.to_numeric(result[col], errors='coerce')
            elif dtype == 'str':
                result[col] = result[col].astype(str)
            elif dtype == 'bool':
                result[col] = result[col].astype(bool)
            elif dtype == 'category':
                result[col] = result[col].astype('category')
            else:
                result[col] = result[col].astype(dtype)
            
            logger.info(f"Converted column '{col}' to {dtype}")
        except Exception as e:
            logger.warning(f"Failed to convert column '{col}' to {dtype}: {str(e)}")
    
    return result


def filter_outliers(
    df: pd.DataFrame,
    column: str,
    method: str = 'iqr',
    threshold: float = 1.5
) -> pd.DataFrame:
    """
    Filter outliers from a numeric column.
    
    Args:
        df: Input DataFrame
        column: Column to check for outliers
        method: Method to detect outliers ('iqr' or 'zscore')
        threshold: Threshold for outlier detection
    
    Returns:
        DataFrame with outliers removed
    """
    logger.info(f"Filtering outliers from column '{column}' using {method} method")
    
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame")
    
    if not pd.api.types.is_numeric_dtype(df[column]):
        raise ValueError(f"Column '{column}' is not numeric")
    
    result = df.copy()
    
    if method == 'iqr':
        q1 = result[column].quantile(0.25)
        q3 = result[column].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - threshold * iqr
        upper_bound = q3 + threshold * iqr
        mask = (result[column] >= lower_bound) & (result[column] <= upper_bound)
    elif method == 'zscore':
        mean = result[column].mean()
        std = result[column].std()
        z_scores = np.abs((result[column] - mean) / std)
        mask = z_scores < threshold
    else:
        raise ValueError(f"Unknown outlier detection method: {method}")
    
    initial_rows = len(result)
    result = result[mask]
    removed = initial_rows - len(result)
    
    logger.info(f"Removed {removed} outliers from column '{column}'")
    return result
