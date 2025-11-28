"""
CSV Extraction Module

This module provides functions to extract data from CSV files.
Supports various CSV configurations and data validation.
"""

import pandas as pd
from typing import Optional, List, Dict, Any
import os
import logging
import glob

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_from_csv(
    filepath: str,
    encoding: str = 'utf-8',
    delimiter: str = ',',
    columns: Optional[List[str]] = None,
    dtypes: Optional[Dict[str, Any]] = None,
    parse_dates: Optional[List[str]] = None,
    skip_rows: int = 0,
    nrows: Optional[int] = None
) -> pd.DataFrame:
    """
    Extract data from a CSV file.
    
    Args:
        filepath: Path to the CSV file
        encoding: File encoding (default: utf-8)
        delimiter: Column delimiter (default: comma)
        columns: List of columns to load (None for all)
        dtypes: Dictionary of column data types
        parse_dates: List of columns to parse as dates
        skip_rows: Number of rows to skip at the beginning
        nrows: Number of rows to read (None for all)
    
    Returns:
        pandas DataFrame with the extracted data
    
    Raises:
        FileNotFoundError: If the CSV file doesn't exist
        pd.errors.EmptyDataError: If the CSV file is empty
    """
    logger.info(f"Extracting data from CSV: {filepath}")
    
    if not os.path.exists(filepath):
        logger.error(f"File not found: {filepath}")
        raise FileNotFoundError(f"CSV file not found: {filepath}")
    
    try:
        df = pd.read_csv(
            filepath,
            encoding=encoding,
            delimiter=delimiter,
            usecols=columns,
            dtype=dtypes,
            parse_dates=parse_dates,
            skiprows=skip_rows,
            nrows=nrows
        )
        
        logger.info(f"Successfully extracted {len(df)} records from CSV")
        logger.info(f"Columns: {list(df.columns)}")
        
        return df
        
    except pd.errors.EmptyDataError:
        logger.warning(f"CSV file is empty: {filepath}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error reading CSV file: {str(e)}")
        raise


def extract_multiple_csv(
    directory: str,
    pattern: str = '*.csv',
    **kwargs
) -> pd.DataFrame:
    """
    Extract and combine data from multiple CSV files in a directory.
    
    Args:
        directory: Path to the directory containing CSV files
        pattern: Glob pattern to match CSV files (default: *.csv)
        **kwargs: Additional arguments to pass to extract_from_csv
    
    Returns:
        pandas DataFrame with combined data from all files
    """
    logger.info(f"Extracting multiple CSV files from: {directory}")
    
    if not os.path.isdir(directory):
        logger.error(f"Directory not found: {directory}")
        raise NotADirectoryError(f"Directory not found: {directory}")
    
    file_pattern = os.path.join(directory, pattern)
    files = glob.glob(file_pattern)
    
    if not files:
        logger.warning(f"No files matching pattern '{pattern}' in {directory}")
        return pd.DataFrame()
    
    logger.info(f"Found {len(files)} files matching pattern")
    
    dataframes = []
    for filepath in files:
        try:
            df = extract_from_csv(filepath, **kwargs)
            df['_source_file'] = os.path.basename(filepath)
            dataframes.append(df)
        except Exception as e:
            logger.warning(f"Failed to read {filepath}: {str(e)}")
    
    if dataframes:
        result = pd.concat(dataframes, ignore_index=True)
        logger.info(f"Combined {len(result)} total records from {len(dataframes)} files")
        return result
    
    return pd.DataFrame()


def validate_csv_structure(
    filepath: str,
    required_columns: List[str],
    delimiter: str = ','
) -> Dict[str, Any]:
    """
    Validate the structure of a CSV file.
    
    Args:
        filepath: Path to the CSV file
        required_columns: List of required column names
        delimiter: Column delimiter (default: comma)
    
    Returns:
        Dictionary with validation results
    """
    logger.info(f"Validating CSV structure: {filepath}")
    
    result = {
        'valid': False,
        'filepath': filepath,
        'missing_columns': [],
        'extra_columns': [],
        'row_count': 0,
        'errors': []
    }
    
    try:
        df = extract_from_csv(filepath, delimiter=delimiter, nrows=0)
        actual_columns = set(df.columns)
        required_set = set(required_columns)
        
        result['missing_columns'] = list(required_set - actual_columns)
        result['extra_columns'] = list(actual_columns - required_set)
        
        # Get full row count
        full_df = extract_from_csv(filepath, delimiter=delimiter)
        result['row_count'] = len(full_df)
        
        result['valid'] = len(result['missing_columns']) == 0
        
        if result['valid']:
            logger.info("CSV structure validation passed")
        else:
            logger.warning(f"CSV validation failed. Missing columns: {result['missing_columns']}")
            
    except Exception as e:
        result['errors'].append(str(e))
        logger.error(f"CSV validation error: {str(e)}")
    
    return result


def get_csv_preview(
    filepath: str,
    rows: int = 5,
    delimiter: str = ','
) -> Dict[str, Any]:
    """
    Get a preview of CSV file contents.
    
    Args:
        filepath: Path to the CSV file
        rows: Number of rows to preview (default: 5)
        delimiter: Column delimiter (default: comma)
    
    Returns:
        Dictionary with file preview information
    """
    logger.info(f"Getting CSV preview: {filepath}")
    
    df = extract_from_csv(filepath, delimiter=delimiter)
    
    return {
        'filepath': filepath,
        'columns': list(df.columns),
        'dtypes': df.dtypes.astype(str).to_dict(),
        'row_count': len(df),
        'preview': df.head(rows).to_dict('records'),
        'memory_usage': df.memory_usage(deep=True).sum()
    }
