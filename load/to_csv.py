"""
CSV Loading Module

This module provides functions to save data to CSV files.
Supports various output configurations and data formatting.
"""

import pandas as pd
from typing import Optional, List, Dict, Any
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_to_csv(
    df: pd.DataFrame,
    filepath: str,
    index: bool = False,
    encoding: str = 'utf-8',
    delimiter: str = ',',
    columns: Optional[List[str]] = None,
    date_format: Optional[str] = None,
    float_format: Optional[str] = None,
    compression: Optional[str] = None
) -> str:
    """
    Save DataFrame to a CSV file.
    
    Args:
        df: DataFrame to save
        filepath: Path to the output CSV file
        index: Whether to include DataFrame index
        encoding: File encoding (default: utf-8)
        delimiter: Column delimiter (default: comma)
        columns: Columns to include (None for all)
        date_format: Format string for datetime columns
        float_format: Format string for float columns (e.g., '%.2f')
        compression: Compression type ('gzip', 'bz2', 'zip', 'xz', None)
    
    Returns:
        Path to the saved file
    """
    logger.info(f"Saving {len(df)} rows to CSV: {filepath}")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    try:
        df.to_csv(
            filepath,
            index=index,
            encoding=encoding,
            sep=delimiter,
            columns=columns,
            date_format=date_format,
            float_format=float_format,
            compression=compression
        )
        
        file_size = os.path.getsize(filepath)
        logger.info(f"Successfully saved CSV ({file_size:,} bytes): {filepath}")
        return filepath
        
    except Exception as e:
        logger.error(f"Failed to save CSV: {str(e)}")
        raise


def load_to_csv_partitioned(
    df: pd.DataFrame,
    output_dir: str,
    partition_column: str,
    filename_prefix: str = 'data',
    **kwargs
) -> List[str]:
    """
    Save DataFrame to multiple CSV files partitioned by a column.
    
    Args:
        df: DataFrame to save
        output_dir: Directory for output files
        partition_column: Column to partition by
        filename_prefix: Prefix for output filenames
        **kwargs: Additional arguments to pass to load_to_csv
    
    Returns:
        List of paths to saved files
    """
    logger.info(f"Saving partitioned CSV by '{partition_column}' to {output_dir}")
    
    if partition_column not in df.columns:
        raise ValueError(f"Partition column '{partition_column}' not found in DataFrame")
    
    os.makedirs(output_dir, exist_ok=True)
    
    saved_files = []
    for partition_value in df[partition_column].unique():
        partition_df = df[df[partition_column] == partition_value]
        
        # Sanitize partition value for filename
        safe_value = str(partition_value).replace('/', '_').replace('\\', '_')
        filename = f"{filename_prefix}_{partition_column}_{safe_value}.csv"
        filepath = os.path.join(output_dir, filename)
        
        load_to_csv(partition_df, filepath, **kwargs)
        saved_files.append(filepath)
    
    logger.info(f"Created {len(saved_files)} partitioned files")
    return saved_files


def load_to_csv_timestamped(
    df: pd.DataFrame,
    output_dir: str,
    filename_prefix: str = 'data',
    timestamp_format: str = '%Y%m%d_%H%M%S',
    **kwargs
) -> str:
    """
    Save DataFrame to CSV with timestamp in filename.
    
    Args:
        df: DataFrame to save
        output_dir: Directory for output file
        filename_prefix: Prefix for output filename
        timestamp_format: Format string for timestamp
        **kwargs: Additional arguments to pass to load_to_csv
    
    Returns:
        Path to saved file
    """
    timestamp = datetime.now().strftime(timestamp_format)
    filename = f"{filename_prefix}_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)
    
    return load_to_csv(df, filepath, **kwargs)


def append_to_csv(
    df: pd.DataFrame,
    filepath: str,
    **kwargs
) -> str:
    """
    Append DataFrame to existing CSV file.
    
    Args:
        df: DataFrame to append
        filepath: Path to the existing CSV file
        **kwargs: Additional arguments to pass to df.to_csv
    
    Returns:
        Path to the file
    """
    logger.info(f"Appending {len(df)} rows to CSV: {filepath}")
    
    # Check if file exists to determine if we need headers
    file_exists = os.path.exists(filepath)
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    try:
        df.to_csv(
            filepath,
            mode='a',
            header=not file_exists,
            index=kwargs.get('index', False),
            encoding=kwargs.get('encoding', 'utf-8'),
            sep=kwargs.get('delimiter', ',')
        )
        
        logger.info(f"Successfully appended {len(df)} rows to {filepath}")
        return filepath
        
    except Exception as e:
        logger.error(f"Failed to append to CSV: {str(e)}")
        raise


def export_summary_report(
    df: pd.DataFrame,
    output_dir: str,
    report_name: str = 'summary_report'
) -> Dict[str, str]:
    """
    Export a summary report of the DataFrame to multiple CSV files.
    
    Args:
        df: DataFrame to summarize
        output_dir: Directory for output files
        report_name: Base name for report files
    
    Returns:
        Dictionary of report type to filepath
    """
    logger.info(f"Generating summary report for DataFrame with shape {df.shape}")
    
    os.makedirs(output_dir, exist_ok=True)
    reports = {}
    
    # Main data export
    main_file = os.path.join(output_dir, f"{report_name}_data.csv")
    load_to_csv(df, main_file)
    reports['data'] = main_file
    
    # Statistics for numeric columns
    numeric_df = df.describe()
    if not numeric_df.empty:
        stats_file = os.path.join(output_dir, f"{report_name}_statistics.csv")
        load_to_csv(numeric_df.reset_index(), stats_file)
        reports['statistics'] = stats_file
    
    # Data types and non-null counts
    info_data = {
        'column': df.columns.tolist(),
        'dtype': df.dtypes.astype(str).tolist(),
        'non_null_count': df.count().tolist(),
        'null_count': df.isnull().sum().tolist(),
        'unique_count': df.nunique().tolist()
    }
    info_df = pd.DataFrame(info_data)
    info_file = os.path.join(output_dir, f"{report_name}_info.csv")
    load_to_csv(info_df, info_file)
    reports['info'] = info_file
    
    logger.info(f"Generated {len(reports)} report files")
    return reports


def validate_output(
    filepath: str,
    expected_rows: Optional[int] = None,
    expected_columns: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Validate a CSV output file.
    
    Args:
        filepath: Path to the CSV file to validate
        expected_rows: Expected number of rows (excluding header)
        expected_columns: Expected column names
    
    Returns:
        Dictionary with validation results
    """
    logger.info(f"Validating output file: {filepath}")
    
    if not os.path.exists(filepath):
        return {
            'valid': False,
            'filepath': filepath,
            'error': 'File does not exist'
        }
    
    try:
        df = pd.read_csv(filepath)
        
        result = {
            'valid': True,
            'filepath': filepath,
            'actual_rows': len(df),
            'actual_columns': list(df.columns),
            'file_size': os.path.getsize(filepath),
            'errors': []
        }
        
        if expected_rows is not None and len(df) != expected_rows:
            result['valid'] = False
            result['errors'].append(
                f"Row count mismatch: expected {expected_rows}, got {len(df)}"
            )
        
        if expected_columns is not None:
            missing = set(expected_columns) - set(df.columns)
            extra = set(df.columns) - set(expected_columns)
            
            if missing:
                result['valid'] = False
                result['errors'].append(f"Missing columns: {list(missing)}")
            if extra:
                result['errors'].append(f"Extra columns: {list(extra)}")
        
        return result
        
    except Exception as e:
        return {
            'valid': False,
            'filepath': filepath,
            'error': str(e)
        }
