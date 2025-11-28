"""
Data Join and Aggregation Module

This module provides functions for joining DataFrames and performing aggregations.
Includes support for various join types and aggregation operations.
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Any, Union, Callable
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def join_dataframes(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: Optional[Union[str, List[str]]] = None,
    left_on: Optional[Union[str, List[str]]] = None,
    right_on: Optional[Union[str, List[str]]] = None,
    how: str = 'inner',
    suffixes: tuple = ('_left', '_right'),
    validate: Optional[str] = None
) -> pd.DataFrame:
    """
    Join two DataFrames.
    
    Args:
        left: Left DataFrame
        right: Right DataFrame
        on: Column(s) to join on (if same in both DataFrames)
        left_on: Column(s) in left DataFrame to join on
        right_on: Column(s) in right DataFrame to join on
        how: Type of join ('inner', 'left', 'right', 'outer')
        suffixes: Suffixes for overlapping column names
        validate: Validate merge ('one_to_one', 'one_to_many', 'many_to_one', 'many_to_many')
    
    Returns:
        Joined pandas DataFrame
    """
    logger.info(f"Joining DataFrames with '{how}' join")
    logger.info(f"Left shape: {left.shape}, Right shape: {right.shape}")
    
    try:
        result = pd.merge(
            left,
            right,
            on=on,
            left_on=left_on,
            right_on=right_on,
            how=how,
            suffixes=suffixes,
            validate=validate
        )
        
        logger.info(f"Join complete. Result shape: {result.shape}")
        return result
        
    except Exception as e:
        logger.error(f"Join failed: {str(e)}")
        raise


def aggregate_data(
    df: pd.DataFrame,
    group_by: Union[str, List[str]],
    aggregations: Dict[str, Union[str, List[str], Callable]],
    reset_index: bool = True
) -> pd.DataFrame:
    """
    Aggregate data by groups.
    
    Args:
        df: Input DataFrame
        group_by: Column(s) to group by
        aggregations: Dictionary mapping columns to aggregation functions
                     e.g., {'sales': 'sum', 'price': ['mean', 'max']}
        reset_index: Whether to reset the index after aggregation
    
    Returns:
        Aggregated pandas DataFrame
    """
    logger.info(f"Aggregating data by: {group_by}")
    
    try:
        result = df.groupby(group_by).agg(aggregations)
        
        if reset_index:
            result = result.reset_index()
        
        # Flatten multi-level columns if present
        if isinstance(result.columns, pd.MultiIndex):
            result.columns = ['_'.join(col).strip('_') if col[1] else col[0] 
                            for col in result.columns.values]
        
        logger.info(f"Aggregation complete. Result shape: {result.shape}")
        return result
        
    except Exception as e:
        logger.error(f"Aggregation failed: {str(e)}")
        raise


def pivot_data(
    df: pd.DataFrame,
    index: Union[str, List[str]],
    columns: str,
    values: Union[str, List[str]],
    aggfunc: str = 'mean',
    fill_value: Optional[Any] = None
) -> pd.DataFrame:
    """
    Create a pivot table from DataFrame.
    
    Args:
        df: Input DataFrame
        index: Column(s) for pivot index
        columns: Column for pivot columns
        values: Column(s) for pivot values
        aggfunc: Aggregation function
        fill_value: Value to fill NaN with
    
    Returns:
        Pivoted pandas DataFrame
    """
    logger.info(f"Creating pivot table. Index: {index}, Columns: {columns}")
    
    try:
        result = pd.pivot_table(
            df,
            index=index,
            columns=columns,
            values=values,
            aggfunc=aggfunc,
            fill_value=fill_value
        )
        
        result = result.reset_index()
        
        logger.info(f"Pivot complete. Result shape: {result.shape}")
        return result
        
    except Exception as e:
        logger.error(f"Pivot failed: {str(e)}")
        raise


def calculate_metrics(
    df: pd.DataFrame,
    group_by: Optional[Union[str, List[str]]] = None
) -> pd.DataFrame:
    """
    Calculate common business metrics from sales data.
    
    Args:
        df: Input DataFrame with sales data
        group_by: Optional column(s) to group by
    
    Returns:
        DataFrame with calculated metrics
    """
    logger.info("Calculating business metrics")
    
    result = df.copy()
    
    # Calculate total value if price and quantity exist
    if 'price' in result.columns and 'quantity' in result.columns:
        result['total_value'] = result['price'] * result['quantity']
        logger.info("Calculated total_value column")
    
    # Calculate revenue metrics if grouped
    if group_by:
        metrics = result.groupby(group_by).agg({
            'total_value': ['sum', 'mean', 'count'] if 'total_value' in result.columns else [],
            'price': ['mean', 'min', 'max'] if 'price' in result.columns else [],
            'quantity': ['sum', 'mean'] if 'quantity' in result.columns else []
        })
        
        # Flatten column names
        metrics.columns = ['_'.join(col).strip() for col in metrics.columns.values]
        metrics = metrics.reset_index()
        
        return metrics
    
    return result


def window_aggregate(
    df: pd.DataFrame,
    column: str,
    window_size: int,
    function: str = 'mean',
    min_periods: int = 1
) -> pd.DataFrame:
    """
    Apply rolling window aggregation.
    
    Args:
        df: Input DataFrame
        column: Column to apply window function to
        window_size: Size of the rolling window
        function: Aggregation function ('mean', 'sum', 'min', 'max', 'std')
        min_periods: Minimum observations required
    
    Returns:
        DataFrame with new column containing window aggregation
    """
    logger.info(f"Applying {function} rolling window of size {window_size} to '{column}'")
    
    result = df.copy()
    new_column = f"{column}_rolling_{function}_{window_size}"
    
    rolling = result[column].rolling(window=window_size, min_periods=min_periods)
    
    if function == 'mean':
        result[new_column] = rolling.mean()
    elif function == 'sum':
        result[new_column] = rolling.sum()
    elif function == 'min':
        result[new_column] = rolling.min()
    elif function == 'max':
        result[new_column] = rolling.max()
    elif function == 'std':
        result[new_column] = rolling.std()
    else:
        raise ValueError(f"Unknown window function: {function}")
    
    logger.info(f"Created new column: {new_column}")
    return result


def rank_data(
    df: pd.DataFrame,
    column: str,
    ascending: bool = False,
    method: str = 'dense',
    group_by: Optional[Union[str, List[str]]] = None
) -> pd.DataFrame:
    """
    Add ranking column to DataFrame.
    
    Args:
        df: Input DataFrame
        column: Column to rank by
        ascending: Whether to rank in ascending order
        method: Ranking method ('average', 'min', 'max', 'first', 'dense')
        group_by: Optional column(s) to rank within groups
    
    Returns:
        DataFrame with ranking column added
    """
    logger.info(f"Ranking data by '{column}'")
    
    result = df.copy()
    rank_column = f"{column}_rank"
    
    if group_by:
        result[rank_column] = result.groupby(group_by)[column].rank(
            method=method, 
            ascending=ascending
        )
    else:
        result[rank_column] = result[column].rank(
            method=method, 
            ascending=ascending
        )
    
    logger.info(f"Created ranking column: {rank_column}")
    return result


def calculate_percentages(
    df: pd.DataFrame,
    value_column: str,
    group_by: Optional[Union[str, List[str]]] = None
) -> pd.DataFrame:
    """
    Calculate percentage of total for a value column.
    
    Args:
        df: Input DataFrame
        value_column: Column to calculate percentage for
        group_by: Optional column(s) to calculate percentage within groups
    
    Returns:
        DataFrame with percentage column added
    """
    logger.info(f"Calculating percentages for '{value_column}'")
    
    result = df.copy()
    pct_column = f"{value_column}_pct"
    
    if group_by:
        result[pct_column] = result.groupby(group_by)[value_column].transform(
            lambda x: (x / x.sum()) * 100
        )
    else:
        result[pct_column] = (result[value_column] / result[value_column].sum()) * 100
    
    logger.info(f"Created percentage column: {pct_column}")
    return result
