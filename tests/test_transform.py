"""
Tests for the Transform module.
Tests data cleaning and joining functionality.
"""

import pytest
import pandas as pd
import numpy as np
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transform.clean import (
    clean_data,
    remove_duplicates,
    fill_missing_values,
    standardize_column_names,
    strip_string_whitespace,
    convert_dtypes,
    filter_outliers
)
from transform.join import (
    join_dataframes,
    aggregate_data,
    pivot_data,
    calculate_metrics,
    rank_data,
    calculate_percentages
)


class TestDataCleaning:
    """Tests for data cleaning functions."""
    
    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame for testing."""
        return pd.DataFrame({
            'id': [1, 2, 2, 3, 4],
            'name': ['  Alice  ', 'Bob', 'Bob', '  Charlie  ', 'David'],
            'value': [100, 200, 200, None, 400],
            'category': ['A', 'B', 'B', 'A', 'C']
        })
    
    def test_clean_data_returns_dataframe(self, sample_df):
        """Test that clean_data returns a DataFrame."""
        result = clean_data(sample_df, handle_missing='drop')
        assert isinstance(result, pd.DataFrame)
    
    def test_clean_data_removes_duplicates(self, sample_df):
        """Test that duplicates are removed."""
        result = clean_data(sample_df, remove_duplicates=True, handle_missing='keep')
        assert len(result) < len(sample_df)
    
    def test_remove_duplicates(self, sample_df):
        """Test duplicate removal function."""
        result = remove_duplicates(sample_df)
        assert len(result) == 4  # One duplicate removed
    
    def test_remove_duplicates_subset(self, sample_df):
        """Test duplicate removal with subset."""
        result = remove_duplicates(sample_df, subset=['name'])
        assert len(result) == 4
    
    def test_fill_missing_values_auto(self, sample_df):
        """Test auto fill missing values."""
        result = fill_missing_values(sample_df, strategy='auto')
        assert result['value'].isna().sum() == 0
    
    def test_fill_missing_values_constant(self, sample_df):
        """Test constant fill missing values."""
        result = fill_missing_values(sample_df, strategy='constant', fill_value=0)
        assert result['value'].isna().sum() == 0
        assert 0 in result['value'].values
    
    def test_fill_missing_values_median(self, sample_df):
        """Test median fill missing values."""
        result = fill_missing_values(sample_df, strategy='median')
        assert result['value'].isna().sum() == 0
    
    def test_standardize_column_names(self):
        """Test column name standardization."""
        df = pd.DataFrame({
            'First Name': [1],
            'lastName': [2],
            'CATEGORY.TYPE': [3]
        })
        result = standardize_column_names(df)
        assert 'first_name' in result.columns
        assert 'last_name' in result.columns
        assert 'category_type' in result.columns
    
    def test_strip_string_whitespace(self, sample_df):
        """Test whitespace stripping."""
        result = strip_string_whitespace(sample_df)
        assert result['name'].iloc[0] == 'Alice'
        assert result['name'].iloc[3] == 'Charlie'
    
    def test_convert_dtypes(self):
        """Test data type conversion."""
        df = pd.DataFrame({
            'num_str': ['1', '2', '3'],
            'date_str': ['2024-01-01', '2024-01-02', '2024-01-03']
        })
        result = convert_dtypes(df, {'num_str': 'int', 'date_str': 'datetime'})
        assert pd.api.types.is_integer_dtype(result['num_str'])
        assert pd.api.types.is_datetime64_any_dtype(result['date_str'])
    
    def test_filter_outliers_iqr(self):
        """Test outlier filtering with IQR method."""
        df = pd.DataFrame({
            'value': [10, 12, 11, 13, 100, 12, 11]  # 100 is outlier
        })
        result = filter_outliers(df, 'value', method='iqr')
        assert 100 not in result['value'].values


class TestDataJoining:
    """Tests for data joining functions."""
    
    @pytest.fixture
    def left_df(self):
        """Create left DataFrame for joining."""
        return pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'dept_id': [10, 20, 10]
        })
    
    @pytest.fixture
    def right_df(self):
        """Create right DataFrame for joining."""
        return pd.DataFrame({
            'dept_id': [10, 20, 30],
            'dept_name': ['Sales', 'Marketing', 'Engineering']
        })
    
    def test_join_dataframes_inner(self, left_df, right_df):
        """Test inner join."""
        result = join_dataframes(left_df, right_df, on='dept_id', how='inner')
        assert len(result) == 3
        assert 'dept_name' in result.columns
    
    def test_join_dataframes_left(self, left_df, right_df):
        """Test left join."""
        result = join_dataframes(left_df, right_df, on='dept_id', how='left')
        assert len(result) == 3
    
    def test_join_dataframes_outer(self, left_df, right_df):
        """Test outer join."""
        result = join_dataframes(left_df, right_df, on='dept_id', how='outer')
        assert len(result) >= 3


class TestDataAggregation:
    """Tests for data aggregation functions."""
    
    @pytest.fixture
    def sales_df(self):
        """Create sales DataFrame for aggregation testing."""
        return pd.DataFrame({
            'product': ['A', 'A', 'B', 'B', 'C'],
            'category': ['X', 'X', 'Y', 'Y', 'X'],
            'price': [100, 150, 200, 250, 300],
            'quantity': [10, 20, 15, 25, 5]
        })
    
    def test_aggregate_data_sum(self, sales_df):
        """Test aggregation with sum."""
        result = aggregate_data(
            sales_df,
            group_by='product',
            aggregations={'price': 'sum', 'quantity': 'sum'}
        )
        assert len(result) == 3
        assert 'price' in result.columns or 'price_sum' in result.columns
    
    def test_aggregate_data_multiple_functions(self, sales_df):
        """Test aggregation with multiple functions."""
        result = aggregate_data(
            sales_df,
            group_by='category',
            aggregations={'price': ['sum', 'mean']}
        )
        assert len(result) == 2
    
    def test_calculate_metrics(self, sales_df):
        """Test metrics calculation."""
        result = calculate_metrics(sales_df)
        assert 'total_value' in result.columns
    
    def test_rank_data(self, sales_df):
        """Test data ranking."""
        result = rank_data(sales_df, column='price')
        assert 'price_rank' in result.columns
        assert result['price_rank'].max() == len(sales_df)
    
    def test_calculate_percentages(self, sales_df):
        """Test percentage calculation."""
        result = calculate_percentages(sales_df, value_column='price')
        assert 'price_pct' in result.columns
        assert abs(result['price_pct'].sum() - 100) < 0.01


class TestPivotData:
    """Tests for pivot functionality."""
    
    @pytest.fixture
    def pivot_df(self):
        """Create DataFrame for pivot testing."""
        return pd.DataFrame({
            'date': ['2024-01', '2024-01', '2024-02', '2024-02'],
            'category': ['A', 'B', 'A', 'B'],
            'value': [100, 200, 150, 250]
        })
    
    def test_pivot_data(self, pivot_df):
        """Test pivot table creation."""
        result = pivot_data(
            pivot_df,
            index='date',
            columns='category',
            values='value',
            aggfunc='sum'
        )
        assert len(result) == 2
