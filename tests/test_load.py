"""
Tests for the Load module.
Tests SQLite and CSV loading functionality.
"""

import pytest
import pandas as pd
import os
import sys
import tempfile
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from load.to_sqlite import (
    create_database,
    load_to_sqlite,
    query_sqlite,
    get_table_info,
    delete_table
)
from load.to_csv import (
    load_to_csv,
    load_to_csv_partitioned,
    load_to_csv_timestamped,
    append_to_csv,
    export_summary_report,
    validate_output
)


class TestSQLiteLoading:
    """Tests for SQLite loading functions."""
    
    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database path."""
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, 'test.db')
        yield db_path
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame for testing."""
        return pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [100, 200, 300]
        })
    
    def test_create_database(self, temp_db_path, sample_df):
        """Test database creation."""
        result = create_database(temp_db_path, tables={'test_table': sample_df})
        assert os.path.exists(result)
    
    def test_load_to_sqlite(self, temp_db_path, sample_df):
        """Test loading data to SQLite."""
        rows_loaded = load_to_sqlite(sample_df, 'test_table', temp_db_path)
        assert rows_loaded == 3
        assert os.path.exists(temp_db_path)
    
    def test_query_sqlite(self, temp_db_path, sample_df):
        """Test querying SQLite database."""
        load_to_sqlite(sample_df, 'test_table', temp_db_path)
        result = query_sqlite(temp_db_path, "SELECT * FROM test_table")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
    
    def test_query_sqlite_with_filter(self, temp_db_path, sample_df):
        """Test querying SQLite with filter."""
        load_to_sqlite(sample_df, 'test_table', temp_db_path)
        result = query_sqlite(temp_db_path, "SELECT * FROM test_table WHERE value > 150")
        assert len(result) == 2
    
    def test_get_table_info(self, temp_db_path, sample_df):
        """Test getting table information."""
        load_to_sqlite(sample_df, 'test_table', temp_db_path)
        info = get_table_info(temp_db_path)
        assert 'test_table' in info['tables']
        assert info['tables']['test_table']['row_count'] == 3
    
    def test_delete_table(self, temp_db_path, sample_df):
        """Test table deletion."""
        load_to_sqlite(sample_df, 'test_table', temp_db_path)
        result = delete_table(temp_db_path, 'test_table')
        assert result is True
        info = get_table_info(temp_db_path)
        assert 'test_table' not in info['tables']
    
    def test_load_to_sqlite_replace(self, temp_db_path, sample_df):
        """Test replacing existing table."""
        load_to_sqlite(sample_df, 'test_table', temp_db_path)
        new_df = pd.DataFrame({'id': [4, 5], 'name': ['Dave', 'Eve'], 'value': [400, 500]})
        load_to_sqlite(new_df, 'test_table', temp_db_path, if_exists='replace')
        result = query_sqlite(temp_db_path, "SELECT * FROM test_table")
        assert len(result) == 2
    
    def test_load_to_sqlite_append(self, temp_db_path, sample_df):
        """Test appending to existing table."""
        load_to_sqlite(sample_df, 'test_table', temp_db_path)
        new_df = pd.DataFrame({'id': [4, 5], 'name': ['Dave', 'Eve'], 'value': [400, 500]})
        load_to_sqlite(new_df, 'test_table', temp_db_path, if_exists='append')
        result = query_sqlite(temp_db_path, "SELECT * FROM test_table")
        assert len(result) == 5


class TestCSVLoading:
    """Tests for CSV loading functions."""
    
    @pytest.fixture
    def temp_output_dir(self):
        """Create a temporary output directory."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame for testing."""
        return pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [100.5, 200.75, 300.25],
            'category': ['A', 'B', 'A']
        })
    
    def test_load_to_csv(self, temp_output_dir, sample_df):
        """Test basic CSV saving."""
        filepath = os.path.join(temp_output_dir, 'test.csv')
        result = load_to_csv(sample_df, filepath)
        assert os.path.exists(result)
        
        # Verify content
        loaded_df = pd.read_csv(result)
        assert len(loaded_df) == 3
    
    def test_load_to_csv_with_columns(self, temp_output_dir, sample_df):
        """Test CSV saving with specific columns."""
        filepath = os.path.join(temp_output_dir, 'test.csv')
        load_to_csv(sample_df, filepath, columns=['id', 'name'])
        
        loaded_df = pd.read_csv(filepath)
        assert list(loaded_df.columns) == ['id', 'name']
    
    def test_load_to_csv_partitioned(self, temp_output_dir, sample_df):
        """Test partitioned CSV saving."""
        files = load_to_csv_partitioned(
            sample_df,
            temp_output_dir,
            partition_column='category',
            filename_prefix='data'
        )
        assert len(files) == 2  # Two categories: A and B
        for f in files:
            assert os.path.exists(f)
    
    def test_load_to_csv_timestamped(self, temp_output_dir, sample_df):
        """Test timestamped CSV saving."""
        result = load_to_csv_timestamped(
            sample_df,
            temp_output_dir,
            filename_prefix='data'
        )
        assert os.path.exists(result)
        assert 'data_' in os.path.basename(result)
    
    def test_append_to_csv(self, temp_output_dir, sample_df):
        """Test appending to CSV."""
        filepath = os.path.join(temp_output_dir, 'test.csv')
        
        # Initial write
        load_to_csv(sample_df, filepath)
        
        # Append
        new_df = pd.DataFrame({'id': [4], 'name': ['Dave'], 'value': [400.0], 'category': ['C']})
        append_to_csv(new_df, filepath)
        
        loaded_df = pd.read_csv(filepath)
        assert len(loaded_df) == 4
    
    def test_export_summary_report(self, temp_output_dir, sample_df):
        """Test summary report export."""
        reports = export_summary_report(sample_df, temp_output_dir, 'test_report')
        assert 'data' in reports
        assert 'statistics' in reports
        assert 'info' in reports
        for filepath in reports.values():
            assert os.path.exists(filepath)
    
    def test_validate_output_valid(self, temp_output_dir, sample_df):
        """Test output validation with valid file."""
        filepath = os.path.join(temp_output_dir, 'test.csv')
        load_to_csv(sample_df, filepath)
        
        result = validate_output(
            filepath,
            expected_rows=3,
            expected_columns=['id', 'name', 'value', 'category']
        )
        assert result['valid'] is True
    
    def test_validate_output_invalid_rows(self, temp_output_dir, sample_df):
        """Test output validation with wrong row count."""
        filepath = os.path.join(temp_output_dir, 'test.csv')
        load_to_csv(sample_df, filepath)
        
        result = validate_output(filepath, expected_rows=10)
        assert result['valid'] is False
    
    def test_validate_output_missing_columns(self, temp_output_dir, sample_df):
        """Test output validation with missing columns."""
        filepath = os.path.join(temp_output_dir, 'test.csv')
        load_to_csv(sample_df, filepath)
        
        result = validate_output(
            filepath,
            expected_columns=['id', 'name', 'value', 'category', 'extra']
        )
        assert result['valid'] is False
