"""
Tests for the Extract module.
Tests API and CSV extraction functionality.
"""

import pytest
import pandas as pd
import os
import tempfile

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extract.api import extract_mock_api_data, extract_from_api
from extract.csv_extractor import (
    extract_from_csv, 
    validate_csv_structure,
    get_csv_preview
)


class TestAPIExtraction:
    """Tests for API extraction functions."""
    
    def test_extract_mock_api_data_returns_dataframe(self):
        """Test that mock API extraction returns a DataFrame."""
        df = extract_mock_api_data()
        assert isinstance(df, pd.DataFrame)
    
    def test_extract_mock_api_data_has_expected_columns(self):
        """Test that mock API data has expected columns."""
        df = extract_mock_api_data()
        expected_columns = ['id', 'product', 'category', 'price', 'quantity', 'region']
        for col in expected_columns:
            assert col in df.columns
    
    def test_extract_mock_api_data_has_data(self):
        """Test that mock API data contains records."""
        df = extract_mock_api_data()
        assert len(df) > 0
    
    def test_extract_mock_api_data_price_is_numeric(self):
        """Test that price column is numeric."""
        df = extract_mock_api_data()
        assert pd.api.types.is_numeric_dtype(df['price'])
    
    def test_extract_mock_api_data_quantity_is_numeric(self):
        """Test that quantity column is numeric."""
        df = extract_mock_api_data()
        assert pd.api.types.is_numeric_dtype(df['quantity'])


class TestCSVExtraction:
    """Tests for CSV extraction functions."""
    
    @pytest.fixture
    def sample_csv_file(self):
        """Create a temporary CSV file for testing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('id,name,value\n')
            f.write('1,Product A,100\n')
            f.write('2,Product B,200\n')
            f.write('3,Product C,300\n')
            temp_path = f.name
        yield temp_path
        os.unlink(temp_path)
    
    def test_extract_from_csv_returns_dataframe(self, sample_csv_file):
        """Test that CSV extraction returns a DataFrame."""
        df = extract_from_csv(sample_csv_file)
        assert isinstance(df, pd.DataFrame)
    
    def test_extract_from_csv_correct_row_count(self, sample_csv_file):
        """Test that CSV extraction returns correct number of rows."""
        df = extract_from_csv(sample_csv_file)
        assert len(df) == 3
    
    def test_extract_from_csv_correct_columns(self, sample_csv_file):
        """Test that CSV extraction returns correct columns."""
        df = extract_from_csv(sample_csv_file)
        assert list(df.columns) == ['id', 'name', 'value']
    
    def test_extract_from_csv_file_not_found(self):
        """Test that FileNotFoundError is raised for non-existent file."""
        with pytest.raises(FileNotFoundError):
            extract_from_csv('/nonexistent/path/file.csv')
    
    def test_validate_csv_structure(self, sample_csv_file):
        """Test CSV structure validation."""
        result = validate_csv_structure(
            sample_csv_file,
            required_columns=['id', 'name', 'value']
        )
        assert result['valid'] is True
        assert result['missing_columns'] == []
    
    def test_validate_csv_structure_missing_columns(self, sample_csv_file):
        """Test CSV validation with missing columns."""
        result = validate_csv_structure(
            sample_csv_file,
            required_columns=['id', 'name', 'value', 'extra_column']
        )
        assert result['valid'] is False
        assert 'extra_column' in result['missing_columns']
    
    def test_get_csv_preview(self, sample_csv_file):
        """Test CSV preview function."""
        preview = get_csv_preview(sample_csv_file, rows=2)
        assert 'columns' in preview
        assert 'row_count' in preview
        assert 'preview' in preview
        assert preview['row_count'] == 3
        assert len(preview['preview']) == 2


class TestSampleDataCSV:
    """Tests for the sample data CSV file."""
    
    @pytest.fixture
    def sample_data_path(self):
        """Get path to sample data CSV."""
        return os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'config',
            'sample_data.csv'
        )
    
    def test_sample_data_exists(self, sample_data_path):
        """Test that sample data file exists."""
        assert os.path.exists(sample_data_path)
    
    def test_sample_data_loads_correctly(self, sample_data_path):
        """Test that sample data loads correctly."""
        df = extract_from_csv(sample_data_path)
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
    
    def test_sample_data_has_required_columns(self, sample_data_path):
        """Test that sample data has required columns."""
        df = extract_from_csv(sample_data_path)
        required_columns = ['id', 'product', 'category', 'price', 'quantity', 'region']
        for col in required_columns:
            assert col in df.columns
