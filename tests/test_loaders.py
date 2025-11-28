"""Tests for Loaders."""
import pytest
import pandas as pd
import os
import tempfile
from pathlib import Path

from src.loaders.file_loader import FileLoader
from src.loaders.database_loader import DatabaseLoader


class TestFileLoader:
    """Test suite for FileLoader."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir

    @pytest.fixture
    def loader(self, temp_dir):
        """Create a file loader instance."""
        return FileLoader(output_dir=temp_dir)

    @pytest.fixture
    def sample_data(self):
        """Create sample test data."""
        return pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [100, 200, 300]
        })

    def test_init(self, loader, temp_dir):
        """Test loader initialization."""
        assert str(loader.output_dir) == temp_dir

    def test_connect(self, loader):
        """Test connection (directory creation)."""
        result = loader.connect()
        assert result is True
        assert loader._connected is True
        assert loader.output_dir.exists()

    def test_disconnect(self, loader):
        """Test disconnection."""
        loader.connect()
        result = loader.disconnect()
        assert result is True
        assert loader._connected is False

    def test_load_csv(self, loader, sample_data, temp_dir):
        """Test CSV file loading."""
        result = loader.load(sample_data, filename="test_output", file_format="csv")
        
        assert result is True
        output_file = Path(temp_dir) / "test_output.csv"
        assert output_file.exists()
        
        loaded_data = pd.read_csv(output_file)
        assert len(loaded_data) == 3

    def test_load_json(self, loader, sample_data, temp_dir):
        """Test JSON file loading."""
        result = loader.load(sample_data, filename="test_output", file_format="json")
        
        assert result is True
        output_file = Path(temp_dir) / "test_output.json"
        assert output_file.exists()
        
        loaded_data = pd.read_json(output_file)
        assert len(loaded_data) == 3

    def test_load_empty_data(self, loader):
        """Test loading empty data."""
        empty_data = pd.DataFrame()
        result = loader.load(empty_data, filename="empty", file_format="csv")
        
        assert result is False

    def test_load_invalid_format(self, loader, sample_data):
        """Test loading with invalid format."""
        result = loader.load(sample_data, filename="test", file_format="invalid")
        
        assert result is False


class TestDatabaseLoader:
    """Test suite for DatabaseLoader."""

    @pytest.fixture
    def loader(self):
        """Create a database loader instance."""
        return DatabaseLoader(connection_string="sqlite:///:memory:")

    @pytest.fixture
    def sample_data(self):
        """Create sample test data."""
        return pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [100, 200, 300]
        })

    def test_init(self, loader):
        """Test loader initialization."""
        assert loader.connection_string == "sqlite:///:memory:"

    def test_connect_disconnect(self, loader):
        """Test connection and disconnection."""
        result = loader.connect()
        assert result is True
        assert loader.engine is not None

        result = loader.disconnect()
        assert result is True
        assert loader.engine is None

    def test_load(self, loader, sample_data):
        """Test data loading to database."""
        loader.connect()
        result = loader.load(sample_data, table="test_table")
        
        assert result is True
        
        # Verify data was loaded
        loaded_data = pd.read_sql("SELECT * FROM test_table", loader.engine)
        assert len(loaded_data) == 3

    def test_load_append(self, loader, sample_data):
        """Test appending data to existing table."""
        loader.connect()
        loader.load(sample_data, table="test_table")
        loader.load(sample_data, table="test_table", if_exists="append")
        
        loaded_data = pd.read_sql("SELECT * FROM test_table", loader.engine)
        assert len(loaded_data) == 6

    def test_load_replace(self, loader, sample_data):
        """Test replacing existing table."""
        loader.connect()
        loader.load(sample_data, table="test_table")
        loader.load(sample_data, table="test_table", if_exists="replace")
        
        loaded_data = pd.read_sql("SELECT * FROM test_table", loader.engine)
        assert len(loaded_data) == 3

    def test_load_empty_data(self, loader):
        """Test loading empty data."""
        empty_data = pd.DataFrame()
        result = loader.load(empty_data, table="test_table")
        
        assert result is False
