"""Tests for Extractors."""
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
import requests

from src.extractors.api_extractor import APIExtractor
from src.extractors.database_extractor import DatabaseExtractor


class TestAPIExtractor:
    """Test suite for APIExtractor."""

    @pytest.fixture
    def extractor(self):
        """Create an API extractor instance."""
        return APIExtractor(
            base_url="https://api.example.com",
            headers={"Authorization": "Bearer test"},
            timeout=30
        )

    def test_init(self, extractor):
        """Test extractor initialization."""
        assert extractor.base_url == "https://api.example.com"
        assert extractor.headers == {"Authorization": "Bearer test"}
        assert extractor.timeout == 30

    def test_connect(self, extractor):
        """Test connection creation."""
        result = extractor.connect()
        assert result is True
        assert extractor.session is not None

    def test_disconnect(self, extractor):
        """Test disconnection."""
        extractor.connect()
        result = extractor.disconnect()
        assert result is True
        assert extractor.session is None

    @patch('requests.Session')
    def test_extract_list_response(self, mock_session_class, extractor):
        """Test extraction with list response."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"id": 1, "name": "Test1"},
            {"id": 2, "name": "Test2"}
        ]
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        extractor.connect()
        result = extractor.extract(endpoint="users")

        assert len(result) == 2
        assert "id" in result.columns
        assert "name" in result.columns

    @patch('requests.Session')
    def test_extract_dict_response(self, mock_session_class, extractor):
        """Test extraction with dict response containing list."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [
                {"id": 1, "name": "Test1"},
                {"id": 2, "name": "Test2"}
            ],
            "total": 2
        }
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        extractor.connect()
        result = extractor.extract(endpoint="users")

        assert len(result) == 2

    @patch('requests.Session')
    def test_extract_request_error(self, mock_session_class, extractor):
        """Test extraction with request error."""
        mock_session = MagicMock()
        mock_session.get.side_effect = requests.exceptions.RequestException("Connection error")
        mock_session_class.return_value = mock_session

        extractor.connect()
        result = extractor.extract(endpoint="users")

        assert result.empty


class TestDatabaseExtractor:
    """Test suite for DatabaseExtractor."""

    @pytest.fixture
    def extractor(self):
        """Create a database extractor instance."""
        return DatabaseExtractor(
            connection_string="sqlite:///:memory:"
        )

    def test_init(self, extractor):
        """Test extractor initialization."""
        assert extractor.connection_string == "sqlite:///:memory:"

    def test_connect_disconnect(self, extractor):
        """Test connection and disconnection."""
        result = extractor.connect()
        assert result is True
        assert extractor.engine is not None

        result = extractor.disconnect()
        assert result is True
        assert extractor.engine is None

    def test_extract_with_query(self, extractor):
        """Test extraction with raw query."""
        extractor.connect()
        
        # Create test table and data
        from sqlalchemy import text
        with extractor.engine.connect() as conn:
            conn.execute(text("CREATE TABLE test_table (id INTEGER, name TEXT)"))
            conn.execute(text("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')"))
            conn.commit()
        
        result = extractor.extract(query="SELECT * FROM test_table")
        
        assert len(result) == 2
        assert "id" in result.columns
        assert "name" in result.columns

    def test_extract_with_table(self, extractor):
        """Test extraction with table name."""
        extractor.connect()
        
        from sqlalchemy import text
        with extractor.engine.connect() as conn:
            conn.execute(text("CREATE TABLE users (id INTEGER, name TEXT, active INTEGER)"))
            conn.execute(text("INSERT INTO users VALUES (1, 'Alice', 1), (2, 'Bob', 0), (3, 'Charlie', 1)"))
            conn.commit()
        
        result = extractor.extract(table="users", columns=["id", "name"])
        
        assert len(result) == 3
        assert list(result.columns) == ["id", "name"]

    def test_extract_with_limit(self, extractor):
        """Test extraction with limit."""
        extractor.connect()
        
        from sqlalchemy import text
        with extractor.engine.connect() as conn:
            conn.execute(text("CREATE TABLE data (id INTEGER)"))
            for i in range(10):
                conn.execute(text(f"INSERT INTO data VALUES ({i})"))
            conn.commit()
        
        result = extractor.extract(table="data", limit=5)
        
        assert len(result) == 5
