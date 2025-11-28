"""Tests for Data Transformer."""
import pytest
import pandas as pd

from src.transformers.data_transformer import DataTransformer


class TestDataTransformer:
    """Test suite for DataTransformer."""

    @pytest.fixture
    def transformer(self):
        """Create a transformer instance."""
        return DataTransformer()

    @pytest.fixture
    def sample_data(self):
        """Create sample test data."""
        return pd.DataFrame({
            "id": [1, 2, 3, 3, 4],
            "name": ["Alice", "Bob", "Charlie", "Charlie", "Diana"],
            "value": [100, 200, None, None, 400],
            "category": ["A", "B", "A", "A", "B"]
        })

    def test_clean_data_drop_duplicates(self, transformer, sample_data):
        """Test duplicate removal."""
        result = transformer.clean_data(sample_data, drop_duplicates=True)
        assert len(result) == 4
        assert result["id"].tolist() == [1, 2, 3, 4]

    def test_clean_data_fill_na(self, transformer, sample_data):
        """Test filling missing values."""
        result = transformer.clean_data(sample_data, drop_duplicates=False, fill_na=0)
        assert result["value"].isna().sum() == 0
        assert result.loc[result["id"] == 3, "value"].iloc[0] == 0

    def test_clean_data_drop_na(self, transformer, sample_data):
        """Test dropping rows with missing values."""
        result = transformer.clean_data(sample_data, drop_duplicates=False, drop_na=True)
        assert len(result) == 3
        assert result["value"].isna().sum() == 0

    def test_rename_columns(self, transformer, sample_data):
        """Test column renaming."""
        result = transformer.rename_columns(sample_data, {"name": "full_name", "value": "amount"})
        assert "full_name" in result.columns
        assert "amount" in result.columns
        assert "name" not in result.columns
        assert "value" not in result.columns

    def test_filter_rows_with_query(self, transformer, sample_data):
        """Test filtering with query string."""
        result = transformer.filter_rows(sample_data, "category == 'A'")
        assert len(result) == 3
        assert all(result["category"] == "A")

    def test_filter_rows_with_callable(self, transformer, sample_data):
        """Test filtering with callable."""
        result = transformer.filter_rows(sample_data, lambda df: df["id"] > 2)
        assert len(result) == 3
        assert all(result["id"] > 2)

    def test_add_column_static_value(self, transformer, sample_data):
        """Test adding column with static value."""
        result = transformer.add_column(sample_data, "status", "active")
        assert "status" in result.columns
        assert all(result["status"] == "active")

    def test_add_column_with_callable(self, transformer, sample_data):
        """Test adding column with callable."""
        result = transformer.add_column(
            sample_data, 
            "double_id", 
            lambda df: df["id"] * 2
        )
        assert "double_id" in result.columns
        assert result["double_id"].tolist() == [2, 4, 6, 6, 8]

    def test_convert_types(self, transformer):
        """Test type conversion."""
        data = pd.DataFrame({
            "id": ["1", "2", "3"],
            "date": ["2023-01-01", "2023-01-02", "2023-01-03"]
        })
        result = transformer.convert_types(data, {"id": "int", "date": "datetime"})
        assert result["id"].dtype == "int64"
        assert pd.api.types.is_datetime64_any_dtype(result["date"])

    def test_aggregate(self, transformer, sample_data):
        """Test aggregation."""
        result = transformer.aggregate(
            sample_data, 
            group_by=["category"], 
            aggregations={"id": "count"}
        )
        assert len(result) == 2
        assert "category" in result.columns
        assert "id" in result.columns

    def test_add_transformation(self, transformer, sample_data):
        """Test adding and running transformations."""
        def add_prefix(df):
            df = df.copy()
            df["name"] = "Mr/Ms " + df["name"]
            return df
        
        transformer.add_transformation(add_prefix)
        result = transformer.transform(sample_data)
        assert all(result["name"].str.startswith("Mr/Ms"))

    def test_chained_transformations(self, transformer, sample_data):
        """Test multiple chained transformations."""
        transformer.add_transformation(
            lambda df: df.drop_duplicates()
        ).add_transformation(
            lambda df: df.fillna(0)
        )
        
        result = transformer.transform(sample_data)
        assert len(result) == 4
        assert result["value"].isna().sum() == 0
