"""Tests for ETL Pipeline."""
import pytest
import pandas as pd
from unittest.mock import Mock, MagicMock

from src.pipeline import ETLPipeline
from src.extractors.base_extractor import BaseExtractor
from src.transformers.data_transformer import DataTransformer
from src.loaders.base_loader import BaseLoader


class MockExtractor(BaseExtractor):
    """Mock extractor for testing."""
    
    def __init__(self, data: pd.DataFrame):
        super().__init__("MockExtractor")
        self.data = data
    
    def connect(self) -> bool:
        return True
    
    def extract(self, **kwargs) -> pd.DataFrame:
        return self.data
    
    def disconnect(self) -> bool:
        return True


class MockLoader(BaseLoader):
    """Mock loader for testing."""
    
    def __init__(self):
        super().__init__("MockLoader")
        self.loaded_data = None
    
    def connect(self) -> bool:
        return True
    
    def load(self, data: pd.DataFrame, **kwargs) -> bool:
        self.loaded_data = data
        return True
    
    def disconnect(self) -> bool:
        return True


class TestETLPipeline:
    """Test suite for ETLPipeline."""

    @pytest.fixture
    def sample_data(self):
        """Create sample test data."""
        return pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [100, 200, 300]
        })

    @pytest.fixture
    def pipeline(self):
        """Create a pipeline instance."""
        return ETLPipeline("test_pipeline")

    def test_init(self, pipeline):
        """Test pipeline initialization."""
        assert pipeline.name == "test_pipeline"
        assert len(pipeline.extractors) == 0
        assert len(pipeline.loaders) == 0
        assert pipeline.transformer is None

    def test_add_extractor(self, pipeline, sample_data):
        """Test adding extractor."""
        extractor = MockExtractor(sample_data)
        result = pipeline.add_extractor(extractor)
        
        assert result is pipeline  # Method chaining
        assert len(pipeline.extractors) == 1

    def test_set_transformer(self, pipeline):
        """Test setting transformer."""
        transformer = DataTransformer()
        result = pipeline.set_transformer(transformer)
        
        assert result is pipeline  # Method chaining
        assert pipeline.transformer is transformer

    def test_add_loader(self, pipeline):
        """Test adding loader."""
        loader = MockLoader()
        result = pipeline.add_loader(loader)
        
        assert result is pipeline  # Method chaining
        assert len(pipeline.loaders) == 1

    def test_run_simple_pipeline(self, pipeline, sample_data):
        """Test running a simple pipeline."""
        extractor = MockExtractor(sample_data)
        loader = MockLoader()
        
        pipeline.add_extractor(extractor)
        pipeline.add_loader(loader)
        
        metrics = pipeline.run(load_kwargs=[{"filename": "test"}])
        
        assert metrics["status"] == "success"
        assert loader.loaded_data is not None
        assert len(loader.loaded_data) == 3

    def test_run_with_transformer(self, pipeline, sample_data):
        """Test pipeline with transformation."""
        extractor = MockExtractor(sample_data)
        transformer = DataTransformer()
        transformer.add_transformation(
            lambda df: df[df["value"] > 150]
        )
        loader = MockLoader()
        
        pipeline.add_extractor(extractor)
        pipeline.set_transformer(transformer)
        pipeline.add_loader(loader)
        
        metrics = pipeline.run(load_kwargs=[{"filename": "test"}])
        
        assert metrics["status"] == "success"
        assert len(loader.loaded_data) == 2  # Only Bob and Charlie

    def test_run_multiple_extractors(self, pipeline):
        """Test pipeline with multiple extractors."""
        data1 = pd.DataFrame({"id": [1], "name": ["Alice"]})
        data2 = pd.DataFrame({"id": [2], "name": ["Bob"]})
        
        extractor1 = MockExtractor(data1)
        extractor2 = MockExtractor(data2)
        loader = MockLoader()
        
        pipeline.add_extractor(extractor1)
        pipeline.add_extractor(extractor2)
        pipeline.add_loader(loader)
        
        metrics = pipeline.run(load_kwargs=[{"filename": "test"}])
        
        assert metrics["status"] == "success"
        assert len(loader.loaded_data) == 2

    def test_run_multiple_loaders(self, pipeline, sample_data):
        """Test pipeline with multiple loaders."""
        extractor = MockExtractor(sample_data)
        loader1 = MockLoader()
        loader2 = MockLoader()
        
        pipeline.add_extractor(extractor)
        pipeline.add_loader(loader1)
        pipeline.add_loader(loader2)
        
        metrics = pipeline.run(load_kwargs=[{"filename": "test1"}, {"filename": "test2"}])
        
        assert metrics["status"] == "success"
        assert loader1.loaded_data is not None
        assert loader2.loaded_data is not None

    def test_pipeline_metrics(self, pipeline, sample_data):
        """Test pipeline metrics collection."""
        extractor = MockExtractor(sample_data)
        loader = MockLoader()
        
        pipeline.add_extractor(extractor)
        pipeline.add_loader(loader)
        
        metrics = pipeline.run(load_kwargs=[{"filename": "test"}])
        
        assert "start_time" in metrics
        assert "end_time" in metrics
        assert "duration_seconds" in metrics
        assert "stages" in metrics
        assert len(metrics["stages"]) == 2  # extract and load
