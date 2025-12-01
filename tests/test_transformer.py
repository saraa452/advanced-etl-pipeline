"""Testes para o módulo de transformação"""

import pytest
import pandas as pd
from transform.transformer import DataTransformer


class TestDataTransformer:
    """Testes para DataTransformer"""
    
    @pytest.fixture
    def config(self):
        """Fixture com configuração de teste"""
        return {'transform': {'validate_schema': True}}
    
    def test_transform_returns_dataframe(self, config):
        """Testa se transform retorna um DataFrame"""
        transformer = DataTransformer(config)
        sample_data = pd.DataFrame({'col1': [1, 2, 3]})
        result = transformer.transform(sample_data)
        assert isinstance(result, pd.DataFrame)
