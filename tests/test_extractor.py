"""Testes para o módulo de extração"""

import pytest
from extract.extractor import DataExtractor


class TestDataExtractor:
    """Testes para DataExtractor"""
    
    @pytest.fixture
    def config(self):
        """Fixture com configuração de teste"""
        return {'extract': {'source_type': 'csv'}}
    
    def test_extract_returns_dataframe(self, config):
        """Testa se extract retorna um DataFrame"""
        extractor = DataExtractor(config)
        result = extractor.extract()
        assert result is not None
