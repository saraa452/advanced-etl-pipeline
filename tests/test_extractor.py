"""Testes para o módulo de extração"""

import pytest
import pandas as pd
from extract.extractor import DataExtractor, CSVExtractor


class TestDataExtractor:
    """Testes para DataExtractor"""
    
    @pytest.fixture
    def config(self):
        """Fixture com configuração de teste"""
        return {
            'extract': {
                'source_type': 'csv',
                'source_path': './config/sample_data.csv'
            }
        }
    
    def test_extract_returns_dataframe(self, config):
        """Testa se extract retorna um DataFrame"""
        extractor = DataExtractor(config)
        result = extractor.extract()
        assert isinstance(result, pd.DataFrame)
    
    def test_csv_extractor_loads_data(self, config):
        """Testa se CSVExtractor carrega dados do arquivo"""
        extractor = CSVExtractor(config)
        result = extractor.extract()
        assert not result.empty, "CSV deve conter dados"
        assert 'country' in result.columns, "Coluna 'country' deve estar presente"
    
    def test_extract_has_required_columns(self, config):
        """Testa se dados extraídos têm colunas obrigatórias"""
        extractor = DataExtractor(config)
        result = extractor.extract()
        required_cols = ['country', 'cases', 'deaths', 'recovered']
        for col in required_cols:
            assert col in result.columns, f"Coluna '{col}' deve estar presente"
