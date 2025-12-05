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
    
    @pytest.fixture
    def sample_data(self):
        """Fixture com dados de exemplo"""
        return pd.DataFrame({
            'country': ['Brazil', 'USA', 'India'],
            'cases': [34000000, 103000000, 44500000],
            'deaths': [650000, 1100000, 527000],
            'recovered': [32000000, 100000000, 43800000],
            'population': [215000000, 335000000, 1417000000]
        })
    
    def test_transform_returns_dataframe(self, config, sample_data):
        """Testa se transform retorna um DataFrame"""
        transformer = DataTransformer(config)
        result = transformer.transform(sample_data)
        assert isinstance(result, pd.DataFrame)
    
    def test_transform_preserves_rows(self, config, sample_data):
        """Testa se transform mantém número de registros"""
        transformer = DataTransformer(config)
        result = transformer.transform(sample_data)
        assert len(result) == len(sample_data), "Número de registros deve ser mantido"
    
    def test_transform_adds_calculated_columns(self, config, sample_data):
        """Testa se transform adiciona colunas calculadas"""
        transformer = DataTransformer(config)
        result = transformer.transform(sample_data)
        assert 'mortality_rate' in result.columns, "Coluna 'mortality_rate' deve ser adicionada"
        assert 'recovery_rate' in result.columns, "Coluna 'recovery_rate' deve ser adicionada"
        assert 'cases_per_million' in result.columns, "Coluna 'cases_per_million' deve ser adicionada"
    
    def test_transform_normalizes_countries(self, config, sample_data):
        """Testa se transform normaliza nomes de países"""
        transformer = DataTransformer(config)
        result = transformer.transform(sample_data)
        # Todos os países devem estar em Title Case
        assert all(result['country'].str.isupper() == False), "País não deve ser todo maiúsculo"
    
    def test_transform_fills_missing_values(self, config):
        """Testa se transform preenche valores faltantes"""
        data_with_nan = pd.DataFrame({
            'country': ['Brazil', 'USA', None],
            'cases': [34000000, None, 44500000],
            'deaths': [650000, 1100000, None],
            'recovered': [32000000, 100000000, 43800000],
            'population': [215000000, 335000000, 1417000000]
        })
        transformer = DataTransformer(config)
        result = transformer.transform(data_with_nan)
        assert result.isnull().sum().sum() == 0, "Não deve haver valores NaN após transformação"
