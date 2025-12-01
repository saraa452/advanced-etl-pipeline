"""Testes para o módulo de carregamento"""

import pytest
import pandas as pd
from load.loader import DataLoader


class TestDataLoader:
    """Testes para DataLoader"""
    
    @pytest.fixture
    def config(self):
        """Fixture com configuração de teste"""
        return {'load': {'destination_type': 'csv'}}
    
    def test_load_accepts_dataframe(self, config):
        """Testa se load aceita um DataFrame"""
        loader = DataLoader(config)
        sample_data = pd.DataFrame({'col1': [1, 2, 3]})
        # Não deve lançar exceção
        loader.load(sample_data)
