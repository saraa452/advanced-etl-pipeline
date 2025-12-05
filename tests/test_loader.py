"""Testes para o módulo de carregamento"""

import pytest
import pandas as pd
from pathlib import Path
from load.loader import DataLoader, CSVLoader, SQLiteLoader


class TestDataLoader:
    """Testes para DataLoader"""
    
    @pytest.fixture
    def config(self):
        """Fixture com configuração de teste"""
        return {
            'load': {
                'destination_type': 'csv',
                'destination_path': './output'
            }
        }
    
    @pytest.fixture
    def sample_data(self):
        """Fixture com dados de exemplo"""
        return pd.DataFrame({
            'country': ['Brazil', 'USA', 'India'],
            'cases': [34000000, 103000000, 44500000],
            'deaths': [650000, 1100000, 527000],
            'recovered': [32000000, 100000000, 43800000],
            'population': [215000000, 335000000, 1417000000],
            'mortality_rate': [1.91, 1.07, 1.18],
            'recovery_rate': [94.12, 97.09, 98.43]
        })
    
    def test_load_accepts_dataframe(self, config, sample_data):
        """Testa se load aceita um DataFrame"""
        loader = DataLoader(config)
        # Não deve lançar exceção
        loader.load(sample_data)
    
    def test_csv_loader_creates_file(self, config, sample_data):
        """Testa se CSVLoader cria arquivo CSV"""
        loader = CSVLoader(config)
        loader.load(sample_data)
        
        csv_path = Path('./output/results.csv')
        assert csv_path.exists(), "Arquivo CSV deve ser criado"
        
        # Verificar que arquivo contém dados
        loaded_df = pd.read_csv(csv_path)
        assert len(loaded_df) == len(sample_data), "CSV deve conter todos os registros"
    
    def test_sqlite_loader_creates_database(self, config, sample_data):
        """Testa se SQLiteLoader cria banco de dados SQLite"""
        loader = SQLiteLoader(config)
        loader.load(sample_data)
        
        db_path = Path('./output/results.db')
        assert db_path.exists(), "Arquivo do banco SQLite deve ser criado"
    
    def test_load_handles_empty_dataframe(self, config):
        """Testa se load lida corretamente com DataFrame vazio"""
        loader = DataLoader(config)
        empty_data = pd.DataFrame()
        # Não deve lançar exceção
        loader.load(empty_data)
