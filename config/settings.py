"""Configurações centralizadas do pipeline ETL"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Carregar variáveis de ambiente
env_file = Path(__file__).parent.parent / '.env'
if env_file.exists():
    load_dotenv(env_file)


def load_config():
    """Carrega configurações do ambiente
    
    Returns:
        dict: Dicionário com configurações
    """
    config = {
        # Configurações de Extração
        'extract': {
            'source_type': os.getenv('EXTRACT_SOURCE_TYPE', 'csv'),
            'source_path': os.getenv('EXTRACT_SOURCE_PATH', './data/input'),
        },
        
        # Configurações de Transformação
        'transform': {
            'validate_schema': os.getenv('TRANSFORM_VALIDATE_SCHEMA', 'true').lower() == 'true',
        },
        
        # Configurações de Carregamento
        'load': {
            'destination_type': os.getenv('LOAD_DESTINATION_TYPE', 'csv'),
            'destination_path': os.getenv('LOAD_DESTINATION_PATH', './data/output'),
        },
        
        # Configurações de Banco de Dados
        'database': {
            'url': os.getenv('DATABASE_URL', 'sqlite:///pipeline.db'),
        },
    }
    
    return config
