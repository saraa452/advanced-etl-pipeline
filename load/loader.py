"""Classe base para loaders de dados"""

import logging
from abc import ABC, abstractmethod
import pandas as pd

logger = logging.getLogger(__name__)


class BaseLoader(ABC):
    """Classe abstrata para implementar diferentes tipos de carregamento"""
    
    def __init__(self, config):
        """Inicializa o loader com configurações
        
        Args:
            config: Dicionário com configurações de carregamento
        """
        self.config = config
    
    @abstractmethod
    def load(self, data):
        """Método abstrato para carregamento de dados
        
        Args:
            data: pd.DataFrame com dados transformados
        """
        pass


class DataLoader(BaseLoader):
    """Loader de dados - implementação padrão"""
    
    def load(self, data):
        """Carrega dados conforme configuração
        
        Args:
            data: pd.DataFrame com dados transformados
        """
        logger.info(f"Carregando {len(data)} registros...")
        
        # TODO: Implementar lógica de carregamento
        # Exemplos: salvar em banco de dados, arquivo, etc
        
        logger.info("Carregamento concluído")
