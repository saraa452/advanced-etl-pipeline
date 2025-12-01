"""Classe base para transformadores de dados"""

import logging
from abc import ABC, abstractmethod
import pandas as pd

logger = logging.getLogger(__name__)


class BaseTransformer(ABC):
    """Classe abstrata para implementar diferentes tipos de transformação"""
    
    def __init__(self, config):
        """Inicializa o transformador com configurações
        
        Args:
            config: Dicionário com configurações de transformação
        """
        self.config = config
    
    @abstractmethod
    def transform(self, data):
        """Método abstrato para transformação de dados
        
        Args:
            data: pd.DataFrame com dados brutos
            
        Returns:
            pd.DataFrame: Dados transformados
        """
        pass


class DataTransformer(BaseTransformer):
    """Transformador de dados - implementação padrão"""
    
    def transform(self, data):
        """Transforma dados conforme configuração
        
        Args:
            data: pd.DataFrame com dados brutos
            
        Returns:
            pd.DataFrame: Dados transformados
        """
        logger.info("Transformando dados...")
        
        # TODO: Implementar lógica de transformação
        # Exemplos: limpeza, validação, normalização
        
        return data
