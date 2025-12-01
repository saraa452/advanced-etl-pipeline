"""Classe base para extratores de dados"""

import logging
from abc import ABC, abstractmethod
import pandas as pd

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Classe abstrata para implementar diferentes tipos de extração"""
    
    def __init__(self, config):
        """Inicializa o extrator com configurações
        
        Args:
            config: Dicionário com configurações de extração
        """
        self.config = config
    
    @abstractmethod
    def extract(self):
        """Método abstrato para extração de dados
        
        Returns:
            pd.DataFrame: Dados extraídos
        """
        pass


class DataExtractor(BaseExtractor):
    """Extrator de dados - implementação padrão"""
    
    def extract(self):
        """Extrai dados conforme configuração
        
        Returns:
            pd.DataFrame: Dados extraídos
        """
        logger.info("Extraindo dados...")
        
        # TODO: Implementar lógica de extração baseada em config
        # Exemplos: API, CSV, Database
        
        # Por enquanto, retorna DataFrame vazio como placeholder
        return pd.DataFrame()
