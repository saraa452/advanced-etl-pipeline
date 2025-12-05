import logging
from abc import ABC, abstractmethod
import pandas as pd
import numpy as np

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
    """Transformador de dados com limpeza, validação e normalização"""
    
    def transform(self, data):
        """Transforma dados com múltiplas operações
        
        Args:
            data: pd.DataFrame com dados brutos
            
        Returns:
            pd.DataFrame: Dados transformados
        """
        if data.empty:
            logger.warning("Dados vazios recebidos para transformação")
            return data
        
        logger.info("Iniciando transformação de dados...")
        
        # 1. Limpeza de dados
        data = self._clean_data(data)
        
        # 2. Validação de schema
        if self.config.get('transform', {}).get('validate_schema', True):
            data = self._validate_schema(data)
        
        # 3. Normalização
        data = self._normalize_data(data)
        
        # 4. Enriquecimento (cálculos derivados)
        data = self._enrich_data(data)
        
        logger.info(f"Transformação concluída: {len(data)} registros processados")
        return data
    
    def _clean_data(self, data):
        """Limpeza de dados
        
        Args:
            data: pd.DataFrame bruto
            
        Returns:
            pd.DataFrame: Dados limpos
        """
        logger.info("Limpando dados...")
        
        # Remover linhas completamente vazias
        data = data.dropna(how='all')
        
        # Renomear colunas para lowercase e sem espaços
        data.columns = data.columns.str.lower().str.replace(' ', '_')
        
        # Tratar valores NaN numéricos
        numeric_cols = data.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            data[col] = data[col].fillna(0)
        
        # Tratar valores NaN de texto
        string_cols = data.select_dtypes(include=['object']).columns
        for col in string_cols:
            data[col] = data[col].fillna('N/A')
        
        logger.info(f"Dados limpos: {len(data)} registros")
        return data
    
    def _validate_schema(self, data):
        """Validação de schema esperado
        
        Args:
            data: pd.DataFrame
            
        Returns:
            pd.DataFrame: Dados válidos
        """
        logger.info("Validando schema...")
        
        required_cols = ['country', 'cases', 'deaths', 'recovered']
        
        for col in required_cols:
            if col not in data.columns:
                logger.warning(f"Coluna obrigatória ausente: {col}")
                # Adicionar coluna vazia se não existir
                data[col] = 0
        
        # Manter apenas colunas conhecidas
        available_cols = [col for col in data.columns if col in 
                         ['country', 'cases', 'deaths', 'recovered', 'population', 'continent']]
        
        if available_cols:
            data = data[available_cols]
        
        logger.info(f"Schema validado: {len(data.columns)} colunas")
        return data
    
    def _normalize_data(self, data):
        """Normalização de dados
        
        Args:
            data: pd.DataFrame
            
        Returns:
            pd.DataFrame: Dados normalizados
        """
        logger.info("Normalizando dados...")
        
        # Garantir tipos de dados corretos
        if 'cases' in data.columns:
            data['cases'] = pd.to_numeric(data['cases'], errors='coerce').fillna(0).astype(int)
        
        if 'deaths' in data.columns:
            data['deaths'] = pd.to_numeric(data['deaths'], errors='coerce').fillna(0).astype(int)
        
        if 'recovered' in data.columns:
            data['recovered'] = pd.to_numeric(data['recovered'], errors='coerce').fillna(0).astype(int)
        
        if 'population' in data.columns:
            data['population'] = pd.to_numeric(data['population'], errors='coerce').fillna(0).astype(int)
        
        # Normalizar nomes de países
        if 'country' in data.columns:
            data['country'] = data['country'].str.strip().str.title()
        
        logger.info(f"Dados normalizados")
        return data
    
    def _enrich_data(self, data):
        """Enriquecimento com cálculos derivados
        
        Args:
            data: pd.DataFrame
            
        Returns:
            pd.DataFrame: Dados enriquecidos
        """
        logger.info("Enriquecendo dados com cálculos...")
        
        # Taxa de mortalidade
        if 'cases' in data.columns and 'deaths' in data.columns:
            data['mortality_rate'] = (
                (data['deaths'] / data['cases'].replace(0, np.nan)) * 100
            ).round(2).fillna(0)
        
        # Taxa de recuperação
        if 'cases' in data.columns and 'recovered' in data.columns:
            data['recovery_rate'] = (
                (data['recovered'] / data['cases'].replace(0, np.nan)) * 100
            ).round(2).fillna(0)
        
        # Taxa por população (por 1 milhão)
        if 'cases' in data.columns and 'population' in data.columns:
            data['cases_per_million'] = (
                (data['cases'] / data['population'].replace(0, np.nan)) * 1000000
            ).round(2).fillna(0)
        
        # Data de processamento
        data['processed_date'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info(f"Dados enriquecidos com {len([c for c in data.columns if 'rate' in c or 'per_million' in c])} novas métricas")
        return data
